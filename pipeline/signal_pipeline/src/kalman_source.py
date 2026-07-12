"""Kalman-filter signal source — genuinely online trend estimation.

Unlike MLSignalSource (24h batch retrain) this source updates its estimate on
every prediction tick using the newest candle's return as a single scalar
measurement. It's a local-level (random-walk) model: the "trend" is a hidden
mean we're trying to track through noisy per-candle returns.

Per-symbol state (mean, variance) is persisted in Redis so it survives
process restarts without needing to replay history — a fresh restart just
resumes filtering from the last known estimate.

warmup() is a no-op: a Kalman filter converges from any reasonable prior
within a handful of updates, so there's no synthetic bootstrap step like
MLSignalSource's _fit_synthetic(). retrain() stays the ABC's no-op default —
this source has no batch step, it's the online complement to the ML source.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import redis

from shared.logger import get_logger
from shared.models import Candle, TradeSide, TradeSignal
from shared.redis_client import get_redis_client

from .base import BaseSignalSource

_logger = get_logger(__name__)

SIGNAL_TTL_SECONDS: int = int(os.getenv("SIGNAL_TTL_SECONDS", "90"))

# Local-level model tuning: process_variance is how much we expect the true
# trend to drift between ticks; observation_variance is how noisy we expect
# a single candle's return to be as a measurement of that trend. A higher
# ratio of observation_variance to process_variance makes the filter trust
# its running estimate more and react to new candles more slowly.
PROCESS_VARIANCE: float = float(os.getenv("KALMAN_PROCESS_VARIANCE", "1e-6"))
OBSERVATION_VARIANCE: float = float(os.getenv("KALMAN_OBSERVATION_VARIANCE", "1e-4"))

# Trend magnitude (in return units) above which the filter calls BUY/SELL
# instead of HOLD. Kept separate from MLSignalSource's SIGNAL_MIN_CONFIDENCE
# since this is a different kind of threshold (a rate of change, not a
# classifier probability).
TREND_THRESHOLD: float = float(os.getenv("KALMAN_TREND_THRESHOLD", "0.0005"))

# Ceiling used to normalize variance into a confidence score. Prior/initial
# variance is well above this, so early predictions after a cold start report
# low confidence until a few updates bring the estimate's variance down.
_MAX_VARIANCE_FOR_CONFIDENCE: float = 1e-3

_STATE_KEY_PREFIX = "kalman:state:"


class _KalmanState:
    """Scalar local-level Kalman filter state for one symbol."""

    __slots__ = ("mean", "variance", "updated_at")

    def __init__(self, mean: float, variance: float, updated_at: datetime) -> None:
        self.mean = mean
        self.variance = variance
        self.updated_at = updated_at

    def to_json(self) -> str:
        """Serialize to a JSON string for Redis storage."""
        return json.dumps(
            {
                "mean": self.mean,
                "variance": self.variance,
                "updated_at": self.updated_at.isoformat(),
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> _KalmanState:
        """Deserialize from the JSON string written by to_json()."""
        data = json.loads(raw)
        return cls(
            mean=float(data["mean"]),
            variance=float(data["variance"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )


class KalmanSignalSource(BaseSignalSource):
    """Online scalar Kalman filter tracking a per-symbol return trend."""

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        self._redis = redis_client if redis_client is not None else get_redis_client()

    # ── BaseSignalSource interface ────────────────────────────────────────────

    def warmup(self) -> None:
        """No-op — the filter converges from any reasonable prior within a few ticks."""

    def predict(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> TradeSignal:
        """Update the running trend estimate with the latest candle and emit a signal.

        Args:
            symbol:   Trading pair (e.g. BTCUSDT).
            candles:  Raw candle list (oldest first); the last two candles'
                      closes give the measurement (single-bar return).
            features: Unused by this source — present for ABC compatibility.

        Returns:
            TradeSignal with side, confidence, TTL, and source="kalman".
        """
        measurement = self._latest_return(candles)
        state = self._load_state(symbol, measurement)
        state = self._update(state, measurement)
        self._save_state(symbol, state)

        return self._signal_from_state(symbol, state)

    # ── Filter mechanics ──────────────────────────────────────────────────────

    @staticmethod
    def _latest_return(candles: list[Candle]) -> float:
        """Single-bar return from the last two candles' closes (0.0 if too few candles)."""
        if len(candles) < 2:
            return 0.0
        previous_close = candles[-2].close
        latest_close = candles[-1].close
        if previous_close == 0:
            return 0.0
        return (latest_close - previous_close) / previous_close

    def _load_state(self, symbol: str, measurement: float) -> _KalmanState:
        """Load persisted state for *symbol*, or initialize a prior from *measurement*."""
        raw = self._redis.get(_STATE_KEY_PREFIX + symbol)
        if raw is not None:
            return _KalmanState.from_json(str(raw))

        return _KalmanState(
            mean=measurement,
            variance=_MAX_VARIANCE_FOR_CONFIDENCE,
            updated_at=datetime.now(tz=timezone.utc),
        )

    def _update(self, state: _KalmanState, measurement: float) -> _KalmanState:
        """One predict+update recursion of the scalar Kalman filter."""
        predicted_mean = state.mean
        predicted_variance = state.variance + PROCESS_VARIANCE

        kalman_gain = predicted_variance / (predicted_variance + OBSERVATION_VARIANCE)
        updated_mean = predicted_mean + kalman_gain * (measurement - predicted_mean)
        updated_variance = (1 - kalman_gain) * predicted_variance

        return _KalmanState(
            mean=updated_mean,
            variance=updated_variance,
            updated_at=datetime.now(tz=timezone.utc),
        )

    def _save_state(self, symbol: str, state: _KalmanState) -> None:
        self._redis.set(_STATE_KEY_PREFIX + symbol, state.to_json())

    def _signal_from_state(self, symbol: str, state: _KalmanState) -> TradeSignal:
        """Map the filter's trend estimate to a BUY/SELL/HOLD signal."""
        if state.mean > TREND_THRESHOLD:
            side = TradeSide.BUY
        elif state.mean < -TREND_THRESHOLD:
            side = TradeSide.SELL
        else:
            side = TradeSide.HOLD

        normalized_variance = min(state.variance / _MAX_VARIANCE_FOR_CONFIDENCE, 1.0)
        confidence = max(0.0, min(1.0, 1.0 - normalized_variance))

        now = datetime.now(tz=timezone.utc)
        return TradeSignal(
            id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            confidence=confidence,
            generated_at=now,
            expires_at=now + timedelta(seconds=SIGNAL_TTL_SECONDS),
            source="kalman",
        )
