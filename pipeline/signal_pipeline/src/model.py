"""LightGBM signal generator — industry-standard for tabular financial data.

Training strategy:
  1. On first startup: train on real candles from TimescaleDB if ≥ MIN_TRAINING_CANDLES
     exist; otherwise fall back to synthetic labels until enough data accumulates.
  2. Daily retrain: runner calls retrain() with fresh candles from the DB,
     replacing the live model atomically.

LightGBM is chosen over RandomForest because:
  - Faster: full retrain in seconds on a CPU, feasible daily
  - More accurate on tabular financial data (gradient boosting > bagging)
  - Handles class imbalance via sample weights
  - No feature scaling needed
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timedelta, timezone

import lightgbm as lgb
import numpy as np
import pandas as pd

from shared.logger import get_logger
from shared.models import TradeSide, TradeSignal

from .features import FEATURE_COLS, build_features
from .labeler import class_weights, label_forward_returns

_logger = get_logger(__name__)

SIGNAL_TTL_SECONDS: int  = int(os.getenv("SIGNAL_TTL_SECONDS",  "90"))
MIN_CONFIDENCE: float     = float(os.getenv("SIGNAL_MIN_CONFIDENCE", "0.5"))
MIN_TRAINING_CANDLES: int = int(os.getenv("MIN_TRAINING_CANDLES", "500"))

# LightGBM hyperparameters tuned for noisy financial data:
# - low learning rate + more trees avoids overfitting
# - min_child_samples=50 prevents leaf nodes with too few training samples
_LGBM_PARAMS: dict[str, object] = {
    "n_estimators":      300,
    "learning_rate":     0.03,
    "max_depth":         5,
    "num_leaves":        24,
    "min_child_samples": 50,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "reg_alpha":         0.1,
    "reg_lambda":        0.1,
    "random_state":      42,
    "verbose":           -1,
    "n_jobs":            -1,
}


class SignalGenerator:
    """Thread-safe LightGBM signal generator with atomic model replacement."""

    def __init__(self) -> None:
        self._lock  = threading.Lock()
        self._model: lgb.LGBMClassifier | None = None
        self._classes: list[str] = ["BUY", "HOLD", "SELL"]
        self._trained_on_real = False

        # Bootstrap with synthetic data so the service can start immediately.
        self._fit_synthetic()

    # ── Public API ────────────────────────────────────────────────────────────

    def predict(self, symbol: str, features: pd.DataFrame) -> TradeSignal:
        """Predict a trading signal for the latest row in *features*.

        Args:
            symbol:   Trading pair (e.g. BTCUSDT).
            features: Feature DataFrame from build_features().

        Returns:
            TradeSignal with side, confidence, and TTL.
        """
        with self._lock:
            model = self._model

        if model is None:
            raise RuntimeError("Model not fitted.")

        latest = features[FEATURE_COLS].tail(1).values
        proba  = model.predict_proba(latest)[0]

        class_idx  = int(np.argmax(proba))
        confidence = float(proba[class_idx])
        label: str = self._classes[class_idx]

        now = datetime.now(tz=timezone.utc)
        return TradeSignal(
            id          = str(uuid.uuid4()),
            symbol      = symbol,
            side        = TradeSide(label),
            confidence  = confidence,
            generated_at = now,
            expires_at  = now + timedelta(seconds=SIGNAL_TTL_SECONDS),
        )

    def retrain(self, candles_by_symbol: dict[str, list]) -> None:
        """Retrain the model on fresh candles from TimescaleDB.

        Called by the runner's daily retrain loop. Replaces the live model
        atomically so predictions continue uninterrupted during training.

        Args:
            candles_by_symbol: Symbol → list[Candle] from DB.
        """
        all_X: list[pd.DataFrame] = []
        all_y: list[pd.Series]    = []
        all_w: list[pd.Series]    = []

        for symbol, candles in candles_by_symbol.items():
            if len(candles) < MIN_TRAINING_CANDLES:
                _logger.warning(
                    "Insufficient candles for retraining.",
                    extra={"symbol": symbol, "count": len(candles),
                           "required": MIN_TRAINING_CANDLES},
                )
                continue

            features = build_features(candles)
            if features is None:
                continue

            labels = label_forward_returns(features).dropna()
            features = features.loc[labels.index]

            if len(labels) < 100:
                continue

            weights_map  = class_weights(labels)
            sample_w     = labels.map(weights_map)

            all_X.append(features[FEATURE_COLS])
            all_y.append(labels)
            all_w.append(sample_w)

        if not all_X:
            _logger.warning("No symbols had enough data — keeping current model.")
            return

        X = pd.concat(all_X).values
        y = pd.concat(all_y).values
        w = pd.concat(all_w).values

        new_model = lgb.LGBMClassifier(**_LGBM_PARAMS)  # type: ignore[arg-type]
        new_model.fit(X, y, sample_weight=w)
        self._classes = list(new_model.classes_)

        with self._lock:
            self._model           = new_model
            self._trained_on_real = True

        _logger.info(
            "Model retrained on real candles.",
            extra={"samples": len(y), "class_distribution": {
                cls: int((y == cls).sum()) for cls in self._classes
            }},
        )

    @property
    def trained_on_real(self) -> bool:
        """True once the model has been trained on at least one real candle batch."""
        return self._trained_on_real

    # ── Private ───────────────────────────────────────────────────────────────

    def _fit_synthetic(self) -> None:
        """Bootstrap on synthetic data so the service starts immediately.

        Uses a geometric random walk with momentum-based labels. The model
        will be replaced at the next retrain cycle once real data is available.
        """
        _logger.info("Bootstrapping on synthetic data (warm-up mode).")
        rng = np.random.default_rng(seed=42)
        n   = 2_000

        prices  = 50_000.0 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
        returns = np.diff(prices) / prices[:-1]

        labels = np.where(
            returns > 0.003, "BUY",
            np.where(returns < -0.003, "SELL", "HOLD"),
        )

        candles_df = pd.DataFrame({
            "close":  prices[: n - 1],
            "volume": rng.uniform(1.0, 5.0, n - 1),
            "high":   prices[: n - 1] * 1.001,
            "low":    prices[: n - 1] * 0.999,
        })

        features = build_features(  # type: ignore[arg-type]
            [_SyntheticCandle(r) for r in zip(
                candles_df["close"], candles_df["volume"],
                candles_df["high"], candles_df["low"]
            )]
        )

        if features is None:
            return

        y = pd.Series(labels[: len(features)], index=features.index)
        X = features[FEATURE_COLS].values

        model = lgb.LGBMClassifier(**_LGBM_PARAMS)  # type: ignore[arg-type]
        model.fit(X, y.values)
        self._classes = list(model.classes_)
        self._model   = model

        _logger.info("Synthetic warm-up training complete.",
                     extra={"samples": len(y)})


class _SyntheticCandle:
    """Minimal candle duck-type for _fit_synthetic() to reuse build_features()."""

    __slots__ = ("close", "volume", "high", "low")

    def __init__(self, row: tuple[float, float, float, float]) -> None:
        self.close, self.volume, self.high, self.low = row
