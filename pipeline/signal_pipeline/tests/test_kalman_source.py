"""Unit tests for the Kalman-filter signal source."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from shared.models import Candle, TradeSide
from signal_pipeline.src.kalman_source import KalmanSignalSource, _KalmanState

from .test_features import _rising_candles


def _fake_redis() -> MagicMock:
    """In-memory fake standing in for redis.Redis — just a dict-backed get/set."""
    store: dict[str, str] = {}
    client = MagicMock()
    client.get.side_effect = lambda key: store.get(key)
    client.set.side_effect = lambda key, value: store.__setitem__(key, value)
    return client


def _falling_candles(n: int, start: float = 100.0, step: float = 1.0) -> list[Candle]:
    now = datetime.now(tz=timezone.utc)
    candles = []
    for i in range(n):
        close = start - i * step
        candles.append(
            Candle(
                symbol="BTCUSDT",
                open=close * 1.001,
                high=close * 1.002,
                low=close * 0.998,
                close=close,
                volume=10.0,
                opened_at=now + timedelta(minutes=i * 5),
                closed_at=now + timedelta(minutes=i * 5 + 5),
            )
        )
    return candles


class TestPredict:
    def test_first_ever_prediction_does_not_crash(self) -> None:
        source = KalmanSignalSource(redis_client=_fake_redis())
        candles = _rising_candles(5)

        signal = source.predict("BTCUSDT", candles, pd.DataFrame())

        assert signal.symbol == "BTCUSDT"
        assert signal.source == "kalman"
        assert 0.0 <= signal.confidence <= 1.0

    def test_single_candle_does_not_crash(self) -> None:
        source = KalmanSignalSource(redis_client=_fake_redis())
        signal = source.predict("BTCUSDT", _rising_candles(1), pd.DataFrame())
        assert signal.side == TradeSide.HOLD

    def test_confidence_always_in_unit_range(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        for candles in (_rising_candles(20), _falling_candles(20), _rising_candles(2)):
            signal = source.predict("BTCUSDT", candles, pd.DataFrame())
            assert 0.0 <= signal.confidence <= 1.0

    def test_state_round_trips_through_redis(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        source.predict("BTCUSDT", _rising_candles(10), pd.DataFrame())
        stored_raw = redis_client.get("kalman:state:BTCUSDT")

        assert stored_raw is not None
        state = _KalmanState.from_json(stored_raw)
        assert isinstance(state.mean, float)
        assert isinstance(state.variance, float)

    def test_sustained_uptrend_converges_to_buy(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        # Feed a long, steadily rising synthetic series one tick at a time so
        # the filter has many updates to converge on the positive trend.
        candles = _rising_candles(200, start=100.0, step=0.5)
        signal = None
        for i in range(2, len(candles)):
            signal = source.predict("BTCUSDT", candles[:i], pd.DataFrame())

        assert signal is not None
        assert signal.side == TradeSide.BUY

    def test_sustained_downtrend_converges_to_sell(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        candles = _falling_candles(200, start=100.0, step=0.5)
        signal = None
        for i in range(2, len(candles)):
            signal = source.predict("BTCUSDT", candles[:i], pd.DataFrame())

        assert signal is not None
        assert signal.side == TradeSide.SELL

    def test_flat_series_stays_hold(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        candles = _rising_candles(30, start=100.0, step=0.0)
        signal = None
        for i in range(2, len(candles)):
            signal = source.predict("BTCUSDT", candles[:i], pd.DataFrame())

        assert signal is not None
        assert signal.side == TradeSide.HOLD

    def test_separate_symbols_track_independent_state(self) -> None:
        redis_client = _fake_redis()
        source = KalmanSignalSource(redis_client=redis_client)

        up_candles = _rising_candles(100, start=100.0, step=0.5)
        down_candles = _falling_candles(100, start=100.0, step=0.5)

        for i in range(2, len(up_candles)):
            btc_signal = source.predict("BTCUSDT", up_candles[:i], pd.DataFrame())
        for i in range(2, len(down_candles)):
            eth_signal = source.predict("ETHUSDT", down_candles[:i], pd.DataFrame())

        assert btc_signal.side == TradeSide.BUY
        assert eth_signal.side == TradeSide.SELL


class TestWarmup:
    def test_warmup_is_a_no_op(self) -> None:
        source = KalmanSignalSource(redis_client=_fake_redis())
        source.warmup()  # must not raise


class TestKalmanState:
    def test_json_round_trip_preserves_values(self) -> None:
        now = datetime.now(tz=timezone.utc)
        state = _KalmanState(mean=0.001, variance=0.0005, updated_at=now)

        restored = _KalmanState.from_json(state.to_json())

        assert restored.mean == pytest.approx(state.mean)
        assert restored.variance == pytest.approx(state.variance)
        assert restored.updated_at == state.updated_at
