"""Unit tests for the technical indicator feature builder."""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from shared.models import Candle
from signal_pipeline.src.features import (
    FEATURE_COLS,
    MIN_CANDLES_REQUIRED,
    _compute_macd,
    _compute_rsi,
    build_features,
)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _candle(close: float, idx: int = 0) -> Candle:
    now = _utcnow() + timedelta(minutes=idx * 5)
    return Candle(
        symbol="BTCUSDT",
        open=close * 0.999,
        high=close * 1.001,
        low=close * 0.998,
        close=close,
        volume=10.0 + idx * 0.1,
        opened_at=now,
        closed_at=now + timedelta(minutes=5),
    )


def _rising_candles(n: int, start: float = 100.0, step: float = 1.0) -> list[Candle]:
    return [_candle(start + i * step, idx=i) for i in range(n)]


class TestBuildFeatures:
    def test_returns_none_below_min_candles(self) -> None:
        assert build_features(_rising_candles(MIN_CANDLES_REQUIRED - 1)) is None

    def test_returns_dataframe_with_all_feature_cols(self) -> None:
        df = build_features(_rising_candles(60))
        assert df is not None
        assert all(col in df.columns for col in FEATURE_COLS)

    def test_no_nan_in_output(self) -> None:
        df = build_features(_rising_candles(60))
        assert df is not None
        assert not df[FEATURE_COLS].isnull().any().any()

    def test_rsi_bounded_0_to_100(self) -> None:
        df = build_features(_rising_candles(60))
        assert df is not None
        assert (df["rsi_14"] >= 0).all() and (df["rsi_14"] <= 100).all()

    def test_ema_cross_positive_in_uptrend(self) -> None:
        df = build_features(_rising_candles(60, step=5.0))
        assert df is not None
        # EMA-9 > EMA-21 in a strong uptrend → positive cross
        assert df["ema_cross"].iloc[-1] > 0

    def test_volume_ratio_near_one_for_constant_volume(self) -> None:
        candles = [_candle(100.0 + i, idx=i) for i in range(60)]
        df = build_features(candles)
        assert df is not None
        # Constant volume → ratio should be ~1.0
        assert abs(df["volume_ratio"].iloc[-1] - 1.0) < 0.1

    def test_hl_range_positive(self) -> None:
        df = build_features(_rising_candles(60))
        assert df is not None
        assert (df["hl_range"] > 0).all()


class TestComputeRsi:
    def test_pure_uptrend_gives_rsi_100(self) -> None:
        prices = pd.Series([100.0 + i for i in range(20)])
        rsi = _compute_rsi(prices, period=14)
        assert rsi.iloc[-1] == pytest.approx(100.0)

    def test_rsi_bounded(self) -> None:
        import numpy as np
        rng    = np.random.default_rng(0)
        prices = pd.Series(100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, 100))))
        rsi    = _compute_rsi(prices, period=14).dropna()
        assert (rsi >= 0).all() and (rsi <= 100).all()


class TestComputeMacd:
    def test_macd_positive_in_uptrend(self) -> None:
        prices = pd.Series([100.0 + i * 2 for i in range(40)])
        macd, _ = _compute_macd(prices)
        assert macd.iloc[-1] > 0

    def test_signal_lags_macd_in_uptrend(self) -> None:
        prices = pd.Series([100.0 + i for i in range(40)])
        macd, signal = _compute_macd(prices)
        assert signal.iloc[-1] < macd.iloc[-1]
