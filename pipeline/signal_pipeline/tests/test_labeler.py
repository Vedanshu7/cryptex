"""Unit tests for forward-return and triple-barrier labeling."""

import numpy as np
import pandas as pd
import pytest

from signal_pipeline.src.labeler import class_weights, label_forward_returns, label_triple_barrier


def _price_df(prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "close":  prices,
        "volume": [10.0] * len(prices),
        "high":   [p * 1.001 for p in prices],
        "low":    [p * 0.999 for p in prices],
    })


class TestLabelForwardReturns:
    def test_rising_prices_produce_buy_labels(self) -> None:
        prices = [100.0 + i * 2 for i in range(20)]  # strong +2% per candle
        df     = _price_df(prices)
        labels = label_forward_returns(df, horizon=1, buy_threshold=0.001)
        valid  = labels.dropna()
        assert (valid == "BUY").all()

    def test_falling_prices_produce_sell_labels(self) -> None:
        prices = [100.0 - i * 2 for i in range(20)]
        df     = _price_df(prices)
        labels = label_forward_returns(df, horizon=1, sell_threshold=-0.001)
        valid  = labels.dropna()
        assert (valid == "SELL").all()

    def test_flat_prices_produce_hold_labels(self) -> None:
        df     = _price_df([100.0] * 20)
        labels = label_forward_returns(df, horizon=1)
        valid  = labels.dropna()
        assert (valid == "HOLD").all()

    def test_last_horizon_rows_are_nan(self) -> None:
        df     = _price_df([100.0 + i for i in range(20)])
        labels = label_forward_returns(df, horizon=3)
        assert labels.iloc[-3:].isna().all()

    def test_labels_are_string_valued(self) -> None:
        df     = _price_df([100.0 + i for i in range(20)])
        labels = label_forward_returns(df).dropna()
        assert set(labels.unique()).issubset({"BUY", "SELL", "HOLD"})


class TestLabelTripleBarrier:
    def test_returns_correct_label_types(self) -> None:
        df     = _price_df([100.0 + i * 0.1 for i in range(50)])
        labels = label_triple_barrier(df)
        assert set(labels.unique()).issubset({"BUY", "SELL", "HOLD"})

    def test_length_matches_input(self) -> None:
        df     = _price_df([100.0] * 50)
        labels = label_triple_barrier(df)
        assert len(labels) == len(df)

    def test_large_spike_produces_buy(self) -> None:
        # A sudden +5% spike should trigger the upper barrier.
        prices = [100.0] * 10 + [105.0] * 40
        df     = _price_df(prices)
        labels = label_triple_barrier(df, horizon=5, profit_factor=1.0, stop_factor=2.0)
        assert labels.iloc[9] == "BUY"


class TestClassWeights:
    def test_minority_class_gets_higher_weight(self) -> None:
        labels = pd.Series(["HOLD"] * 60 + ["BUY"] * 20 + ["SELL"] * 20)
        weights = class_weights(labels)
        assert weights["BUY"] > weights["HOLD"]
        assert weights["SELL"] > weights["HOLD"]

    def test_balanced_labels_give_equal_weights(self) -> None:
        labels  = pd.Series(["BUY"] * 10 + ["SELL"] * 10 + ["HOLD"] * 10)
        weights = class_weights(labels)
        assert abs(weights["BUY"] - weights["SELL"]) < 1e-6
        assert abs(weights["BUY"] - weights["HOLD"]) < 1e-6
