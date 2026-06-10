"""Labeling strategies for supervised learning on candle data.

Forward-return labeling is the standard for crypto trading signal generation:
label each candle based on the realized return N candles into the future.

Triple-barrier labeling (Marcos Lopez de Prado) is the more sophisticated
approach used at institutional shops: label based on which barrier
(profit target, stop loss, or time limit) is hit first.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def label_forward_returns(
    df: pd.DataFrame,
    horizon: int = 3,
    buy_threshold: float = 0.003,
    sell_threshold: float = -0.003,
) -> pd.Series:
    """Label each candle by its forward return over *horizon* candles.

    Args:
        df: Feature DataFrame with a 'close' column.
        horizon: How many candles ahead to compute the return.
                 At 5-min candles: horizon=3 → 15-min return.
        buy_threshold:  Forward return above this → BUY label.
        sell_threshold: Forward return below this → SELL label.

    Returns:
        Series of 'BUY' / 'SELL' / 'HOLD' strings, same index as df.
        The last *horizon* rows are NaN (future unknown at those points).
    """
    forward_return = df["close"].pct_change(horizon).shift(-horizon)

    labels = np.where(
        forward_return > buy_threshold,
        "BUY",
        np.where(forward_return < sell_threshold, "SELL", "HOLD"),
    )

    return pd.Series(labels, index=df.index, name="label").where(
        forward_return.notna()
    )


def label_triple_barrier(
    df: pd.DataFrame,
    horizon: int = 12,
    profit_factor: float = 1.5,
    stop_factor: float = 1.0,
    vol_lookback: int = 21,
) -> pd.Series:
    """Label each candle using the triple-barrier method (Lopez de Prado).

    For each candle t, the holding period ends when one of three barriers is hit:
      - Upper barrier: close rises by profit_factor × σ  → BUY
      - Lower barrier: close falls by stop_factor  × σ   → SELL
      - Vertical barrier: horizon candles elapsed          → HOLD

    σ is the rolling realized volatility (std of returns over vol_lookback).

    Args:
        df: DataFrame with 'close' column.
        horizon: Maximum holding period in candles.
        profit_factor: Upper barrier = profit_factor × σ above entry.
        stop_factor:   Lower barrier = stop_factor  × σ below entry.
        vol_lookback:  Lookback window for realized volatility.

    Returns:
        Series of 'BUY' / 'SELL' / 'HOLD' labels.
    """
    returns  = df["close"].pct_change()
    vol      = returns.rolling(vol_lookback).std().fillna(returns.std())
    closes   = df["close"].values
    vols     = vol.values
    n        = len(closes)
    labels   = np.full(n, "HOLD", dtype=object)

    for i in range(n - 1):
        entry  = closes[i]
        sigma  = vols[i]
        upper  = entry * (1 + profit_factor * sigma)
        lower  = entry * (1 - stop_factor   * sigma)

        for j in range(i + 1, min(i + horizon + 1, n)):
            price = closes[j]
            if price >= upper:
                labels[i] = "BUY"
                break
            if price <= lower:
                labels[i] = "SELL"
                break

    return pd.Series(labels, index=df.index, name="label")


def class_weights(labels: pd.Series) -> dict[str, float]:
    """Compute inverse-frequency class weights to handle HOLD imbalance.

    In practice, HOLD dominates (~60–70% of labels). These weights are passed
    to LightGBM's sample_weight to balance training signal across classes.

    Returns:
        Dict mapping label string → sample weight float.
    """
    counts  = labels.value_counts()
    total   = len(labels)
    n_classes = len(counts)
    return {cls: total / (n_classes * cnt) for cls, cnt in counts.items()}
