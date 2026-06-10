"""Feature engineering for the ML signal pipeline.

Computes technical and statistical features from a candle sequence.
All output features are stationary (returns, z-scores, ratios) — raw prices
are excluded to avoid non-stationarity issues in the model.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from shared.models import Candle

# Minimum candles needed to produce any non-NaN features (MACD slow period = 26).
MIN_CANDLES_REQUIRED = 26


def build_features(candles: list[Candle]) -> pd.DataFrame | None:
    """Compute feature DataFrame from a candle sequence.

    Args:
        candles: Candles ordered oldest → newest.

    Returns:
        DataFrame with one row per candle (warm-up NaN rows dropped),
        or None if fewer than MIN_CANDLES_REQUIRED candles supplied.
    """
    if len(candles) < MIN_CANDLES_REQUIRED:
        return None

    df = pd.DataFrame(
        {
            "close":  [c.close  for c in candles],
            "volume": [c.volume for c in candles],
            "high":   [c.high   for c in candles],
            "low":    [c.low    for c in candles],
        }
    )

    # ── Trend / momentum ─────────────────────────────────────────────────────
    df["ema_9"]  = df["close"].ewm(span=9,  adjust=False).mean()
    df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_cross"] = df["ema_9"] - df["ema_21"]          # sign = trend direction
    df["rsi_14"] = _compute_rsi(df["close"], period=14)
    df["macd"], df["macd_signal"] = _compute_macd(df["close"])
    df["macd_hist"] = df["macd"] - df["macd_signal"]       # histogram = momentum

    # ── Returns / volatility ─────────────────────────────────────────────────
    df["return_1"]    = df["close"].pct_change(1)
    df["return_3"]    = df["close"].pct_change(3)
    df["volatility_21"] = df["return_1"].rolling(21).std() # realized vol regime

    # Z-score of 1-period return: how unusual is this candle vs recent history
    ret_mean = df["return_1"].rolling(21).mean()
    ret_std  = df["return_1"].rolling(21).std().replace(0, np.nan)
    df["return_z"] = (df["return_1"] - ret_mean) / ret_std

    # ── Volume ───────────────────────────────────────────────────────────────
    df["volume_ma_10"] = df["volume"].rolling(10).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma_10"].replace(0, np.nan)

    # ── High-low range (volatility proxy within candle) ───────────────────────
    df["hl_range"] = (df["high"] - df["low"]) / df["close"]

    df = df.dropna()
    return df if not df.empty else None


FEATURE_COLS: list[str] = [
    "ema_cross", "rsi_14", "macd", "macd_signal", "macd_hist",
    "return_1", "return_3", "volatility_21", "return_z",
    "volume_ratio", "hl_range",
]


def _compute_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI using Wilder's smoothing. Returns 100 for pure uptrends, 0 for down."""
    delta = prices.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rsi = pd.Series(
        np.where(
            avg_loss == 0,
            np.where(avg_gain == 0, 50.0, 100.0),
            100.0 - (100.0 / (1.0 + avg_gain / avg_loss)),
        ),
        index=prices.index,
    )
    rsi[delta.isna()] = np.nan
    return rsi


def _compute_macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series]:
    """Compute MACD line and signal line."""
    ema_fast   = prices.ewm(span=fast,   adjust=False).mean()
    ema_slow   = prices.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line
