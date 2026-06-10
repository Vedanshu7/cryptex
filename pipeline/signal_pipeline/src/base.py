"""Abstract base class for all trade signal sources.

A signal source encapsulates one way of generating trade signals.
Implementations must be thread-safe; the runner may call predict()
for different symbols without holding any lock.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from shared.models import Candle, TradeSignal


class BaseSignalSource(ABC):
    """Pluggable contract for trade signal generators.

    Lifecycle:
      1. __init__  — lightweight only; no I/O, no model training.
      2. warmup()  — called once before the first prediction loop tick.
                     Use for expensive startup work (model training, API
                     key validation, etc.).
      3. predict() — called on every loop tick; must be thread-safe.
      4. retrain() — called on a slow schedule (e.g. 24 h).
                     Implementations that do not need retraining leave it
                     as a no-op via the default.
    """

    @abstractmethod
    def predict(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> TradeSignal:
        """Generate a trade signal for the given symbol.

        Args:
            symbol:   Trading pair, e.g. "BTCUSDT".
            candles:  Raw candle list (oldest first), length ≤ CANDLE_LOOKBACK.
            features: Pre-built feature DataFrame from build_features().

        Returns:
            A fully populated TradeSignal.
        """

    def warmup(self) -> None:
        """Optional warm-up called once before the prediction loop starts.

        Default is a no-op. Override to defer expensive startup work out
        of __init__ (e.g. ML bootstrap training, LLM API key validation).
        """

    def retrain(self, candles_by_symbol: dict[str, list[Candle]]) -> None:
        """Optional periodic retraining on fresh market data.

        Default is a no-op. Override only when the source has a model that
        benefits from periodic retraining (ML sources only).

        Args:
            candles_by_symbol: symbol → candle list fetched from TimescaleDB.
        """
