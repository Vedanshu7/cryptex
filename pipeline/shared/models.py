"""Pydantic domain models shared across all pipeline services.

These models define the canonical message schemas flowing through Kafka topics.
Any change here affects every service that produces or consumes that topic.
"""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, field_validator


class TradeSide(str, Enum):
    """Direction of a trade order."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class Tick(BaseModel):
    """Single trade execution event from the exchange WebSocket stream."""

    symbol: str
    price: float
    volume: float
    timestamp: datetime

    @field_validator("price", "volume")
    @classmethod
    def must_be_positive(cls, value: float) -> float:
        """Reject non-positive prices and volumes immediately at ingestion."""
        if value <= 0:
            raise ValueError("Price and volume must be positive.")
        return value


class Candle(BaseModel):
    """Aggregated OHLCV candle for a fixed timeframe window."""

    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    opened_at: datetime
    closed_at: datetime
    timeframe: str = "5m"

    @field_validator("open", "high", "low", "close", "volume")
    @classmethod
    def must_be_positive(cls, value: float) -> float:
        """Reject degenerate candles with non-positive OHLCV values."""
        if value <= 0:
            raise ValueError("OHLCV values must be positive.")
        return value


class TradeSignal(BaseModel):
    """ML-generated directional signal for a trading symbol."""

    id: str
    symbol: str
    side: TradeSide
    confidence: float
    generated_at: datetime
    expires_at: datetime

    @field_validator("confidence")
    @classmethod
    def confidence_in_range(cls, value: float) -> float:
        """Confidence must be a valid probability in [0, 1]."""
        if not 0.0 <= value <= 1.0:
            raise ValueError("Confidence must be between 0 and 1.")
        return value


class OrderRequest(BaseModel):
    """Per-tenant order request published to the order-requests Kafka topic.

    Produced by the signal_router; consumed by the OMS.
    """

    id: str
    tenant_id: str
    symbol: str
    side: TradeSide
    quantity: float
    signal_id: str
    created_at: datetime

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, value: float) -> float:
        """Reject zero or negative quantities before they reach the OMS."""
        if value <= 0:
            raise ValueError("Order quantity must be positive.")
        return value
