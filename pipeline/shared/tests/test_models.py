"""Unit tests for shared Pydantic models."""

from datetime import datetime, timezone

import pytest

from shared.models import Candle, OrderRequest, Tick, TradeSide, TradeSignal


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class TestTick:
    def test_valid_tick_parses(self) -> None:
        tick = Tick(
            symbol="BTCUSDT",
            price=50_000.0,
            volume=0.5,
            timestamp=_utcnow(),
        )
        assert tick.symbol == "BTCUSDT"

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            Tick(symbol="BTCUSDT", price=-1.0, volume=0.5, timestamp=_utcnow())

    def test_zero_volume_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            Tick(symbol="BTCUSDT", price=100.0, volume=0.0, timestamp=_utcnow())

    def test_json_roundtrip(self) -> None:
        tick = Tick(symbol="ETHUSDT", price=2_000.0, volume=1.0, timestamp=_utcnow())
        restored = Tick.model_validate_json(tick.model_dump_json())
        assert restored.price == tick.price


class TestCandle:
    def test_valid_candle_parses(self) -> None:
        now = _utcnow()
        candle = Candle(
            symbol="BTCUSDT",
            open=49_000.0,
            high=51_000.0,
            low=48_500.0,
            close=50_000.0,
            volume=100.0,
            opened_at=now,
            closed_at=now,
        )
        assert candle.timeframe == "5m"

    def test_zero_close_rejected(self) -> None:
        now = _utcnow()
        with pytest.raises(ValueError, match="positive"):
            Candle(
                symbol="BTCUSDT",
                open=1.0,
                high=1.0,
                low=1.0,
                close=0.0,
                volume=1.0,
                opened_at=now,
                closed_at=now,
            )


class TestTradeSignal:
    def test_confidence_out_of_range_rejected(self) -> None:
        now = _utcnow()
        with pytest.raises(ValueError, match="Confidence"):
            TradeSignal(
                id="sig-1",
                symbol="BTCUSDT",
                side=TradeSide.BUY,
                confidence=1.5,
                generated_at=now,
                expires_at=now,
            )

    def test_hold_signal_valid(self) -> None:
        now = _utcnow()
        signal = TradeSignal(
            id="sig-2",
            symbol="ETHUSDT",
            side=TradeSide.HOLD,
            confidence=0.5,
            generated_at=now,
            expires_at=now,
        )
        assert signal.side == TradeSide.HOLD


class TestOrderRequest:
    def test_negative_quantity_rejected(self) -> None:
        now = _utcnow()
        with pytest.raises(ValueError, match="positive"):
            OrderRequest(
                id="ord-1",
                tenant_id="tenant-uuid",
                symbol="BTCUSDT",
                side=TradeSide.BUY,
                quantity=-0.1,
                signal_id="sig-1",
                created_at=now,
            )

    def test_valid_order_request(self) -> None:
        now = _utcnow()
        req = OrderRequest(
            id="ord-1",
            tenant_id="tenant-uuid",
            symbol="BTCUSDT",
            side=TradeSide.BUY,
            quantity=0.01,
            signal_id="sig-1",
            created_at=now,
        )
        assert req.quantity == 0.01
