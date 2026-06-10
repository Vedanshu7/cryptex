"""Unit tests for the signal router."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from shared.models import TradeSide, TradeSignal
from signal_router.src.router import SignalRouter, _is_stale


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _signal(
    *,
    symbol: str = "BTCUSDT",
    side: TradeSide = TradeSide.BUY,
    expires_at: datetime | None = None,
) -> TradeSignal:
    now = _utcnow()
    return TradeSignal(
        id="sig-1",
        symbol=symbol,
        side=side,
        confidence=0.85,
        generated_at=now,
        expires_at=expires_at or (now + timedelta(seconds=90)),
    )


class TestIsStale:
    def test_fresh_signal_not_stale(self) -> None:
        assert not _is_stale(_signal())

    def test_expired_signal_is_stale(self) -> None:
        past = _utcnow() - timedelta(seconds=1)
        stale = _signal(expires_at=past)
        assert _is_stale(stale)


@pytest.fixture()
def router() -> SignalRouter:
    with patch("signal_router.src.router.KafkaClientFactory"):
        r = SignalRouter()
    r._producer = MagicMock()
    return r


class TestSignalRouter:
    def test_stale_signal_not_published(self, router: SignalRouter) -> None:
        past = _utcnow() - timedelta(seconds=1)
        stale = _signal(expires_at=past)

        with patch("signal_router.src.router.get_matching_tenants") as mock_tenants:
            router._route_signal(stale)
            mock_tenants.assert_not_called()

        router._producer.produce.assert_not_called()

    def test_fresh_signal_published_per_tenant(self, router: SignalRouter) -> None:
        sig = _signal()
        tenants = [
            {"tenant_id": "tenant-a", "position_size": 0.01},
            {"tenant_id": "tenant-b", "position_size": 0.02},
        ]

        with patch("signal_router.src.router.get_matching_tenants", return_value=tenants):
            router._route_signal(sig)

        assert router._producer.produce.call_count == 2
        assert router._producer.flush.call_count == 2

    def test_no_matching_tenants_nothing_published(self, router: SignalRouter) -> None:
        sig = _signal()

        with patch("signal_router.src.router.get_matching_tenants", return_value=[]):
            router._route_signal(sig)

        router._producer.produce.assert_not_called()

    def test_order_request_uses_correct_tenant_id(self, router: SignalRouter) -> None:
        sig = _signal(symbol="ETHUSDT")
        tenants = [{"tenant_id": "tenant-xyz", "position_size": 0.5}]

        with patch("signal_router.src.router.get_matching_tenants", return_value=tenants):
            router._route_signal(sig)

        call_kwargs = router._producer.produce.call_args
        assert call_kwargs.kwargs["key"] == "tenant-xyz"
