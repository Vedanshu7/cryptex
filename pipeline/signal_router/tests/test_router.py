"""Unit tests for the signal router.

Tests target the pure `_is_stale` / `expand_to_order_requests` functions
directly rather than the Quix Streams topology wiring (`build_application`) —
that wiring needs a live Kafka broker to exercise meaningfully and is left to
integration testing.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from shared.models import TradeSide, TradeSignal
from shared.universe import Universe
from shared.universe_consistency import TenantUniverseMismatch
from signal_router.src.router import (
    _is_stale,
    _refresh_tenant_universe_gauge,
    _region_supports_symbol,
    expand_to_order_requests,
)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _signal_dict(
    *,
    symbol: str = "BTCUSDT",
    side: TradeSide = TradeSide.BUY,
    expires_at: datetime | None = None,
) -> dict[str, object]:
    now = _utcnow()
    signal = TradeSignal(
        id="sig-1",
        symbol=symbol,
        side=side,
        confidence=0.85,
        generated_at=now,
        expires_at=expires_at or (now + timedelta(seconds=90)),
    )
    return signal.model_dump(mode="json")


@pytest.fixture()
def permissive_universe() -> Universe:
    """A universe where every region trades both BTCUSDT and ETHUSDT.

    Used by tests that aren't specifically exercising the region/exchange
    symbol-mismatch filter, so they aren't coupled to real-world exchange
    quirks (e.g. Deribit not listing spot pairs).
    """
    return Universe.model_validate(
        {
            "version": 1,
            "regions": {
                "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT", "ETHUSDT"]},
                "sgp": {"exchange": "crypto_com", "symbols": ["BTCUSDT", "ETHUSDT"]},
                "eu": {"exchange": "deribit", "symbols": ["BTCUSDT", "ETHUSDT"]},
            },
        }
    )


class TestIsStale:
    def test_fresh_signal_not_stale(self) -> None:
        signal = TradeSignal.model_validate(_signal_dict())
        assert not _is_stale(signal)

    def test_expired_signal_is_stale(self) -> None:
        past = _utcnow() - timedelta(seconds=1)
        signal = TradeSignal.model_validate(_signal_dict(expires_at=past))
        assert _is_stale(signal)


class TestRegionSupportsSymbol:
    def test_true_when_universe_lists_symbol_for_region(
        self, permissive_universe: Universe
    ) -> None:
        with patch("signal_router.src.router.get_universe", return_value=permissive_universe):
            assert _region_supports_symbol("tokyo", "BTCUSDT") is True

    def test_false_when_region_exchange_does_not_list_symbol(self) -> None:
        restrictive = Universe.model_validate(
            {
                "version": 1,
                "regions": {"eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]}},
            }
        )
        with patch("signal_router.src.router.get_universe", return_value=restrictive):
            assert _region_supports_symbol("eu", "BTCUSDT") is False


class TestExpandToOrderRequests:
    def test_stale_signal_yields_nothing(self, permissive_universe: Universe) -> None:
        past = _utcnow() - timedelta(seconds=1)
        stale = _signal_dict(expires_at=past)

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants") as mock_tenants,
        ):
            result = expand_to_order_requests(stale)
            mock_tenants.assert_not_called()

        assert result == []

    def test_fresh_signal_yields_one_payload_per_tenant(
        self, permissive_universe: Universe
    ) -> None:
        sig = _signal_dict()
        tenants = [
            {"tenant_id": "tenant-a", "position_size": 0.01, "region": "tokyo"},
            {"tenant_id": "tenant-b", "position_size": 0.02, "region": "sgp"},
        ]

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        assert len(result) == 2

    def test_no_matching_tenants_yields_nothing(self, permissive_universe: Universe) -> None:
        sig = _signal_dict()

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants", return_value=[]),
        ):
            result = expand_to_order_requests(sig)

        assert result == []

    def test_payload_uses_correct_tenant_id(self, permissive_universe: Universe) -> None:
        sig = _signal_dict(symbol="ETHUSDT")
        tenants = [{"tenant_id": "tenant-xyz", "position_size": 0.5, "region": "eu"}]

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        assert result[0]["tenant_id"] == "tenant-xyz"

    def test_payload_carries_region_for_topic_routing(self, permissive_universe: Universe) -> None:
        sig = _signal_dict(symbol="BTCUSDT")
        tenants = [{"tenant_id": "tenant-tokyo", "position_size": 0.01, "region": "tokyo"}]

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        assert result[0]["_region"] == "tokyo"

    def test_different_tenants_carry_different_regions(self, permissive_universe: Universe) -> None:
        sig = _signal_dict(symbol="BTCUSDT")
        tenants = [
            {"tenant_id": "tenant-a", "position_size": 0.01, "region": "tokyo"},
            {"tenant_id": "tenant-b", "position_size": 0.01, "region": "eu"},
        ]

        with (
            patch("signal_router.src.router.get_universe", return_value=permissive_universe),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        regions = {r["_region"] for r in result}
        assert regions == {"tokyo", "eu"}

    def test_tenant_in_unsupported_region_is_skipped(self) -> None:
        """A Binance-only symbol routed toward a Deribit-only region is dropped."""
        restrictive = Universe.model_validate(
            {
                "version": 1,
                "regions": {
                    "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT"]},
                    "eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]},
                },
            }
        )
        sig = _signal_dict(symbol="BTCUSDT")
        tenants = [
            {"tenant_id": "tenant-tokyo", "position_size": 0.01, "region": "tokyo"},
            {"tenant_id": "tenant-eu", "position_size": 0.01, "region": "eu"},
        ]

        with (
            patch("signal_router.src.router.get_universe", return_value=restrictive),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        assert len(result) == 1
        assert result[0]["tenant_id"] == "tenant-tokyo"

    def test_all_tenants_in_unsupported_regions_yields_nothing(self) -> None:
        restrictive = Universe.model_validate(
            {"version": 1, "regions": {"eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]}}}
        )
        sig = _signal_dict(symbol="BTCUSDT")
        tenants = [{"tenant_id": "tenant-eu", "position_size": 0.01, "region": "eu"}]

        with (
            patch("signal_router.src.router.get_universe", return_value=restrictive),
            patch("signal_router.src.router.get_matching_tenants", return_value=tenants),
        ):
            result = expand_to_order_requests(sig)

        assert result == []


class TestRefreshTenantUniverseGauge:
    def test_gauge_set_to_zero_when_no_mismatches(self) -> None:
        with (
            patch("signal_router.src.router.find_tenant_universe_mismatches", return_value=[]),
            patch("signal_router.src.router.tenant_universe_mismatches") as mock_gauge,
        ):
            _refresh_tenant_universe_gauge()

        mock_gauge.labels.assert_any_call(reason="unknown_region")
        mock_gauge.labels.assert_any_call(reason="symbol_not_in_region")
        for call in mock_gauge.labels.return_value.set.call_args_list:
            assert call.args == (0,)

    def test_gauge_reflects_mismatch_counts_by_reason(self) -> None:
        mismatches = [
            TenantUniverseMismatch(
                tenant_id="t1", tenant_name="Alice", symbol="X", region="r", reason="unknown_region"
            ),
            TenantUniverseMismatch(
                tenant_id="t2",
                tenant_name="Bob",
                symbol="Y",
                region="sgp",
                reason="symbol_not_in_region",
            ),
            TenantUniverseMismatch(
                tenant_id="t3",
                tenant_name="Carol",
                symbol="Z",
                region="sgp",
                reason="symbol_not_in_region",
            ),
        ]

        with (
            patch(
                "signal_router.src.router.find_tenant_universe_mismatches",
                return_value=mismatches,
            ),
            patch("signal_router.src.router.tenant_universe_mismatches") as mock_gauge,
        ):
            _refresh_tenant_universe_gauge()

        # The code always pairs labels(reason=X).set(count) as one expression,
        # so the two call_args_lists line up positionally in call order.
        labels_calls = [c.kwargs["reason"] for c in mock_gauge.labels.call_args_list]
        set_values = [c.args[0] for c in mock_gauge.labels.return_value.set.call_args_list]
        counts_by_reason = dict(zip(labels_calls, set_values, strict=True))

        assert counts_by_reason["unknown_region"] == 1
        assert counts_by_reason["symbol_not_in_region"] == 2

    def test_continues_past_a_single_bad_reason_key(self) -> None:
        # Sanity: a reason value outside the two known ones still gets counted
        # and doesn't crash the refresh (defensive against a future new reason).
        mismatches = [
            TenantUniverseMismatch(
                tenant_id="t1",
                tenant_name="Alice",
                symbol="X",
                region="r",
                reason="unknown_region",
            )
        ]

        with (
            patch(
                "signal_router.src.router.find_tenant_universe_mismatches",
                return_value=mismatches,
            ),
            patch("signal_router.src.router.tenant_universe_mismatches"),
        ):
            _refresh_tenant_universe_gauge()  # must not raise
