"""Unit tests for tenant/universe consistency checking."""

from unittest.mock import MagicMock, patch

from shared.universe import Universe
from shared.universe_consistency import find_tenant_universe_mismatches


def _universe() -> Universe:
    return Universe.model_validate(
        {
            "version": 1,
            "regions": {
                "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT", "ETHUSDT"]},
                "sgp": {"exchange": "crypto_com", "symbols": ["BTCUSDT"]},
            },
        }
    )


def _mock_db_rows(rows: list[tuple[str, str, str, str]]) -> MagicMock:
    """Build a get_db_connection() mock whose cursor.fetchall() returns `rows`."""
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = rows
    context_manager = MagicMock()
    context_manager.__enter__.return_value = mock_conn
    return context_manager


class TestFindTenantUniverseMismatches:
    def test_no_mismatches_when_all_strategies_valid(self) -> None:
        rows = [("t1", "Alice", "BTCUSDT", "tokyo"), ("t2", "Bob", "BTCUSDT", "sgp")]

        with patch(
            "shared.universe_consistency.get_db_connection", return_value=_mock_db_rows(rows)
        ):
            mismatches = find_tenant_universe_mismatches(_universe())

        assert mismatches == []

    def test_unknown_region_is_flagged(self) -> None:
        rows = [("t1", "Alice", "BTCUSDT", "atlantis")]

        with patch(
            "shared.universe_consistency.get_db_connection", return_value=_mock_db_rows(rows)
        ):
            mismatches = find_tenant_universe_mismatches(_universe())

        assert len(mismatches) == 1
        assert mismatches[0].reason == "unknown_region"
        assert mismatches[0].tenant_name == "Alice"

    def test_symbol_not_in_region_is_flagged(self) -> None:
        # sgp/crypto_com only lists BTCUSDT in this universe, not ETHUSDT.
        rows = [("t1", "Alice", "ETHUSDT", "sgp")]

        with patch(
            "shared.universe_consistency.get_db_connection", return_value=_mock_db_rows(rows)
        ):
            mismatches = find_tenant_universe_mismatches(_universe())

        assert len(mismatches) == 1
        assert mismatches[0].reason == "symbol_not_in_region"

    def test_mixed_valid_and_invalid_strategies(self) -> None:
        rows = [
            ("t1", "Alice", "BTCUSDT", "tokyo"),  # valid
            ("t2", "Bob", "ETHUSDT", "sgp"),  # symbol not in region
            ("t3", "Carol", "BTCUSDT", "eu"),  # unknown region
        ]

        with patch(
            "shared.universe_consistency.get_db_connection", return_value=_mock_db_rows(rows)
        ):
            mismatches = find_tenant_universe_mismatches(_universe())

        assert len(mismatches) == 2
        reasons = {m.tenant_name: m.reason for m in mismatches}
        assert reasons == {"Bob": "symbol_not_in_region", "Carol": "unknown_region"}
