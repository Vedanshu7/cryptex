"""Unit tests for cache-first tenant matching."""

import json
from unittest.mock import MagicMock, patch

import pytest

from shared.exceptions import DatabaseError, TenantLookupError
from signal_router.src import tenant_config


@pytest.fixture(autouse=True)
def _reset_cache_singleton() -> None:
    tenant_config._cache = None
    yield
    tenant_config._cache = None


@pytest.fixture()
def mock_redis() -> MagicMock:
    client = MagicMock()
    with patch("signal_router.src.tenant_config.get_redis_client", return_value=client):
        yield client


_ROWS = [("tenant-a", 0.01, "tokyo")]
_EXPECTED = [{"tenant_id": "tenant-a", "position_size": 0.01, "region": "tokyo"}]


def _db_conn_returning(rows: list[tuple[object, ...]]) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__.return_value = conn
    return conn


class TestGetMatchingTenants:
    def test_cache_hit_skips_database(self, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = json.dumps(_EXPECTED)

        with patch("signal_router.src.tenant_config.get_db_connection") as mock_db:
            result = tenant_config.get_matching_tenants("BTCUSDT")

        mock_db.assert_not_called()
        assert result == _EXPECTED

    def test_cache_miss_queries_db_and_populates_cache(self, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = None
        conn = _db_conn_returning(_ROWS)

        with patch("signal_router.src.tenant_config.get_db_connection", return_value=conn):
            result = tenant_config.get_matching_tenants("BTCUSDT")

        assert result == _EXPECTED
        mock_redis.setex.assert_called_once()
        mock_redis.set.assert_called_once()

    def test_db_failure_serves_stale_cache(self, mock_redis: MagicMock) -> None:
        # First get() call (fresh key) misses; get_stale()'s get() call hits.
        mock_redis.get.side_effect = [None, json.dumps(_EXPECTED)]

        with patch(
            "signal_router.src.tenant_config.get_db_connection",
            side_effect=DatabaseError("connection refused"),
        ):
            result = tenant_config.get_matching_tenants("BTCUSDT")

        assert result == _EXPECTED

    def test_db_failure_with_no_cache_raises(self, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = None

        with (
            patch(
                "signal_router.src.tenant_config.get_db_connection",
                side_effect=DatabaseError("connection refused"),
            ),
            pytest.raises(TenantLookupError),
        ):
            tenant_config.get_matching_tenants("BTCUSDT")
