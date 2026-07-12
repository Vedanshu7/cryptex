"""Unit tests for the Redis-backed tenant-mapping cache."""

import json
from unittest.mock import MagicMock

import pytest
import redis

from shared.tenant_cache import TenantCache, TenantCacheEntry


@pytest.fixture()
def mock_redis() -> MagicMock:
    return MagicMock(spec=redis.Redis)


@pytest.fixture()
def cache(mock_redis: MagicMock) -> TenantCache:
    return TenantCache(mock_redis, ttl_seconds=30)


_ENTRIES = [
    TenantCacheEntry(tenant_id="t1", position_size=0.01, region="tokyo"),
    TenantCacheEntry(tenant_id="t2", position_size=0.02, region="sgp"),
]


class TestGet:
    def test_miss_returns_none(self, cache: TenantCache, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = None
        assert cache.get("BTCUSDT") is None
        mock_redis.get.assert_called_once_with("tenants:BTCUSDT")

    def test_hit_returns_entries(self, cache: TenantCache, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = json.dumps([e.model_dump() for e in _ENTRIES])

        result = cache.get("BTCUSDT")

        assert result == _ENTRIES

    def test_redis_error_treated_as_miss(self, cache: TenantCache, mock_redis: MagicMock) -> None:
        mock_redis.get.side_effect = redis.RedisError("connection refused")
        assert cache.get("BTCUSDT") is None


class TestGetStale:
    def test_reads_stale_key(self, cache: TenantCache, mock_redis: MagicMock) -> None:
        mock_redis.get.return_value = json.dumps([e.model_dump() for e in _ENTRIES])

        result = cache.get_stale("BTCUSDT")

        mock_redis.get.assert_called_once_with("tenants:stale:BTCUSDT")
        assert result == _ENTRIES


class TestSet:
    def test_writes_both_fresh_and_stale_keys(
        self, cache: TenantCache, mock_redis: MagicMock
    ) -> None:
        cache.set("BTCUSDT", _ENTRIES)

        mock_redis.setex.assert_called_once()
        setex_args = mock_redis.setex.call_args.args
        assert setex_args[0] == "tenants:BTCUSDT"
        assert setex_args[1] == 30

        mock_redis.set.assert_called_once()
        set_args = mock_redis.set.call_args.args
        assert set_args[0] == "tenants:stale:BTCUSDT"

    def test_redis_error_on_write_does_not_raise(
        self, cache: TenantCache, mock_redis: MagicMock
    ) -> None:
        mock_redis.setex.side_effect = redis.RedisError("connection refused")
        cache.set("BTCUSDT", _ENTRIES)  # must not raise
