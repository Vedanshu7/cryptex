"""Redis-backed cache of symbol -> matching-tenant lookups.

Reuses the reload/fall-back-to-last-known-good idea from UniverseStore
(shared/universe.py), adapted to Redis: a short-TTL key serves normal traffic,
and a second, never-expiring "stale" key (overwritten on every successful
database read) is served instead when the database is unavailable and the
TTL'd key has expired.
"""

import json
from typing import cast

import redis
from pydantic import BaseModel

from shared.logger import get_logger

_logger = get_logger(__name__)

_FRESH_KEY_PREFIX = "tenants:"
_STALE_KEY_PREFIX = "tenants:stale:"


class TenantCacheEntry(BaseModel):
    """One tenant's strategy match for a symbol."""

    tenant_id: str
    position_size: float
    region: str


class TenantCache:
    """TTL'd Redis cache of symbol -> matching tenants, with a stale fallback."""

    def __init__(self, redis_client: redis.Redis, ttl_seconds: int = 30) -> None:
        self._redis = redis_client
        self._ttl_seconds = ttl_seconds

    def get(self, symbol: str) -> list[TenantCacheEntry] | None:
        """Return the fresh, TTL'd cache entry for `symbol`, or None on a miss."""
        return self._read(_FRESH_KEY_PREFIX + symbol)

    def get_stale(self, symbol: str) -> list[TenantCacheEntry] | None:
        """Return the last-known-good entry for `symbol`, ignoring TTL."""
        return self._read(_STALE_KEY_PREFIX + symbol)

    def set(self, symbol: str, tenants: list[TenantCacheEntry]) -> None:
        """Write-through both the TTL'd and the never-expiring stale key."""
        payload = json.dumps([t.model_dump() for t in tenants])
        try:
            self._redis.setex(_FRESH_KEY_PREFIX + symbol, self._ttl_seconds, payload)
            self._redis.set(_STALE_KEY_PREFIX + symbol, payload)
        except redis.RedisError as exc:
            _logger.warning(
                "Failed to write tenant cache — continuing without it.",
                extra={"symbol": symbol, "error": str(exc)},
            )

    def _read(self, key: str) -> list[TenantCacheEntry] | None:
        try:
            raw = self._redis.get(key)
        except redis.RedisError as exc:
            _logger.warning(
                "Failed to read tenant cache — treating as a miss.",
                extra={"key": key, "error": str(exc)},
            )
            return None

        if raw is None:
            return None

        # redis-py's stubs type Redis.get() as Awaitable[Any] | Any to cover both
        # the sync and async client — this client is always sync, hence the cast.
        entries: list[dict[str, object]] = json.loads(cast(str, raw))
        return [TenantCacheEntry.model_validate(e) for e in entries]
