"""Database helpers for fetching tenant strategy configuration.

Cache-first: a Redis-backed TenantCache (shared/tenant_cache.py) fronts the
Postgres query. On a database outage, a last-known-good ("stale") cache entry
is served instead of silently dropping the signal; only when both the
database and the cache are unavailable does this raise TenantLookupError.
"""

from shared.db_client import get_db_connection
from shared.exceptions import DatabaseError, TenantLookupError
from shared.logger import get_logger
from shared.redis_client import get_redis_client
from shared.tenant_cache import TenantCache, TenantCacheEntry

_logger = get_logger(__name__)

_cache: TenantCache | None = None


def _get_cache() -> TenantCache:
    """Return the process-wide TenantCache, creating it on first use."""
    global _cache
    if _cache is None:
        _cache = TenantCache(get_redis_client())
    return _cache


def _query_postgres(symbol: str) -> list[dict[str, object]]:
    """Query tenants with an active strategy for the given symbol.

    Raises:
        DatabaseError: When the query fails (propagated from get_db_connection).
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT tenant_id::text, position_size, region
            FROM tenant_strategies
            WHERE symbol = %s
              AND enabled = TRUE
            """,
            (symbol,),
        )
        rows = cur.fetchall()

    return [{"tenant_id": row[0], "position_size": float(row[1]), "region": row[2]} for row in rows]


def get_matching_tenants(symbol: str) -> list[dict[str, object]]:
    """Return tenants with an active strategy for the given symbol.

    Cache-first (30s TTL). On a cache miss, queries Postgres and writes
    through to the cache. On a database failure, falls back to the last
    known-good cached value for this symbol.

    Returns:
        List of dicts with keys: tenant_id (str), position_size (float), region (str).

    Raises:
        TenantLookupError: The database is unavailable and no cached value
                            (fresh or stale) exists for this symbol.
    """
    cache = _get_cache()

    cached = cache.get(symbol)
    if cached is not None:
        return [c.model_dump() for c in cached]

    try:
        tenants = _query_postgres(symbol)
    except DatabaseError as exc:
        stale = cache.get_stale(symbol)
        if stale is not None:
            _logger.warning(
                "Database unavailable — serving stale tenant cache.",
                extra={"symbol": symbol, "error": str(exc)},
            )
            return [c.model_dump() for c in stale]

        _logger.error(
            "Database unavailable and no cached tenants for signal.",
            extra={"symbol": symbol, "error": str(exc)},
        )
        raise TenantLookupError(
            f"Tenant lookup failed for {symbol} and no cache available."
        ) from exc

    cache.set(symbol, [TenantCacheEntry.model_validate(t) for t in tenants])

    _logger.debug(
        "Fetched matching tenants.",
        extra={"symbol": symbol, "count": len(tenants)},
    )
    return tenants
