"""Detects tenant strategies whose (region, symbol) pair has fallen out of
sync with the current trading universe.

The signal router already skips a mismatched tenant reactively — but only
when a live signal actually arrives for that symbol (see
signal_router/src/router.py::_region_supports_symbol). If nothing ever
generates a signal for a misconfigured symbol, that tenant's strategy sits
silently broken with no live traffic to expose it. This module answers a
different question: independent of live signal traffic, which *configured*
tenant strategies don't match the current universe right now?

Two distinct failure reasons are reported:
  - unknown_region: the strategy's region doesn't exist in the universe at all
    (e.g. a typo, or a region that was retired).
  - symbol_not_in_region: the region exists, but its exchange doesn't list
    this symbol (e.g. the symbol was delisted, or never valid there).
"""

from dataclasses import dataclass
from typing import Literal

from shared.db_client import get_db_connection
from shared.logger import get_logger
from shared.universe import Universe

_logger = get_logger(__name__)

MismatchReason = Literal["unknown_region", "symbol_not_in_region"]


@dataclass(frozen=True)
class TenantUniverseMismatch:
    """One enabled tenant_strategies row that the current universe no longer supports."""

    tenant_id: str
    tenant_name: str
    symbol: str
    region: str
    reason: MismatchReason


def _fetch_enabled_strategies() -> list[tuple[str, str, str, str]]:
    """Return (tenant_id, tenant_name, symbol, region) for every enabled strategy.

    Uses get_db_connection() with no tenant_id — a cross-tenant query, same
    pattern signal_router/src/tenant_config.py already uses for routing.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id::text, t.name, ts.symbol, ts.region
            FROM tenant_strategies ts
            JOIN tenants t ON t.id = ts.tenant_id
            WHERE ts.enabled = TRUE
            """
        )
        return [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]


def find_tenant_universe_mismatches(universe: Universe) -> list[TenantUniverseMismatch]:
    """Return every enabled tenant strategy that the given universe can't route.

    Args:
        universe: The universe to check against — pass get_universe() for the
            live universe, or a specific Universe instance in tests.
    """
    mismatches: list[TenantUniverseMismatch] = []

    for tenant_id, tenant_name, symbol, region in _fetch_enabled_strategies():
        if region not in universe.regions:
            mismatches.append(
                TenantUniverseMismatch(
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    symbol=symbol,
                    region=region,
                    reason="unknown_region",
                )
            )
        elif not universe.has_symbol(region, symbol):
            mismatches.append(
                TenantUniverseMismatch(
                    tenant_id=tenant_id,
                    tenant_name=tenant_name,
                    symbol=symbol,
                    region=region,
                    reason="symbol_not_in_region",
                )
            )

    return mismatches
