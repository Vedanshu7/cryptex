"""Database helpers for fetching tenant strategy configuration."""

from shared.db_client import get_db_connection
from shared.exceptions import DatabaseError
from shared.logger import get_logger

_logger = get_logger(__name__)


def get_matching_tenants(symbol: str) -> list[dict[str, object]]:
    """Query tenants with an active strategy for the given symbol.

    Returns an empty list (no orders routed) if the database is unavailable
    rather than crashing the service — Kafka offset is committed and the signal
    is effectively dropped for this cycle.

    Returns:
        List of dicts with keys: tenant_id (str), position_size (float).
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id::text, position_size
                    FROM tenant_strategies
                    WHERE symbol = %s
                      AND enabled = TRUE
                    """,
                    (symbol,),
                )
                rows = cur.fetchall()
    except DatabaseError as exc:
        _logger.error(
            "Database unavailable — skipping tenant lookup for signal.",
            extra={"symbol": symbol, "error": str(exc)},
        )
        return []

    tenants = [
        {"tenant_id": row[0], "position_size": float(row[1])}
        for row in rows
    ]

    _logger.debug(
        "Fetched matching tenants.",
        extra={"symbol": symbol, "count": len(tenants)},
    )
    return tenants
