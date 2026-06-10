"""Shared PostgreSQL connection helper.

Provides a context manager that handles commits, rollbacks, and connection
cleanup automatically. All pipeline services use this to avoid duplicating
connection logic.

The tenant_id parameter sets app.current_tenant_id in the session so PostgreSQL
Row Level Security policies can enforce tenant isolation automatically.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager

import psycopg2
import psycopg2.extensions

from shared.exceptions import DatabaseError
from shared.logger import get_logger

_logger = get_logger(__name__)


@contextmanager
def get_db_connection(
    tenant_id: str | None = None,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a managed PostgreSQL connection with optional tenant context.

    Sets app.current_tenant_id session variable when tenant_id is provided,
    enabling Row Level Security policies to enforce isolation transparently.

    Args:
        tenant_id: UUID string of the current tenant. Pass None for
                   system-level operations that span all tenants.

    Yields:
        An open psycopg2 connection. Commits on clean exit, rolls back on error.

    Raises:
        DatabaseError: When a connection or query fails unexpectedly.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise DatabaseError("DATABASE_URL environment variable is not set.")

    conn: psycopg2.extensions.connection | None = None
    try:
        conn = psycopg2.connect(dsn)

        if tenant_id is not None:
            with conn.cursor() as cur:
                # RLS policies read this session variable.
                cur.execute(
                    "SET LOCAL app.current_tenant_id = %s",
                    (tenant_id,),
                )

        yield conn
        conn.commit()

    except psycopg2.Error as exc:
        if conn is not None:
            conn.rollback()
        _logger.error(
            "Database operation failed.",
            extra={"error": str(exc), "tenant_id": tenant_id},
        )
        raise DatabaseError(f"Database error: {exc}") from exc

    finally:
        if conn is not None:
            conn.close()
