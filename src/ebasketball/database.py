"""Database connections and transactions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extras import RealDictCursor

from ebasketball.config import get_settings


def connect() -> Connection:
    """Open a PostgreSQL connection. The caller owns its lifecycle."""
    settings = get_settings()

    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=settings.db_connect_timeout,
        cursor_factory=RealDictCursor,
        application_name="ebasketball",
    )


@contextmanager
def get_connection() -> Iterator[Connection]:
    """Open a connection and transaction.

    Commit on success, roll back on failure, and always close.
    """
    conn = connect()

    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def check_database() -> dict[str, Any]:
    """Return basic connectivity and database identity information."""
    with get_connection() as conn, conn.cursor() as cursor:
        cursor.execute(
            """
                SELECT
                    current_database() AS database_name,
                    current_user AS database_user,
                    current_timestamp AS checked_at
                """
        )
        row = cursor.fetchone()

    return dict(row)
