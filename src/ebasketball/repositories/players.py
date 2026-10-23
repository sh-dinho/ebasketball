"""Player persistence."""

from __future__ import annotations

from psycopg2.extensions import connection as Connection


def normalize_handle(handle: str) -> str:
    if not isinstance(handle, str):
        raise ValueError("Player handle must be a string.")

    handle = handle.strip()

    if not handle or len(handle) > 50:
        raise ValueError(
            "Player handle must contain 1–50 characters."
        )

    return handle


def get_or_create_player(
    conn: Connection,
    handle: str,
    display_name: str | None = None,
) -> int:
    """Resolve a stable player ID without creating duplicates."""
    handle = normalize_handle(handle)

    if display_name is not None:
        display_name = display_name.strip() or None

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO players (handle, display_name)
            VALUES (%s, %s)
            ON CONFLICT (handle)
            DO UPDATE SET
                display_name = COALESCE(
                    EXCLUDED.display_name,
                    players.display_name
                ),
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (handle, display_name),
        )

        row = cursor.fetchone()

    return row["id"]
