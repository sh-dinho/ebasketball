"""Database integration tests."""

from ebasketball.database import get_connection


def test_database_connection_and_required_objects():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            tables = {row["table_name"] for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'public'
                """
            )
            views = {row["table_name"] for row in cursor.fetchall()}

    assert {
        "players", "matches", "match_sources", "odds_history"
    } <= tables

    assert "latest_match_odds" in views
