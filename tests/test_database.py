"""PostgreSQL migration integration tests."""

from pathlib import Path


MIGRATION_DIR = Path(__file__).resolve().parents[1] / "sql" / "migrations"


def test_migrations_create_required_schema_and_are_repeatable(migrated_connection):
    migration_files = sorted(MIGRATION_DIR.glob("[0-9][0-9][0-9]_*.sql"))

    with migrated_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
            """
        )
        tables = {row["table_name"] for row in cursor.fetchall()}
        assert {"players", "matches", "match_sources", "odds_history"} <= tables

        cursor.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
            """
        )
        columns = {}
        for row in cursor.fetchall():
            columns.setdefault(row["table_name"], set()).add(
                row["column_name"]
            )
        assert {
            "id", "handle", "display_name", "created_at", "updated_at"
        } <= columns["players"]
        assert {
            "id", "match_time", "home_player_id", "away_player_id",
            "status", "home_score", "away_score", "data_source",
            "is_synthetic", "created_at", "updated_at",
        } <= columns["matches"]
        assert {
            "id", "match_id", "source", "source_event_id", "source_url",
            "source_match_key", "source_updated_at", "first_seen_at",
            "last_seen_at", "last_checked_at", "raw_hash", "created_at",
        } <= columns["match_sources"]
        assert {
            "id", "match_id", "market_type", "outcome", "line", "odds",
            "source", "is_synthetic", "observed_at",
        } <= columns["odds_history"]

        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'matches'
              AND column_name IN ('home_score', 'away_score')
            """
        )
        assert [row["data_type"] for row in cursor.fetchall()] == [
            "integer", "integer"
        ]

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = current_schema()
            """
        )
        assert "latest_match_odds" in {
            row["table_name"] for row in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
            """
        )
        indexes = {row["indexname"] for row in cursor.fetchall()}
        assert {
            "uq_matches_fixture",
            "uq_match_sources_external_id",
            "uq_match_sources_unkeyed",
            "idx_matches_match_time",
            "idx_matches_status_time",
            "idx_matches_home_player",
            "idx_matches_away_player",
            "idx_odds_history_match_time",
            "idx_odds_history_market",
            "idx_odds_history_source_time",
            "idx_match_sources_match",
            "idx_match_sources_last_seen",
        } <= indexes

        cursor.execute(
            """
            SELECT trigger_name
            FROM information_schema.triggers
            WHERE trigger_schema = current_schema()
            """
        )
        assert {
            "trg_players_updated_at", "trg_matches_updated_at"
        } <= {row["trigger_name"] for row in cursor.fetchall()}

        cursor.execute(
            """
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = current_schema()
            """
        )
        constraints = {row["constraint_name"] for row in cursor.fetchall()}
        assert {
            "ck_matches_different_players",
            "ck_matches_nonnegative_scores",
            "ck_completed_matches_have_scores",
            "uq_match_source_key",
            "ck_odds_market_outcome",
        } <= constraints

        cursor.execute(
            """
            SELECT COUNT(*) AS foreign_key_count
            FROM information_schema.table_constraints
            WHERE table_schema = current_schema()
              AND constraint_type = 'FOREIGN KEY'
            """
        )
        assert cursor.fetchone()["foreign_key_count"] == 4

        # Reapplying these idempotent scripts models a retry after a partial
        # deployment and must preserve all created objects and rows.
        for migration in migration_files:
            cursor.execute(migration.read_text(encoding="utf-8"))

        cursor.execute("SELECT COUNT(*) AS player_count FROM players")
        assert cursor.fetchone()["player_count"] == 0


def test_duplicate_legacy_provenance_aborts_migration_without_data_loss(
    migrated_connection,
):
    import pytest
    import psycopg2

    with migrated_connection.cursor() as cursor:
        cursor.execute("DROP INDEX uq_match_sources_unkeyed")
        cursor.execute("DROP INDEX uq_match_sources_external_id")
        cursor.execute(
            """
            INSERT INTO players (handle)
            VALUES ('legacy-home'), ('legacy-away')
            RETURNING id, handle
            """
        )
        player_ids = {row["handle"]: row["id"] for row in cursor.fetchall()}
        cursor.execute(
            """
            INSERT INTO matches (
                match_time, home_player_id, away_player_id
            )
            VALUES (CURRENT_TIMESTAMP, %s, %s)
            RETURNING id
            """,
            (player_ids["legacy-home"], player_ids["legacy-away"]),
        )
        match_id = cursor.fetchone()["id"]
        cursor.execute(
            """
            INSERT INTO match_sources (match_id, source)
            VALUES (%s, 'h2hgg'), (%s, 'h2hgg')
            """,
            (match_id, match_id),
        )

    provenance_migration = MIGRATION_DIR / "002_source_provenance.sql"
    with migrated_connection.cursor() as cursor:
        with pytest.raises(psycopg2.Error, match="duplicate unkeyed"):
            cursor.execute(provenance_migration.read_text(encoding="utf-8"))
        cursor.execute("ROLLBACK")

        cursor.execute(
            """
            SELECT COUNT(*) AS provenance_count
            FROM match_sources WHERE match_id = %s
            """,
            (match_id,),
        )
        assert cursor.fetchone()["provenance_count"] == 2

        cursor.execute(
            """
            SELECT COUNT(*) AS created_index_count
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = 'uq_match_sources_external_id'
            """
        )
        assert cursor.fetchone()["created_index_count"] == 0
