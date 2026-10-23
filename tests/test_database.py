"""PostgreSQL migration integration tests."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from ebasketball.repositories.predictions import (
    create_prediction,
    list_predictions,
    prediction_performance,
    reconcile_predictions,
)

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
        assert {"prediction_records", "prediction_evaluation_events"} <= tables

        cursor.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
            """
        )
        columns = {}
        for row in cursor.fetchall():
            columns.setdefault(row["table_name"], set()).add(row["column_name"])
        assert {"id", "handle", "display_name", "created_at", "updated_at"} <= columns[
            "players"
        ]
        assert {
            "id",
            "match_time",
            "home_player_id",
            "away_player_id",
            "status",
            "home_score",
            "away_score",
            "data_source",
            "is_synthetic",
            "created_at",
            "updated_at",
            "spread_line",
            "away_spread_line",
            "total_line",
            "home_moneyline",
            "away_moneyline",
            "home_spread_odds",
            "away_spread_odds",
            "over_odds",
            "under_odds",
        } <= columns["matches"]
        assert {
            "id",
            "match_id",
            "source",
            "source_event_id",
            "source_url",
            "source_match_key",
            "source_updated_at",
            "first_seen_at",
            "last_seen_at",
            "last_checked_at",
            "raw_hash",
            "created_at",
        } <= columns["match_sources"]
        assert {
            "id",
            "match_id",
            "market_type",
            "outcome",
            "line",
            "odds",
            "source",
            "is_synthetic",
            "observed_at",
        } <= columns["odds_history"]
        assert {
            "prediction_run_id",
            "prediction_created_at",
            "predicted_winner_player_id",
            "supporting_statistics",
            "sample_size",
            "evidence_strength",
            "method_version",
            "status",
            "evaluated_at",
            "current_result_fingerprint",
        } <= columns["prediction_records"]
        assert {
            "prediction_id",
            "result_fingerprint",
            "result_signature",
            "status",
            "home_score",
            "away_score",
            "actual_winner_player_id",
            "evaluated_at",
        } <= columns["prediction_evaluation_events"]

        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'matches'
              AND column_name IN ('home_score', 'away_score')
            """
        )
        assert [row["data_type"] for row in cursor.fetchall()] == ["integer", "integer"]

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = current_schema()
            """
        )
        assert {"latest_match_odds", "real_completed_matches"} <= {
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
            "uq_matches_fixture_canonical",
            "idx_matches_real_completed_time",
            "idx_prediction_records_match",
            "idx_prediction_records_status",
            "idx_prediction_records_method",
            "idx_prediction_events_history",
        } <= indexes

        cursor.execute(
            """
            SELECT trigger_name
            FROM information_schema.triggers
            WHERE trigger_schema = current_schema()
            """
        )
        assert {
            "trg_players_updated_at",
            "trg_matches_updated_at",
            "trg_prediction_fixture_valid",
            "trg_prediction_records_immutable",
            "trg_prediction_evaluation_events_append_only",
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
            "ck_completed_matches_decisive",
            "ck_matches_synthetic_source",
            "uq_match_source_key",
            "ck_odds_market_outcome",
            "ck_odds_synthetic_source",
            "uq_prediction_run_fixture",
            "ck_prediction_distinct_players",
            "ck_prediction_winner_is_participant",
        } <= constraints

        cursor.execute(
            """
            SELECT COUNT(*) AS foreign_key_count
            FROM information_schema.table_constraints
            WHERE table_schema = current_schema()
              AND constraint_type = 'FOREIGN KEY'
            """
        )
        assert cursor.fetchone()["foreign_key_count"] >= 10

        # Reapplying these idempotent scripts models a retry after a partial
        # deployment and must preserve all created objects and rows.
        for migration in migration_files:
            cursor.execute(migration.read_text(encoding="utf-8"))

        cursor.execute("SELECT COUNT(*) AS player_count FROM players")
        assert cursor.fetchone()["player_count"] == 0


def test_forward_migration_rerun_preserves_observation_rows(migrated_connection):
    migration = MIGRATION_DIR / "003_legacy_observations_and_integrity.sql"
    with migrated_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO players (handle) VALUES ('rerun-home'), ('rerun-away') "
            "RETURNING id, handle"
        )
        player_ids = {row["handle"]: row["id"] for row in cursor.fetchall()}
        cursor.execute(
            """
            INSERT INTO matches (
                match_time, home_player_id, away_player_id, status
            ) VALUES (%s, %s, %s, 'scheduled') RETURNING id
            """,
            (
                datetime(2026, 10, 20, tzinfo=timezone.utc),
                player_ids["rerun-home"],
                player_ids["rerun-away"],
            ),
        )
        match_id = cursor.fetchone()["id"]
        cursor.execute(
            """
            INSERT INTO odds_history (
                match_id, market_type, outcome, odds, source
            ) VALUES (%s, 'moneyline', 'home', 1.950, 'h2hgg')
            """,
            (match_id,),
        )

    with migrated_connection.cursor() as cursor:
        cursor.execute(migration.read_text(encoding="utf-8"))
        cursor.execute(
            "SELECT COUNT(*) AS count FROM odds_history WHERE match_id = %s",
            (match_id,),
        )
        assert cursor.fetchone()["count"] == 1
        cursor.execute("SELECT COUNT(*) AS count FROM matches")
        assert cursor.fetchone()["count"] == 1


def test_schema_sql_fresh_install_has_compatibility_objects(empty_test_connection):
    schema = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
    with empty_test_connection.cursor() as cursor:
        cursor.execute(schema.read_text(encoding="utf-8"))
        # A repeat initialization is safe and does not remove observations.
        cursor.execute(schema.read_text(encoding="utf-8"))
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'matches'
            """
        )
        columns = {row["column_name"] for row in cursor.fetchall()}
        assert {
            "home_moneyline",
            "away_moneyline",
            "spread_line",
            "total_line",
        } <= columns
        cursor.execute("SELECT to_regclass('odds_history') AS table_name")
        assert cursor.fetchone()["table_name"] == "odds_history"
        cursor.execute("SELECT to_regclass('latest_match_odds') AS view_name")
        assert cursor.fetchone()["view_name"] == "latest_match_odds"
        cursor.execute("SELECT to_regclass('prediction_records') AS table_name")
        assert cursor.fetchone()["table_name"] == "prediction_records"
        cursor.execute(
            "SELECT to_regclass('prediction_evaluation_events') AS table_name"
        )
        assert cursor.fetchone()["table_name"] == "prediction_evaluation_events"


def test_canonical_index_migration_failure_keeps_duplicate_rows(
    migrated_connection,
):
    import psycopg2
    import pytest

    with migrated_connection.cursor() as cursor:
        cursor.execute("DROP INDEX uq_matches_fixture_canonical")
        cursor.execute(
            "INSERT INTO players (handle) VALUES ('reverse-home'), ('reverse-away') "
            "RETURNING id, handle"
        )
        player_ids = {row["handle"]: row["id"] for row in cursor.fetchall()}
        timestamp = datetime(2026, 10, 21, tzinfo=timezone.utc)
        cursor.execute(
            """
            INSERT INTO matches (match_time, home_player_id, away_player_id)
            VALUES (%s, %s, %s), (%s, %s, %s)
            """,
            (
                timestamp,
                player_ids["reverse-home"],
                player_ids["reverse-away"],
                timestamp,
                player_ids["reverse-away"],
                player_ids["reverse-home"],
            ),
        )

    forward = MIGRATION_DIR / "003_legacy_observations_and_integrity.sql"
    with migrated_connection.cursor() as cursor:
        with pytest.raises(psycopg2.Error, match="Reversed-orientation duplicate"):
            cursor.execute(forward.read_text(encoding="utf-8"))
        cursor.execute("ROLLBACK")
        cursor.execute("SELECT COUNT(*) AS count FROM matches")
        assert cursor.fetchone()["count"] == 2
        cursor.execute(
            """
            SELECT COUNT(*) AS count FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = 'uq_matches_fixture_canonical'
            """
        )
        assert cursor.fetchone()["count"] == 0


def test_duplicate_legacy_provenance_aborts_migration_without_data_loss(
    migrated_connection,
):
    import psycopg2
    import pytest

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


def test_prediction_snapshot_reconciliation_and_score_correction(migrated_connection):
    conn = migrated_connection
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO players (handle) VALUES ('prediction-home'), ('prediction-away') "
            "RETURNING id, handle"
        )
        players = {row["handle"]: row["id"] for row in cursor.fetchall()}
        cursor.execute(
            """
            INSERT INTO matches (match_time, home_player_id, away_player_id, status)
            VALUES (CURRENT_TIMESTAMP + INTERVAL '1 day', %s, %s, 'scheduled')
            RETURNING id
            """,
            (players["prediction-home"], players["prediction-away"]),
        )
        match_id = cursor.fetchone()["id"]

    run_id = uuid4()
    snapshot = {"team_a": {"wins": 8}, "win_probabilities": None}
    created = create_prediction(
        conn,
        match_id=match_id,
        prediction_run_id=run_id,
        predicted_winner="prediction-home",
        supporting_statistics=snapshot,
        sample_size=12,
        evidence_strength="medium",
        method_version="descriptive-v1",
    )
    duplicate = create_prediction(
        conn,
        match_id=match_id,
        prediction_run_id=run_id,
        predicted_winner="prediction-home",
        supporting_statistics=snapshot,
        sample_size=12,
        evidence_strength="medium",
        method_version="descriptive-v1",
    )
    assert created["created"] is True
    assert duplicate["created"] is False
    assert created["id"] == duplicate["id"]

    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE matches SET status = 'completed', home_score = 90, away_score = 80 "
            "WHERE id = %s",
            (match_id,),
        )
    first = reconcile_predictions(conn, match_id=match_id)
    second = reconcile_predictions(conn, match_id=match_id)
    assert first["correct"] == 1
    assert second["unchanged"] == 1

    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE matches SET home_score = 70, away_score = 80 WHERE id = %s",
            (match_id,),
        )
    corrected = reconcile_predictions(conn, match_id=match_id)
    assert corrected["incorrect"] == 1

    history = list_predictions(conn)
    assert len(history) == 1
    assert history[0]["status"] == "incorrect"
    assert history[0]["supporting_statistics"] == snapshot
    metrics = prediction_performance(conn)
    assert metrics["total_predictions"] == 1
    assert metrics["evaluated_predictions"] == 1
    assert metrics["evaluation_coverage"] == 1.0
    assert metrics["eligible_fixtures"] == 1
    assert metrics["fixtures_with_predictions"] == 1
    assert metrics["prediction_coverage"] == 1.0
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) AS count FROM prediction_evaluation_events WHERE prediction_id = %s",
            (created["id"],),
        )
        assert cursor.fetchone()["count"] == 2
        cursor.execute(
            (MIGRATION_DIR / "004_prediction_tracking.sql").read_text(encoding="utf-8")
        )
        cursor.execute(
            "SELECT COUNT(*) AS count FROM prediction_evaluation_events WHERE prediction_id = %s",
            (created["id"],),
        )
        assert cursor.fetchone()["count"] == 2
