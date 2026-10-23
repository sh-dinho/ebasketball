from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from ebasketball.repositories.matches import upsert_match, validate_fixture
from ebasketball.repositories.players import get_or_create_player, normalize_handle


@pytest.fixture
def conn(migrated_connection):
    return migrated_connection


def test_player_resolution_is_idempotent(conn):
    handle = f"TEST_{uuid4().hex[:12]}"

    first = get_or_create_player(conn, handle)
    second = get_or_create_player(conn, handle)

    assert first == second


def test_player_handle_validation(conn):
    with pytest.raises(ValueError):
        get_or_create_player(conn, "   ")

    with pytest.raises(ValueError):
        get_or_create_player(conn, "X" * 51)


def test_player_handle_rejects_non_string_type():
    with pytest.raises(TypeError, match="must be a string"):
        normalize_handle(None)


def test_fixture_rejects_non_datetime_type():
    fixture = {
        "match_time": "2026-10-22T12:00:00Z",
        "home_handle": "PLAYER_A",
        "away_handle": "PLAYER_B",
    }
    with pytest.raises(TypeError, match="must be a datetime"):
        validate_fixture(fixture, "h2hgg")


def test_source_update_timestamp_is_preserved_with_provenance(conn):
    suffix = uuid4().hex[:10]
    updated_at = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    fixture = {
        "match_time": datetime(2026, 11, 1, tzinfo=timezone.utc),
        "home_handle": f"SOURCE_HOME_{suffix}",
        "away_handle": f"SOURCE_AWAY_{suffix}",
        "status": "scheduled",
        "source_updated_at": updated_at,
    }
    upsert_match(conn, fixture, "h2hgg", source_event_id=f"source-{suffix}")
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT source_updated_at FROM match_sources WHERE source_event_id = %s",
            (f"source-{suffix}",),
        )
        assert cursor.fetchone()["source_updated_at"] == updated_at


def test_newer_source_result_correction_updates_scores_but_stale_import_does_not(conn):
    suffix = uuid4().hex[:10]
    event_id = f"correction-{suffix}"
    fixture = {
        "match_time": datetime(2026, 10, 20, tzinfo=timezone.utc),
        "home_handle": f"CORRECTION_HOME_{suffix}",
        "away_handle": f"CORRECTION_AWAY_{suffix}",
        "status": "completed",
        "home_score": 90,
        "away_score": 80,
        "source_updated_at": datetime(2026, 10, 20, 1, tzinfo=timezone.utc),
    }
    match_id = upsert_match(conn, fixture, "h2hgg", source_event_id=event_id)
    corrected = {
        **fixture,
        "home_score": 70,
        "away_score": 80,
        "source_updated_at": datetime(2026, 10, 20, 2, tzinfo=timezone.utc),
    }
    upsert_match(conn, corrected, "h2hgg", source_event_id=event_id)
    stale = {
        **fixture,
        "home_score": 99,
        "away_score": 1,
        "source_updated_at": datetime(2026, 10, 20, 1, 30, tzinfo=timezone.utc),
    }
    upsert_match(conn, stale, "h2hgg", source_event_id=event_id)
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT home_score, away_score FROM matches WHERE id = %s",
            (match_id,),
        )
        assert cursor.fetchone() == {"home_score": 70, "away_score": 80}


def test_unkeyed_source_provenance_is_idempotent(conn):
    suffix = uuid4().hex[:10]
    handle_home = f"HOME_{suffix}"
    handle_away = f"AWAY_{suffix}"
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": handle_home,
        "away_handle": handle_away,
        "status": "scheduled",
    }

    match_id = upsert_match(conn, fixture, "h2hgg")
    upsert_match(conn, fixture, "h2hgg")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS provenance_count
            FROM match_sources
            WHERE match_id = %s
              AND source = %s
              AND source_event_id IS NULL
              AND source_match_key IS NULL
            """,
            (match_id, "h2hgg"),
        )
        assert cursor.fetchone()["provenance_count"] == 1


def test_fixture_rejects_naive_datetime(conn):
    fixture = {
        "match_time": datetime(2026, 10, 22, 12, 0),
        "home_handle": "PLAYER_A",
        "away_handle": "PLAYER_B",
    }

    with pytest.raises(ValueError, match="timezone-aware"):
        upsert_match(conn, fixture, "h2hgg")


def test_fixture_rejects_identical_players(conn):
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": "SAME_PLAYER",
        "away_handle": "same_player",
    }

    with pytest.raises(ValueError, match="must differ"):
        upsert_match(conn, fixture, "h2hgg")


def test_normalized_completed_fixture_is_saved_with_integer_scores():
    from ebasketball.ingestion.normalize import normalize_fixture

    raw = {
        "match_time": "2026-10-22T18:30:00Z",
        "home_handle": "HOME",
        "away_handle": "AWAY",
        "status": "Finished",
        "home_score": "105",
        "away_score": 98,
    }
    fixture = normalize_fixture(raw, "h2hgg")
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, {"id": 123}]
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    conn = MagicMock()
    conn.cursor.return_value = cursor_context

    with patch(
        "ebasketball.repositories.matches.get_or_create_player",
        side_effect=[1, 2],
    ):
        match_id = upsert_match(
            conn,
            fixture,
            "h2hgg",
            source_event_id="event-123",
        )

    assert match_id == 123
    insert_call = next(
        call
        for call in cursor.execute.call_args_list
        if "INSERT INTO matches" in call.args[0]
    )
    persisted_values = insert_call.args[1]
    assert persisted_values["home_score"] == 105
    assert persisted_values["away_score"] == 98


@pytest.mark.parametrize("bad_score", [-1, 1.5, Decimal("1.5"), True])
def test_repository_validation_rejects_invalid_scores(bad_score):
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": "HOME",
        "away_handle": "AWAY",
        "home_score": bad_score,
    }

    with pytest.raises(ValueError, match="nonnegative integer"):
        validate_fixture(fixture, "h2hgg")


def test_repository_validation_allows_missing_scores_for_scheduled_match():
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": "HOME",
        "away_handle": "AWAY",
        "status": "scheduled",
        "home_score": None,
        "away_score": None,
    }

    validate_fixture(fixture, "h2hgg")


def test_repository_validation_rejects_missing_scores_for_completed_match():
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": "HOME",
        "away_handle": "AWAY",
        "status": "completed",
        "home_score": None,
        "away_score": None,
    }

    with pytest.raises(ValueError, match="both scores"):
        validate_fixture(fixture, "h2hgg")
