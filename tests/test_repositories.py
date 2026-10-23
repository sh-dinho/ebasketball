
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from ebasketball.repositories.matches import upsert_match, validate_fixture
from ebasketball.repositories.odds import record_fixture_odds
from ebasketball.repositories.players import get_or_create_player


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


def test_fixture_and_odds_upsert(conn):
    suffix = uuid4().hex[:10]
    home = f"HOME_{suffix}"
    away = f"AWAY_{suffix}"

    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": home,
        "away_handle": away,
        "home_team": home,
        "away_team": away,
        "status": "scheduled",
        "home_moneyline": 1.91,
        "away_moneyline": 1.95,
        "spread_line": -2.5,
        "away_spread_line": 2.5,
        "home_spread_odds": 1.91,
        "away_spread_odds": 1.91,
        "total_line": 105.5,
        "over_odds": 1.91,
        "under_odds": 1.91,
    }

    first_id = upsert_match(
        conn, fixture, "h2hgg",
        source_event_id=f"event-{suffix}",
        source_url="https://h2hggl.com/en/ebasketball/schedule",
    )
    second_id = upsert_match(
        conn, fixture, "h2hgg",
        source_event_id=f"event-{suffix}",
        source_url="https://h2hggl.com/en/ebasketball/schedule",
    )

    assert first_id == second_id
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS provenance_count
            FROM match_sources
            WHERE source = %s AND source_event_id = %s
            """,
            ("h2hgg", f"event-{suffix}"),
        )
        assert cursor.fetchone()["provenance_count"] == 1

    inserted = record_fixture_odds(conn, first_id, fixture, "h2hgg")
    assert inserted == 6

    unchanged = record_fixture_odds(
        conn, first_id, fixture, "h2hgg"
    )
    assert unchanged == 0

    changed_fixture = {**fixture, "home_moneyline": 2.05}
    changed = record_fixture_odds(
        conn, first_id, changed_fixture, "h2hgg"
    )
    assert changed == 1


def test_match_outcome_and_source_observation_time_are_preserved(conn):
    suffix = uuid4().hex[:10]
    observed_at = datetime(2026, 10, 22, 18, 29, tzinfo=timezone.utc)
    fixture = {
        "match_time": datetime(2026, 10, 22, 18, 30, tzinfo=timezone.utc),
        "home_handle": f"HOME_{suffix}",
        "away_handle": f"AWAY_{suffix}",
        "status": "scheduled",
        "home_moneyline": Decimal("1.95"),
        "odds_observed_at": observed_at,
    }

    inserted = upsert_match(
        conn, fixture, "h2hgg", source_event_id=f"event-{suffix}",
        return_status=True,
    )
    updated = upsert_match(
        conn, fixture, "h2hgg", source_event_id=f"event-{suffix}",
        return_status=True,
    )
    assert inserted == {"id": inserted["id"], "inserted": True}
    assert updated == {"id": inserted["id"], "inserted": False}

    assert record_fixture_odds(conn, inserted["id"], fixture, "h2hgg") == 1
    assert record_fixture_odds(conn, inserted["id"], fixture, "h2hgg") == 0
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT observed_at, source
            FROM odds_history
            WHERE match_id = %s AND market_type = 'moneyline'
              AND outcome = 'home'
            """,
            (inserted["id"],),
        )
        row = cursor.fetchone()
    assert row["observed_at"] == observed_at
    assert row["source"] == "h2hgg"


def test_match_and_odds_persistence_are_source_specific(conn):
    suffix = uuid4().hex[:10]
    fixture = {
        "match_time": datetime.now(timezone.utc),
        "home_handle": f"HOME_{suffix}",
        "away_handle": f"AWAY_{suffix}",
        "status": "scheduled",
        "home_moneyline": 1.91,
        "away_moneyline": 2.05,
    }

    h2h_match_id = upsert_match(
        conn, fixture, "h2hgg", source_event_id=f"h2h-{suffix}"
    )
    ace_match_id = upsert_match(
        conn, fixture, "aceodds", source_event_id=f"ace-{suffix}"
    )
    assert h2h_match_id == ace_match_id

    h2h_inserted = record_fixture_odds(
        conn, h2h_match_id, fixture, "h2hgg"
    )
    ace_fixture = {**fixture, "home_moneyline": 2.1}
    ace_inserted = record_fixture_odds(
        conn, ace_match_id, ace_fixture, "aceodds"
    )
    assert h2h_inserted == 2
    assert ace_inserted == 2
    assert record_fixture_odds(
        conn, h2h_match_id, fixture, "h2hgg"
    ) == 0
    assert record_fixture_odds(
        conn, ace_match_id, ace_fixture, "aceodds"
    ) == 0

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT source, COUNT(*) AS observation_count
            FROM odds_history
            WHERE match_id = %s
            GROUP BY source
            """,
            (h2h_match_id,),
        )
        counts = {
            row["source"]: row["observation_count"]
            for row in cursor.fetchall()
        }
    assert counts == {"h2hgg": 2, "aceodds": 2}


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
    cursor.fetchone.return_value = {"id": 123}
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
    persisted_values = cursor.execute.call_args_list[0].args[1]
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
