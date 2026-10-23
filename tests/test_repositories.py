
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from ebasketball.database import connect
from ebasketball.repositories.matches import upsert_match
from ebasketball.repositories.odds import record_fixture_odds
from ebasketball.repositories.players import get_or_create_player


@pytest.fixture
def conn():
    connection = connect()
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


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
