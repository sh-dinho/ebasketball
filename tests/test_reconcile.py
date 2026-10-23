"""Reconciliation tests."""
from datetime import datetime, timedelta, timezone

import pytest

from ebasketball.ingestion.reconcile import (
    fixtures_match,
    reconcile_fixtures,
)


@pytest.fixture
def fixture_a():
    return {
        "match_time": datetime(
            2026, 10, 22, 18, 0, tzinfo=timezone.utc
        ),
        "home_handle": "PlayerA",
        "away_handle": "PlayerB",
    }


def test_matching_fixtures_allow_small_time_difference(fixture_a):
    second = {
        **fixture_a,
        "match_time": fixture_a["match_time"] + timedelta(seconds=120),
        "home_handle": " playera ",
        "away_handle": "PLAYERB",
    }

    assert fixtures_match(fixture_a, second)


def test_different_players_do_not_match(fixture_a):
    second = {
        **fixture_a,
        "away_handle": "PlayerC",
    }

    assert not fixtures_match(fixture_a, second)


def test_reversed_players_do_not_match(fixture_a):
    second = {
        **fixture_a,
        "home_handle": "PlayerB",
        "away_handle": "PlayerA",
    }

    assert not fixtures_match(fixture_a, second)


def test_large_time_difference_does_not_match(fixture_a):
    second = {
        **fixture_a,
        "match_time": fixture_a["match_time"] + timedelta(minutes=10),
    }

    assert not fixtures_match(fixture_a, second)


def test_naive_datetime_is_rejected(fixture_a):
    second = {
        **fixture_a,
        "match_time": datetime(2026, 10, 22, 18, 0),
    }

    with pytest.raises(ValueError, match="timezone-aware"):
        fixtures_match(fixture_a, second)


def test_reconcile_groups_two_sources(fixture_a):
    second = {
        **fixture_a,
        "match_time": fixture_a["match_time"] + timedelta(seconds=30),
    }

    result = reconcile_fixtures({
        "h2hgg": [fixture_a],
        "aceodds": [second],
    })

    assert len(result["groups"]) == 1
    assert result["groups"][0]["sources"] == ["aceodds", "h2hgg"]
    assert len(result["groups"][0]["records"]) == 2


def test_reconcile_keeps_distinct_fixtures_separate(fixture_a):
    second = {
        **fixture_a,
        "away_handle": "PlayerC",
    }

    result = reconcile_fixtures({
        "h2hgg": [fixture_a],
        "aceodds": [second],
    })

    assert len(result["groups"]) == 2


def test_negative_tolerance_is_rejected(fixture_a):
    with pytest.raises(ValueError, match="cannot be negative"):
        fixtures_match(fixture_a, fixture_a, tolerance_seconds=-1)
