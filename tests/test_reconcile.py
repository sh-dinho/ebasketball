"""Reconciliation tests."""

from datetime import datetime, timedelta, timezone

import pytest

from ebasketball.ingestion.reconcile import (
    fixtures_match,
    reconcile_fixtures,
)

BASE_TIME = datetime(
    2026,
    1,
    1,
    20,
    0,
    tzinfo=timezone.utc,
)


def make_fixture(
    minutes: int = 0,
    *,
    home: str = "Player A",
    away: str = "Player B",
) -> dict:
    return {
        "match_time": BASE_TIME + timedelta(minutes=minutes),
        "home_handle": home,
        "away_handle": away,
    }


@pytest.fixture
def fixture_a():
    return {
        "match_time": datetime(
            2026,
            10,
            22,
            18,
            0,
            tzinfo=timezone.utc,
        ),
        "home_handle": "PlayerA",
        "away_handle": "PlayerB",
    }


def test_matching_fixtures_allow_small_time_difference(
    fixture_a,
):
    second = {
        **fixture_a,
        "match_time": (fixture_a["match_time"] + timedelta(seconds=120)),
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


def test_reversed_players_match_as_same_fixture(fixture_a):
    second = {
        **fixture_a,
        "home_handle": "PlayerB",
        "away_handle": "PlayerA",
    }

    assert fixtures_match(fixture_a, second)


def test_large_time_difference_does_not_match(
    fixture_a,
):
    second = {
        **fixture_a,
        "match_time": (fixture_a["match_time"] + timedelta(minutes=10)),
    }

    assert not fixtures_match(fixture_a, second)


def test_naive_datetime_is_rejected(fixture_a):
    second = {
        **fixture_a,
        "match_time": datetime(
            2026,
            10,
            22,
            18,
            0,
        ),
    }

    with pytest.raises(
        ValueError,
        match="timezone-aware",
    ):
        fixtures_match(fixture_a, second)


def test_negative_tolerance_is_rejected(
    fixture_a,
):
    with pytest.raises(
        ValueError,
        match="cannot be negative",
    ):
        fixtures_match(
            fixture_a,
            fixture_a,
            tolerance_seconds=-1,
        )


def test_reconcile_groups_two_sources(
    fixture_a,
):
    second = {
        **fixture_a,
        "match_time": (fixture_a["match_time"] + timedelta(seconds=30)),
    }

    result = reconcile_fixtures(
        {
            "h2hgg": [fixture_a],
            "aceodds": [second],
        }
    )

    assert len(result["groups"]) == 1
    assert result["groups"][0]["sources"] == [
        "aceodds",
        "h2hgg",
    ]
    assert len(result["groups"][0]["records"]) == 2

    # R1 regression:
    # matched fixtures must not remain unmatched.
    assert result["unmatched"] == []


def test_reconcile_keeps_distinct_fixtures_separate(
    fixture_a,
):
    second = {
        **fixture_a,
        "away_handle": "PlayerC",
    }

    result = reconcile_fixtures(
        {
            "h2hgg": [fixture_a],
            "aceodds": [second],
        }
    )

    assert len(result["groups"]) == 2
    assert len(result["unmatched"]) == 2


def test_previously_matched_fixture_is_not_unmatched():
    fixture_a = make_fixture()
    fixture_b = make_fixture(minutes=2)

    assert (
        fixtures_match(
            fixture_a,
            fixture_b,
        )
        is True
    )

    result = reconcile_fixtures(
        {
            "h2hgg": [fixture_a],
            "aceodds": [fixture_b],
        }
    )

    assert len(result["groups"]) == 1
    assert len(result["groups"][0]["records"]) == 2
    assert result["unmatched"] == []


def test_truly_unmatched_fixtures():
    fixture_a = make_fixture()

    fixture_b = make_fixture(
        home="Player C",
        away="Player D",
    )

    result = reconcile_fixtures(
        {
            "h2hgg": [
                fixture_a,
                fixture_b,
            ],
        }
    )

    assert len(result["groups"]) == 2
    assert len(result["unmatched"]) == 2

    assert len(result["groups"][0]["records"]) == 1
    assert len(result["groups"][1]["records"]) == 1


def test_transitive_matching_uses_any_record_in_group():
    fixture_a = make_fixture(minutes=0)
    fixture_b = make_fixture(minutes=4)
    fixture_c = make_fixture(minutes=8)

    assert (
        fixtures_match(
            fixture_a,
            fixture_b,
        )
        is True
    )

    assert (
        fixtures_match(
            fixture_b,
            fixture_c,
        )
        is True
    )

    assert (
        fixtures_match(
            fixture_a,
            fixture_c,
        )
        is False
    )

    result = reconcile_fixtures(
        {
            "h2hgg": [fixture_a],
            "aceodds": [fixture_b],
            "third_source": [fixture_c],
        },
        tolerance_seconds=300,
    )

    assert len(result["groups"]) == 1

    assert len(result["groups"][0]["records"]) == 3

    assert result["groups"][0]["sources"] == [
        "aceodds",
        "h2hgg",
        "third_source",
    ]

    assert result["unmatched"] == []


def test_boundary_tolerance_is_inclusive():
    fixture_a = make_fixture(minutes=0)
    fixture_b = make_fixture(minutes=5)

    assert (
        fixtures_match(
            fixture_a,
            fixture_b,
            tolerance_seconds=300,
        )
        is True
    )


def test_over_tolerance_fixtures_do_not_match():
    fixture_a = make_fixture(minutes=0)

    fixture_b = {
        **make_fixture(minutes=5),
        "match_time": (
            BASE_TIME
            + timedelta(
                minutes=5,
                seconds=1,
            )
        ),
    }

    assert (
        fixtures_match(
            fixture_a,
            fixture_b,
            tolerance_seconds=300,
        )
        is False
    )


def test_ambiguous_candidate_does_not_modify_groups():
    group_a = make_fixture(minutes=0)
    group_b = make_fixture(minutes=4)

    candidate = make_fixture(minutes=2)

    result = reconcile_fixtures(
        {
            "h2hgg": [
                group_a,
                group_b,
            ],
            "aceodds": [candidate],
        },
        tolerance_seconds=300,
    )

    assert len(result["groups"]) == 2

    assert len(result["ambiguous"]) == 1
    assert result["ambiguous"][0]["fixture"] is candidate

    assert len(result["groups"][0]["records"]) == 1

    assert len(result["groups"][1]["records"]) == 1

    assert all(
        candidate not in (record["fixture"] for record in group["records"])
        for group in result["groups"]
    )
