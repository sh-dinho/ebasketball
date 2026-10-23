"""Tests for the legacy DataFrame descriptive-statistics helpers."""

import pandas as pd

from ebasketball.analytics import head_to_head, player_record


def test_player_record_counts_decisive_completed_matches_only():
    matches = pd.DataFrame(
        [
            {
                "status": "completed",
                "home_handle": "A",
                "away_handle": "B",
                "home_score": 90,
                "away_score": 80,
            },
            {
                "status": "completed",
                "home_handle": "B",
                "away_handle": "A",
                "home_score": 80,
                "away_score": 90,
            },
            {
                "status": "completed",
                "home_handle": "A",
                "away_handle": "B",
                "home_score": 80,
                "away_score": 80,
            },
            {
                "status": "scheduled",
                "home_handle": "A",
                "away_handle": "B",
                "home_score": None,
                "away_score": None,
            },
        ]
    )
    assert player_record(matches, "A") == {
        "played": 2,
        "wins": 2,
        "losses": 0,
        "win_rate": 1.0,
    }


def test_head_to_head_excludes_tied_and_incomplete_matches():
    matches = pd.DataFrame(
        [
            {
                "status": "completed",
                "home_handle": "A",
                "away_handle": "B",
                "home_score": 90,
                "away_score": 80,
            },
            {
                "status": "completed",
                "home_handle": "B",
                "away_handle": "A",
                "home_score": 70,
                "away_score": 70,
            },
            {
                "status": "completed",
                "home_handle": "A",
                "away_handle": "B",
                "home_score": None,
                "away_score": 70,
            },
        ]
    )
    assert head_to_head(matches, "A", "B") == {
        "played": 1,
        "player_a_wins": 1,
        "player_b_wins": 0,
    }
