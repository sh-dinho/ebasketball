"""eBasketball analytics."""

from __future__ import annotations

import pandas as pd


def player_record(
    matches: pd.DataFrame,
    player_handle: str,
) -> dict[str, int | float]:
    """Calculate a player's wins, losses, and win rate."""
    completed = matches.loc[
        (matches["status"] == "completed")
        & matches["home_score"].notna()
        & matches["away_score"].notna()
    ]

    home = completed.loc[completed["home_handle"] == player_handle]
    away = completed.loc[completed["away_handle"] == player_handle]

    wins = int(
        (home["home_score"] > home["away_score"]).sum()
        + (away["away_score"] > away["home_score"]).sum()
    )

    played = len(home) + len(away)

    return {
        "played": played,
        "wins": wins,
        "losses": played - wins,
        "win_rate": wins / played if played else 0.0,
    }


def head_to_head(
    matches: pd.DataFrame,
    player_a: str,
    player_b: str,
) -> dict[str, int]:
    """Summarize completed matches between two players."""
    completed = matches.loc[
        (matches["status"] == "completed")
        & matches["home_score"].notna()
        & matches["away_score"].notna()
    ]

    matchups = completed.loc[
        (
            (completed["home_handle"] == player_a)
            & (completed["away_handle"] == player_b)
        )
        | (
            (completed["home_handle"] == player_b)
            & (completed["away_handle"] == player_a)
        )
    ]

    home_won = matchups["home_score"] > matchups["away_score"]
    away_won = matchups["away_score"] > matchups["home_score"]

    a_wins = int(
        (
            ((matchups["home_handle"] == player_a) & home_won)
            | ((matchups["away_handle"] == player_a) & away_won)
        ).sum()
    )

    b_wins = int(
        (
            ((matchups["home_handle"] == player_b) & home_won)
            | ((matchups["away_handle"] == player_b) & away_won)
        ).sum()
    )

    return {
        "played": len(matchups),
        "player_a_wins": a_wins,
        "player_b_wins": b_wins,
    }


def implied_probability(decimal_odds: float) -> float:
    """Convert decimal odds into implied probability."""
    if not 1.0 < decimal_odds < 10000.0:
        raise ValueError(
            "Decimal odds must be greater than 1 and less than 10000."
        )

    return 1.0 / decimal_odds
