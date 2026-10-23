"""Descriptive statistics for completed eBasketball matches."""

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
        & (matches["home_score"] != matches["away_score"])
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
        & (matches["home_score"] != matches["away_score"])
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
