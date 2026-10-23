"""Transparent, historical match-winner analysis.

The baseline uses descriptive records only. It intentionally emits no win
probabilities: a calibrated, chronological out-of-sample probability model has
not been established for the currently available data.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time, timezone
from math import sqrt
from statistics import mean, pstdev
from typing import Any

from psycopg2.extensions import connection as Connection

METHOD_VERSION = "descriptive-v1"


def _utc(value: Any) -> datetime | None:
    """Parse a datetime or ISO-8601 value into UTC, rejecting naive values."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min, tzinfo=timezone.utc)
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def load_historical_matches(
    conn: Connection,
    *,
    before: datetime,
) -> list[dict[str, Any]]:
    """Load completed score records before ``before`` using a read query."""
    cutoff = _utc(before)
    if cutoff is None:
        raise ValueError("before must be a timezone-aware datetime")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT m.match_time, hp.handle AS home_handle,
                   ap.handle AS away_handle, m.home_score, m.away_score,
                   m.status
            FROM matches AS m
            JOIN players AS hp ON hp.id = m.home_player_id
            JOIN players AS ap ON ap.id = m.away_player_id
            WHERE m.status = %s AND m.match_time < %s
              AND m.is_synthetic = FALSE
              AND m.data_source <> 'synthetic'
              AND m.home_score IS NOT NULL
              AND m.away_score IS NOT NULL
            ORDER BY m.match_time, m.id
            """,
            ("completed", cutoff),
        )
        return [dict(row) for row in cursor.fetchall()]


def validate_history(
    fixtures: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Validate completed fixture rows and deduplicate canonical fixtures.

    Exact repeats are counted once. Conflicting records for the same timestamp
    and participants are excluded as ambiguous instead of picking a score.
    """
    candidates: dict[tuple[datetime, str, str], dict[str, Any]] = {}
    canonical_scores: dict[tuple[datetime, str, str], tuple[int, int]] = {}
    conflicts: set[tuple[datetime, str, str]] = set()
    warnings: list[str] = []
    for index, item in enumerate(fixtures):
        if not isinstance(item, Mapping):
            warnings.append(f"Record {index}: not an object; ignored.")
            continue
        status = item.get("status")
        if item.get("is_synthetic") or item.get("data_source") == "synthetic":
            warnings.append(f"Record {index}: synthetic fixture excluded.")
            continue
        if status is None:
            warnings.append(f"Record {index}: missing match status; ignored.")
            continue
        if status != "completed":
            continue
        played_at = _utc(item.get("match_time"))
        home = item.get("home_handle")
        away = item.get("away_handle")
        home_score = item.get("home_score")
        away_score = item.get("away_score")
        if played_at is None or not isinstance(home, str) or not isinstance(away, str):
            warnings.append(f"Record {index}: missing or invalid time/team; ignored.")
            continue
        home, away = home.strip(), away.strip()
        if not home or not away or home.casefold() == away.casefold():
            warnings.append(f"Record {index}: invalid participants; ignored.")
            continue
        scores = (home_score, away_score)
        if any(
            isinstance(score, bool) or not isinstance(score, int) or score < 0
            for score in scores
        ):
            warnings.append(
                f"Record {index}: invalid or missing integer scores; ignored."
            )
            continue
        if home_score == away_score:
            warnings.append(
                f"Record {index}: tied score is not a valid winner result; ignored."
            )
            continue
        fixture = {
            "match_time": played_at,
            "home_handle": home,
            "away_handle": away,
            "home_score": home_score,
            "away_score": away_score,
            "status": "completed",
        }
        home_key, away_key = home.casefold(), away.casefold()
        key = (played_at, *sorted((home_key, away_key)))
        score_signature = (
            (home_score, away_score)
            if home_key < away_key
            else (away_score, home_score)
        )
        previous = candidates.get(key)
        if previous is not None:
            if canonical_scores[key] != score_signature:
                conflicts.add(key)
            continue
        candidates[key] = fixture
        canonical_scores[key] = score_signature
    for key in conflicts:
        candidates.pop(key, None)
        warnings.append(
            "Conflicting duplicate fixture at "
            f"{key[0].isoformat()} for {key[1]} vs {key[2]}; excluded."
        )
    return sorted(candidates.values(), key=lambda row: row["match_time"]), warnings


def _team_rows(
    history: list[dict[str, Any]],
    team: str,
    before: datetime,
) -> list[dict[str, Any]]:
    folded = team.casefold()
    return [
        row
        for row in history
        if row["match_time"] < before
        and folded in (row["home_handle"].casefold(), row["away_handle"].casefold())
    ]


def _team_stats(
    history: list[dict[str, Any]],
    team: str,
    before: datetime,
) -> dict[str, Any]:
    rows = _team_rows(history, team, before)
    rows.sort(key=lambda row: row["match_time"])
    folded = team.casefold()
    outcomes: list[dict[str, Any]] = []
    differentials: list[int] = []
    scored: list[int] = []
    conceded: list[int] = []
    for row in rows:
        is_home = row["home_handle"].casefold() == folded
        points_for = row["home_score"] if is_home else row["away_score"]
        points_against = row["away_score"] if is_home else row["home_score"]
        scored.append(points_for)
        conceded.append(points_against)
        differentials.append(points_for - points_against)
        outcomes.append(
            {
                "win": points_for > points_against,
                "opponent": row["away_handle"] if is_home else row["home_handle"],
            }
        )
    wins = sum(outcome["win"] for outcome in outcomes)

    # Build opponent records in one pass over pre-target history. This avoids
    # rescanning every historical match for each opponent of each team.
    opponent_wins: dict[str, int] = defaultdict(int)
    opponent_games: dict[str, int] = defaultdict(int)
    for row in history:
        if row["match_time"] >= before:
            continue
        home_key = row["home_handle"].casefold()
        away_key = row["away_handle"].casefold()
        opponent_games[home_key] += 1
        opponent_games[away_key] += 1
        if row["home_score"] > row["away_score"]:
            opponent_wins[home_key] += 1
        else:
            opponent_wins[away_key] += 1
    opponent_rates = [
        opponent_wins[outcome["opponent"].casefold()]
        / opponent_games[outcome["opponent"].casefold()]
        for outcome in outcomes
        if opponent_games[outcome["opponent"].casefold()]
    ]

    recent: dict[str, dict[str, int | float]] = {}
    for count in (5, 10, 20):
        subset = outcomes[-count:]
        recent_wins = sum(outcome["win"] for outcome in subset)
        recent[count] = {
            "played": len(subset),
            "wins": recent_wins,
            "losses": len(subset) - recent_wins,
            "win_rate": recent_wins / len(subset) if subset else 0.0,
        }
    return {
        "played": len(rows),
        "wins": wins,
        "losses": len(rows) - wins,
        "win_rate": wins / len(rows) if rows else None,
        "recent": recent,
        "points_scored_avg": mean(scored) if scored else None,
        "points_conceded_avg": mean(conceded) if conceded else None,
        "scoring_differential_avg": mean(differentials) if differentials else None,
        "scoring_differential_stddev": (
            pstdev(differentials)
            if len(differentials) > 1
            else 0.0
            if differentials
            else None
        ),
        "outcome_variation": (
            sqrt((wins / len(rows)) * (1 - wins / len(rows))) if rows else None
        ),
        "opponent_adjusted_win_rate": (
            mean(opponent_rates) if opponent_rates else None
        ),
        "last_match_time": rows[-1]["match_time"] if rows else None,
    }


def calculate_features(
    fixtures: Iterable[Mapping[str, Any]],
    team_a: str,
    team_b: str,
    *,
    before: datetime,
) -> dict[str, Any]:
    """Calculate comparable historical features using strictly prior results."""
    cutoff = _utc(before)
    if cutoff is None:
        raise ValueError("before must be a timezone-aware datetime")
    if (
        not team_a.strip()
        or not team_b.strip()
        or team_a.casefold() == team_b.casefold()
    ):
        raise ValueError("team_a and team_b must be distinct non-empty names")
    history, warnings = validate_history(fixtures)
    prior = [row for row in history if row["match_time"] < cutoff]
    h2h_rows = [
        row
        for row in prior
        if {row["home_handle"].casefold(), row["away_handle"].casefold()}
        == {team_a.casefold(), team_b.casefold()}
    ]
    a_h2h_wins = sum(
        (
            row["home_handle"].casefold() == team_a.casefold()
            and row["home_score"] > row["away_score"]
        )
        or (
            row["away_handle"].casefold() == team_a.casefold()
            and row["away_score"] > row["home_score"]
        )
        for row in h2h_rows
    )
    return {
        "team_a": _team_stats(prior, team_a, cutoff),
        "team_b": _team_stats(prior, team_b, cutoff),
        "head_to_head": {
            "played": len(h2h_rows),
            "team_a_wins": a_h2h_wins,
            "team_b_wins": len(h2h_rows) - a_h2h_wins,
        },
        "matches_analyzed": len(prior),
        "date_range": {
            "start": prior[0]["match_time"].date().isoformat() if prior else None,
            "end": prior[-1]["match_time"].date().isoformat() if prior else None,
        },
        "warnings": warnings,
    }


def predict_winner(
    fixtures: Iterable[Mapping[str, Any]],
    team_a: str,
    team_b: str,
    *,
    match_time: datetime,
    minimum_matches: int = 5,
) -> dict[str, Any]:
    """Predict a likely winner from pre-match descriptive performance.

    A comparison score orders recent form (5, 10, 20), overall win rate,
    head-to-head record, opponent-adjusted win rate, then average score
    differential. It is a ranking rule, not a probability model. Ties in all
    available measures are inconclusive.
    """
    if (
        isinstance(minimum_matches, bool)
        or not isinstance(minimum_matches, int)
        or minimum_matches < 1
    ):
        raise ValueError("minimum_matches must be a positive integer")
    cutoff = _utc(match_time)
    if cutoff is None:
        raise ValueError("match_time must be timezone-aware")
    source_rows = list(fixtures)
    features = calculate_features(source_rows, team_a, team_b, before=cutoff)
    stats_a, stats_b = features["team_a"], features["team_b"]
    freshness_days = {
        team: (
            max(
                0.0,
                (cutoff - stats["last_match_time"]).total_seconds() / 86400,
            )
            if stats["last_match_time"] is not None
            else None
        )
        for team, stats in ((team_a, stats_a), (team_b, stats_b))
    }
    warnings = list(features["warnings"])
    if stats_a["played"] < minimum_matches or stats_b["played"] < minimum_matches:
        warnings.append(
            f"At least {minimum_matches} prior matches per team are required."
        )
        status, winner, confidence = "insufficient_data", None, "low"
        explanation = "There is not enough prior match history for both teams."
    else:
        h2h = features["head_to_head"]
        score_a = (
            *(stats_a["recent"][n]["win_rate"] for n in (5, 10, 20)),
            stats_a["win_rate"],
            h2h["team_a_wins"] / h2h["played"] if h2h["played"] else 0.5,
            (
                stats_a["opponent_adjusted_win_rate"]
                if stats_a["opponent_adjusted_win_rate"] is not None
                else 0.5
            ),
            stats_a["scoring_differential_avg"],
        )
        score_b = (
            *(stats_b["recent"][n]["win_rate"] for n in (5, 10, 20)),
            stats_b["win_rate"],
            h2h["team_b_wins"] / h2h["played"] if h2h["played"] else 0.5,
            (
                stats_b["opponent_adjusted_win_rate"]
                if stats_b["opponent_adjusted_win_rate"] is not None
                else 0.5
            ),
            stats_b["scoring_differential_avg"],
        )
        comparison_labels = (
            "last-5 win rate",
            "last-10 win rate",
            "last-20 win rate",
            "overall win rate",
            "head-to-head win rate",
            "opponent-adjusted win rate",
            "average scoring differential",
        )
        if score_a == score_b:
            status, winner, confidence = "insufficient_data", None, "low"
            explanation = (
                "The available historical indicators do not distinguish the teams."
            )
            warnings.append(
                "No team has a stronger value under the transparent comparison rule."
            )
        else:
            winner = team_a if score_a > score_b else team_b
            favored, other = (
                (stats_a, stats_b) if winner == team_a else (stats_b, stats_a)
            )
            favored_values, other_values = (
                (score_a, score_b) if winner == team_a else (score_b, score_a)
            )
            deciding_index = next(
                index
                for index, (left, right) in enumerate(zip(score_a, score_b))
                if left != right
            )
            if deciding_index < 6:
                reason_values = (
                    f"{favored_values[deciding_index]:.0%} versus "
                    f"{other_values[deciding_index]:.0%}"
                )
            else:
                reason_values = (
                    f"{favored_values[deciding_index]:.1f} versus "
                    f"{other_values[deciding_index]:.1f} points per match"
                )
            days_old = max(
                freshness
                for freshness in freshness_days.values()
                if freshness is not None
            )
            volume = min(favored["played"], other["played"])
            variability = (favored["scoring_differential_stddev"] or 0.0) + (
                other["scoring_differential_stddev"] or 0.0
            )
            confidence = (
                "high"
                if volume >= 20 and days_old <= 14 and variability < 30
                else "medium"
                if volume >= 10 and days_old <= 45
                else "low"
            )
            status = "predicted"
            explanation = (
                f"{winner} is favored because its {comparison_labels[deciding_index]} "
                f"was {reason_values}. This is a descriptive ranking, not a "
                "calibrated estimate of match chances."
            )
    date_range = features["date_range"]
    return {
        "method_version": METHOD_VERSION,
        "status": status,
        "team_a": team_a,
        "team_b": team_b,
        "predicted_winner": winner,
        "win_probabilities": None,
        "statistics": {
            "team_a": stats_a,
            "team_b": stats_b,
            "head_to_head": features["head_to_head"],
        },
        "matches_analyzed": features["matches_analyzed"],
        "date_range": date_range,
        "freshness_days": freshness_days,
        "confidence": confidence,
        "warnings": warnings,
        "explanation": explanation,
    }


def backtest(
    fixtures: Iterable[Mapping[str, Any]],
    *,
    minimum_matches: int = 5,
) -> dict[str, Any]:
    """Evaluate predictions chronologically, adding each result only afterward."""
    if (
        isinstance(minimum_matches, bool)
        or not isinstance(minimum_matches, int)
        or minimum_matches < 1
    ):
        raise ValueError("minimum_matches must be a positive integer")
    history, validation_warnings = validate_history(fixtures)
    outcomes: list[dict[str, Any]] = []
    for index, fixture in enumerate(history):
        prediction = predict_winner(
            history[:index],
            fixture["home_handle"],
            fixture["away_handle"],
            match_time=fixture["match_time"],
            minimum_matches=minimum_matches,
        )
        actual = (
            fixture["home_handle"]
            if fixture["home_score"] > fixture["away_score"]
            else fixture["away_handle"]
        )
        outcomes.append(
            {
                "match_time": fixture["match_time"],
                "status": prediction["status"],
                "predicted_winner": prediction["predicted_winner"],
                "actual_winner": actual,
                "home_side_won": fixture["home_score"] > fixture["away_score"],
                "correct": (
                    prediction["predicted_winner"] == actual
                    if prediction["predicted_winner"] is not None
                    else None
                ),
                "matches_analyzed": prediction["matches_analyzed"],
            }
        )
    scored = [row for row in outcomes if row["predicted_winner"] is not None]
    accuracy = sum(row["correct"] for row in scored) / len(scored) if scored else None
    # A transparent always-home baseline is evaluated on the same matches.
    home_wins = sum(row["home_side_won"] for row in scored)
    sample_bins: dict[str, list[bool | None]] = defaultdict(list)
    for row in outcomes:
        label = (
            "0-4"
            if row["matches_analyzed"] < 5
            else "5-9"
            if row["matches_analyzed"] < 10
            else "10+"
        )
        sample_bins[label].append(row["correct"])
    period_bins: dict[str, list[bool | None]] = defaultdict(list)
    for row in outcomes:
        period_bins[str(row["match_time"].year)].append(row["correct"])

    def grouped_metrics(values: list[bool | None]) -> dict[str, Any]:
        evaluated = [value for value in values if value is not None]
        return {
            "matches": len(values),
            "predictions": len(evaluated),
            "accuracy": (sum(evaluated) / len(evaluated) if evaluated else None),
        }

    return {
        "method_version": METHOD_VERSION,
        "matches_evaluated": len(outcomes),
        "predictions_scored": len(scored),
        "coverage": len(scored) / len(outcomes) if outcomes else 0.0,
        "winner_accuracy": accuracy,
        "home_side_baseline_accuracy": home_wins / len(scored) if scored else None,
        "home_side_baseline_matches": len(scored),
        "log_loss": None,
        "brier_score": None,
        "by_sample_size": {
            label: grouped_metrics(values) for label, values in sample_bins.items()
        },
        "by_period": {
            label: grouped_metrics(values)
            for label, values in sorted(period_bins.items())
        },
        "warnings": validation_warnings
        + (
            [
                "Probabilistic metrics are unavailable because no calibrated probability model is used."
            ]
            if history
            else []
        ),
        "predictions": outcomes,
    }
