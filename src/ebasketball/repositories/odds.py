"""Odds history persistence."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

from psycopg2.extensions import connection as Connection

from ebasketball.repositories.matches import VALID_SOURCES

VALID_MARKETS = {
    "moneyline": {"home", "away"},
    "spread": {"home", "away"},
    "total": {"over", "under"},
}


def _decimal(value, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc

    if not number.is_finite():
        raise ValueError(f"{field} must be finite.")

    return number


def _normalize_observation(item: dict) -> dict:
    market = item.get("market_type")
    outcome = item.get("outcome")

    if market not in VALID_MARKETS:
        raise ValueError(f"Unsupported market: {market}")

    if outcome not in VALID_MARKETS[market]:
        raise ValueError(
            f"Invalid outcome {outcome!r} for {market!r}."
        )

    odds = _decimal(item.get("odds"), "odds")

    if not Decimal("1.000") < odds < Decimal("10000"):
        raise ValueError("Decimal odds must be between 1 and 10000.")

    odds = odds.quantize(Decimal("0.001"))

    line = item.get("line")
    if line is not None:
        line = _decimal(line, "line").quantize(Decimal("0.01"))

    return {
        "market_type": market,
        "outcome": outcome,
        "line": line,
        "odds": odds,
    }


def record_odds(
    conn: Connection,
    match_id: int,
    observations: Iterable[dict],
    source: str = "h2hgg",
) -> int:
    """
    Append changed market observations.

    Comparison is source-specific: H2HGG and AceOdds may quote
    different prices for the same fixture.

    Returns the number of inserted observations.
    The caller owns the transaction.
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    inserted = 0

    for raw_item in observations:
        item = _normalize_observation(raw_item)

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT odds, line
                FROM odds_history
                WHERE match_id = %s
                  AND market_type = %s
                  AND outcome = %s
                  AND source = %s
                ORDER BY observed_at DESC, id DESC
                LIMIT 1
                """,
                (
                    match_id,
                    item["market_type"],
                    item["outcome"],
                    source,
                ),
            )

            previous = cursor.fetchone()

            if (
                previous is not None
                and previous["odds"] == item["odds"]
                and previous["line"] == item["line"]
            ):
                continue

            cursor.execute(
                """
                INSERT INTO odds_history (
                    match_id, market_type, outcome, line,
                    odds, source, is_synthetic
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    match_id,
                    item["market_type"],
                    item["outcome"],
                    item["line"],
                    item["odds"],
                    source,
                    source == "synthetic",
                ),
            )

            inserted += 1

    return inserted


def observations_from_fixture(fixture: dict) -> list[dict]:
    """Convert normalized fixture market fields to observations."""
    return [
        {
            "market_type": "moneyline",
            "outcome": "home",
            "line": None,
            "odds": fixture.get("home_moneyline"),
        },
        {
            "market_type": "moneyline",
            "outcome": "away",
            "line": None,
            "odds": fixture.get("away_moneyline"),
        },
        {
            "market_type": "spread",
            "outcome": "home",
            "line": fixture.get("spread_line"),
            "odds": fixture.get("home_spread_odds"),
        },
        {
            "market_type": "spread",
            "outcome": "away",
            "line": fixture.get("away_spread_line"),
            "odds": fixture.get("away_spread_odds"),
        },
        {
            "market_type": "total",
            "outcome": "over",
            "line": fixture.get("total_line"),
            "odds": fixture.get("over_odds"),
        },
        {
            "market_type": "total",
            "outcome": "under",
            "line": fixture.get("total_line"),
            "odds": fixture.get("under_odds"),
        },
    ]


def record_fixture_odds(
    conn: Connection,
    match_id: int,
    fixture: dict,
    source: str = "h2hgg",
) -> int:
    observations = [
        item
        for item in observations_from_fixture(fixture)
        if item["odds"] is not None
    ]

    return record_odds(conn, match_id, observations, source)
