"""Match persistence."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any

from psycopg2.extensions import connection as Connection

from ebasketball.repositories.players import get_or_create_player

VALID_SOURCES = {"h2hgg", "aceodds", "manual", "synthetic"}

MATCH_FIELDS = (
    "spread_line",
    "away_spread_line",
    "total_line",
    "home_moneyline",
    "away_moneyline",
    "home_spread_odds",
    "away_spread_odds",
    "over_odds",
    "under_odds",
)


def validate_fixture(fixture: dict, source: str) -> None:
    if source not in VALID_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    for field in ("match_time", "home_handle", "away_handle"):
        if fixture.get(field) is None:
            raise ValueError(f"Missing required field: {field}")

    match_time = fixture["match_time"]

    if not isinstance(match_time, datetime):
        raise ValueError("match_time must be a datetime.")

    if match_time.tzinfo is None or match_time.utcoffset() is None:
        raise ValueError("match_time must be timezone-aware.")

    home = fixture["home_handle"].strip()
    away = fixture["away_handle"].strip()

    if not home or not away:
        raise ValueError("Player handles cannot be empty.")

    if home.casefold() == away.casefold():
        raise ValueError("Home and away handles must differ.")

    status = fixture.get("status", "scheduled")
    if status not in {
        "scheduled", "live", "completed",
        "postponed", "cancelled", "unknown",
    }:
        raise ValueError(f"Unsupported match status: {status}")

    if status == "completed" and (
        fixture.get("home_score") is None
        or fixture.get("away_score") is None
    ):
        raise ValueError(
            "Completed matches must include both scores."
        )

    for field in ("home_score", "away_score"):
        value = fixture.get(field)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise ValueError(f"{field} must be a nonnegative integer.")


def _record_source(
    conn: Connection,
    match_id: int,
    source: str,
    source_event_id: str | None,
    source_url: str | None,
    source_match_key: str | None,
    raw_hash: str | None,
) -> None:
    if source_event_id is not None:
        source_event_id = str(source_event_id).strip() or None

    if source_match_key is not None:
        source_match_key = str(source_match_key).strip() or None

    with conn.cursor() as cursor:
        if source_event_id:
            cursor.execute(
                """
                INSERT INTO match_sources (
                    match_id, source, source_event_id, source_url,
                    source_match_key, raw_hash,
                    first_seen_at, last_seen_at, last_checked_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (source, source_event_id)
                    WHERE source_event_id IS NOT NULL
                DO UPDATE SET
                    match_id = EXCLUDED.match_id,
                    source_url = COALESCE(
                        EXCLUDED.source_url,
                        match_sources.source_url
                    ),
                    source_match_key = COALESCE(
                        EXCLUDED.source_match_key,
                        match_sources.source_match_key
                    ),
                    raw_hash = COALESCE(
                        EXCLUDED.raw_hash,
                        match_sources.raw_hash
                    ),
                    last_seen_at = CURRENT_TIMESTAMP,
                    last_checked_at = CURRENT_TIMESTAMP
                """,
                (
                    match_id, source, source_event_id, source_url,
                    source_match_key, raw_hash,
                ),
            )
        else:
            if source_match_key is None:
                cursor.execute(
                    """
                    INSERT INTO match_sources (
                        match_id, source, source_url,
                        source_match_key, raw_hash,
                        first_seen_at, last_seen_at, last_checked_at
                    )
                    VALUES (
                        %s, %s, %s, NULL, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (match_id, source)
                        WHERE source_event_id IS NULL
                          AND source_match_key IS NULL
                    DO UPDATE SET
                        source_url = COALESCE(
                            EXCLUDED.source_url,
                            match_sources.source_url
                        ),
                        raw_hash = COALESCE(
                            EXCLUDED.raw_hash,
                            match_sources.raw_hash
                        ),
                        last_seen_at = CURRENT_TIMESTAMP,
                        last_checked_at = CURRENT_TIMESTAMP
                    """,
                    (match_id, source, source_url, raw_hash),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO match_sources (
                        match_id, source, source_url,
                        source_match_key, raw_hash,
                        first_seen_at, last_seen_at, last_checked_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (match_id, source, source_match_key)
                    DO UPDATE SET
                        source_url = COALESCE(
                            EXCLUDED.source_url,
                            match_sources.source_url
                        ),
                        raw_hash = COALESCE(
                            EXCLUDED.raw_hash,
                            match_sources.raw_hash
                        ),
                        last_seen_at = CURRENT_TIMESTAMP,
                        last_checked_at = CURRENT_TIMESTAMP
                    """,
                    (
                        match_id, source, source_url,
                        source_match_key, raw_hash,
                    ),
                )


def upsert_match(
    conn: Connection,
    fixture: dict,
    source: str = "h2hgg",
    *,
    source_event_id: str | None = None,
    source_url: str | None = None,
    source_match_key: str | None = None,
    raw_hash: str | None = None,
    return_status: bool = False,
) -> int | dict[str, Any]:
    """
    Insert or update a fixture and its source mapping.

    The caller owns the transaction. This function never commits.
    """
    validate_fixture(fixture, source)

    home_handle = fixture["home_handle"].strip()
    away_handle = fixture["away_handle"].strip()

    home_id = get_or_create_player(
        conn, home_handle, fixture.get("home_team")
    )
    away_id = get_or_create_player(
        conn, away_handle, fixture.get("away_team")
    )

    values = {
        field: fixture.get(field)
        for field in MATCH_FIELDS
    }

    is_synthetic = source == "synthetic"

    if raw_hash is None and fixture.get("raw_data") is not None:
        raw = json.dumps(
            fixture["raw_data"],
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        raw_hash = sha256(raw.encode("utf-8")).hexdigest()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO matches (
                match_time, home_team, home_player_id,
                away_team, away_player_id, status,
                home_score, away_score,
                spread_line, away_spread_line, total_line,
                home_moneyline, away_moneyline,
                home_spread_odds, away_spread_odds,
                over_odds, under_odds,
                data_source, is_synthetic
            )
            VALUES (
                %(match_time)s, %(home_team)s, %(home_id)s,
                %(away_team)s, %(away_id)s, %(status)s,
                %(home_score)s, %(away_score)s,
                %(spread_line)s, %(away_spread_line)s, %(total_line)s,
                %(home_moneyline)s, %(away_moneyline)s,
                %(home_spread_odds)s, %(away_spread_odds)s,
                %(over_odds)s, %(under_odds)s,
                %(data_source)s, %(is_synthetic)s
            )
            ON CONFLICT (
                match_time, home_player_id, away_player_id
            )
            DO UPDATE SET
                home_team = COALESCE(
                    EXCLUDED.home_team, matches.home_team
                ),
                away_team = COALESCE(
                    EXCLUDED.away_team, matches.away_team
                ),
                status = CASE
                    WHEN matches.status = 'completed'
                        THEN matches.status
                    WHEN EXCLUDED.status = 'completed'
                        THEN EXCLUDED.status
                    ELSE EXCLUDED.status
                END,
                home_score = COALESCE(
                    EXCLUDED.home_score, matches.home_score
                ),
                away_score = COALESCE(
                    EXCLUDED.away_score, matches.away_score
                ),
                spread_line = COALESCE(
                    EXCLUDED.spread_line, matches.spread_line
                ),
                away_spread_line = COALESCE(
                    EXCLUDED.away_spread_line, matches.away_spread_line
                ),
                total_line = COALESCE(
                    EXCLUDED.total_line, matches.total_line
                ),
                home_moneyline = COALESCE(
                    EXCLUDED.home_moneyline, matches.home_moneyline
                ),
                away_moneyline = COALESCE(
                    EXCLUDED.away_moneyline, matches.away_moneyline
                ),
                home_spread_odds = COALESCE(
                    EXCLUDED.home_spread_odds, matches.home_spread_odds
                ),
                away_spread_odds = COALESCE(
                    EXCLUDED.away_spread_odds, matches.away_spread_odds
                ),
                over_odds = COALESCE(
                    EXCLUDED.over_odds, matches.over_odds
                ),
                under_odds = COALESCE(
                    EXCLUDED.under_odds, matches.under_odds
                ),
                data_source = CASE
                    WHEN matches.is_synthetic AND NOT EXCLUDED.is_synthetic
                        THEN EXCLUDED.data_source
                    ELSE matches.data_source
                END,
                is_synthetic = matches.is_synthetic
                    AND EXCLUDED.is_synthetic,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, (xmax = 0) AS inserted
            """,
            {
                **values,
                "match_time": fixture["match_time"],
                "home_team": fixture.get("home_team"),
                "home_id": home_id,
                "away_team": fixture.get("away_team"),
                "away_id": away_id,
                "status": fixture.get("status", "scheduled"),
                "home_score": fixture.get("home_score"),
                "away_score": fixture.get("away_score"),
                "data_source": source,
                "is_synthetic": is_synthetic,
            },
        )

        result = cursor.fetchone()
        match_id = result["id"]

    _record_source(
        conn,
        match_id,
        source,
        source_event_id,
        source_url,
        source_match_key,
        raw_hash,
    )

    if return_status:
        return {"id": match_id, "inserted": bool(result["inserted"])}
    return match_id
