"""Match persistence."""

from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256
from typing import Any

from psycopg2.extensions import connection as Connection

from ebasketball.constants import FIXTURE_MATCH_TOLERANCE_SECONDS
from ebasketball.repositories.players import get_or_create_player

VALID_SOURCES = {"h2hgg", "aceodds", "manual", "synthetic"}


class AmbiguousFixtureMatchError(ValueError):
    """Raised when a fixture could match multiple stored fixtures."""


def validate_fixture(fixture: dict, source: str) -> None:
    if source not in VALID_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    for field in ("match_time", "home_handle", "away_handle"):
        if fixture.get(field) is None:
            raise ValueError(f"Missing required field: {field}")

    match_time = fixture["match_time"]

    if not isinstance(match_time, datetime):
        raise TypeError("match_time must be a datetime.")

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
        "scheduled",
        "live",
        "completed",
        "postponed",
        "cancelled",
        "unknown",
    }:
        raise ValueError(f"Unsupported match status: {status}")

    if status == "completed" and (
        fixture.get("home_score") is None or fixture.get("away_score") is None
    ):
        raise ValueError("Completed matches must include both scores.")

    for field in ("home_score", "away_score"):
        value = fixture.get(field)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
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
    source_updated_at: datetime | None,
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
                    source_updated_at,
                    first_seen_at, last_seen_at, last_checked_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
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
                    source_updated_at = CASE
                        WHEN EXCLUDED.source_updated_at IS NOT NULL
                         AND (match_sources.source_updated_at IS NULL
                              OR EXCLUDED.source_updated_at > match_sources.source_updated_at)
                        THEN EXCLUDED.source_updated_at
                        ELSE match_sources.source_updated_at
                    END,
                    raw_hash = COALESCE(
                        EXCLUDED.raw_hash,
                        match_sources.raw_hash
                    ),
                    last_seen_at = CURRENT_TIMESTAMP,
                    last_checked_at = CURRENT_TIMESTAMP
                """,
                (
                    match_id,
                    source,
                    source_event_id,
                    source_url,
                    source_match_key,
                    raw_hash,
                    source_updated_at,
                ),
            )
        else:
            if source_match_key is None:
                cursor.execute(
                    """
                    INSERT INTO match_sources (
                        match_id, source, source_url,
                        source_match_key, raw_hash,
                        source_updated_at,
                        first_seen_at, last_seen_at, last_checked_at
                    )
                    VALUES (
                        %s, %s, %s, NULL, %s, %s,
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
                        source_updated_at = CASE
                            WHEN EXCLUDED.source_updated_at IS NOT NULL
                             AND (match_sources.source_updated_at IS NULL
                                  OR EXCLUDED.source_updated_at > match_sources.source_updated_at)
                            THEN EXCLUDED.source_updated_at
                            ELSE match_sources.source_updated_at
                        END,
                        last_seen_at = CURRENT_TIMESTAMP,
                        last_checked_at = CURRENT_TIMESTAMP
                    """,
                    (match_id, source, source_url, raw_hash, source_updated_at),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO match_sources (
                        match_id, source, source_url,
                        source_match_key, raw_hash,
                        source_updated_at,
                        first_seen_at, last_seen_at, last_checked_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
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
                        source_updated_at = CASE
                            WHEN EXCLUDED.source_updated_at IS NOT NULL
                             AND (match_sources.source_updated_at IS NULL
                                  OR EXCLUDED.source_updated_at > match_sources.source_updated_at)
                            THEN EXCLUDED.source_updated_at
                            ELSE match_sources.source_updated_at
                        END,
                        last_seen_at = CURRENT_TIMESTAMP,
                        last_checked_at = CURRENT_TIMESTAMP
                    """,
                    (
                        match_id,
                        source,
                        source_url,
                        source_match_key,
                        raw_hash,
                        source_updated_at,
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
    source_updated_at: datetime | None = None,
    return_status: bool = False,
) -> int | dict[str, Any]:
    """
    Insert or update a fixture and its source mapping.

    The caller owns the transaction. This function never commits.
    """
    validate_fixture(fixture, source)
    fixture = dict(fixture)

    source_updated_at = source_updated_at or fixture.get("source_updated_at")
    if source_updated_at is not None and (
        not isinstance(source_updated_at, datetime)
        or source_updated_at.tzinfo is None
        or source_updated_at.utcoffset() is None
    ):
        raise ValueError("source_updated_at must be a timezone-aware datetime")

    home_handle = fixture["home_handle"].strip()
    away_handle = fixture["away_handle"].strip()

    home_id = get_or_create_player(conn, home_handle, fixture.get("home_team"))
    away_id = get_or_create_player(conn, away_handle, fixture.get("away_team"))

    # Serialize candidate resolution for this unordered player pair. A lock
    # collision only causes extra serialization; it cannot merge fixtures.
    player_pair = ":".join(str(value) for value in sorted((home_id, away_id)))
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (player_pair,)
        )

    is_synthetic = source == "synthetic"

    if raw_hash is None and fixture.get("raw_data") is not None:
        raw = json.dumps(
            fixture["raw_data"],
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        raw_hash = sha256(raw.encode("utf-8")).hexdigest()

    allow_score_correction = False
    previous_source = None
    if source_event_id:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT ms.source_updated_at, ms.match_id,
                       m.match_time, m.home_player_id, m.away_player_id,
                       m.status
                FROM match_sources AS ms
                JOIN matches AS m ON m.id = ms.match_id
                WHERE ms.source = %s AND ms.source_event_id = %s
                FOR UPDATE OF ms, m
                """,
                (source, str(source_event_id).strip()),
            )
            previous_source = cursor.fetchone()
        allow_score_correction = (
            previous_source is not None
            and source_updated_at is not None
            and (
                previous_source["source_updated_at"] is None
                or source_updated_at > previous_source["source_updated_at"]
            )
        )

    # Resolve slight cross-run kickoff differences using the same bounded
    # tolerance as in-memory reconciliation. Multiple candidates are unsafe:
    # leave them distinct and make the caller report the ambiguity.
    if previous_source is None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, match_time, home_player_id, away_player_id
                FROM matches
                WHERE is_synthetic = %s
                  AND EXISTS (
                      SELECT 1 FROM match_sources AS corroborating_source
                      WHERE corroborating_source.match_id = matches.id
                        AND corroborating_source.source <> %s
                  )
                  AND ((home_player_id = %s AND away_player_id = %s)
                       OR (home_player_id = %s AND away_player_id = %s))
                  AND match_time BETWEEN %s - (%s * INTERVAL '1 second')
                                     AND %s + (%s * INTERVAL '1 second')
                ORDER BY match_time, id
                FOR UPDATE
                """,
                (
                    is_synthetic,
                    source,
                    home_id,
                    away_id,
                    away_id,
                    home_id,
                    fixture["match_time"],
                    FIXTURE_MATCH_TOLERANCE_SECONDS,
                    fixture["match_time"],
                    FIXTURE_MATCH_TOLERANCE_SECONDS,
                ),
            )
            candidates = cursor.fetchall()
        if len(candidates) > 1:
            raise AmbiguousFixtureMatchError(
                "Fixture matches multiple stored candidates within "
                f"{FIXTURE_MATCH_TOLERANCE_SECONDS} seconds"
            )
        if candidates:
            candidate = candidates[0]
            if (home_id, away_id) == (
                candidate["away_player_id"],
                candidate["home_player_id"],
            ):
                for left, right in (
                    ("home_handle", "away_handle"),
                    ("home_team", "away_team"),
                    ("home_score", "away_score"),
                ):
                    fixture[left], fixture[right] = (
                        fixture.get(right),
                        fixture.get(left),
                    )
                home_id, away_id = away_id, home_id
            fixture["match_time"] = candidate["match_time"]

    if previous_source is not None:
        prior_home = previous_source["home_player_id"]
        prior_away = previous_source["away_player_id"]
        if (home_id, away_id) == (prior_away, prior_home):
            for left, right in (
                ("home_handle", "away_handle"),
                ("home_team", "away_team"),
                ("home_score", "away_score"),
            ):
                fixture[left], fixture[right] = (fixture.get(right), fixture.get(left))
            home_id, away_id = prior_home, prior_away
        elif (home_id, away_id) != (prior_home, prior_away):
            raise ValueError(
                "Source event ID is already associated with different participants"
            )

        existing_time = previous_source["match_time"]
        if allow_score_correction and previous_source["status"] != "completed":
            if fixture["match_time"] != existing_time:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE matches SET match_time = %s WHERE id = %s",
                        (fixture["match_time"], previous_source["match_id"]),
                    )
        else:
            fixture["match_time"] = existing_time

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO matches (
                match_time, home_team, home_player_id,
                away_team, away_player_id, status,
                home_score, away_score,
                data_source, is_synthetic
            )
            VALUES (
                %(match_time)s, %(home_team)s, %(home_id)s,
                %(away_team)s, %(away_id)s, %(status)s,
                %(home_score)s, %(away_score)s,
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
                    WHEN %(allow_score_correction)s
                        THEN EXCLUDED.status
                    WHEN matches.status = 'completed'
                        THEN matches.status
                    WHEN EXCLUDED.status = 'completed'
                        THEN EXCLUDED.status
                    ELSE EXCLUDED.status
                END,
                home_score = CASE
                    WHEN %(allow_score_correction)s
                        THEN EXCLUDED.home_score
                    WHEN matches.home_score IS NULL
                      OR (%(allow_score_correction)s AND EXCLUDED.status = 'completed')
                        THEN COALESCE(EXCLUDED.home_score, matches.home_score)
                    ELSE matches.home_score
                END,
                away_score = CASE
                    WHEN %(allow_score_correction)s
                        THEN EXCLUDED.away_score
                    WHEN matches.away_score IS NULL
                      OR (%(allow_score_correction)s AND EXCLUDED.status = 'completed')
                        THEN COALESCE(EXCLUDED.away_score, matches.away_score)
                    ELSE matches.away_score
                END,
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
                "allow_score_correction": allow_score_correction,
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
        source_updated_at,
    )

    if return_status:
        return {"id": match_id, "inserted": bool(result["inserted"])}
    return match_id
