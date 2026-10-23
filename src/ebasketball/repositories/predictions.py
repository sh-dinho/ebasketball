"""Persistence and evaluation history for educational match predictions."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any
from uuid import UUID

from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json

EVALUATED_STATUSES = {"correct", "incorrect"}
NON_EVALUATED_STATUSES = {"pending", "void", "unable_to_evaluate"}


def classify_result(
    *,
    prediction_created_at: datetime,
    result_available_at: datetime,
    match_status: str,
    home_score: Any,
    away_score: Any,
    home_player_id: int,
    away_player_id: int,
    predicted_winner_player_id: int | None,
    is_synthetic: bool = False,
) -> tuple[str, int | None] | None:
    """Classify one result; return ``None`` for fixtures still in progress."""
    if is_synthetic:
        return None
    if match_status in {"scheduled", "live"}:
        return None
    if match_status in {"cancelled", "postponed"}:
        return "void", None
    if match_status != "completed":
        return "unable_to_evaluate", None
    if (
        isinstance(home_score, bool)
        or isinstance(away_score, bool)
        or not isinstance(home_score, int)
        or not isinstance(away_score, int)
        or home_score < 0
        or away_score < 0
    ):
        return "unable_to_evaluate", None
    if home_score == away_score:
        return "void", None
    if result_available_at <= prediction_created_at:
        return "unable_to_evaluate", None
    actual_winner = home_player_id if home_score > away_score else away_player_id
    if predicted_winner_player_id is None:
        return "unable_to_evaluate", actual_winner
    return (
        "correct" if predicted_winner_player_id == actual_winner else "incorrect",
        actual_winner,
    )


def create_prediction(
    conn: Connection,
    *,
    match_id: int,
    prediction_run_id: UUID | str,
    predicted_winner: str | None,
    supporting_statistics: dict[str, Any],
    sample_size: int,
    evidence_strength: str,
    method_version: str,
    source: str | None = None,
    source_event_id: str | None = None,
    expected_participants: tuple[str, str] | None = None,
    expected_match_time: datetime | None = None,
) -> dict[str, Any]:
    """Persist a pre-match snapshot; the caller owns the transaction.

    Reusing prediction_run_id for a fixture is idempotent. A different
    run ID represents a separate, legitimate prediction snapshot.
    """
    run_id = str(UUID(str(prediction_run_id)))

    if sample_size < 0:
        raise ValueError("sample_size must be nonnegative")
    if evidence_strength not in {"low", "medium", "high"}:
        raise ValueError("evidence_strength must be low, medium, or high")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT match_time, status, is_synthetic, home_player_id,
                   away_player_id, data_source
            FROM matches
            WHERE id = %s
            FOR UPDATE
            """,
            (match_id,),
        )
        fixture = cursor.fetchone()

        if fixture is None:
            raise ValueError("Prediction target match does not exist")

        if expected_participants is not None:
            cursor.execute(
                "SELECT id, handle FROM players WHERE id IN (%s, %s)",
                (
                    fixture["home_player_id"],
                    fixture["away_player_id"],
                ),
            )
            handles = {row["id"]: row["handle"] for row in cursor.fetchall()}
            expected = {name.strip().casefold() for name in expected_participants}
            actual = {
                handles[fixture["home_player_id"]].casefold(),
                handles[fixture["away_player_id"]].casefold(),
            }
            if expected != actual:
                raise ValueError("Prediction teams do not match the stored fixture")

        if (
            expected_match_time is not None
            and abs((fixture["match_time"] - expected_match_time).total_seconds()) > 300
        ):
            raise ValueError("Prediction time does not match the stored fixture")

        winner_id = None
        if predicted_winner is not None:
            cursor.execute(
                "SELECT id, handle FROM players WHERE id IN (%s, %s)",
                (
                    fixture["home_player_id"],
                    fixture["away_player_id"],
                ),
            )
            participants = {
                row["handle"].casefold(): row["id"] for row in cursor.fetchall()
            }
            winner_id = participants.get(predicted_winner.strip().casefold())
            if winner_id is None:
                raise ValueError(
                    "Predicted winner must be one of the fixture participants"
                )

        cursor.execute(
            """
            SELECT id,
                   status,prediction_created_at,
                   predicted_winner_player_id,supporting_statistics,
                   sample_size,evidence_strength,method_version
            FROM prediction_records
            WHERE match_id = %s
              AND prediction_run_id = %s
            """,
            (match_id, run_id),
        )
        existing = cursor.fetchone()

        normalized_statistics = json.loads(
            json.dumps(supporting_statistics, default=str)
        )

        if existing is not None:
            if (
                existing["predicted_winner_player_id"] != winner_id
                or existing["sample_size"] != sample_size
                or existing["evidence_strength"] != evidence_strength
                or existing["method_version"] != method_version
                or existing["supporting_statistics"] != normalized_statistics
            ):
                raise ValueError(
                    "prediction_run_id already identifies a different prediction"
                )
            return {**dict(existing), "created": False}

        if fixture["status"] != "scheduled" or fixture["is_synthetic"]:
            raise ValueError("Only real scheduled matches can receive predictions")

        cursor.execute("SELECT CURRENT_TIMESTAMP AS now")
        created_at = cursor.fetchone()["now"]

        if created_at >= fixture["match_time"]:
            raise ValueError("Prediction must be recorded before the fixture starts")

        event_source_id = source_event_id
        if event_source_id is None:
            cursor.execute(
                """
                SELECT source, source_event_id
                FROM match_sources
                WHERE match_id = %s
                ORDER BY last_seen_at DESC, id DESC
                LIMIT 1
                """,
                (match_id,),
            )
            provenance = cursor.fetchone()
            if provenance:
                event_source_id = provenance["source_event_id"]
                source = source or provenance["source"]

        status = "pending" if winner_id is not None else "unable_to_evaluate"

        cursor.execute(
            """
            INSERT INTO prediction_records (
                match_id, source, source_event_id, prediction_run_id,
                prediction_created_at, home_player_id, away_player_id,
                predicted_winner_player_id, supporting_statistics,
                sample_size, evidence_strength, method_version, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id, prediction_run_id) DO NOTHING
            RETURNING id, status, prediction_created_at
            """,
            (
                match_id,
                source or fixture["data_source"],
                event_source_id,
                run_id,
                created_at,
                fixture["home_player_id"],
                fixture["away_player_id"],
                winner_id,
                Json(
                    supporting_statistics,
                    dumps=lambda value: json.dumps(value, default=str),
                ),
                sample_size,
                evidence_strength,
                method_version,
                status,
            ),
        )

        result = cursor.fetchone()
        if result is not None:
            return {**dict(result), "created": True}

        # Another transaction may have inserted this run after our SELECT.
        # Re-read it so concurrent idempotent requests are handled safely.
        cursor.execute(
            """
            SELECT id, status, prediction_created_at,
                   predicted_winner_player_id, supporting_statistics,
                   sample_size, evidence_strength, method_version
            FROM prediction_records
            WHERE match_id = %s
              AND prediction_run_id = %s
            """,
            (match_id, run_id),
        )
        existing = cursor.fetchone()

        if existing is None:
            raise RuntimeError("Prediction conflict occurred but its row was not found")

        if (
            existing["predicted_winner_player_id"] != winner_id
            or existing["sample_size"] != sample_size
            or existing["evidence_strength"] != evidence_strength
            or existing["method_version"] != method_version
            or existing["supporting_statistics"] != normalized_statistics
        ):
            raise ValueError(
                "prediction_run_id already identifies a different prediction"
            )

        return {**dict(existing), "created": False}


def _fingerprint(row: dict[str, Any]) -> str:
    value = {
        "match_id": row["match_id"],
        "status": row["match_status"],
        "home_score": row["home_score"],
        "away_score": row["away_score"],
        "home_player_id": row["home_player_id"],
        "away_player_id": row["away_player_id"],
        "is_synthetic": row["is_synthetic"],
    }
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def reconcile_predictions(
    conn: Connection, *, match_id: int | None = None
) -> dict[str, int]:
    """Append evaluations for changed trustworthy results, idempotently."""
    query = """
        SELECT p.id AS prediction_id, p.match_id, p.status AS prediction_status,
               p.prediction_created_at, p.predicted_winner_player_id,
               p.current_result_fingerprint,
               m.status AS match_status, m.home_score, m.away_score,
               m.home_player_id, m.away_player_id, m.is_synthetic,
               m.updated_at AS result_available_at,
               previous.result_signature AS previous_signature
        FROM prediction_records AS p
        JOIN matches AS m ON m.id = p.match_id
        LEFT JOIN LATERAL (
            SELECT result_signature
            FROM prediction_evaluation_events
            WHERE prediction_id = p.id
            ORDER BY id DESC LIMIT 1
        ) AS previous ON TRUE
        WHERE (%s IS NULL OR p.match_id = %s)
        ORDER BY p.id
        FOR UPDATE OF p, m
    """
    counts: Counter[str] = Counter()
    with conn.cursor() as cursor:
        cursor.execute(query, (match_id, match_id))
        records = cursor.fetchall()
    for record in records:
        row = dict(record)
        classification = classify_result(
            prediction_created_at=row["prediction_created_at"],
            result_available_at=row["result_available_at"],
            match_status=row["match_status"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            home_player_id=row["home_player_id"],
            away_player_id=row["away_player_id"],
            predicted_winner_player_id=row["predicted_winner_player_id"],
            is_synthetic=row["is_synthetic"],
        )
        if classification is None:
            counts["pending"] += 1
            continue
        status, classified_winner_id = classification

        signature = _fingerprint(row)
        if row["previous_signature"] == signature:
            counts["unchanged"] += 1
            continue
        previous_version = row["current_result_fingerprint"] or ""
        version = hashlib.sha256(f"{previous_version}:{signature}".encode()).hexdigest()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO prediction_evaluation_events (
                    prediction_id, result_fingerprint, result_signature,
                    status, result_status, home_score, away_score,
                    actual_winner_player_id, result_available_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prediction_id, result_fingerprint) DO NOTHING
                """,
                (
                    row["prediction_id"],
                    version,
                    signature,
                    status,
                    row["match_status"],
                    row["home_score"],
                    row["away_score"],
                    classified_winner_id,
                    row["result_available_at"],
                ),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                cursor.execute(
                    """
                    UPDATE prediction_records
                    SET status = %s, evaluated_at = CURRENT_TIMESTAMP,
                        current_result_fingerprint = %s
                    WHERE id = %s
                    """,
                    (status, version, row["prediction_id"]),
                )
        counts[status if inserted else "unchanged"] += 1
    return {
        "evaluated": counts["correct"] + counts["incorrect"],
        "correct": counts["correct"],
        "incorrect": counts["incorrect"],
        "void": counts["void"],
        "unable_to_evaluate": counts["unable_to_evaluate"],
        "pending": counts["pending"],
        "unchanged": counts["unchanged"],
    }


def list_predictions(
    conn: Connection, *, pending_only: bool = False
) -> list[dict[str, Any]]:
    """Return stored snapshots with their current fixture participants."""
    status_filter = "AND p.status = 'pending'" if pending_only else ""
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT p.id, p.match_id, p.source, p.source_event_id,
                   p.prediction_run_id, p.prediction_created_at,
                   hp.handle AS home_player, ap.handle AS away_player,
                   wp.handle AS predicted_winner, p.supporting_statistics,
                   p.sample_size, p.evidence_strength, p.method_version,
                   p.status, p.evaluated_at, m.match_time
            FROM prediction_records AS p
            JOIN matches AS m ON m.id = p.match_id
            JOIN players AS hp ON hp.id = p.home_player_id
            JOIN players AS ap ON ap.id = p.away_player_id
            LEFT JOIN players AS wp ON wp.id = p.predicted_winner_player_id
            WHERE TRUE {status_filter}
            ORDER BY p.prediction_created_at DESC, p.id DESC
            """
        )
        return [dict(row) for row in cursor.fetchall()]


def prediction_performance(
    conn: Connection,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """Summarize outcomes with explicit denominators and same-fixture baseline."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT p.id, p.status, p.method_version, p.evidence_strength,
                   m.match_time, hp.handle AS home_player, ap.handle AS away_player,
                   (m.match_time AT TIME ZONE 'UTC')::date AS match_date_utc,
                   p.predicted_winner_player_id, m.home_player_id,
                   e.status AS evaluation_status,
                   e.actual_winner_player_id, e.home_score, e.away_score
            FROM prediction_records AS p
            JOIN matches AS m ON m.id = p.match_id
            JOIN players AS hp ON hp.id = p.home_player_id
            JOIN players AS ap ON ap.id = p.away_player_id
            LEFT JOIN prediction_evaluation_events AS e
              ON e.prediction_id = p.id
             AND e.result_fingerprint = p.current_result_fingerprint
            WHERE (%s IS NULL OR (m.match_time AT TIME ZONE 'UTC')::date >= %s)
              AND (%s IS NULL OR (m.match_time AT TIME ZONE 'UTC')::date <= %s)
              AND m.is_synthetic = FALSE
              AND m.data_source <> 'synthetic'
            ORDER BY m.match_time, p.id
            """,
            (start_date, start_date, end_date, end_date),
        )
        records = [dict(row) for row in cursor.fetchall()]

        # Prediction coverage is distinct from evaluation coverage: the former
        # measures how many eligible fixtures received any snapshot, while the
        # latter measures how many recorded snapshots have a decisive result.
        cursor.execute(
            """
            SELECT COUNT(DISTINCT m.id) AS eligible_fixtures,
                   COUNT(DISTINCT p.match_id) AS fixtures_with_predictions
            FROM matches AS m
            LEFT JOIN prediction_records AS p ON p.match_id = m.id
            WHERE m.status IN ('scheduled', 'completed')
              AND m.is_synthetic = FALSE
              AND m.data_source <> 'synthetic'
              AND (%s IS NULL OR (m.match_time AT TIME ZONE 'UTC')::date >= %s)
              AND (%s IS NULL OR (m.match_time AT TIME ZONE 'UTC')::date <= %s)
            """,
            (start_date, start_date, end_date, end_date),
        )
        coverage_row = dict(cursor.fetchone())
    eligible_fixtures = coverage_row["eligible_fixtures"]
    fixtures_with_predictions = coverage_row["fixtures_with_predictions"]

    status_counts = Counter(record["status"] for record in records)
    evaluated = [
        record
        for record in records
        if record["evaluation_status"] in EVALUATED_STATUSES
    ]
    correct = sum(record["evaluation_status"] == "correct" for record in evaluated)
    home_baseline_correct = sum(
        record["home_player_id"] == record["actual_winner_player_id"]
        for record in evaluated
    )

    def grouped(field: str) -> dict[str, dict[str, Any]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            groups[str(record[field])].append(record)
        return {key: _metrics(rows) for key, rows in sorted(groups.items())}

    by_player: dict[str, dict[str, Any]] = {}
    for record in records:
        for player in (record["home_player"], record["away_player"]):
            by_player.setdefault(player, []).append(record)
    return {
        "total_predictions": len(records),
        "evaluated_predictions": len(evaluated),
        "pending_predictions": status_counts["pending"],
        "correct_predictions": correct,
        "incorrect_predictions": status_counts["incorrect"],
        "void_predictions": status_counts["void"],
        "unable_to_evaluate_predictions": status_counts["unable_to_evaluate"],
        "accuracy": correct / len(evaluated) if evaluated else None,
        "accuracy_denominator": len(evaluated),
        "evaluation_coverage": len(evaluated) / len(records) if records else 0.0,
        "evaluation_coverage_denominator": len(records),
        "eligible_fixtures": eligible_fixtures,
        "fixtures_with_predictions": fixtures_with_predictions,
        "prediction_coverage": (
            fixtures_with_predictions / eligible_fixtures if eligible_fixtures else 0.0
        ),
        "prediction_coverage_denominator": eligible_fixtures,
        "same_fixture_home_baseline_accuracy": (
            home_baseline_correct / len(evaluated) if evaluated else None
        ),
        "same_fixture_home_baseline_denominator": len(evaluated),
        "by_method_version": grouped("method_version"),
        "by_evidence_strength": grouped("evidence_strength"),
        "by_player": {key: _metrics(rows) for key, rows in sorted(by_player.items())},
        "by_match_date": {
            key: _metrics(rows) for key, rows in sorted(_group_by_date(records).items())
        },
        "warnings": (
            [
                "Small samples should not be interpreted as reliable performance estimates."
            ]
            if len(evaluated) < 30
            else []
        ),
    }


def _metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [r for r in records if r["evaluation_status"] in EVALUATED_STATUSES]
    correct = sum(r["evaluation_status"] == "correct" for r in evaluated)
    return {
        "predictions": len(records),
        "evaluated": len(evaluated),
        "correct": correct,
        "incorrect": len(evaluated) - correct,
        "accuracy": correct / len(evaluated) if evaluated else None,
        "coverage": len(evaluated) / len(records) if records else 0.0,
    }


def _group_by_date(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[
            (record.get("match_date_utc") or record["match_time"].date()).isoformat()
        ].append(record)
    return grouped
