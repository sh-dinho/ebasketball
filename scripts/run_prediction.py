"""Run an educational winner analysis or chronological backtest on JSON data."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2

from ebasketball.database import get_connection
from ebasketball.ingestion.normalize import normalize_fixture
from ebasketball.prediction import METHOD_VERSION, backtest, predict_winner
from ebasketball.repositories.predictions import (
    create_prediction,
    list_prediction_evaluation_history,
    list_predictions,
    prediction_performance,
    reconcile_predictions,
)
from ebasketball.sources.live import LiveProviderUnavailable, require_live_provider


def _read_records(path: Path) -> list[Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("fixtures", "games", "events"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise ValueError("Input JSON must be an array or contain a fixtures array")


def _normalize_records(
    records: list[Any], source: str
) -> tuple[list[dict[str, Any]], list[str]]:
    normalized: list[dict[str, Any]] = []
    warnings: list[str] = []
    for index, record in enumerate(records):
        try:
            normalized.append(normalize_fixture(record, source))
        except (TypeError, ValueError) as exc:
            warnings.append(f"Record {index}: {type(exc).__name__}: {exc}")
    return normalized, warnings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Analyze historical match winners; no prediction probabilities are produced."
    )
    parser.add_argument("--source", choices=("h2hgg", "aceodds"))
    parser.add_argument("--input", type=Path, help="JSON export for offline analysis")
    parser.add_argument("--team-a")
    parser.add_argument("--team-b")
    parser.add_argument(
        "--match-time", help="Target match time as an ISO-8601 timestamp"
    )
    parser.add_argument(
        "--backtest", action="store_true", help="Run chronological evaluation instead"
    )
    parser.add_argument("--minimum-matches", type=int, default=5)
    parser.add_argument(
        "--match-id",
        type=int,
        help="Stored scheduled fixture to associate with a prediction",
    )
    parser.add_argument(
        "--run-id",
        type=UUID,
        help="Prediction run UUID; reuse it for idempotency, change it for a new snapshot",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Persist this prediction or reconcile results",
    )
    parser.add_argument(
        "--list", dest="list_all", action="store_true", help="List recorded predictions"
    )
    parser.add_argument(
        "--pending", action="store_true", help="List predictions awaiting a result"
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Evaluate predictions against current results",
    )
    parser.add_argument(
        "--metrics", action="store_true", help="Summarize prediction performance"
    )
    parser.add_argument(
        "--evaluation-history",
        action="store_true",
        help="List append-only prediction evaluation and retraction events",
    )
    parser.add_argument(
        "--prediction-record-id",
        type=int,
        help="Limit --evaluation-history to one prediction record",
    )
    parser.add_argument(
        "--start-date", type=lambda value: datetime.fromisoformat(value).date()
    )
    parser.add_argument(
        "--end-date", type=lambda value: datetime.fromisoformat(value).date()
    )
    parser.add_argument(
        "--match-id-filter",
        type=int,
        help="Limit reconciliation or evaluation-history output to one fixture",
    )
    parser.add_argument(
        "--export", type=Path, help="Write database output as JSON or CSV"
    )
    args = parser.parse_args(argv)

    actions = [
        args.list_all,
        args.pending,
        args.reconcile,
        args.metrics,
        args.evaluation_history,
    ]
    if sum(actions) > 1:
        parser.error(
            "choose only one of --list, --pending, --reconcile, --metrics, "
            "or --evaluation-history"
        )
    if args.prediction_record_id is not None and not args.evaluation_history:
        parser.error("--prediction-record-id requires --evaluation-history")
    if args.persist and not (args.reconcile or args.input):
        parser.error("--persist requires prediction input or --reconcile")
    if args.reconcile and not args.persist:
        parser.error("--reconcile writes evaluation history; pass --persist explicitly")
    if args.persist and args.input and args.match_id is None:
        parser.error("persisting a prediction requires --match-id")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")

    try:
        if (
            args.list_all
            or args.pending
            or args.reconcile
            or args.metrics
            or args.evaluation_history
        ):
            with get_connection() as conn:
                if args.list_all or args.pending:
                    result = list_predictions(conn, pending_only=args.pending)
                elif args.reconcile:
                    result = reconcile_predictions(conn, match_id=args.match_id_filter)
                elif args.evaluation_history:
                    result = list_prediction_evaluation_history(
                        conn,
                        prediction_id=args.prediction_record_id,
                        match_id=args.match_id_filter,
                    )
                else:
                    result = prediction_performance(
                        conn, start_date=args.start_date, end_date=args.end_date
                    )
            _write_output(result, args.export)
            return 0

        if args.input is None:
            if args.source is None:
                parser.error("--source is required for a prediction or backtest")
            require_live_provider(args.source)
        if args.source is None:
            parser.error("--source is required for a prediction or backtest")
        if not args.input.is_file():
            raise ValueError(f"Input file does not exist: {args.input}")
        raw = _read_records(args.input)
        fixtures, warnings = _normalize_records(raw, args.source)
        if args.backtest:
            result = backtest(fixtures, minimum_matches=args.minimum_matches)
        else:
            if not args.team_a or not args.team_b or not args.match_time:
                parser.error("prediction requires --team-a, --team-b, and --match-time")
            match_time = datetime.fromisoformat(args.match_time.replace("Z", "+00:00"))
            if match_time.tzinfo is None or match_time.utcoffset() is None:
                raise ValueError("--match-time must include a timezone")
            result = predict_winner(
                fixtures,
                args.team_a,
                args.team_b,
                match_time=match_time.astimezone(timezone.utc),
                minimum_matches=args.minimum_matches,
            )
            if args.persist:
                with get_connection() as conn:
                    stored = create_prediction(
                        conn,
                        match_id=args.match_id,
                        prediction_run_id=args.run_id
                        or uuid5(
                            NAMESPACE_URL,
                            f"ebasketball:{args.match_id}:{METHOD_VERSION}",
                        ),
                        predicted_winner=result["predicted_winner"],
                        supporting_statistics=result,
                        sample_size=result["matches_analyzed"],
                        evidence_strength=result["confidence"],
                        method_version=METHOD_VERSION,
                        source=args.source,
                        expected_participants=(args.team_a, args.team_b),
                        expected_match_time=match_time,
                    )
                result["stored_prediction"] = stored
        result["input_records"] = len(raw)
        result["normalization_warnings"] = warnings
        _write_output(result, args.export)
        return 1 if warnings else 0
    except LiveProviderUnavailable as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 3
    except psycopg2.Error:
        print(json.dumps({"error": "PostgreSQL operation failed."}), file=sys.stderr)
        return 4
    except (OSError, ValueError, json.JSONDecodeError, RuntimeError) as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 2


def _write_output(result: Any, output_path: Path | None) -> None:
    payload = json.dumps(result, default=str, sort_keys=True, indent=2)
    if output_path is None:
        print(payload)
        return
    if output_path.suffix.casefold() == ".csv":
        rows = result if isinstance(result, list) else [result]
        if not rows:
            output_path.write_text("", encoding="utf-8")
            return
        fieldnames = sorted({key for row in rows for key in row})
        with output_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        key: json.dumps(value, default=str)
                        if isinstance(value, (dict, list))
                        else value
                        for key, value in row.items()
                    }
                )
    elif output_path.suffix.casefold() == ".json":
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        raise ValueError("--export path must end in .json or .csv")


if __name__ == "__main__":
    raise SystemExit(main())
