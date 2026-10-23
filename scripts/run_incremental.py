
"""Run incremental ingestion with a configurable UTC overlap window."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from ebasketball.ingestion.normalize import normalize_match_time
from ebasketball.ingestion.pipeline import run_ingestion
from ebasketball.sources.json_file import JsonFileAdapter


def _parse_match_date(raw: Mapping[str, Any]) -> date | None:
    """Return the UTC calendar date of a match timestamp.

    Supports canonical ``match_time`` and H2HGG ``startDate`` fields.
    Missing timestamps return None so the ingestion normalizer can report
    malformed records instead of silently discarding them.
    """
    value = raw.get("match_time")
    if value is None or (isinstance(value, str) and not value.strip()):
        value = raw.get("startDate")
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return normalize_match_time(value).date()
    except (TypeError, ValueError, OverflowError, OSError):
        # Keep malformed dates for the canonical ingestion validator to report.
        return None


def filter_recent_records(
    records: Sequence[Any], *, overlap_days: int, today_utc: date
) -> tuple[list[Any], int, date]:
    """Keep records from the UTC cutoff through today, retaining invalid rows."""
    if not isinstance(overlap_days, int) or isinstance(overlap_days, bool) or overlap_days < 0:
        raise ValueError("overlap_days must be a nonnegative integer")
    cutoff_date = today_utc - timedelta(days=overlap_days)
    filtered: list[Any] = []
    excluded = 0
    for raw in records:
        if not isinstance(raw, Mapping):
            filtered.append(raw)
            continue
        match_date = _parse_match_date(raw)
        if match_date is None or cutoff_date <= match_date <= today_utc:
            filtered.append(raw)
        else:
            excluded += 1
    return filtered, excluded, cutoff_date


class FilteredAdapter:
    """Expose a prefiltered collection through the existing source interface."""

    def __init__(self, name: str, records: Sequence[Mapping[str, Any]]):
        self.name = name
        self._records = records

    def fetch_records(self) -> Sequence[Mapping[str, Any]]:
        return self._records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run incremental ingestion with a UTC overlap window "
            "for late-arriving scores and corrections."
        )
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=("h2hgg", "aceodds"),
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=2,
        help="Days back from the current UTC date, inclusive (default: 2)",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Write records to PostgreSQL; otherwise preview without DB access",
    )
    args = parser.parse_args(argv)

    if args.overlap_days < 0:
        parser.error("--overlap-days must be zero or greater")

    try:
        adapter = JsonFileAdapter(args.source, args.input)
        raw_records = adapter.fetch_records()

        today_utc = datetime.now(timezone.utc).date()
        filtered_records, excluded, cutoff_date = filter_recent_records(
            raw_records, overlap_days=args.overlap_days, today_utc=today_utc
        )

        source_adapter = FilteredAdapter(args.source, filtered_records)
        summary = run_ingestion(
            [source_adapter],
            dry_run=not args.persist,
        )

    except (OSError, ValueError, RuntimeError) as exc:
        print(
            json.dumps({"error": f"{type(exc).__name__}: {exc}"}),
            file=sys.stderr,
        )
        return 2

    result = summary.to_dict()
    result["incremental"] = {
        "source": args.source,
        "utc_today": today_utc.isoformat(),
        "cutoff_date": cutoff_date.isoformat(),
        "overlap_days": args.overlap_days,
        "input_records": len(raw_records),
        "filtered_out": excluded,
        "persist_requested": args.persist,
    }

    print(json.dumps(result, indent=2, sort_keys=True))
    if summary.source_failures or any(
        error.get("stage") in {"persist", "reconcile"}
        for error in summary.errors
        if isinstance(error, Mapping)
    ):
        return 3
    if summary.failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
