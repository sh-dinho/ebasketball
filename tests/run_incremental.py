"""Run incremental ingestion for recent and live matches with overlap."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

from ebasketball.ingestion.pipeline import run_ingestion
from ebasketball.sources.json_file import JsonFileAdapter


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run incremental ingestion with an overlap window for late-arriving results."
    )
    parser.add_argument("--source", required=True, choices=("h2hgg", "aceodds"))
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=2,
        help="Number of past days to re-scan for score updates and corrections",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Write records to PostgreSQL (default is dry-run preview)",
    )
    args = parser.parse_args(argv)

    try:
        adapter = JsonFileAdapter(args.source, args.input)
        summary = run_ingestion([adapter], dry_run=not args.persist)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 2

    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    return 1 if summary.failed or summary.source_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())