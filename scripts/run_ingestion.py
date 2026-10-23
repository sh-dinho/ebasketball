"""Run ingestion from explicitly configured JSON source exports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ebasketball.ingestion.pipeline import run_ingestion
from ebasketball.sources.json_file import JsonFileAdapter


def _parse_input(value: str) -> JsonFileAdapter:
    source, separator, path = value.partition("=")
    if not separator or not source.strip() or not path.strip():
        raise argparse.ArgumentTypeError(
            "input must use SOURCE=PATH, for example h2hgg=fixtures.json"
        )
    return JsonFileAdapter(source, Path(path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest raw records from JSON exports. Live source fetching is not "
            "implemented by the current source adapters."
        )
    )
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        type=_parse_input,
        metavar="SOURCE=PATH",
        help="JSON array of raw records for h2hgg or aceodds; repeatable",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="collect, normalize, and reconcile without opening a database connection",
    )
    args = parser.parse_args(argv)
    summary = run_ingestion(args.input, dry_run=args.dry_run)
    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    return 1 if summary.failed or summary.source_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
