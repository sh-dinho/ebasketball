"""Run ingestion from explicitly configured JSON source exports."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
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
    persistence = parser.add_mutually_exclusive_group()
    persistence.add_argument(
        "--persist",
        action="store_true",
        help="write records to PostgreSQL (preview without database access by default)",
    )
    persistence.add_argument(
        "--dry-run",
        action="store_false",
        dest="persist",
        help="explicitly preview without database access; retained for compatibility",
    )
    parser.set_defaults(persist=False)
    args = parser.parse_args(argv)
    try:
        summary = run_ingestion(args.input, dry_run=not args.persist)
    except (OSError, ValueError) as exc:
        print(
            json.dumps({"error": f"{type(exc).__name__}: {exc}"}),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    if summary.source_failures or any(
        error.get("stage") in {"reconcile", "persist"}
        for error in summary.errors
        if isinstance(error, Mapping)
    ):
        return 3
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
