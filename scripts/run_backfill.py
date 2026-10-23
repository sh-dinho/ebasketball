"""Preview or persist a historical backfill from an offline export."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from itertools import pairwise
from pathlib import Path

from ebasketball.ingestion.backfill import (
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    FileImportSource,
    run_adaptive_backfill,
    run_backfill,
)
from ebasketball.sources.live import LiveProviderUnavailable, require_live_provider


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill completed fixtures. Live collection is disabled until "
            "an official provider API is verified; pass --input for an offline export."
        )
    )
    parser.add_argument("--source", required=True, choices=("h2hgg", "aceodds"))
    parser.add_argument(
        "--input",
        type=Path,
        help="Offline JSON/CSV export. Omit to request live collection (currently disabled).",
    )
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=date(2026, 7, 25)
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, default=date(2026, 10, 23)
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument(
        "--window-sizes",
        type=_parse_window_sizes,
        default=(90, 60, 30),
        help="Adaptive window lengths in descending days, e.g. 90,60,30",
    )
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument(
        "--adaptive",
        action="store_true",
        help="Try 90, 60, then 30 days when the source reports failures",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Write completed fixtures to PostgreSQL; preview is the default",
    )
    args = parser.parse_args(argv)
    if args.persist and args.checkpoint is None:
        parser.error("--persist requires --checkpoint")
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if args.max_retries < 0 or args.retry_delay < 0:
        parser.error("--max-retries and --retry-delay must be nonnegative")

    try:
        if args.input is None:
            require_live_provider(args.source)
        source = FileImportSource(args.source, args.input)

        def report_progress(item):
            print(json.dumps({"progress": item}, sort_keys=True), file=sys.stderr)

        kwargs = {
            "batch_size": args.batch_size,
            "dry_run": not args.persist,
            "max_retries": args.max_retries,
            "retry_base_delay": args.retry_delay,
            "progress": report_progress,
        }

        if args.adaptive:
            summary = run_adaptive_backfill(
                source,
                args.start_date,
                args.end_date,
                window_sizes=args.window_sizes,
                base_checkpoint_path=args.checkpoint,
                **kwargs,
            )
        else:
            summary = run_backfill(
                source,
                args.start_date,
                args.end_date,
                checkpoint_path=args.checkpoint,
                **kwargs,
            )

    except LiveProviderUnavailable as exc:
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}), file=sys.stderr)
        return 3
    except (OSError, ValueError, RuntimeError) as exc:
        print(
            json.dumps({"error": f"{type(exc).__name__}: {exc}"}),
            file=sys.stderr,
        )
        return 2

    print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    if summary.provider_failures or any(
        error.get("stage") in {"persist", "reconcile", "checkpoint"}
        for error in summary.errors
        if isinstance(error, dict)
    ):
        return 3
    return 1 if summary.failed or not summary.complete or summary.fetched == 0 else 0


def _parse_window_sizes(value: str) -> tuple[int, ...]:
    try:
        sizes = tuple(int(item) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "window sizes must be comma-separated integers"
        ) from exc
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("window sizes must be positive")
    if any(left <= right for left, right in pairwise(sizes)):
        raise argparse.ArgumentTypeError(
            "window sizes must be in strictly decreasing order"
        )
    return sizes


if __name__ == "__main__":
    raise SystemExit(main())
