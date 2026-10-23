"""Root-level entry point for incremental ingestion."""

from scripts.run_incremental import main


if __name__ == "__main__":
    raise SystemExit(main())
