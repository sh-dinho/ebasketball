"""Deterministic adapter for source records exported as a JSON array."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


class JsonFileAdapter:
    """Load a JSON array of raw source records from a local file."""

    def __init__(self, name: str, path: str | Path):
        self.name = name.strip().casefold()
        self.path = Path(path)

    def fetch_records(self) -> Iterable[Mapping[str, Any]]:
        """Read records; filesystem and JSON errors are surfaced to the runner."""
        with self.path.open(encoding="utf-8") as input_file:
            records = json.load(input_file)
        if not isinstance(records, list):
            raise ValueError("JSON source input must contain a top-level array.")
        return records
