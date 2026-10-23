"""Interfaces shared by source collection adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class SourceAdapter(Protocol):
    """A configured collector for one source.

    Adapters return raw mappings and raise on unavailable or invalid input.
    They must not translate source failures into an empty successful result.
    """

    name: str

    def fetch_records(self) -> Iterable[Mapping[str, Any]]:
        """Fetch or load raw source records."""

