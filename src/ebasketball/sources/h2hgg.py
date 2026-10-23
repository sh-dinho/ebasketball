"""Primary H2HGG schedule adapter."""

from datetime import date
from typing import Any, Mapping

from ebasketball.ingestion.normalize import (
    DEFAULT_SOURCE_TIMEZONE,
    SOURCE_URLS,
    normalize_fixture,
)


SCHEDULE_URL = SOURCE_URLS["h2hgg"]


def normalize_h2hgg_record(
    record: Mapping[str, Any],
    *,
    source_timezone: str = DEFAULT_SOURCE_TIMEZONE,
    board_date: date | None = None,
) -> dict[str, Any]:
    """Normalize one H2HGG event into the shared fixture format."""
    return normalize_fixture(
        record,
        "h2hgg",
        source_url=SCHEDULE_URL,
        source_timezone=source_timezone,
        board_date=board_date,
    )
