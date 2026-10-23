"""Reconcile fixtures collected from multiple data sources."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from ebasketball.constants import FIXTURE_MATCH_TOLERANCE_SECONDS

DEFAULT_TIME_TOLERANCE_SECONDS = FIXTURE_MATCH_TOLERANCE_SECONDS


def _normalize_handle(value: Any) -> str:
    """Normalize a player handle for cross-source comparisons."""
    if value is None:
        return ""

    return " ".join(str(value).casefold().split())


def _utc_time(value: Any) -> datetime:
    """Validate and normalize a timestamp to UTC."""
    if not isinstance(value, datetime):
        raise TypeError("match_time must be a datetime")

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("match_time must be timezone-aware")

    return value.astimezone(timezone.utc)


def _fixture_identity(
    fixture: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the ordered home and away player identities."""
    home = _normalize_handle(fixture.get("home_handle"))
    away = _normalize_handle(fixture.get("away_handle"))

    if not home or not away:
        raise ValueError("Both player handles are required")

    if home == away:
        raise ValueError("Home and away handles must differ")

    return home, away


def _validate_tolerance(tolerance_seconds: int) -> None:
    """Validate the allowed kickoff-time difference."""
    if isinstance(tolerance_seconds, bool) or not isinstance(tolerance_seconds, int):
        raise TypeError("tolerance_seconds must be an integer")

    if tolerance_seconds < 0:
        raise ValueError("tolerance_seconds cannot be negative")


def fixtures_match(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    *,
    tolerance_seconds: int = DEFAULT_TIME_TOLERANCE_SECONDS,
) -> bool:
    """
    Determine whether two source records represent the same fixture.

    Participant identities may appear in either order. Kickoff times may
    differ by up to tolerance_seconds. This function does not mutate records;
    callers choosing a canonical orientation must swap scores with players.
    """
    _validate_tolerance(tolerance_seconds)

    if set(_fixture_identity(first)) != set(_fixture_identity(second)):
        return False

    first_time = _utc_time(first.get("match_time"))
    second_time = _utc_time(second.get("match_time"))

    return abs((first_time - second_time).total_seconds()) <= tolerance_seconds


def reconcile_fixtures(
    fixtures_by_source: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    tolerance_seconds: int = DEFAULT_TIME_TOLERANCE_SECONDS,
) -> dict[str, Any]:
    """
    Group matching source records into canonical fixture candidates.

    The result contains:
      - groups: groups of records believed to represent one fixture
      - unmatched: records that could not be matched to another source
      - ambiguous: records matching more than one existing group

    Input order is preserved. No source record is silently discarded.
    Each group contains the original fixture mappings.
    """
    _validate_tolerance(tolerance_seconds)

    groups: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []

    for source, records in fixtures_by_source.items():
        source_name = str(source).strip().casefold()

        if not source_name:
            raise ValueError("Source names cannot be empty")

        for fixture in records:
            record = {
                "source": source_name,
                "fixture": fixture,
            }

            # Validate identity and timestamp before matching.
            _fixture_identity(fixture)
            _utc_time(fixture.get("match_time"))

            candidates = [
                group
                for group in groups
                if source_name not in group["sources"]
                and any(
                    fixtures_match(
                        fixture,
                        existing["fixture"],
                        tolerance_seconds=tolerance_seconds,
                    )
                    for existing in group["records"]
                )
            ]

            if len(candidates) > 1:
                ambiguous.append(record)
                continue

            if len(candidates) == 1:
                candidates[0]["records"].append(record)
                candidates[0]["sources"].add(source_name)
                continue

            groups.append(
                {
                    "records": [record],
                    "sources": {source_name},
                }
            )

    normalized_groups = [
        {
            "records": group["records"],
            "sources": sorted(group["sources"]),
        }
        for group in groups
    ]

    unmatched = [group["records"][0] for group in groups if len(group["records"]) == 1]

    return {
        "groups": normalized_groups,
        "unmatched": unmatched,
        "ambiguous": ambiguous,
    }
