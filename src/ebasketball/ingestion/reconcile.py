"""Reconcile fixtures collected from multiple data sources."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping


DEFAULT_TIME_TOLERANCE_SECONDS = 300


def _normalize_handle(value: Any) -> str:
    """Normalize a player handle for cross-source comparisons."""
    if value is None:
        return ""

    return " ".join(str(value).casefold().split())


def _utc_time(value: Any) -> datetime:
    """Validate and normalize a timestamp to UTC."""
    from datetime import timezone

    if not isinstance(value, datetime):
        raise ValueError("match_time must be a datetime")

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


def fixtures_match(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    *,
    tolerance_seconds: int = DEFAULT_TIME_TOLERANCE_SECONDS,
) -> bool:
    """
    Determine whether two source records represent the same fixture.

    Home and away positions must match. Kickoff times may differ by up
    to tolerance_seconds. This function does not mutate either record.
    """
    if tolerance_seconds < 0:
        raise ValueError("tolerance_seconds cannot be negative")

    if _fixture_identity(first) != _fixture_identity(second):
        return False

    first_time = _utc_time(first.get("match_time"))
    second_time = _utc_time(second.get("match_time"))

    return (
        abs((first_time - second_time).total_seconds())
        <= tolerance_seconds
    )


def reconcile_fixtures(
    fixtures_by_source: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    tolerance_seconds: int = DEFAULT_TIME_TOLERANCE_SECONDS,
) -> dict[str, Any]:
    """
    Group matching source records into canonical fixture candidates.

    The result contains:
      - groups: groups of records believed to represent one fixture
      - unmatched: records that could not be matched
      - ambiguous: records matching more than one existing group

    Input order is preserved. No source record is silently discarded.
    Each group contains the original fixture mappings.
    """
    if tolerance_seconds < 0:
        raise ValueError("tolerance_seconds cannot be negative")

    groups: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
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

            # Validate identity and timestamp before attempting matching.
            _fixture_identity(fixture)
            _utc_time(fixture.get("match_time"))

            candidates = [
                group
                for group in groups
                if source_name not in group["sources"]
                and fixtures_match(
                    fixture,
                    group["records"][0]["fixture"],
                    tolerance_seconds=tolerance_seconds,
                )
            ]

            if len(candidates) > 1:
                ambiguous.append(record)
                continue

            if len(candidates) == 1:
                candidates[0]["records"].append(record)
                candidates[0]["sources"].add(source_name)
                continue

            # Do not combine multiple fixtures from the same source.
            # A later source can still match this candidate.
            groups.append({
                "records": [record],
                "sources": {source_name},
            })

    # Expose ordinary serializable source lists rather than sets.
    normalized_groups = [
        {
            "records": group["records"],
            "sources": sorted(group["sources"]),
        }
        for group in groups
    ]

    return {
        "groups": normalized_groups,
        "unmatched": unmatched,
        "ambiguous": ambiguous,
    }
