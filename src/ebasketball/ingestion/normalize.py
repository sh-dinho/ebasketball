"""Normalize source records into a shared eBasketball fixture format."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from math import isfinite
from typing import Any
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_SOURCE_TIMEZONE = "America/Denver"

SOURCE_URLS = {
    "h2hgg": "https://h2hggl.com/en/ebasketball/schedule",
    "aceodds": (
        "https://www.aceodds.com/bet365-live-streaming/"
        "basketball/ebasketball-h2h-gg-league-4x5mins.html"
    ),
}

FIELD_ALIASES = {
    "source_event_id": (
        "source_event_id",
        "event_id",
        "externalId",
        "fixture_id",
        "match_id",
        "eventId",
        "fixtureId",
        "id",
    ),
    "source_match_key": (
        "source_match_key",
        "source_event_key",
        "external_match_key",
        "match_key",
    ),
    "source_updated_at": (
        "source_updated_at",
        "sourceUpdatedAt",
        "updated_at",
        "lastUpdated",
    ),
    "match_time": (
        "match_time",
        "start_time",
        "scheduled_at",
        "kickoff",
        "datetime",
        "date_time",
        "startTime",
        "startDate",
    ),
    "home_handle": (
        "home_handle",
        "home_player",
        "home_name",
        "home_team",
        "home",
        "player1",
        "participant1",
        "participantAName",
    ),
    "away_handle": (
        "away_handle",
        "away_player",
        "away_name",
        "away_team",
        "away",
        "player2",
        "participant2",
        "participantBName",
    ),
    "home_team": ("home_team", "home_name", "home", "home_handle", "teamAName"),
    "away_team": ("away_team", "away_name", "away", "away_handle", "teamBName"),
    "status": ("status", "match_status", "event_status", "state", "matchStatus"),
    "home_score": ("home_score", "score_home", "homeScore", "teamAScore"),
    "away_score": ("away_score", "score_away", "awayScore", "teamBScore"),
    "source_url": ("source_url", "event_url", "url", "link"),
}

STATUS_ALIASES = {
    "scheduled": "scheduled",
    "upcoming": "scheduled",
    "not started": "scheduled",
    "notstarted": "scheduled",
    "pending": "scheduled",
    "pre match": "scheduled",
    "prematch": "scheduled",
    "live": "live",
    "in play": "live",
    "in progress": "live",
    "inplay": "live",
    "started": "live",
    "half time": "live",
    "halftime": "live",
    "finished": "completed",
    "match ended": "completed",
    "finish": "completed",
    "complete": "completed",
    "completed": "completed",
    "ended": "completed",
    "final": "completed",
    "full time": "completed",
    "fulltime": "completed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "postponed": "postponed",
    "abandoned": "unknown",
}


def _clean_text(value: Any) -> str | None:
    """Convert a populated value to normalized text."""
    if value is None:
        return None

    text = re.sub(r"\s+", " ", str(value)).strip()

    if not text or text.casefold() in {
        "none",
        "null",
        "n/a",
        "na",
        "-",
    }:
        return None

    return text


def _first(
    record: Mapping[str, Any],
    aliases: tuple[str, ...],
) -> Any:
    """Return the first populated value matching the alias list."""
    for key in aliases:
        if key not in record:
            continue

        value = record[key]

        if value is None:
            continue

        if isinstance(value, str) and not value.strip():
            continue

        return value

    return None


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().casefold() in {"true", "yes", "1"}


def _decimal(
    value: Any,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal | None:
    """Parse a finite numeric value without inventing a default."""
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, float) and not isfinite(value):
        return None

    text = _clean_text(value)

    if text is None:
        return None

    text = text.replace(",", "")

    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return None

    if not result.is_finite():
        return None

    if minimum is not None and result < minimum:
        return None

    if maximum is not None and result > maximum:
        return None

    return result


def _score(value: Any) -> int | None:
    """Parse scores as nonnegative whole numbers without truncation."""
    score = _decimal(value)

    if score is None:
        return None

    if score < 0:
        raise ValueError("Scores cannot be negative.")

    if score != score.to_integral_value():
        raise ValueError("Scores must be whole numbers.")

    if score > Decimal(100000):
        return None

    return int(score)


def normalize_status(value: Any) -> str:
    """Map source statuses to the internal status vocabulary."""
    text = _clean_text(value)

    if text is None:
        return "scheduled"

    normalized = re.sub(
        r"[\s_-]+",
        " ",
        text.casefold(),
    ).strip()

    return STATUS_ALIASES.get(normalized, "unknown")


def normalize_match_time(
    value: Any,
    *,
    source_timezone: str = DEFAULT_SOURCE_TIMEZONE,
    board_date: date | None = None,
) -> datetime:
    """Parse a match timestamp and return a UTC-aware datetime.

    Naive datetimes are interpreted in source_timezone.
    Time-only values require board_date.
    Numeric values are Unix timestamps in seconds, or milliseconds
    when their magnitude indicates milliseconds.
    """
    try:
        source_tz = ZoneInfo(source_timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Invalid source timezone: {source_timezone!r}") from exc

    if isinstance(value, bool):
        raise TypeError("Boolean values are not valid match timestamps")

    if isinstance(value, datetime):
        parsed = value

    elif isinstance(value, time):
        if board_date is None:
            raise ValueError("board_date is required for time-only match times")

        parsed = datetime.combine(board_date, value)

    elif isinstance(value, date):
        raise TypeError("A date without a time is not a complete match timestamp")

    elif isinstance(value, (int, float, Decimal)):
        try:
            timestamp = float(value)
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"Invalid numeric match timestamp: {value!r}") from exc

        if not isfinite(timestamp):
            raise ValueError(f"Invalid numeric match timestamp: {value!r}")

        if abs(timestamp) >= 100_000_000_000:
            timestamp /= 1000

        try:
            parsed = datetime.fromtimestamp(
                timestamp,
                tz=timezone.utc,
            )
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError(f"Match timestamp is out of range: {value!r}") from exc

    elif isinstance(value, str):
        text = value.strip()

        if not text:
            raise ValueError("Match timestamp cannot be empty")

        iso_text = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text

        try:
            parsed = datetime.fromisoformat(iso_text)
        except ValueError:
            parsed = None

        if parsed is None:
            if board_date is None:
                raise ValueError(
                    "Unrecognized or time-only timestamp; "
                    "provide an ISO timestamp or board_date"
                )

            parsed_time = None

            for fmt in (
                "%H:%M",
                "%H:%M:%S",
                "%I:%M %p",
                "%I:%M:%S %p",
            ):
                try:
                    parsed_time = datetime.strptime(text, fmt).time()
                    break
                except ValueError:
                    continue

            if parsed_time is None:
                raise ValueError(f"Unrecognized match timestamp: {value!r}")

            parsed = datetime.combine(board_date, parsed_time)

    else:
        raise TypeError(f"Unsupported match timestamp type: {type(value).__name__}")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=source_tz)

    return parsed.astimezone(timezone.utc)


def _resolve_source_url(
    value: Any,
    *,
    base_url: str | None = None,
) -> str | None:
    """Resolve a source URL and require a valid HTTP(S) address."""
    text = _clean_text(value)

    if text is None:
        return None

    if base_url:
        text = urljoin(base_url, text)

    try:
        parts = urlsplit(text)

        if parts.scheme.casefold() not in {"http", "https"}:
            return None

        if not parts.hostname:
            return None

        if parts.username is not None or parts.password is not None:
            return None

        _ = parts.port

    except ValueError:
        return None

    return text


def normalize_fixture(
    raw: Mapping[str, Any],
    source: str,
    *,
    source_url: str | None = None,
    source_timezone: str = DEFAULT_SOURCE_TIMEZONE,
    board_date: date | None = None,
) -> dict[str, Any]:
    """Convert a source record into the shared fixture format.

    This function is pure: it does not scrape websites, connect to
    PostgreSQL, or fabricate scores or event identifiers. Legacy market
    fields in source exports are intentionally ignored.
    """
    if not isinstance(raw, Mapping):
        raise TypeError("Fixture record must be a mapping")

    source_name = _clean_text(source)

    if source_name is None:
        raise ValueError("Source name is required")

    source_name = source_name.casefold()

    if source_name not in SOURCE_URLS:
        raise ValueError(f"Unsupported fixture source: {source!r}")

    event_id = _clean_text(_first(raw, FIELD_ALIASES["source_event_id"]))
    source_match_key = _clean_text(_first(raw, FIELD_ALIASES["source_match_key"]))
    raw_source_updated_at = _first(raw, FIELD_ALIASES["source_updated_at"])
    source_updated_at = (
        normalize_match_time(
            raw_source_updated_at,
            source_timezone=source_timezone,
            board_date=board_date,
        )
        if raw_source_updated_at is not None
        else None
    )

    raw_time = _first(raw, FIELD_ALIASES["match_time"])

    if raw_time is None:
        raise ValueError("Missing required match_time")

    match_time = normalize_match_time(
        raw_time,
        source_timezone=source_timezone,
        board_date=board_date,
    )

    home_handle = _clean_text(_first(raw, FIELD_ALIASES["home_handle"]))
    away_handle = _clean_text(_first(raw, FIELD_ALIASES["away_handle"]))

    if home_handle is None or away_handle is None:
        raise ValueError("Both home_handle and away_handle are required")

    if home_handle.casefold() == away_handle.casefold():
        raise ValueError("Home and away handles must differ")

    home_team = _clean_text(_first(raw, FIELD_ALIASES["home_team"]))
    away_team = _clean_text(_first(raw, FIELD_ALIASES["away_team"]))
    raw_status = _first(raw, FIELD_ALIASES["status"])
    if source_name == "h2hgg" and _is_true(raw.get("isCancelled")):
        raw_status = "cancelled"

    resolved_url = (
        _resolve_source_url(
            _first(raw, FIELD_ALIASES["source_url"]),
            base_url=source_url or SOURCE_URLS[source_name],
        )
        or _resolve_source_url(source_url)
        or SOURCE_URLS[source_name]
    )

    return {
        "match_time": match_time,
        "home_handle": home_handle,
        "away_handle": away_handle,
        "home_team": home_team or home_handle,
        "away_team": away_team or away_handle,
        "status": normalize_status(raw_status),
        "home_score": _score(_first(raw, FIELD_ALIASES["home_score"])),
        "away_score": _score(_first(raw, FIELD_ALIASES["away_score"])),
        "data_source": source_name,
        "source_event_id": event_id,
        "source_match_key": source_match_key,
        "source_updated_at": source_updated_at,
        "source_url": resolved_url,
    }
