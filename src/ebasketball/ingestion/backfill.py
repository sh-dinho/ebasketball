"""Bounded historical ingestion from paged providers or JSON/CSV exports.

No live provider is implemented here. Providers may implement
``HistoricalPageSource`` after their access and response behavior is verified.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

from ebasketball.ingestion.normalize import normalize_fixture, normalize_match_time
from ebasketball.ingestion.pipeline import IngestionRunSummary, run_ingestion

MAX_BATCH_SIZE = 1000
DEFAULT_BATCH_SIZE = 250
CHECKPOINT_VERSION = 1
_DATABASE_URL = re.compile(r"postgres(?:ql)?://[^\s\"']+", re.IGNORECASE)
_SECRET = re.compile(
    r"(?i)(password|passwd|pwd|api[_-]?key|authorization)(\s*[=:]\s*)"
    r"(?:bearer\s+)?[^\s,;]+"
)


class BackfillError(RuntimeError):
    """Invalid backfill input or checkpoint state."""


class ProviderSourceError(RuntimeError):
    """A classified, non-programming failure from a provider adapter."""


class ProviderTruncationError(ProviderSourceError):
    """The provider explicitly reports depth, page, or result truncation."""


class TransientSourceError(ProviderSourceError):
    """A provider adapter can raise this for retryable service responses."""

    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class BackfillPage:
    records: Sequence[Any]
    next_cursor: str | None


class HistoricalPageSource(Protocol):
    """A source returning bounded pages for an inclusive UTC date range."""

    name: str
    fingerprint: str

    def fetch_page(
        self,
        start_date: date,
        end_date: date,
        *,
        limit: int,
        cursor: str | None,
    ) -> BackfillPage:
        """Return a page and its next opaque cursor, if any.

        Raise ``TransientSourceError`` for retryable transport/response errors,
        ``ProviderTruncationError`` when the requested history was truncated,
        and ``ProviderSourceError`` for other classified provider failures.
        Unexpected programming errors are allowed to propagate.
        """


@dataclass
class BackfillSummary:
    """Measured result of one bounded historical backfill.

    ``fetched`` counts returned raw records. ``normalized`` counts records
    normalized and inside the requested UTC date range. ``completed`` counts
    completed records with both scores. ``reconciled`` is the existing
    ingestion pipeline's count of records in multi-record canonical groups.
    ``inserted`` and ``updated`` count match upserts. ``skipped`` counts
    out-of-range records, exact duplicates, and ambiguous reconciliation
    candidates. ``failed`` counts malformed records and persistence failures.
    Incomplete statuses and completed records without scores are separately
    counted and not persisted.
    """

    source: str
    start_date: str
    end_date: str
    fetched: int = 0
    normalized: int = 0
    completed: int = 0
    reconciled: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    incomplete_records: int = 0
    missing_scores: int = 0
    batches_committed: int = 0
    unavailable_dates: list[str] = field(default_factory=list)
    incomplete_status_counts: dict[str, int] = field(default_factory=dict)
    provider_failures: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    complete: bool = False
    dry_run: bool = False
    selected_window_days: int | None = None
    fallback_history: list[dict[str, Any]] = field(default_factory=list)
    requested_dates: list[str] = field(default_factory=list)
    observed_dates: list[str] = field(default_factory=list)
    pages_fetched: int = 0
    raw_records: int = 0
    coverage_verified: bool = False
    invalid_reasons: dict[str, int] = field(default_factory=dict)
    duplicate_fixture_count: int = 0
    out_of_range_record_count: int = 0
    unique_player_handles: list[str] = field(default_factory=list)
    unique_fixture_keys: list[str] = field(default_factory=list)
    match_counts_by_day: dict[str, int] = field(default_factory=dict)
    match_counts_by_source: dict[str, int] = field(default_factory=dict)
    earliest_observed_completed_date: str | None = None
    latest_observed_completed_date: str | None = None
    coverage_status: str = "observed"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable summary."""
        result = asdict(self)
        result.update(
            {
                "requested_start_date": self.start_date,
                "requested_end_date": self.end_date,
                "total_input_records": self.fetched,
                "valid_completed_fixtures": self.completed,
                "unique_players": len(self.unique_player_handles),
                "unique_fixtures": len(self.unique_fixture_keys),
                "missing_calendar_dates": self.unavailable_dates,
                "source_coverage": {
                    source: {
                        "observed_completed_fixtures": count,
                        "coverage_verified": False,
                    }
                    for source, count in self.match_counts_by_source.items()
                },
                "freshness_days": (
                    max(
                        0,
                        (
                            datetime.now(timezone.utc).date()
                            - date.fromisoformat(self.latest_observed_completed_date)
                        ).days,
                    )
                    if self.latest_observed_completed_date
                    else None
                ),
            }
        )
        return result


def _date_window(start_date: date, end_date: date) -> list[date]:
    if not isinstance(start_date, date) or isinstance(start_date, datetime):
        raise TypeError("start_date must be a date, not a datetime")
    if not isinstance(end_date, date) or isinstance(end_date, datetime):
        raise TypeError("end_date must be a date, not a datetime")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    return [
        date.fromordinal(day)
        for day in range(start_date.toordinal(), end_date.toordinal() + 1)
    ]


def _safe_error(exc: Exception) -> str:
    """Describe errors without exposing credentials or complete database URLs."""
    message = _DATABASE_URL.sub("[redacted database URL]", str(exc))
    message = _SECRET.sub(r"\1\2[redacted]", message)
    return f"{type(exc).__name__}: {message}"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().casefold() in {"true", "yes", "1"}


def _map_import_record(source: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """Map the published H2HGG schedule example fields for offline imports."""
    if source != "h2hgg" or not any(
        key in raw for key in ("externalId", "startDate", "participantAName")
    ):
        return dict(raw)
    status = raw.get("matchStatus")
    if _truthy(raw.get("isCancelled")):
        status = "cancelled"
    elif status is None or not str(status).strip():
        status = "scheduled"
    elif str(status).strip().casefold() in {"match_ended", "match ended"}:
        status = "completed"
    return {
        **raw,
        "source_event_id": raw.get("externalId"),
        "match_time": raw.get("startDate"),
        "home_handle": raw.get("participantAName"),
        "away_handle": raw.get("participantBName"),
        "home_team": raw.get("teamAName"),
        "away_team": raw.get("teamBName"),
        "home_score": raw.get("teamAScore"),
        "away_score": raw.get("teamBScore"),
        "status": status,
        "source_url": raw.get("source_url") or raw.get("event_url") or raw.get("url"),
    }


class FileImportSource:
    """Page JSON-array or CSV exports through the provider interface."""

    def __init__(self, name: str, path: str | Path):
        self.name = name.strip().casefold()
        self.path = Path(path)
        if self.name not in {"h2hgg", "aceodds"}:
            raise ValueError(f"Unsupported import source: {name!r}")
        if self.path.suffix.casefold() not in {".json", ".csv"}:
            raise ValueError("Backfill imports must be .json or .csv files")
        self.fingerprint = hashlib.sha256(self.path.read_bytes()).hexdigest()
        self._records: list[Any] | None = None

    def _load_records(self) -> list[Any]:
        if self._records is not None:
            return self._records
        if self.path.suffix.casefold() == ".csv":
            with self.path.open(encoding="utf-8", newline="") as input_file:
                self._records = list(csv.DictReader(input_file))
            return self._records
        with self.path.open(encoding="utf-8") as input_file:
            payload = json.load(input_file)
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, Mapping) and isinstance(payload.get("games"), list):
            records = payload["games"]
        elif (
            isinstance(payload, Mapping)
            and isinstance(payload.get("data"), Mapping)
            and isinstance(payload["data"].get("games"), list)
        ):
            records = payload["data"]["games"]
        else:
            raise BackfillError(
                "JSON import must be an array or contain a games array."
            )
        self._records = records
        return records

    def fetch_page(
        self,
        start_date: date,
        end_date: date,
        *,
        limit: int,
        cursor: str | None,
    ) -> BackfillPage:
        del start_date, end_date  # UTC filtering uses normalized record timestamps.
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= MAX_BATCH_SIZE
        ):
            raise ValueError(f"limit must be between 1 and {MAX_BATCH_SIZE}")
        try:
            offset = int(cursor or 0)
        except (TypeError, ValueError) as exc:
            raise BackfillError("Import cursor must be a nonnegative integer") from exc
        if offset < 0:
            raise BackfillError("Import cursor must be a nonnegative integer")
        records = self._load_records()
        page = records[offset : offset + limit]
        next_offset = offset + len(page)
        next_cursor = str(next_offset) if next_offset < len(records) else None
        return BackfillPage(page, next_cursor)


def _retry_fetch(
    source: HistoricalPageSource,
    start_date: date,
    end_date: date,
    *,
    limit: int,
    cursor: str | None,
    max_retries: int,
    base_delay: float,
    sleep: Callable[[float], None],
) -> BackfillPage:
    for attempt in range(max_retries + 1):
        try:
            return source.fetch_page(start_date, end_date, limit=limit, cursor=cursor)
        except (TransientSourceError, TimeoutError, ConnectionError) as exc:
            if attempt >= max_retries:
                raise
            retry_after = getattr(exc, "retry_after", None)
            delay = min(300.0, base_delay * (2 ** min(attempt, 32)))
            if retry_after is not None:
                try:
                    candidate = float(retry_after)
                except (TypeError, ValueError, OverflowError):
                    candidate = math.nan
                if math.isfinite(candidate) and candidate >= 0:
                    delay = min(300.0, candidate)
            sleep(delay)
    raise AssertionError("retry loop exhausted unexpectedly")


def _load_checkpoint(
    path: Path | None,
    *,
    source: HistoricalPageSource,
    start_date: date,
    end_date: date,
) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillError("Could not read backfill checkpoint") from exc
    if not isinstance(state, Mapping):
        raise BackfillError("Backfill checkpoint must contain a JSON object")
    expected = {
        "version": CHECKPOINT_VERSION,
        "source": source.name,
        "fingerprint": source.fingerprint,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    for key, value in expected.items():
        if state.get(key) != value:
            raise BackfillError(
                f"Checkpoint {key} does not match this source and date range"
            )
    if not isinstance(state.get("summary"), Mapping):
        raise BackfillError("Checkpoint summary is missing or invalid")
    cursor = state.get("next_cursor")
    complete = state.get("complete")
    if not isinstance(complete, bool) or (
        cursor is not None and not isinstance(cursor, str)
    ):
        raise BackfillError("Checkpoint pagination state is invalid")
    if complete != (cursor is None):
        raise BackfillError("Checkpoint completion and cursor state disagree")
    if not isinstance(state.get("seen_keys"), list) or any(
        not isinstance(key, str) for key in state["seen_keys"]
    ):
        raise BackfillError("Checkpoint duplicate state is invalid")
    if not isinstance(state.get("covered_dates"), list):
        raise BackfillError("Checkpoint coverage state is invalid")
    for day_text in state["covered_dates"]:
        if not isinstance(day_text, str):
            raise BackfillError("Checkpoint coverage state is invalid")
        try:
            day = date.fromisoformat(day_text)
        except ValueError as exc:
            raise BackfillError("Checkpoint coverage date is invalid") from exc
        if day.isoformat() != day_text or not start_date <= day <= end_date:
            raise BackfillError(
                "Checkpoint coverage date is outside the requested range"
            )
    return dict(state)


def _save_checkpoint(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_name = temporary_file.name
            json.dump(state, temporary_file, sort_keys=True)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


class _BatchAdapter:
    def __init__(self, name: str, records: Sequence[Mapping[str, Any]]):
        self.name = name
        self._records = records

    def fetch_records(self) -> Sequence[Mapping[str, Any]]:
        return self._records


def _new_summary(
    source: HistoricalPageSource, start_date: date, end_date: date, *, dry_run: bool
) -> BackfillSummary:
    return BackfillSummary(
        source=source.name,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        dry_run=dry_run,
        requested_dates=[day.isoformat() for day in _date_window(start_date, end_date)],
    )


def _checkpoint_state(
    source: HistoricalPageSource,
    start_date: date,
    end_date: date,
    summary: BackfillSummary,
    *,
    next_cursor: str | None,
    seen_keys: set[str],
    covered_dates: set[str],
) -> dict[str, Any]:
    return {
        "version": CHECKPOINT_VERSION,
        "source": source.name,
        "fingerprint": source.fingerprint,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "next_cursor": next_cursor,
        "complete": next_cursor is None,
        "seen_keys": sorted(seen_keys),
        "covered_dates": sorted(covered_dates),
        "summary": summary.to_dict(),
    }


def _restore_summary(state: Mapping[str, Any], *, dry_run: bool) -> BackfillSummary:
    try:
        data = dict(state["summary"])
        data["dry_run"] = dry_run
        data = {
            key: value
            for key, value in data.items()
            if key in BackfillSummary.__dataclass_fields__
        }
        return BackfillSummary(**data)
    except (TypeError, KeyError) as exc:
        raise BackfillError("Backfill checkpoint summary is incompatible") from exc


def run_backfill(
    source: HistoricalPageSource,
    start_date: date,
    end_date: date,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    checkpoint_path: str | Path | None = None,
    max_retries: int = 3,
    retry_base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    progress: Callable[[Mapping[str, Any]], None] | None = None,
) -> BackfillSummary:
    """Ingest completed fixtures in inclusive UTC date bounds.

    A persistent run requires a checkpoint path. Each page is persisted via
    the existing ingestion pipeline and the checkpoint advances only when all
    writes on that page succeed. Dry runs never connect to PostgreSQL and
    never read or write checkpoint state.
    """
    dates = _date_window(start_date, end_date)
    if source.name not in {"h2hgg", "aceodds"}:
        raise ValueError("source.name must be 'h2hgg' or 'aceodds'")
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or not 1 <= batch_size <= MAX_BATCH_SIZE
    ):
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    if (
        not isinstance(max_retries, int)
        or isinstance(max_retries, bool)
        or max_retries < 0
    ):
        raise ValueError("max_retries must be a nonnegative integer")
    if (
        isinstance(retry_base_delay, bool)
        or not isinstance(retry_base_delay, (int, float))
        or not math.isfinite(retry_base_delay)
        or retry_base_delay < 0
    ):
        raise ValueError("retry_base_delay must be a finite nonnegative number")
    checkpoint = Path(checkpoint_path) if checkpoint_path is not None else None
    if not dry_run and checkpoint is None:
        raise ValueError("checkpoint_path is required for a persistent backfill")

    state = (
        None
        if dry_run
        else _load_checkpoint(
            checkpoint, source=source, start_date=start_date, end_date=end_date
        )
    )
    summary = (
        _restore_summary(state, dry_run=dry_run)
        if state
        else _new_summary(source, start_date, end_date, dry_run=dry_run)
    )
    cursor = state.get("next_cursor") if state else None
    if state and state.get("complete"):
        summary.complete = True
        covered_dates = set(state.get("covered_dates", []))
        summary.observed_dates = sorted(covered_dates)
        summary.unavailable_dates = [
            day.isoformat() for day in dates if day.isoformat() not in covered_dates
        ]
        return summary
    if state and cursor is None:
        raise BackfillError("Incomplete checkpoint is missing its pagination cursor")
    seen_keys = set(state.get("seen_keys", [])) if state else set()
    covered_dates = set(state.get("covered_dates", [])) if state else set()
    visited_cursors: set[str | None] = set()

    while True:
        if cursor in visited_cursors:
            message = "Source returned a pagination cursor cycle"
            summary.provider_failures.append(message)
            summary.errors.append({"stage": "pagination", "error": message})
            break
        visited_cursors.add(cursor)
        try:
            page = _retry_fetch(
                source,
                start_date,
                end_date,
                limit=batch_size,
                cursor=cursor,
                max_retries=max_retries,
                base_delay=retry_base_delay,
                sleep=sleep,
            )
        except ProviderTruncationError as exc:
            message = _safe_error(exc)
            summary.provider_failures.append(message)
            summary.errors.append(
                {"stage": "fetch", "error": message, "truncated": True}
            )
            break
        except (
            ProviderSourceError,
            TimeoutError,
            ConnectionError,
            OSError,
            json.JSONDecodeError,
            BackfillError,
        ) as exc:
            message = _safe_error(exc)
            summary.provider_failures.append(message)
            summary.errors.append({"stage": "fetch", "error": message})
            break
        if not isinstance(page, BackfillPage):
            message = "Source returned an invalid page response"
            summary.provider_failures.append(message)
            summary.errors.append({"stage": "fetch", "error": message})
            break
        if (
            not isinstance(page.records, Sequence)
            or isinstance(page.records, (str, bytes))
            or (page.next_cursor is not None and not isinstance(page.next_cursor, str))
        ):
            message = "Source returned malformed page records or cursor"
            summary.provider_failures.append(message)
            summary.errors.append({"stage": "fetch", "error": message})
            break
        if page.next_cursor is not None and page.next_cursor in visited_cursors:
            message = "Source returned a pagination cursor cycle"
            summary.provider_failures.append(message)
            summary.errors.append({"stage": "pagination", "error": message})
            break

        summary.fetched += len(page.records)
        summary.raw_records += len(page.records)
        summary.pages_fetched += 1
        persistable: list[Mapping[str, Any]] = []
        page_start_index = summary.fetched - len(page.records)
        for index, raw_record in enumerate(page.records):
            record_index = page_start_index + index
            if not isinstance(raw_record, Mapping):
                summary.failed += 1
                summary.invalid_reasons["record_not_object"] = (
                    summary.invalid_reasons.get("record_not_object", 0) + 1
                )
                summary.errors.append(
                    {
                        "stage": "normalize",
                        "record_index": record_index,
                        "error": "TypeError: Source record must be a mapping",
                    }
                )
                continue
            raw = _map_import_record(source.name, raw_record)
            try:
                if raw.get("match_time") is not None:
                    timestamp = normalize_match_time(raw["match_time"])
                    match_date = timestamp.date()
                    if match_date < start_date or match_date > end_date:
                        summary.skipped += 1
                        summary.out_of_range_record_count += 1
                        summary.errors.append(
                            {
                                "stage": "skip",
                                "reason": "outside requested UTC date range",
                                "match_date": match_date.isoformat(),
                            }
                        )
                        continue
                    covered_dates.add(match_date.isoformat())
                fixture = normalize_fixture(raw, source.name)
            except (TypeError, ValueError) as exc:
                summary.failed += 1
                reason_key = type(exc).__name__
                summary.invalid_reasons[reason_key] = (
                    summary.invalid_reasons.get(reason_key, 0) + 1
                )
                summary.errors.append(
                    {
                        "stage": "normalize",
                        "record_index": record_index,
                        "error": _safe_error(exc),
                    }
                )
                continue
            match_date = fixture["match_time"].date()
            if match_date < start_date or match_date > end_date:
                summary.skipped += 1
                summary.out_of_range_record_count += 1
                summary.errors.append(
                    {
                        "stage": "skip",
                        "reason": "outside requested UTC date range",
                        "match_date": match_date.isoformat(),
                    }
                )
                continue
            covered_dates.add(match_date.isoformat())
            summary.normalized += 1
            fingerprint = hashlib.sha256(
                json.dumps(
                    raw_record, sort_keys=True, default=str, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest()
            event_id = fixture.get("source_event_id")
            seen_key = f"{event_id or ''}:{fingerprint}"
            if seen_key in seen_keys:
                summary.skipped += 1
                summary.duplicate_fixture_count += 1
                summary.errors.append(
                    {
                        "stage": "skip",
                        "reason": "duplicate event",
                        "event_id": event_id,
                    }
                )
                continue
            seen_keys.add(seen_key)
            if fixture["status"] != "completed":
                summary.incomplete_records += 1
                status = fixture["status"]
                summary.incomplete_status_counts[status] = (
                    summary.incomplete_status_counts.get(status, 0) + 1
                )
                summary.errors.append(
                    {
                        "stage": "incomplete",
                        "status": status,
                        "event_id": event_id,
                    }
                )
                continue
            if fixture["home_score"] is None or fixture["away_score"] is None:
                summary.incomplete_records += 1
                summary.missing_scores += 1
                summary.errors.append(
                    {
                        "stage": "incomplete",
                        "reason": "completed event is missing one or more scores",
                        "event_id": event_id,
                    }
                )
                continue
            if fixture["home_score"] == fixture["away_score"]:
                summary.failed += 1
                summary.invalid_reasons["tied_completed_score"] = (
                    summary.invalid_reasons.get("tied_completed_score", 0) + 1
                )
                summary.errors.append(
                    {
                        "stage": "validate",
                        "reason": "completed basketball fixture has a tied score",
                        "event_id": event_id,
                    }
                )
                continue
            home_key = fixture["home_handle"].strip().casefold()
            away_key = fixture["away_handle"].strip().casefold()
            ordered_players = sorted((home_key, away_key))
            fixture_key = (
                f"{fixture['match_time'].isoformat()}|"
                f"{ordered_players[0]}|{ordered_players[1]}"
            )
            if fixture_key in summary.unique_fixture_keys:
                summary.duplicate_fixture_count += 1
                summary.skipped += 1
                summary.errors.append(
                    {
                        "stage": "duplicate",
                        "fixture_key": fixture_key,
                        "reason": "duplicate canonical fixture",
                    }
                )
                continue
            summary.unique_fixture_keys.append(fixture_key)
            for player in ordered_players:
                if player not in summary.unique_player_handles:
                    summary.unique_player_handles.append(player)
            day_key = match_date.isoformat()
            summary.match_counts_by_day[day_key] = (
                summary.match_counts_by_day.get(day_key, 0) + 1
            )
            summary.match_counts_by_source[source.name] = (
                summary.match_counts_by_source.get(source.name, 0) + 1
            )
            if summary.earliest_observed_completed_date is None:
                summary.earliest_observed_completed_date = day_key
            if summary.latest_observed_completed_date is None:
                summary.latest_observed_completed_date = day_key
            else:
                summary.earliest_observed_completed_date = min(
                    summary.earliest_observed_completed_date, day_key
                )
                summary.latest_observed_completed_date = max(
                    summary.latest_observed_completed_date, day_key
                )
            summary.completed += 1
            persistable.append(raw)

        page_result = IngestionRunSummary(dry_run=dry_run)
        if persistable:
            page_result = run_ingestion(
                [_BatchAdapter(source.name, persistable)], dry_run=dry_run
            )
            summary.inserted += page_result.inserted
            summary.updated += page_result.updated
            summary.reconciled += page_result.reconciled
            summary.skipped += page_result.skipped
            summary.failed += page_result.failed
            summary.errors.extend(page_result.errors)
        page_succeeded = page_result.failed == 0 and not page_result.source_failures
        # A malformed record is reported but does not block advancing past it;
        # otherwise one irreparable source record could prevent all later pages.
        if not dry_run and page_succeeded and checkpoint is not None:
            summary.batches_committed += 1
            summary.observed_dates = sorted(covered_dates)
            if page.next_cursor is None:
                summary.complete = True
                summary.unavailable_dates = [
                    day.isoformat()
                    for day in dates
                    if day.isoformat() not in covered_dates
                ]
            _save_checkpoint(
                checkpoint,
                _checkpoint_state(
                    source,
                    start_date,
                    end_date,
                    summary,
                    next_cursor=page.next_cursor,
                    seen_keys=seen_keys,
                    covered_dates=covered_dates,
                ),
            )
        elif not dry_run:
            summary.errors.append(
                {
                    "stage": "checkpoint",
                    "error": "Page was not checkpointed because persistence failed",
                }
            )
            break
        elif page.next_cursor is None:
            summary.complete = True
            summary.unavailable_dates = [
                day.isoformat() for day in dates if day.isoformat() not in covered_dates
            ]
        summary.observed_dates = sorted(covered_dates)

        if progress is not None:
            progress(
                {
                    "source": source.name,
                    "cursor": page.next_cursor,
                    "fetched": summary.fetched,
                    "normalized": summary.normalized,
                    "completed": summary.completed,
                    "reconciled": summary.reconciled,
                    "batches_committed": summary.batches_committed,
                    "dry_run": dry_run,
                }
            )
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    return summary


def run_adaptive_backfill(
    source: HistoricalPageSource,
    target_start_date: date,
    end_date: date,
    *,
    window_sizes: Sequence[int] = (90, 60, 30),
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = True,
    base_checkpoint_path: str | Path | None = None,
    max_retries: int = 3,
    retry_base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    progress: Callable[[Mapping[str, Any]], None] | None = None,
) -> BackfillSummary:
    """Try the largest historical window, falling back on incomplete runs.

    Window lengths are inclusive. Persistent checkpoint files are isolated by
    suffix (for example ``backfill_90d.json``); dry runs never touch them.
    An empty result is not treated as verified coverage.
    """
    _date_window(target_start_date, end_date)
    if (
        not isinstance(window_sizes, Sequence)
        or isinstance(window_sizes, (str, bytes))
        or not window_sizes
    ):
        raise ValueError("window_sizes must be a nonempty sequence")
    if any(
        not isinstance(days, int) or isinstance(days, bool) or days <= 0
        for days in window_sizes
    ):
        raise ValueError("window_sizes must contain positive integers")
    if any(left <= right for left, right in pairwise(window_sizes)):
        raise ValueError("window_sizes must be strictly decreasing")
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or not 1 <= batch_size <= MAX_BATCH_SIZE
    ):
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    if (
        not isinstance(max_retries, int)
        or isinstance(max_retries, bool)
        or max_retries < 0
    ):
        raise ValueError("max_retries must be a nonnegative integer")
    if (
        isinstance(retry_base_delay, bool)
        or not isinstance(retry_base_delay, (int, float))
        or not math.isfinite(retry_base_delay)
        or retry_base_delay < 0
    ):
        raise ValueError("retry_base_delay must be a finite nonnegative number")

    failures: list[dict[str, Any]] = []
    last_summary: BackfillSummary | None = None
    for days in window_sizes:
        current_start = max(target_start_date, end_date - timedelta(days=days - 1))
        checkpoint_path = None
        if base_checkpoint_path is not None:
            base = Path(base_checkpoint_path)
            checkpoint_path = base.with_name(f"{base.stem}_{days}d{base.suffix}")
        try:
            summary = run_backfill(
                source,
                current_start,
                end_date,
                batch_size=batch_size,
                dry_run=dry_run,
                checkpoint_path=checkpoint_path,
                max_retries=max_retries,
                retry_base_delay=retry_base_delay,
                sleep=sleep,
                progress=progress,
            )
        except ProviderSourceError as exc:
            failures.append({"window_days": days, "reason": _safe_error(exc)})
            continue
        last_summary = summary
        reason: str | None = None
        if summary.provider_failures:
            reason = "provider failures remain: " + "; ".join(summary.provider_failures)
        elif not summary.complete:
            reason = "Backfill did not complete"
        elif summary.completed == 0:
            reason = (
                "Provider returned no in-range completed fixtures with scores; "
                "coverage is unverified"
            )
        elif summary.failed:
            reason = "One or more records failed validation or persistence"
        if reason is not None:
            failures.append({"window_days": days, "reason": reason})
            continue
        summary.selected_window_days = days
        summary.fallback_history = list(failures)
        summary.errors.append(
            {
                "stage": "adaptive_window",
                "selected_window_days": days,
                "fallback_history": list(failures),
            }
        )
        return summary

    if last_summary is None:
        last_summary = _new_summary(
            source, target_start_date, end_date, dry_run=dry_run
        )
        last_summary.provider_failures.extend(str(item["reason"]) for item in failures)
    last_summary.complete = False
    last_summary.selected_window_days = None
    last_summary.fallback_history = list(failures)
    # Do not erase provider failures from the final attempt. If the last
    # attempt was an empty response, explicitly report why no window succeeded.
    last_summary.errors.append(
        {
            "stage": "adaptive_window",
            "fallback_history": list(failures),
            "reason": "No configured window established a complete, nonempty result",
        }
    )
    return last_summary
