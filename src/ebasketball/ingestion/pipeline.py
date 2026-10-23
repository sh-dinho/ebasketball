"""Collect, normalize, reconcile, and persist source fixtures."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from psycopg2 import Error as PsycopgError

from ebasketball.database import get_connection
from ebasketball.ingestion.normalize import normalize_fixture
from ebasketball.ingestion.reconcile import reconcile_fixtures
from ebasketball.repositories.matches import AmbiguousFixtureMatchError, upsert_match
from ebasketball.sources.interface import SourceAdapter
from ebasketball.sources.live import LiveProviderUnavailable

_SUPPORTED_SOURCES = {"h2hgg", "aceodds"}
_DATABASE_URL = re.compile(r"postgres(?:ql)?://[^\s\"']+", re.IGNORECASE)
_PASSWORD = re.compile(r"(?i)(password|passwd|pwd)(\s*[=:]\s*)[^\s,;]+")


@dataclass
class IngestionRunSummary:
    """Measured results and explicit errors for one ingestion run.

    ``fetched`` counts raw records yielded by adapters, including malformed and
    repeated inputs (also records yielded before an adapter later fails).
    ``normalized`` counts records that normalized successfully, before exact
    duplicate filtering. ``reconciled`` counts nonduplicate records assigned
    to a group containing more than one source; ambiguous records are excluded.
    ``inserted`` and ``updated`` count persisted match upserts; both stay zero
    in dry-run mode. ``skipped`` counts exact repeated inputs
    and records with ambiguous reconciliation candidates. ``failed`` counts
    individual normalization, reconciliation, or persistence failures.
    Adapter-level failures are listed in ``source_failures`` and do not
    increment ``failed`` because they do not necessarily identify a record.
    ``errors`` contains record diagnostics and explicit skip reasons.
    """

    fetched: int = 0
    normalized: int = 0
    reconciled: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    source_failures: list[dict[str, str]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable summary."""
        return asdict(self)


def _safe_error(exc: Exception) -> str:
    """Describe errors without emitting credentials or complete DSNs."""
    message = str(exc)
    message = _DATABASE_URL.sub("[redacted database URL]", message)
    message = _PASSWORD.sub(r"\1\2[redacted]", message)
    return f"{type(exc).__name__}: {message}"


def _raw_hash(raw: Mapping[str, Any]) -> str:
    serialized = json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def save_fixture(
    fixture: dict[str, Any],
    source: str = "h2hgg",
    *,
    source_event_id: str | None = None,
    source_url: str | None = None,
    source_match_key: str | None = None,
    raw_hash: str | None = None,
) -> dict[str, Any]:
    """Persist a fixture and source provenance atomically."""
    with get_connection() as conn:
        upsert_result = upsert_match(
            conn,
            fixture,
            source,
            source_event_id=source_event_id,
            source_url=source_url,
            source_match_key=source_match_key,
            raw_hash=raw_hash,
            return_status=True,
        )
        assert isinstance(upsert_result, dict)
    return {
        "match_id": upsert_result["id"],
        "inserted": upsert_result["inserted"],
        "updated": not upsert_result["inserted"],
        "source": source,
    }


def _persistable_records(
    adapters: Iterable[SourceAdapter],
    summary: IngestionRunSummary,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Collect and normalize records, retaining per-record provenance."""
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None, str]] = set()

    for adapter in adapters:
        source = str(getattr(adapter, "name", "")).strip().casefold()
        if source not in _SUPPORTED_SOURCES:
            summary.source_failures.append(
                {
                    "source": source or "<unnamed>",
                    "error": f"Unsupported or missing source adapter name: {source!r}.",
                }
            )
            continue

        raw_records: list[Any] = []
        try:
            records = adapter.fetch_records()
            for raw in records:
                raw_records.append(raw)
                summary.fetched += 1
        except (OSError, TypeError, ValueError, LiveProviderUnavailable) as exc:
            summary.source_failures.append(
                {"source": source, "error": _safe_error(exc)}
            )

        for index, raw in enumerate(raw_records):
            try:
                if not isinstance(raw, Mapping):
                    raise TypeError("Source record must be a mapping.")
                fixture = normalize_fixture(raw, source)
                fingerprint = _raw_hash(raw)
                summary.normalized += 1
            except (TypeError, ValueError) as exc:
                summary.failed += 1
                summary.errors.append(
                    {
                        "source": source,
                        "record_index": index,
                        "stage": "normalize",
                        "error": _safe_error(exc),
                    }
                )
                continue

            event_id = fixture.get("source_event_id")
            duplicate_key = (source, event_id, fingerprint)
            if duplicate_key in seen:
                summary.skipped += 1
                skipped.append(
                    {
                        "source": source,
                        "record_index": index,
                        "reason": "duplicate input record",
                    }
                )
                continue
            seen.add(duplicate_key)
            by_source[source].append(
                {
                    "source": source,
                    "fixture": fixture,
                    "raw_hash": fingerprint,
                    "record_index": index,
                }
            )

    return by_source, skipped


def run_ingestion(
    adapters: Iterable[SourceAdapter],
    *,
    dry_run: bool = False,
    tolerance_seconds: int = 300,
) -> IngestionRunSummary:
    """Run collection through persistence, isolating failures per record.

    Each saved record gets its own repository transaction. A failed source is
    recorded separately from malformed or unpersistable records. In dry-run
    mode no database connection is opened.
    """
    adapter_list = list(adapters)
    if not adapter_list:
        raise ValueError("At least one source adapter must be configured.")

    summary = IngestionRunSummary(dry_run=dry_run)
    fixtures_by_source, skipped = _persistable_records(adapter_list, summary)
    reconciliation_input = {
        source: [record["fixture"] for record in records]
        for source, records in fixtures_by_source.items()
    }

    try:
        reconciled = reconcile_fixtures(
            reconciliation_input,
            tolerance_seconds=tolerance_seconds,
        )
    except (TypeError, ValueError) as exc:
        all_records = [
            record for records in fixtures_by_source.values() for record in records
        ]
        summary.failed += len(all_records)
        summary.errors.append({"stage": "reconcile", "error": _safe_error(exc)})
        return summary

    prepared_by_fixture = {
        id(record["fixture"]): record
        for records in fixtures_by_source.values()
        for record in records
    }
    ambiguous_ids = {id(record["fixture"]) for record in reconciled["ambiguous"]}
    for ambiguous in reconciled["ambiguous"]:
        prepared = prepared_by_fixture[id(ambiguous["fixture"])]
        summary.skipped += 1
        skipped.append(
            {
                "source": prepared["source"],
                "record_index": prepared["record_index"],
                "reason": "ambiguous reconciliation candidates",
            }
        )

    summary.reconciled = sum(
        len(group["records"])
        for group in reconciled["groups"]
        if len(group["records"]) > 1
    )
    summary.errors.extend({"stage": "skip", **item} for item in skipped)

    if dry_run:
        return summary

    for group in reconciled["groups"]:
        group_records = [
            prepared_by_fixture[id(item["fixture"])]
            for item in group["records"]
            if id(item["fixture"]) not in ambiguous_ids
        ]
        if not group_records:
            continue

        canonical_record = min(
            group_records,
            key=lambda item: (
                item["fixture"]["match_time"],
                item["source"],
                item["fixture"].get("source_event_id") or "",
            ),
        )
        canonical_fixture = dict(canonical_record["fixture"])
        if (
            canonical_fixture["home_handle"].casefold()
            > canonical_fixture["away_handle"].casefold()
        ):
            for left, right in (
                ("home_handle", "away_handle"),
                ("home_team", "away_team"),
                ("home_score", "away_score"),
            ):
                canonical_fixture[left], canonical_fixture[right] = (
                    canonical_fixture.get(right),
                    canonical_fixture.get(left),
                )
        canonical = canonical_fixture["match_time"]

        for record in group_records:
            fixture = {**record["fixture"], "match_time": canonical}
            reversed_orientation = (
                fixture["home_handle"].casefold()
                == canonical_fixture["away_handle"].casefold()
                and fixture["away_handle"].casefold()
                == canonical_fixture["home_handle"].casefold()
            )
            if reversed_orientation:
                for left, right in (
                    ("home_handle", "away_handle"),
                    ("home_team", "away_team"),
                    ("home_score", "away_score"),
                ):
                    fixture[left], fixture[right] = (
                        fixture.get(right),
                        fixture.get(left),
                    )
            try:
                outcome = save_fixture(
                    fixture,
                    record["source"],
                    source_event_id=fixture.get("source_event_id"),
                    source_url=fixture.get("source_url"),
                    source_match_key=fixture.get("source_match_key"),
                    raw_hash=record["raw_hash"],
                )
                summary.inserted += int(outcome["inserted"])
                summary.updated += int(outcome["updated"])
            except AmbiguousFixtureMatchError as exc:
                summary.skipped += 1
                summary.errors.append(
                    {
                        "source": record["source"],
                        "record_index": record["record_index"],
                        "stage": "identity",
                        "error": _safe_error(exc),
                    }
                )
            except (PsycopgError, TypeError, ValueError) as exc:
                summary.failed += 1
                summary.errors.append(
                    {
                        "source": record["source"],
                        "record_index": record["record_index"],
                        "stage": "persist",
                        "error": _safe_error(exc),
                    }
                )

    return summary
