"""Offline tests for historical backfill boundaries and resumability."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from ebasketball.ingestion import backfill
from ebasketball.ingestion.pipeline import IngestionRunSummary


def h2h(event_id: str, timestamp: str, **updates):
    return {
        "externalId": event_id,
        "startDate": timestamp,
        "participantAName": "Player A",
        "participantBName": "Player B",
        "teamAName": "Team A",
        "teamBName": "Team B",
        "matchStatus": "MATCH_ENDED",
        "teamAScore": 70,
        "teamBScore": 65,
        "event_url": f"https://provider.example/events/{event_id}",
        **updates,
    }


class PageSource:
    name = "h2hgg"
    fingerprint = "fixture-fingerprint"

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def fetch_page(self, start_date, end_date, *, limit, cursor):
        self.calls.append((start_date, end_date, limit, cursor))
        value = self.pages[cursor]
        if isinstance(value, Exception):
            raise value
        return value


def test_inclusive_utc_date_boundaries_and_provider_field_mapping():
    source = PageSource({
        None: backfill.BackfillPage([
            h2h("before", "2026-07-24T23:59:59Z"),
            h2h("offset-before", "2026-07-25T01:00:00+02:00"),
            h2h("first", "2026-07-25T00:00:00Z"),
            h2h("last", "2026-10-23T23:59:59Z"),
            h2h("after", "2026-10-24T00:00:00Z"),
        ], None)
    })
    with patch.object(backfill, "run_ingestion", return_value=IngestionRunSummary(dry_run=True)) as ingest:
        summary = backfill.run_backfill(
            source, date(2026, 7, 25), date(2026, 10, 23), dry_run=True
        )
    assert summary.fetched == 5
    assert summary.normalized == 2
    assert summary.completed == 2
    assert summary.skipped == 3
    assert "2026-07-25" not in summary.unavailable_dates
    assert "2026-10-23" not in summary.unavailable_dates
    assert len(summary.unavailable_dates) == 89
    records = ingest.call_args.args[0][0].fetch_records()
    assert [record["externalId"] for record in records] == ["first", "last"]


def test_duplicate_events_across_pages_are_counted_once():
    duplicate = h2h("same", "2026-10-22T18:00:00Z")
    source = PageSource({
        None: backfill.BackfillPage([duplicate], "next"),
        "next": backfill.BackfillPage([duplicate], None),
    })
    with patch.object(backfill, "run_ingestion", return_value=IngestionRunSummary(dry_run=True)) as ingest:
        summary = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22), dry_run=True, batch_size=1
        )
    assert summary.fetched == 2
    assert summary.completed == 1
    assert summary.skipped == 1
    assert ingest.call_count == 1


@pytest.mark.parametrize("score", [-1, 12.5])
def test_invalid_scores_are_reported_without_persistence(score):
    source = PageSource({None: backfill.BackfillPage([
        h2h("invalid", "2026-10-22T18:00:00Z", teamAScore=score)
    ], None)})
    with patch.object(backfill, "run_ingestion") as ingest:
        summary = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22), dry_run=True
        )
    assert summary.failed == 1
    assert summary.completed == 0
    ingest.assert_not_called()


def test_missing_scores_and_noncompleted_status_are_reported():
    source = PageSource({None: backfill.BackfillPage([
        h2h("no-score", "2026-10-22T18:00:00Z", teamAScore=None),
        h2h("live", "2026-10-22T19:00:00Z", matchStatus="LIVE"),
    ], None)})
    with patch.object(backfill, "run_ingestion") as ingest:
        summary = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22), dry_run=True
        )
    assert summary.incomplete_records == 2
    assert summary.missing_scores == 1
    assert summary.incomplete_status_counts == {"live": 1}
    ingest.assert_not_called()


def test_transient_provider_failure_retries_and_malformed_response_is_reported():
    transient = PageSource({None: backfill.BackfillPage([], None)})
    sleeps = []
    transient.fetch_page = _fail_once(transient, TimeoutError("timeout"))
    summary = backfill.run_backfill(
        transient, date(2026, 10, 22), date(2026, 10, 22), dry_run=True,
        sleep=sleeps.append,
    )
    assert summary.complete
    assert sleeps

    malformed = PageSource({None: backfill.BackfillPage([None], None)})
    summary = backfill.run_backfill(
        malformed, date(2026, 10, 22), date(2026, 10, 22), dry_run=True
    )
    assert summary.failed == 1
    assert summary.complete


def _fail_once(source, error):
    original = source.fetch_page
    count = 0

    def fetch(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 1:
            raise error
        return original(*args, **kwargs)

    return fetch


def test_failed_page_does_not_advance_checkpoint_then_resume(tmp_path):
    checkpoint = tmp_path / "checkpoint.json"
    source = PageSource({
        None: backfill.BackfillPage([h2h("one", "2026-10-22T18:00:00Z")], "next"),
        "next": backfill.BackfillPage([h2h("two", "2026-10-22T19:00:00Z")], None),
    })
    results = iter([
        IngestionRunSummary(failed=1),
        IngestionRunSummary(inserted=1),
        IngestionRunSummary(inserted=1),
    ])
    with patch.object(backfill, "run_ingestion", side_effect=lambda *a, **k: next(results)):
        failed = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22),
            checkpoint_path=checkpoint,
        )
        assert failed.failed == 1
        assert not checkpoint.exists()
        recovered = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22),
            checkpoint_path=checkpoint,
        )
    assert recovered.inserted == 2
    assert recovered.complete
    assert recovered.batches_committed == 2
    state = json.loads(checkpoint.read_text())
    assert state["complete"] is True
    assert state["next_cursor"] is None
    assert state["summary"]["batches_committed"] == 2


def test_completed_checkpoint_makes_rerun_idempotent(tmp_path):
    checkpoint = tmp_path / "checkpoint.json"
    source = PageSource({None: backfill.BackfillPage([], None)})
    with patch.object(backfill, "run_ingestion") as ingest:
        first = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22), checkpoint_path=checkpoint
        )
        second = backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22), checkpoint_path=checkpoint
        )
    assert first.complete and second.complete
    assert source.calls == [(date(2026, 10, 22), date(2026, 10, 22), 250, None)]
    ingest.assert_not_called()


def test_checkpoint_rejects_changed_source_fingerprint(tmp_path):
    checkpoint = tmp_path / "checkpoint.json"
    original = PageSource({None: backfill.BackfillPage([], None)})
    backfill.run_backfill(
        original, date(2026, 10, 22), date(2026, 10, 22),
        checkpoint_path=checkpoint,
    )
    changed = PageSource({None: backfill.BackfillPage([], None)})
    changed.fingerprint = "changed"
    with pytest.raises(backfill.BackfillError, match="fingerprint"):
        backfill.run_backfill(
            changed, date(2026, 10, 22), date(2026, 10, 22),
            checkpoint_path=checkpoint,
        )


def test_provider_retry_exhaustion_is_not_reported_as_success():
    source = PageSource({None: backfill.TransientSourceError("busy")})
    summary = backfill.run_backfill(
        source, date(2026, 10, 22), date(2026, 10, 22), dry_run=True,
        max_retries=2, sleep=lambda _delay: None,
    )
    assert len(source.calls) == 3
    assert summary.provider_failures
    assert not summary.complete


def test_provider_error_messages_redact_credentials_and_database_urls():
    message = backfill._safe_error(RuntimeError(
        "authorization: Bearer secret-token password=topsecret "
        "postgresql://user:secret@host/database"
    ))
    assert "secret-token" not in message
    assert "topsecret" not in message
    assert "user:secret" not in message
    assert "[redacted database URL]" in message


def test_dry_run_never_connects_or_writes_checkpoint(tmp_path):
    path = tmp_path / "export.json"
    path.write_text(json.dumps([h2h("e1", "2026-10-22T18:00:00Z")]))
    checkpoint = tmp_path / "must-not-exist.json"
    with patch("ebasketball.database.get_connection", side_effect=AssertionError("DB opened")):
        summary = backfill.run_backfill(
            backfill.FileImportSource("h2hgg", path),
            date(2026, 10, 22), date(2026, 10, 22),
            dry_run=True, checkpoint_path=checkpoint,
        )
    assert summary.dry_run is True
    assert summary.complete
    assert not checkpoint.exists()


def test_csv_import_paginates_and_maps_documented_columns(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text(
        "externalId,startDate,participantAName,participantBName,teamAScore,teamBScore,matchStatus\n"
        "x,2026-10-22T18:00:00Z,A,B,11,9,MATCH_ENDED\n",
        encoding="utf-8",
    )
    source = backfill.FileImportSource("h2hgg", path)
    page = source.fetch_page(date(2026, 10, 22), date(2026, 10, 22), limit=1, cursor=None)
    assert page.next_cursor is None
    assert backfill._map_import_record("h2hgg", page.records[0])["status"] == "completed"


def test_postgres_backfill_persists_match_and_source_provenance(
    migrated_connection, tmp_path, monkeypatch
):
    """The fixture uses a UUID schema in a database explicitly named *_test."""
    from ebasketball.ingestion import pipeline

    migrated_connection.autocommit = False

    @contextmanager
    def isolated_connection():
        yield migrated_connection

    monkeypatch.setattr(pipeline, "get_connection", isolated_connection)
    source = PageSource({None: backfill.BackfillPage([
        h2h("historical-55", "2026-10-22T18:00:00Z", event_url="https://source.example/55")
    ], None)})
    summary = backfill.run_backfill(
        source, date(2026, 10, 22), date(2026, 10, 22),
        checkpoint_path=tmp_path / "checkpoint.json",
    )

    assert summary.inserted == 1
    assert summary.failed == 0
    with migrated_connection.cursor() as cursor:
        cursor.execute(
            "SELECT source, source_event_id, source_url FROM match_sources"
        )
        provenance = cursor.fetchone()
        cursor.execute("SELECT status, home_score, away_score FROM matches")
        match = cursor.fetchone()
    assert provenance["source"] == "h2hgg"
    assert provenance["source_event_id"] == "historical-55"
    assert provenance["source_url"] == "https://source.example/55"
    assert match["status"] == "completed"
    assert match["home_score"] == 70
    assert match["away_score"] == 65


def test_adaptive_backfill_falls_back_from_90_to_60_then_30():
    class AdaptiveMockSource:
        name = "h2hgg"
        fingerprint = "adaptive-fp"

        def fetch_page(self, start_date, end_date, *, limit, cursor):
            span = (end_date - start_date).days
            if span >= 80:  # 90-day window fails with truncation/error
                raise backfill.ProviderTruncationError("History depth exceeds provider limit")
            if span >= 50:  # 60-day window also fails transiently
                raise backfill.TransientSourceError("Rate limited")
            # 30-day window succeeds
            return backfill.BackfillPage([
                h2h("recent-match", "2026-10-22T18:00:00Z")
            ], None)

    source = AdaptiveMockSource()
    summary = backfill.run_adaptive_backfill(
        source,
        date(2026, 7, 25),
        date(2026, 10, 23),
        window_sizes=(90, 60, 30),
        dry_run=True,
        sleep=lambda _delay: None,
    )
    assert summary.complete
    # Verify fallback metadata was recorded in errors/summary
    adaptive_meta = [e for e in summary.errors if e.get("stage") == "adaptive_window"]
    assert adaptive_meta
    assert adaptive_meta[0]["selected_window_days"] == 30
    assert len(adaptive_meta[0]["fallback_history"]) == 2


def test_checkpoints_are_isolated_per_window(tmp_path):
    base_checkpoint = tmp_path / "chk.json"
    source = PageSource({None: backfill.BackfillPage([], None)})

    summary = backfill.run_adaptive_backfill(
        source,
        date(2026, 9, 23),
        date(2026, 10, 23),
        window_sizes=(30,),
        base_checkpoint_path=base_checkpoint,
        dry_run=False,
    )
    # Persistent attempts use the window suffix; dry runs do not create it.
    expected_path = tmp_path / "chk_30d.json"
    assert expected_path.exists()
    assert not base_checkpoint.exists()
    assert not summary.complete
    assert summary.selected_window_days is None


def test_adaptive_empty_results_never_claim_a_successful_window():
    source = PageSource({None: backfill.BackfillPage([], None)})
    summary = backfill.run_adaptive_backfill(
        source, date(2026, 9, 23), date(2026, 10, 23),
        window_sizes=(90, 60, 30), dry_run=True,
    )
    assert not summary.complete
    assert summary.selected_window_days is None
    assert len(summary.fallback_history) == 3


def test_adaptive_dry_run_does_not_create_window_checkpoints(tmp_path):
    source = PageSource({None: backfill.BackfillPage([
        h2h("one", "2026-10-22T18:00:00Z")
    ], None)})
    base = tmp_path / "backfill.json"
    summary = backfill.run_adaptive_backfill(
        source, date(2026, 10, 22), date(2026, 10, 22),
        window_sizes=(30,), base_checkpoint_path=base, dry_run=True,
    )
    assert summary.complete
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"max_retries": -1},
        {"retry_base_delay": -0.1},
    ],
)
def test_backfill_validates_batch_and_retry_configuration(kwargs):
    source = PageSource({None: backfill.BackfillPage([], None)})
    with pytest.raises(ValueError):
        backfill.run_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22),
            dry_run=True, **kwargs,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"window_sizes": ()},
        {"window_sizes": (30, 60)},
        {"window_sizes": (90, 0)},
        {"batch_size": 1001},
        {"max_retries": -1},
        {"retry_base_delay": float("nan")},
    ],
)
def test_adaptive_backfill_validates_window_and_retry_configuration(kwargs):
    source = PageSource({None: backfill.BackfillPage([], None)})
    with pytest.raises(ValueError):
        backfill.run_adaptive_backfill(
            source, date(2026, 10, 22), date(2026, 10, 22),
            dry_run=True, **kwargs,
        )
