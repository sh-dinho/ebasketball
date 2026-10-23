"""Additional adaptive-backfill guarantees beyond the core backfill tests."""

from __future__ import annotations

import json
from datetime import date

import pytest

from ebasketball.ingestion import backfill
from ebasketball.ingestion.pipeline import IngestionRunSummary


def completed_fixture(event_id: str = "event") -> dict[str, object]:
    return {
        "source_event_id": event_id,
        "match_time": "2026-10-22T18:00:00Z",
        "home_handle": "Player A",
        "away_handle": "Player B",
        "status": "completed",
        "home_score": 71,
        "away_score": 66,
    }


class AdaptiveSource:
    name = "h2hgg"
    fingerprint = "adaptive-test-source-v1"

    def __init__(self, fetch):
        self.fetch = fetch
        self.calls: list[tuple[int, date, date, str | None]] = []

    def fetch_page(self, start_date, end_date, *, limit, cursor):
        span = (end_date - start_date).days
        self.calls.append((span, start_date, end_date, cursor))
        return self.fetch(span, cursor)


def test_adaptive_truncation_and_retry_exhaustion_fall_back_90_60_30():
    def fetch(span, _cursor):
        if span >= 80:
            raise backfill.ProviderTruncationError("provider truncated the range")
        if span >= 50:
            raise backfill.TransientSourceError("provider rate limited", retry_after=0)
        return backfill.BackfillPage([completed_fixture("thirty-day")], None)

    source = AdaptiveSource(fetch)
    summary = backfill.run_adaptive_backfill(
        source,
        date(2026, 7, 25),
        date(2026, 10, 23),
        max_retries=2,
        sleep=lambda _delay: None,
    )

    assert [call[0] for call in source.calls] == [89, 59, 59, 59, 29]
    assert summary.complete
    assert summary.selected_window_days == 30
    assert len(summary.fallback_history) == 2
    assert "truncated" in summary.fallback_history[0]["reason"].casefold()
    assert "provider failures remain" in summary.fallback_history[1]["reason"]


def test_window_checkpoints_are_isolated_and_resume_state_is_not_reused(
    tmp_path, monkeypatch
):
    def fetch(span, cursor):
        if span >= 80:
            if cursor is None:
                return backfill.BackfillPage([completed_fixture("partial-90")], "next")
            raise backfill.ProviderTruncationError("truncated after first page")
        return backfill.BackfillPage([completed_fixture("complete-60")], None)

    source = AdaptiveSource(fetch)
    monkeypatch.setattr(
        backfill,
        "run_ingestion",
        lambda *_args, **_kwargs: IngestionRunSummary(inserted=1),
    )
    base = tmp_path / "backfill.json"
    summary = backfill.run_adaptive_backfill(
        source,
        date(2026, 7, 25),
        date(2026, 10, 23),
        base_checkpoint_path=base,
        dry_run=False,
        sleep=lambda _delay: None,
    )

    checkpoint_90 = tmp_path / "backfill_90d.json"
    checkpoint_60 = tmp_path / "backfill_60d.json"
    assert checkpoint_90.exists()
    assert checkpoint_60.exists()
    assert not (tmp_path / "backfill_30d.json").exists()
    state_90 = json.loads(checkpoint_90.read_text())
    state_60 = json.loads(checkpoint_60.read_text())
    assert state_90["next_cursor"] == "next"
    assert state_90["complete"] is False
    assert state_60["complete"] is True
    assert state_90["start_date"] != state_60["start_date"]
    assert summary.selected_window_days == 60
    assert summary.inserted == 1


def test_checkpoint_corruption_is_not_hidden_by_adaptive_fallback(tmp_path):
    source = AdaptiveSource(lambda _span, _cursor: backfill.BackfillPage([], None))
    checkpoint = tmp_path / "backfill_90d.json"
    checkpoint.write_text("not-json", encoding="utf-8")

    with pytest.raises(backfill.BackfillError, match="checkpoint"):
        backfill.run_adaptive_backfill(
            source,
            date(2026, 7, 25),
            date(2026, 10, 23),
            base_checkpoint_path=tmp_path / "backfill.json",
            window_sizes=(90, 60, 30),
            dry_run=False,
        )
    assert source.calls == []


def test_adaptive_rejects_invalid_dates_and_does_not_call_provider():
    source = AdaptiveSource(lambda _span, _cursor: backfill.BackfillPage([], None))
    with pytest.raises(ValueError, match="on or before"):
        backfill.run_adaptive_backfill(
            source, date(2026, 10, 24), date(2026, 10, 23), dry_run=True
        )
    assert source.calls == []


def test_empty_or_provider_failed_windows_never_return_success():
    empty_source = AdaptiveSource(lambda _span, _cursor: backfill.BackfillPage([], None))
    empty = backfill.run_adaptive_backfill(
        empty_source,
        date(2026, 10, 20),
        date(2026, 10, 23),
        window_sizes=(90, 60, 30),
        dry_run=True,
    )
    assert not empty.complete
    assert empty.selected_window_days is None
    assert len(empty.fallback_history) == 3

    failed_source = AdaptiveSource(
        lambda _span, _cursor: (_ for _ in ()).throw(
            backfill.ProviderSourceError("provider unavailable")
        )
    )
    failed = backfill.run_adaptive_backfill(
        failed_source,
        date(2026, 10, 20),
        date(2026, 10, 23),
        window_sizes=(90, 60, 30),
        dry_run=True,
    )
    assert not failed.complete
    assert failed.provider_failures
    assert failed.selected_window_days is None


def test_only_out_of_range_records_do_not_count_as_historical_coverage():
    source = AdaptiveSource(
        lambda _span, _cursor: backfill.BackfillPage(
            [{**completed_fixture("before-window"), "match_time": "2026-07-25T12:00:00Z"}],
            None,
        )
    )
    summary = backfill.run_adaptive_backfill(
        source,
        date(2026, 7, 25),
        date(2026, 10, 23),
        dry_run=True,
    )
    assert summary.fetched == 1
    assert summary.normalized == 0
    assert summary.completed == 0
    assert not summary.complete
    assert summary.selected_window_days is None


def test_programming_errors_are_not_mistaken_for_provider_failures():
    source = AdaptiveSource(
        lambda _span, _cursor: (_ for _ in ()).throw(NameError("implementation bug"))
    )
    with pytest.raises(NameError, match="implementation bug"):
        backfill.run_adaptive_backfill(
            source,
            date(2026, 10, 20),
            date(2026, 10, 23),
            window_sizes=(90, 60, 30),
            dry_run=True,
        )
    assert len(source.calls) == 1


def test_cursor_cycle_stops_before_advancing_to_the_repeated_cursor():
    pages = {
        None: backfill.BackfillPage([], "next"),
        "next": backfill.BackfillPage([], "again"),
        "again": backfill.BackfillPage([], "next"),
    }
    source = AdaptiveSource(lambda _span, cursor: pages[cursor])
    summary = backfill.run_backfill(
        source, date(2026, 10, 22), date(2026, 10, 23), dry_run=True
    )
    assert len(source.calls) == 3
    assert summary.provider_failures == ["Source returned a pagination cursor cycle"]
    assert not summary.complete
