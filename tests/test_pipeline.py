"""Tests for the source-to-persistence ingestion workflow."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from ebasketball.ingestion.pipeline import run_ingestion
from ebasketball.sources.json_file import JsonFileAdapter


BASE_TIME = datetime(2026, 10, 22, 18, 30, tzinfo=timezone.utc)


class MemoryAdapter:
    def __init__(self, name, records):
        self.name = name
        self.records = records

    def fetch_records(self):
        if isinstance(self.records, Exception):
            raise self.records
        return self.records


def record(**updates):
    return {
        "event_id": "evt-1",
        "match_time": BASE_TIME.isoformat(),
        "home_handle": "PLAYER_A",
        "away_handle": "PLAYER_B",
        "status": "Upcoming",
        "home_moneyline": "1.95",
        "away_moneyline": "1.91",
        "odds_observed_at": "2026-10-22T18:29:00Z",
        **updates,
    }


def test_multiple_sources_reconcile_and_keep_source_odds_provenance():
    h2h_record = record(
        event_id="h2h-provider-42",
        source_url="https://h2h.example/events/42",
    )
    ace_record = record(
        event_id="ace-1",
        match_time=(BASE_TIME + timedelta(seconds=30)).isoformat(),
        home_moneyline="2.10",
        odds_observed_at="2026-10-22T18:29:30Z",
        event_url="https://ace.example/matches/ace-901",
    )
    canonical_ids = {}
    resolved_ids = []

    def persist_by_canonical_fixture(fixture, _source, **_provenance):
        key = (
            fixture["match_time"],
            fixture["home_handle"].casefold(),
            fixture["away_handle"].casefold(),
        )
        inserted = key not in canonical_ids
        if inserted:
            canonical_ids[key] = 501
        resolved_ids.append(canonical_ids[key])
        return {
            "match_id": canonical_ids[key],
            "inserted": inserted,
            "updated": not inserted,
        }

    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        side_effect=persist_by_canonical_fixture,
    ) as save:
        summary = run_ingestion(
            [
                MemoryAdapter("h2hgg", [h2h_record]),
                MemoryAdapter("aceodds", [ace_record]),
            ]
        )

    assert summary.fetched == 2
    assert summary.normalized == 2
    assert summary.reconciled == 2
    assert summary.inserted == 1
    assert summary.updated == 1
    assert summary.failed == 0
    assert len(canonical_ids) == 1
    assert resolved_ids == [501, 501]
    assert [item.args[1] for item in save.call_args_list] == ["h2hgg", "aceodds"]
    saved_fixtures = [item.args[0] for item in save.call_args_list]
    assert saved_fixtures[0]["match_time"] == saved_fixtures[1]["match_time"]
    assert saved_fixtures[0]["home_moneyline"] == Decimal("1.95")
    assert saved_fixtures[1]["home_moneyline"] == Decimal("2.10")
    assert saved_fixtures[0]["odds_observed_at"] == datetime(
        2026, 10, 22, 18, 29, tzinfo=timezone.utc
    )
    assert save.call_args_list[0].kwargs["source_event_id"] == "h2h-provider-42"
    assert save.call_args_list[0].kwargs["source_url"] == "https://h2h.example/events/42"
    assert save.call_args_list[1].kwargs["source_event_id"] == "ace-1"
    assert save.call_args_list[1].kwargs["source_url"] == "https://ace.example/matches/ace-901"


@pytest.mark.parametrize("score", [-1, 1.5])
def test_invalid_scores_are_reported_as_record_failures(score):
    with patch("ebasketball.ingestion.pipeline.save_fixture") as save:
        summary = run_ingestion(
            [MemoryAdapter("h2hgg", [record(home_score=score)])]
        )
    save.assert_not_called()
    assert summary.fetched == 1
    assert summary.normalized == 0
    assert summary.failed == 1
    assert summary.errors[0]["stage"] == "normalize"


def test_invalid_timestamp_and_missing_teams_are_reported():
    inputs = [
        record(match_time="not-a-time"),
        record(home_handle=" "),
    ]
    summary = run_ingestion([MemoryAdapter("h2hgg", inputs)], dry_run=True)
    assert summary.failed == 2
    assert summary.normalized == 0
    assert summary.dry_run is True


def test_source_timeout_is_distinguished_from_record_failures():
    summary = run_ingestion(
        [MemoryAdapter("h2hgg", TimeoutError("collector timed out"))]
    )
    assert summary.fetched == 0
    assert summary.failed == 0
    assert summary.source_failures == [
        {"source": "h2hgg", "error": "TimeoutError: collector timed out"}
    ]


def test_partial_source_timeout_processes_records_already_yielded():
    def records_then_timeout():
        yield record()
        raise TimeoutError("read timed out")

    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        return_value={"inserted": True, "updated": False},
    ) as save:
        summary = run_ingestion(
            [MemoryAdapter("h2hgg", records_then_timeout())]
        )
    assert summary.fetched == 1
    assert summary.source_failures
    assert summary.inserted == 1
    save.assert_called_once()


def test_ambiguous_reconciliation_records_are_skipped_not_merged():
    first = record(event_id="h1", match_time=BASE_TIME.isoformat())
    second = record(
        event_id="h2",
        match_time=(BASE_TIME + timedelta(minutes=4)).isoformat(),
    )
    ambiguous = record(
        event_id="a1",
        match_time=(BASE_TIME + timedelta(minutes=2)).isoformat(),
    )
    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        return_value={"inserted": True, "updated": False},
    ) as save:
        summary = run_ingestion(
            [
                MemoryAdapter("h2hgg", [first, second]),
                MemoryAdapter("aceodds", [ambiguous]),
            ]
        )
    assert summary.skipped == 1
    assert summary.failed == 0
    assert save.call_count == 2
    assert [item.args[1] for item in save.call_args_list] == ["h2hgg", "h2hgg"]
    saved_times = [item.args[0]["match_time"] for item in save.call_args_list]
    assert saved_times[0] != saved_times[1]
    assert any(
        item.get("reason") == "ambiguous reconciliation candidates"
        for item in summary.errors
    )


def test_unrelated_fixtures_remain_separate():
    home_fixture = record(event_id="h2h-home")
    unrelated_fixture = record(
        event_id="ace-other",
        home_handle="PLAYER_C",
        away_handle="PLAYER_D",
        match_time=(BASE_TIME + timedelta(seconds=20)).isoformat(),
    )
    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        return_value={"inserted": True, "updated": False},
    ) as save:
        summary = run_ingestion(
            [
                MemoryAdapter("h2hgg", [home_fixture]),
                MemoryAdapter("aceodds", [unrelated_fixture]),
            ]
        )

    assert summary.reconciled == 0
    assert summary.skipped == 0
    assert save.call_count == 2
    assert [item.args[1] for item in save.call_args_list] == ["h2hgg", "aceodds"]
    assert save.call_args_list[0].args[0]["match_time"] != (
        save.call_args_list[1].args[0]["match_time"]
    )


def test_duplicate_input_is_skipped_and_repeat_run_uses_repository_idempotency():
    duplicate = record()
    adapter = MemoryAdapter("h2hgg", [duplicate, duplicate.copy()])
    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        side_effect=[
            {"inserted": True, "updated": False},
            {"inserted": False, "updated": True},
        ],
    ) as save:
        first = run_ingestion([adapter])
        second = run_ingestion([MemoryAdapter("h2hgg", [duplicate])])
    assert first.skipped == 1
    assert first.inserted == 1
    assert second.updated == 1
    assert save.call_count == 2


def test_database_failure_is_counted_per_record_and_other_records_continue():
    with patch(
        "ebasketball.ingestion.pipeline.save_fixture",
        side_effect=[
            RuntimeError("database unavailable"),
            {"inserted": True, "updated": False},
        ],
    ) as save:
        summary = run_ingestion(
            [
                MemoryAdapter(
                    "h2hgg",
                    [record(event_id="evt-a"), record(event_id="evt-b")],
                )
            ]
        )
    assert summary.failed == 1
    assert summary.inserted == 1
    assert [item["stage"] for item in summary.errors if "stage" in item] == [
        "persist"
    ]
    assert save.call_count == 2


def test_dry_run_never_calls_persistence_or_opens_connection():
    with (
        patch("ebasketball.ingestion.pipeline.save_fixture") as save,
        patch("ebasketball.ingestion.pipeline.get_connection") as get_connection,
    ):
        summary = run_ingestion(
            [MemoryAdapter("h2hgg", [record()])], dry_run=True
        )
    save.assert_not_called()
    get_connection.assert_not_called()
    assert summary.normalized == 1
    assert summary.inserted == summary.updated == 0


def test_json_file_adapter_requires_a_top_level_array(tmp_path):
    source_file = tmp_path / "records.json"
    source_file.write_text('{"record": true}', encoding="utf-8")
    adapter = JsonFileAdapter("h2hgg", source_file)
    with pytest.raises(ValueError, match="top-level array"):
        list(adapter.fetch_records())


def test_cli_runs_a_dry_run_from_json_without_database_configuration(tmp_path):
    input_path = tmp_path / "fixtures.json"
    input_path.write_text(json.dumps([record()]), encoding="utf-8")
    project_root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(project_root / "scripts" / "run_ingestion.py"),
            "--input",
            f"h2hgg={input_path}",
            "--dry-run",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    summary = json.loads(completed.stdout)
    assert summary["fetched"] == 1
    assert summary["normalized"] == 1
    assert summary["dry_run"] is True
