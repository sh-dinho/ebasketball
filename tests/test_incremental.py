"""Recent-window filtering and CLI behavior for incremental ingestion."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import patch

import pytest

from ebasketball.ingestion.pipeline import IngestionRunSummary
from scripts import run_incremental


def test_timestamp_filter_handles_canonical_and_h2hgg_utc_offsets():
    assert run_incremental._parse_match_date(
        {"match_time": "2026-10-23T00:30:00+02:00"}
    ) == date(2026, 10, 22)
    assert run_incremental._parse_match_date(
        {"startDate": "2026-10-22T23:30:00-02:00"}
    ) == date(2026, 10, 23)


def test_filter_includes_cutoff_and_today_and_retains_invalid_records():
    rows = [
        {"match_time": "2026-10-19T23:59:59Z"},
        {"startDate": "2026-10-20T00:00:00Z"},
        {"match_time": "2026-10-22T23:59:59Z"},
        {"match_time": "2026-10-23T00:00:00Z"},
        {"match_time": "bad-time"},
        {"home_handle": "undated"},
        None,
    ]
    filtered, excluded, cutoff = run_incremental.filter_recent_records(
        rows, overlap_days=2, today_utc=date(2026, 10, 22)
    )
    assert cutoff == date(2026, 10, 20)
    assert excluded == 2
    assert filtered == rows[1:3] + rows[4:]


def test_zero_overlap_means_today_and_negative_is_rejected():
    rows = [
        {"startDate": "2026-10-21T23:00:00Z"},
        {"startDate": "2026-10-22T00:00:00Z"},
        {"startDate": "2026-10-23T00:00:00Z"},
        {"startDate": "invalid"},
    ]
    filtered, excluded, cutoff = run_incremental.filter_recent_records(
        rows, overlap_days=0, today_utc=date(2026, 10, 22)
    )
    assert cutoff == date(2026, 10, 22)
    assert filtered == rows[1:2] + rows[3:]
    assert excluded == 2
    with pytest.raises(ValueError, match="nonnegative"):
        run_incremental.filter_recent_records(
            rows, overlap_days=-1, today_utc=date(2026, 10, 22)
        )


def test_incremental_cli_defaults_to_preview_and_reports_json(capsys):
    summary = IngestionRunSummary(dry_run=True)
    with (
        patch.object(run_incremental, "JsonFileAdapter") as adapter_type,
        patch.object(run_incremental, "run_ingestion", return_value=summary) as ingest,
        patch.object(run_incremental, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value.date.return_value = date(2026, 10, 22)
        adapter_type.return_value.fetch_records.return_value = [
            {"startDate": "2026-10-22T00:00:00Z"},
            {"startDate": "malformed"},
            {"home_handle": "missing time"},
        ]
        assert (
            run_incremental.main(
                ["--source", "h2hgg", "--input", "export.json", "--overlap-days", "2"]
            )
            == 0
        )
    ingest.assert_called_once()
    assert ingest.call_args.kwargs["dry_run"] is True
    assert len(ingest.call_args.args[0][0].fetch_records()) == 3
    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is True
    assert output["incremental"]["cutoff_date"] == "2026-10-20"
    assert output["incremental"]["filtered_out"] == 0


def test_h2h_start_date_and_fields_are_accepted_by_ingestion_normalizer(capsys):
    with (
        patch.object(run_incremental, "JsonFileAdapter") as adapter_type,
        patch.object(run_incremental, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value.date.return_value = date(2026, 10, 22)
        adapter_type.return_value.fetch_records.return_value = [
            {
                "externalId": "h2h-1",
                "startDate": "2026-10-22T18:00:00Z",
                "participantAName": "Player A",
                "participantBName": "Player B",
                "teamAName": "Team A",
                "teamBName": "Team B",
                "teamAScore": 70,
                "teamBScore": 65,
                "matchStatus": "MATCH_ENDED",
            }
        ]
        assert run_incremental.main(["--source", "h2hgg", "--input", "x.json"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["normalized"] == 1
    assert output["failed"] == 0


def test_incremental_cli_rejects_negative_overlap(capsys):
    with pytest.raises(SystemExit) as exc:
        run_incremental.main(
            ["--source", "h2hgg", "--input", "export.json", "--overlap-days", "-1"]
        )
    assert exc.value.code == 2


def test_incremental_persistence_requires_explicit_flag(capsys):
    with (
        patch.object(run_incremental, "JsonFileAdapter") as adapter_type,
        patch.object(
            run_incremental, "run_ingestion", return_value=IngestionRunSummary()
        ) as ingest,
        patch.object(run_incremental, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value.date.return_value = date(2026, 10, 22)
        adapter_type.return_value.fetch_records.return_value = []
        assert (
            run_incremental.main(
                ["--source", "h2hgg", "--input", "x.json", "--persist"]
            )
            == 0
        )
    assert ingest.call_args.kwargs["dry_run"] is False
    assert json.loads(capsys.readouterr().out)["incremental"]["persist_requested"]


def test_incremental_exit_codes_distinguish_validation_and_ingestion_failures(capsys):
    with (
        patch.object(run_incremental, "JsonFileAdapter") as adapter_type,
        patch.object(
            run_incremental,
            "run_ingestion",
            return_value=IngestionRunSummary(
                failed=1, errors=[{"stage": "normalize", "error": "bad score"}]
            ),
        ),
        patch.object(run_incremental, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value.date.return_value = date(2026, 10, 22)
        adapter_type.return_value.fetch_records.return_value = [{"match_time": "bad"}]
        assert run_incremental.main(["--source", "h2hgg", "--input", "x.json"]) == 1
        assert json.loads(capsys.readouterr().out)["failed"] == 1

    with (
        patch.object(run_incremental, "JsonFileAdapter") as adapter_type,
        patch.object(
            run_incremental,
            "run_ingestion",
            return_value=IngestionRunSummary(
                source_failures=[{"source": "h2hgg", "error": "provider failed"}]
            ),
        ),
        patch.object(run_incremental, "datetime") as datetime_mock,
    ):
        datetime_mock.now.return_value.date.return_value = date(2026, 10, 22)
        adapter_type.return_value.fetch_records.return_value = []
        assert run_incremental.main(["--source", "h2hgg", "--input", "x.json"]) == 3
