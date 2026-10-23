from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from ebasketball.ingestion.normalize import (
    normalize_fixture,
    normalize_match_time,
    normalize_status,
)
from ebasketball.sources.aceodds import normalize_aceodds_record
from ebasketball.sources.h2hgg import normalize_h2hgg_record


@pytest.fixture
def raw_fixture():
    return {
        "event_id": "h2h-123",
        "match_time": "2026-10-22T18:30:00-06:00",
        "home_handle": "STARFIRE",
        "away_handle": "PHANTOM",
        "home_team": "Starfire",
        "away_team": "Phantom",
        "status": "Upcoming",
        "home_moneyline": "1.91",
        "away_moneyline": "2.05",
        "spread_line": "-2.5",
        "away_spread_line": "+2.5",
        "home_spread_odds": "1.90",
        "away_spread_odds": "1.95",
        "total_line": "105.5",
        "over_odds": "1.88",
        "under_odds": "2.00",
    }


def test_normalize_fixture_standardizes_timestamp(raw_fixture):
    result = normalize_fixture(raw_fixture, "h2hgg")

    assert result["match_time"].tzinfo is not None
    assert result["match_time"].utcoffset().total_seconds() == 0
    assert result["match_time"] == datetime(2026, 10, 23, 0, 30, tzinfo=timezone.utc)


def test_normalize_fixture_preserves_source_provenance(raw_fixture):
    result = normalize_h2hgg_record(raw_fixture)

    assert result["data_source"] == "h2hgg"
    assert result["source_event_id"] == "h2h-123"
    assert result["source_url"].startswith("https://h2hggl.com/")
    assert result["home_handle"] == "STARFIRE"
    assert result["away_handle"] == "PHANTOM"


def test_source_update_timestamp_and_market_exclusion_are_preserved(raw_fixture):
    raw_fixture["sourceUpdatedAt"] = "2026-10-23T08:15:00+02:00"
    result = normalize_fixture(raw_fixture, "h2hgg")
    assert result["source_updated_at"] == datetime(
        2026, 10, 23, 6, 15, tzinfo=timezone.utc
    )
    assert not any("odds" in key or "line" in key for key in result)


def test_aceodds_adapter_uses_aceodds_source(raw_fixture):
    result = normalize_aceodds_record(raw_fixture)

    assert result["data_source"] == "aceodds"
    assert "aceodds.com" in result["source_url"]


def test_normalize_fixture_ignores_legacy_market_fields(raw_fixture):
    result = normalize_fixture(raw_fixture, "h2hgg")

    assert not any("odds" in key or "line" in key for key in result)
    assert "home_moneyline" not in result


def test_missing_scores_remain_none(raw_fixture):
    result = normalize_fixture(raw_fixture, "h2hgg")

    assert result["home_score"] is None
    assert result["away_score"] is None


def test_scores_are_parsed_as_integers_without_fabrication(raw_fixture):
    raw_fixture["home_score"] = "105"
    raw_fixture["away_score"] = 98

    result = normalize_fixture(raw_fixture, "h2hgg")

    assert result["home_score"] == 105
    assert result["away_score"] == 98
    assert isinstance(result["home_score"], int)
    assert isinstance(result["away_score"], int)


@pytest.mark.parametrize("bad_score", ["-1", -1])
def test_negative_scores_are_rejected(raw_fixture, bad_score):
    raw_fixture["home_score"] = bad_score

    with pytest.raises(ValueError, match="cannot be negative"):
        normalize_fixture(raw_fixture, "h2hgg")


@pytest.mark.parametrize("fractional_score", ["105.5", 105.5, Decimal("105.5")])
def test_fractional_scores_are_rejected(raw_fixture, fractional_score):
    raw_fixture["home_score"] = fractional_score

    with pytest.raises(ValueError, match="whole numbers"):
        normalize_fixture(raw_fixture, "h2hgg")


def test_self_match_is_rejected(raw_fixture):
    raw_fixture["away_handle"] = "starfire"

    with pytest.raises(ValueError, match="must differ"):
        normalize_fixture(raw_fixture, "h2hgg")


def test_missing_required_fields_are_rejected():
    with pytest.raises(ValueError, match="match_time"):
        normalize_fixture(
            {"home_handle": "A", "away_handle": "B"},
            "h2hgg",
        )


def test_time_only_requires_board_date():
    with pytest.raises(ValueError, match="board_date"):
        normalize_match_time("18:30")

    result = normalize_match_time(
        "18:30",
        board_date=date(2026, 10, 22),
        source_timezone="America/Denver",
    )

    assert result.tzinfo is not None


def test_status_normalization():
    assert normalize_status("Upcoming") == "scheduled"
    assert normalize_status("In-Play") == "live"
    assert normalize_status("Finished") == "completed"
    assert normalize_status("Cancelled") == "cancelled"
    assert normalize_status("unrecognized value") == "unknown"


def test_unknown_source_is_rejected(raw_fixture):
    with pytest.raises(ValueError, match="Unsupported"):
        normalize_fixture(raw_fixture, "unknown_source")


def test_source_record_url_is_preserved(raw_fixture):
    raw_fixture["source_url"] = "https://example.com/event/123"

    result = normalize_fixture(raw_fixture, "h2hgg")

    assert result["source_url"] == "https://example.com/event/123"


"""Normalization tests."""
