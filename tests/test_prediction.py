"""Tests for historical match winner analysis and chronological evaluation."""

from datetime import datetime, timedelta, timezone

from ebasketball.prediction import (
    backtest,
    calculate_features,
    load_historical_matches,
    predict_winner,
    validate_history,
)
from ebasketball.repositories.predictions import classify_result, prediction_performance

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def fixture(day, home, away, home_score, away_score):
    return {
        "match_time": BASE + timedelta(days=day),
        "home_handle": home,
        "away_handle": away,
        "home_score": home_score,
        "away_score": away_score,
        "status": "completed",
    }


def balanced_history(count=10):
    rows = []
    for index in range(count):
        # Both teams have a record against each other and independent rivals.
        rows.append(fixture(index, "A", "B", 80 + index, 70))
        rows.append(fixture(index, "B", f"R{index}", 60, 75 + index))
        rows.append(fixture(index, f"R{index}", "A", 60, 75 + index))
    return rows


def test_features_include_form_h2h_scoring_and_date_range():
    features = calculate_features(
        balanced_history(), "A", "B", before=BASE + timedelta(days=20)
    )
    assert features["team_a"]["played"] == 20
    assert features["team_b"]["played"] == 20
    assert features["team_a"]["recent"][5]["played"] == 5
    assert features["team_a"]["points_scored_avg"] is not None
    assert features["team_a"]["scoring_differential_stddev"] is not None
    assert features["team_a"]["opponent_adjusted_win_rate"] is not None
    assert features["head_to_head"]["played"] == 10
    assert features["date_range"] == {"start": "2026-01-01", "end": "2026-01-10"}


def test_prediction_has_no_uncalibrated_probabilities():
    result = predict_winner(
        balanced_history(), "A", "B", match_time=BASE + timedelta(days=20)
    )
    assert result["status"] == "predicted"
    assert result["predicted_winner"] in {"A", "B"}
    assert result["win_probabilities"] is None
    assert result["method_version"] == "descriptive-v1"
    assert "descriptive ranking" in result["explanation"]


def test_database_metrics_distinguish_prediction_from_evaluation_coverage():
    class Cursor:
        def __init__(self):
            self.queries = []
            self.result_index = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            self.queries.append((query, params))

        def fetchall(self):
            self.result_index += 1
            return []

        def fetchone(self):
            self.result_index += 1
            return {"eligible_fixtures": 4, "fixtures_with_predictions": 2}

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

    conn = Connection()
    result = prediction_performance(conn)

    assert result["eligible_fixtures"] == 4
    assert result["fixtures_with_predictions"] == 2
    assert result["prediction_coverage"] == 0.5
    assert result["prediction_coverage_denominator"] == 4
    assert result["evaluation_coverage"] == 0.0
    assert len(conn.cursor_instance.queries) == 2


def test_missing_history_and_unseen_team_return_insufficient_data():
    result = predict_winner(
        balanced_history(), "A", "NEW", match_time=BASE + timedelta(days=20)
    )
    assert result["status"] == "insufficient_data"
    assert result["predicted_winner"] is None
    assert result["win_probabilities"] is None


def test_invalid_missing_negative_and_tied_scores_are_excluded():
    rows = balanced_history()
    rows.extend(
        [
            fixture(12, "A", "B", 80, 80),
            fixture(13, "A", "B", -1, 80),
            fixture(14, "A", "B", None, 80),
            fixture(15, "A", "B", 80.5, 80),
        ]
    )
    valid, warnings = validate_history(rows)
    assert len(valid) == 30
    assert any("tied score" in warning for warning in warnings)
    assert any("invalid or missing integer scores" in warning for warning in warnings)


def test_duplicate_fixtures_count_once_and_conflicts_are_excluded():
    repeated = fixture(1, "A", "B", 90, 80)
    valid, warnings = validate_history([repeated, dict(repeated)])
    assert len(valid) == 1
    assert not warnings
    conflicting = dict(repeated, home_score=89)
    valid, warnings = validate_history([repeated, conflicting])
    assert valid == []
    assert any("Conflicting duplicate" in warning for warning in warnings)


def test_reversed_duplicate_fixture_is_counted_once():
    original = fixture(1, "A", "B", 90, 80)
    reversed_source_view = fixture(1, "B", "A", 80, 90)
    valid, warnings = validate_history([original, reversed_source_view])
    assert len(valid) == 1
    assert not warnings


def test_synthetic_fixtures_are_excluded_from_prediction_and_backtest():
    synthetic = {**fixture(1, "A", "B", 100, 10), "is_synthetic": True}
    valid, warnings = validate_history([synthetic])
    assert valid == []
    assert any("synthetic" in warning for warning in warnings)


def test_target_and_future_results_do_not_leak_into_features():
    rows = balanced_history()
    target_time = BASE + timedelta(days=20)
    future = fixture(30, "A", "B", 999, 0)
    before = calculate_features(rows, "A", "B", before=target_time)
    with_future = calculate_features(rows + [future], "A", "B", before=target_time)
    assert before == with_future
    prediction = predict_winner(
        rows + [fixture(20, "A", "B", 0, 999), future],
        "A",
        "B",
        match_time=target_time,
    )
    assert prediction["statistics"]["team_a"]["played"] == before["team_a"]["played"]


def test_backtest_is_chronological_and_reports_simple_baseline():
    rows = balanced_history()
    report = backtest(rows)
    assert report["matches_evaluated"] == len(rows)
    assert report["predictions_scored"] < len(rows)
    assert report["home_side_baseline_accuracy"] is not None
    assert report["log_loss"] is None
    assert report["brier_score"] is None
    assert all(
        prediction["matches_analyzed"] <= index
        for index, prediction in enumerate(report["predictions"])
    )
    assert report["predictions"][0]["matches_analyzed"] == 0


def test_naive_time_and_invalid_minimum_sample_are_rejected():
    try:
        predict_winner(balanced_history(), "A", "B", match_time=datetime(2026, 2, 1))
    except ValueError as exc:
        assert "timezone-aware" in str(exc)
    else:
        raise AssertionError("naive target timestamp must be rejected")
    try:
        predict_winner(balanced_history(), "A", "B", match_time=BASE, minimum_matches=0)
    except ValueError as exc:
        assert "positive integer" in str(exc)
    else:
        raise AssertionError("invalid minimum sample must be rejected")


def test_prediction_result_classification_handles_result_states_safely():
    predicted_at = BASE
    available_at = BASE + timedelta(days=1)
    common = {
        "prediction_created_at": predicted_at,
        "result_available_at": available_at,
        "home_player_id": 10,
        "away_player_id": 20,
        "predicted_winner_player_id": 10,
    }
    assert classify_result(
        **common, match_status="completed", home_score=90, away_score=80
    ) == ("correct", 10)
    assert classify_result(
        **common, match_status="completed", home_score=80, away_score=90
    ) == ("incorrect", 20)
    assert classify_result(
        **common, match_status="completed", home_score=80, away_score=80
    ) == ("void", None)
    assert classify_result(
        **common, match_status="completed", home_score=-1, away_score=80
    ) == ("unable_to_evaluate", None)
    assert classify_result(
        **common, match_status="cancelled", home_score=None, away_score=None
    ) == ("void", None)
    assert (
        classify_result(
            **common, match_status="scheduled", home_score=None, away_score=None
        )
        is None
    )


def test_result_available_before_prediction_cannot_validate_prediction():
    classification = classify_result(
        prediction_created_at=BASE + timedelta(days=2),
        result_available_at=BASE + timedelta(days=1),
        match_status="completed",
        home_score=90,
        away_score=80,
        home_player_id=10,
        away_player_id=20,
        predicted_winner_player_id=10,
    )
    assert classification == ("unable_to_evaluate", None)


def test_database_history_reader_is_read_only_and_parameterized():
    class Cursor:
        def __init__(self):
            self.query = None
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params):
            self.query, self.params = query, params

        def fetchall(self):
            return [{"home_handle": "A", "away_handle": "B"}]

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

    conn = Connection()
    cutoff = BASE + timedelta(days=3)
    result = load_historical_matches(conn, before=cutoff)
    assert result == [{"home_handle": "A", "away_handle": "B"}]
    assert "INSERT" not in conn.cursor_instance.query.upper()
    assert conn.cursor_instance.params == ("completed", cutoff)
