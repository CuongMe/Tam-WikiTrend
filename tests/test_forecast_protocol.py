import json
from datetime import datetime
from pathlib import Path

from spark_jobs.forecast_evaluation import make_folds


def test_make_folds_uses_fixed_rolling_windows() -> None:
    folds = make_folds(
        datetime(2026, 8, 1),
        datetime(2026, 8, 2, 12),
        training_window_hours=24,
        horizon_hours=1,
        stride_hours=6,
    )

    assert len(folds) == 2
    assert folds[0].origin_hour == datetime(2026, 8, 2, 1)
    assert folds[0].training_start_hour == datetime(2026, 8, 1)
    assert folds[0].training_end_hour == datetime(2026, 8, 2)
    assert folds[1].origin_hour == datetime(2026, 8, 2, 7)
    assert folds[1].training_start_hour == datetime(2026, 8, 1, 6)


def test_make_folds_does_not_create_a_future_training_end() -> None:
    folds = make_folds(
        datetime(2026, 8, 1),
        datetime(2026, 8, 1, 23),
        training_window_hours=12,
        horizon_hours=1,
        stride_hours=1,
    )

    assert folds[-1].training_end_hour < folds[-1].origin_hour


def test_shared_manifest_separates_development_and_final_holdout() -> None:
    manifest_path = Path(__file__).parents[1] / "configs" / "forecast_fold_manifest_v2.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["manifest_version"] == 2
    assert len(payload["outer_blocks"]) >= 5
    assert payload["final_holdout"]["untouched"] is True
    outer_end = max(
        datetime.fromisoformat(block["evaluation_end_hour_exclusive"])
        for block in payload["outer_blocks"]
    )
    holdout_start = datetime.fromisoformat(payload["final_holdout"]["evaluation_start_hour"])
    assert outer_end <= holdout_start
