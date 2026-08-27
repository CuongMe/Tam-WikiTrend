from __future__ import annotations

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from wikitrend.forecasting import (
    MODEL_NAMES,
    _feature_row,
    _ridge_predict,
    _supervised_frame,
    assert_forecast_writable,
    build_forecast_layer,
    make_rolling_splits,
)


def write_hourly_gold(gold_dir, *, hours: int = 72) -> None:
    start = datetime(2026, 1, 1)
    rows = []
    for offset in range(hours):
        ds = start + timedelta(hours=offset)
        rows.append(
            {
                "date": ds.date(),
                "hour": ds.hour,
                "project": "en",
                "access_mode": "desktop",
                "page_rows": 100 + offset,
                "total_views": 1_000 + offset * 10 + (ds.hour * 3),
                "total_response_size": 10_000 + offset,
                "max_page_views": 100 + offset,
                "avg_page_views": 10.0 + offset,
                "approx_distinct_pages": 80 + offset,
            }
        )
    table_dir = gold_dir / "hourly_project_access"
    table_dir.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(table_dir / "part-00000.parquet", index=False)


def test_make_rolling_splits_uses_fixed_windows_for_72_hours() -> None:
    splits = make_rolling_splits(
        72,
        train_window_hours=36,
        evaluation_horizon_hours=12,
        step_hours=12,
    )

    assert len(splits) == 3
    assert list(splits[0][0]) == list(range(0, 36))
    assert list(splits[0][1]) == list(range(36, 48))
    assert list(splits[-1][0]) == list(range(24, 60))
    assert list(splits[-1][1]) == list(range(60, 72))


def test_regression_features_are_scale_normalized_and_target_transformed() -> None:
    history = pd.DataFrame(
        {
            "ds": pd.date_range("2026-01-01", periods=30, freq="h"),
            "project": "en",
            "access_mode": "desktop",
            "y": [float(100 + idx * 10) for idx in range(30)],
        }
    )

    feature = _feature_row(history, pd.Timestamp("2026-01-02 06:00:00"))
    expected_scale = history["y"].median()

    assert feature["target_scale"] == pytest.approx(expected_scale)
    assert feature["lag_1h_normalized"] == pytest.approx(history["y"].iloc[-1] / expected_scale)
    assert feature["rolling_median_24h_normalized"] == pytest.approx(
        history["y"].tail(24).median() / expected_scale
    )
    assert "series_scale_log1p" in feature
    assert "change_from_24h_normalized" in feature

    supervised = _supervised_frame(history, transform_target=True)
    first_target_scale = supervised.iloc[0]["target_scale"]
    expected_first_y = np.log1p(history["y"].iloc[1] / first_target_scale)
    assert supervised.iloc[0]["y"] == pytest.approx(expected_first_y)


def test_scaled_ridge_predictions_are_returned_in_pageview_units() -> None:
    history = pd.DataFrame(
        {
            "ds": pd.date_range("2026-01-01", periods=48, freq="h"),
            "project": "en",
            "access_mode": "desktop",
            "y": [float(10_000 + idx * 100) for idx in range(48)],
        }
    )
    future_ds = pd.Series(pd.date_range("2026-01-03", periods=6, freq="h"))

    predictions = _ridge_predict(history, future_ds)

    assert len(predictions) == 6
    assert all(prediction >= 0 for prediction in predictions)
    assert max(predictions) > 1_000


def test_assert_forecast_writable_refuses_existing_payload(tmp_path) -> None:
    forecast_dir = tmp_path / "forecast" / "hourly_project_access"
    forecast_dir.mkdir(parents=True)
    (forecast_dir / "old.parquet").write_bytes(b"old")

    with pytest.raises(FileExistsError, match="Refusing to write forecast outputs"):
        assert_forecast_writable(forecast_dir, overwrite=False)

    assert_forecast_writable(forecast_dir, overwrite=True)


def test_build_forecast_layer_compares_models_with_rolling_backtest(tmp_path) -> None:
    gold_dir = tmp_path / "gold"
    forecast_root = tmp_path / "forecast"
    write_hourly_gold(gold_dir)

    summary = build_forecast_layer(
        gold_dir=gold_dir,
        forecast_dir=forecast_root,
        train_window_hours=36,
        evaluation_horizon_hours=12,
        step_hours=12,
        forecast_horizon_hours=24,
    )

    target_dir = forecast_root / "hourly_project_access"
    assert summary.feature_rows == 72
    assert summary.series_count == 1
    assert summary.fold_count == 3
    assert summary.backtest_rows == 3 * 12 * len(MODEL_NAMES)
    assert summary.forecast_rows == 24 * len(MODEL_NAMES)
    assert (target_dir / "features.parquet").exists()
    assert (target_dir / "backtest_predictions.parquet").exists()
    assert (target_dir / "metrics.parquet").exists()
    assert (target_dir / "forecast.parquet").exists()

    metrics = pd.read_parquet(target_dir / "metrics.parquet")
    assert set(metrics["model"]) == set(MODEL_NAMES)
    assert {"mdae", "mase", "rmase", "mdape", "mdsmape"}.issubset(metrics.columns)
    assert {"mae", "rmse", "smape"}.isdisjoint(metrics.columns)
    overall_naive = metrics[
        (metrics["project"] == "__all__") & (metrics["model"] == "seasonal_naive_24h")
    ].iloc[0]
    assert overall_naive["rmase"] == pytest.approx(1.0)

    manifest = json.loads((target_dir / "forecast_manifest.json").read_text(encoding="utf-8"))
    assert manifest["models"] == list(MODEL_NAMES)
    assert manifest["evaluation_horizon_hours"] == 12
    assert manifest["forecast_horizon_hours"] == 24
