from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from wikitrend.silver import path_has_payload

TARGET_TABLE = "hourly_project_access"
MODEL_NAMES = ("seasonal_naive_24h", "ridge_lag", "elasticnet_lag", "lightgbm_lag")
LAG_HOURS = (1, 2, 3, 6, 12, 24)


@dataclass(frozen=True)
class ForecastSummary:
    forecast_dir: Path
    feature_rows: int
    series_count: int
    fold_count: int
    backtest_rows: int
    forecast_rows: int
    metric_rows: int
    train_window_hours: int
    evaluation_horizon_hours: int
    step_hours: int
    forecast_horizon_hours: int
    overwrite: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "forecast_dir": str(self.forecast_dir),
            "feature_rows": self.feature_rows,
            "series_count": self.series_count,
            "fold_count": self.fold_count,
            "backtest_rows": self.backtest_rows,
            "forecast_rows": self.forecast_rows,
            "metric_rows": self.metric_rows,
            "models": list(MODEL_NAMES),
            "train_window_hours": self.train_window_hours,
            "evaluation_horizon_hours": self.evaluation_horizon_hours,
            "step_hours": self.step_hours,
            "forecast_horizon_hours": self.forecast_horizon_hours,
            "overwrite": self.overwrite,
        }


def assert_forecast_writable(forecast_dir: Path, overwrite: bool) -> None:
    if overwrite:
        return
    if path_has_payload(forecast_dir):
        msg = (
            "Refusing to write forecast outputs because data already exists. "
            f"Use --overwrite only when intentionally replacing it: {forecast_dir}"
        )
        raise FileExistsError(msg)


def make_rolling_splits(
    row_count: int,
    *,
    train_window_hours: int = 36,
    evaluation_horizon_hours: int = 12,
    step_hours: int = 12,
) -> list[tuple[np.ndarray, np.ndarray]]:
    if train_window_hours <= 0 or evaluation_horizon_hours <= 0 or step_hours <= 0:
        msg = "train_window_hours, evaluation_horizon_hours, and step_hours must be positive"
        raise ValueError(msg)

    splits = []
    start = 0
    while start + train_window_hours + evaluation_horizon_hours <= row_count:
        train_start = start
        train_end = start + train_window_hours
        test_end = train_end + evaluation_horizon_hours
        splits.append((np.arange(train_start, train_end), np.arange(train_end, test_end)))
        start += step_hours
    return splits


def load_hourly_project_access(gold_dir: Path) -> pd.DataFrame:
    table_dir = gold_dir / TARGET_TABLE
    parquet_files = sorted(table_dir.rglob("*.parquet")) if table_dir.exists() else []
    if not parquet_files:
        msg = f"Missing Gold forecast source table: {table_dir}"
        raise FileNotFoundError(msg)

    frame = pd.read_parquet(table_dir)
    required = {"date", "hour", "project", "access_mode", "total_views"}
    missing = required.difference(frame.columns)
    if missing:
        msg = f"Gold table {TARGET_TABLE} is missing required columns: {sorted(missing)}"
        raise ValueError(msg)

    frame = frame.loc[:, ["date", "hour", "project", "access_mode", "total_views"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame["hour"] = frame["hour"].astype(int)
    frame["ds"] = pd.to_datetime(frame["date"].astype(str)) + pd.to_timedelta(
        frame["hour"], unit="h"
    )
    frame["y"] = frame["total_views"].astype(float)
    return frame.loc[:, ["ds", "project", "access_mode", "y"]]


def build_forecasting_features(gold_dir: Path) -> pd.DataFrame:
    source = load_hourly_project_access(gold_dir)
    series_frames: list[pd.DataFrame] = []

    for (project, access_mode), group in source.groupby(["project", "access_mode"], sort=True):
        hourly = (
            group.groupby("ds", as_index=False)["y"].sum().sort_values("ds").reset_index(drop=True)
        )
        full_index = pd.date_range(hourly["ds"].min(), hourly["ds"].max(), freq="h")
        completed = (
            hourly.set_index("ds")
            .reindex(full_index)
            .rename_axis("ds")
            .reset_index()
            .assign(project=project, access_mode=access_mode)
        )
        completed["y"] = completed["y"].fillna(0.0)
        series_frames.append(completed.loc[:, ["ds", "project", "access_mode", "y"]])

    if not series_frames:
        msg = "No project/access_mode time series were found for forecasting"
        raise ValueError(msg)
    return pd.concat(series_frames, ignore_index=True)


def _calendar_features(ds: pd.Timestamp) -> dict[str, float]:
    hour = float(ds.hour)
    day_of_week = float(ds.dayofweek)
    return {
        "hour": hour,
        "day_of_week": day_of_week,
        "hour_sin": float(np.sin(2 * np.pi * hour / 24)),
        "hour_cos": float(np.cos(2 * np.pi * hour / 24)),
        "day_sin": float(np.sin(2 * np.pi * day_of_week / 7)),
        "day_cos": float(np.cos(2 * np.pi * day_of_week / 7)),
    }


def _series_scale(history: pd.DataFrame) -> float:
    values = history["y"].to_numpy(dtype=float)
    positive_values = values[np.isfinite(values) & (values > 0)]
    if len(positive_values) == 0:
        return 1.0
    scale = float(np.median(positive_values))
    return max(scale, 1.0)


def _feature_row(history: pd.DataFrame, ds: pd.Timestamp) -> dict[str, float]:
    y = history["y"].to_numpy(dtype=float)
    row = _calendar_features(ds)
    fallback = float(y[-1]) if len(y) else 0.0
    scale = _series_scale(history)
    row["target_scale"] = scale
    row["series_scale_log1p"] = float(np.log1p(scale))

    for lag in LAG_HOURS:
        value = float(y[-lag]) if len(y) >= lag else fallback
        row[f"lag_{lag}h"] = value
        row[f"lag_{lag}h_normalized"] = value / scale

    for window in (3, 6, 12, 24):
        window_values = y[-window:] if len(y) else np.array([0.0])
        rolling_median = float(np.median(window_values))
        rolling_std = float(np.std(window_values))
        row[f"rolling_median_{window}h"] = rolling_median
        row[f"rolling_median_{window}h_normalized"] = rolling_median / scale
        row[f"rolling_std_{window}h_normalized"] = rolling_std / scale

    if len(y) >= 24:
        lag_24h = float(y[-24])
        row["change_from_24h_normalized"] = (fallback - lag_24h) / scale
        row["pct_change_from_24h"] = (fallback - lag_24h) / max(abs(lag_24h), 1.0)
    else:
        row["change_from_24h_normalized"] = 0.0
        row["pct_change_from_24h"] = 0.0

    row["last_value"] = fallback
    row["last_value_normalized"] = fallback / scale
    return row


def _model_features(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop(columns=["y", "target_scale"], errors="ignore")


def _supervised_frame(history: pd.DataFrame, *, transform_target: bool = False) -> pd.DataFrame:
    rows = []
    ordered = history.sort_values("ds").reset_index(drop=True)
    for idx in range(1, len(ordered)):
        past = ordered.iloc[:idx]
        target = ordered.iloc[idx]
        row = _feature_row(past, pd.Timestamp(target["ds"]))
        target_y = float(target["y"])
        row["y"] = np.log1p(target_y / row["target_scale"]) if transform_target else target_y
        rows.append(row)
    return pd.DataFrame(rows)


def _seasonal_naive_predict(train: pd.DataFrame, future_ds: pd.Series) -> list[float]:
    lookup = {
        pd.Timestamp(row.ds): float(row.y)
        for row in train.loc[:, ["ds", "y"]].itertuples(index=False)
    }
    fallback = float(train["y"].iloc[-1])
    predictions = []
    for ds in future_ds:
        timestamp = pd.Timestamp(ds)
        predictions.append(lookup.get(timestamp - timedelta(hours=24), fallback))
    return predictions


def _recursive_regression_predict(
    model: Any,
    train: pd.DataFrame,
    future_ds: pd.Series,
    *,
    transform_target: bool = False,
) -> list[float]:
    history = train.loc[:, ["ds", "y"]].sort_values("ds").reset_index(drop=True).copy()
    predictions = []
    for ds in future_ds:
        timestamp = pd.Timestamp(ds)
        feature_row = _feature_row(history, timestamp)
        target_scale = feature_row.pop("target_scale")
        prediction_input = pd.DataFrame([feature_row])
        model_prediction = float(model.predict(prediction_input)[0])
        if transform_target:
            prediction = np.expm1(model_prediction) * target_scale
        else:
            prediction = model_prediction
        prediction = max(0.0, float(prediction))
        predictions.append(prediction)
        history = pd.concat(
            [
                history,
                pd.DataFrame([{"ds": timestamp, "y": prediction}]),
            ],
            ignore_index=True,
        )
    return predictions


def _ridge_predict(train: pd.DataFrame, future_ds: pd.Series) -> list[float]:
    supervised = _supervised_frame(train, transform_target=True)
    if len(supervised) < 2:
        return [float(train["y"].iloc[-1])] * len(future_ds)
    x_train = _model_features(supervised)
    y_train = supervised["y"]
    model = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    model.fit(x_train, y_train)
    return _recursive_regression_predict(model, train, future_ds, transform_target=True)


def _elasticnet_predict(train: pd.DataFrame, future_ds: pd.Series) -> list[float]:
    supervised = _supervised_frame(train, transform_target=True)
    if len(supervised) < 4:
        return [float(train["y"].iloc[-1])] * len(future_ds)
    x_train = _model_features(supervised)
    y_train = supervised["y"]
    model = make_pipeline(
        StandardScaler(),
        ElasticNet(
            alpha=0.1,
            l1_ratio=0.25,
            max_iter=20_000,
            random_state=42,
        ),
    )
    model.fit(x_train, y_train)
    return _recursive_regression_predict(model, train, future_ds, transform_target=True)


def _lightgbm_predict(train: pd.DataFrame, future_ds: pd.Series) -> list[float]:
    try:
        from lightgbm import LGBMRegressor
    except ImportError as exc:
        msg = "lightgbm is required for the lightgbm_lag forecast model"
        raise ImportError(msg) from exc

    supervised = _supervised_frame(train, transform_target=True)
    if len(supervised) < 8:
        return [float(train["y"].iloc[-1])] * len(future_ds)
    x_train = _model_features(supervised)
    y_train = supervised["y"]
    model = LGBMRegressor(
        objective="regression",
        n_estimators=80,
        learning_rate=0.05,
        num_leaves=15,
        min_child_samples=2,
        random_state=42,
        verbosity=-1,
    )
    model.fit(x_train, y_train)
    return _recursive_regression_predict(model, train, future_ds, transform_target=True)


def _predict(model_name: str, train: pd.DataFrame, future_ds: pd.Series) -> list[float]:
    if model_name == "seasonal_naive_24h":
        return _seasonal_naive_predict(train, future_ds)
    if model_name == "ridge_lag":
        return _ridge_predict(train, future_ds)
    if model_name == "elasticnet_lag":
        return _elasticnet_predict(train, future_ds)
    if model_name == "lightgbm_lag":
        return _lightgbm_predict(train, future_ds)
    msg = f"Unsupported forecast model: {model_name}"
    raise ValueError(msg)


def _metric_frame(backtest: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, series_group in backtest.groupby(["project", "access_mode"], sort=True):
        project, access_mode = keys
        baseline_mase = _baseline_mase(series_group)
        for model_name, group in series_group.groupby("model", sort=True):
            rows.append(_metrics_for_group(group, project, access_mode, model_name, baseline_mase))

    baseline_mase = _baseline_mase(backtest)
    for model_name, group in backtest.groupby("model", sort=True):
        rows.append(_metrics_for_group(group, "__all__", "__all__", model_name, baseline_mase))
    return pd.DataFrame(rows)


def _seasonal_median_absolute_scale(
    history: pd.DataFrame,
    *,
    seasonal_lag_hours: int = 24,
) -> float:
    values = history["y"].to_numpy(dtype=float)
    if len(values) > seasonal_lag_hours:
        diffs = np.abs(values[seasonal_lag_hours:] - values[:-seasonal_lag_hours])
        finite_diffs = diffs[np.isfinite(diffs)]
        if len(finite_diffs):
            scale = float(np.median(finite_diffs))
            if scale > 0:
                return scale
    return _series_scale(history)


def _median_absolute_scaled_error(group: pd.DataFrame) -> float:
    errors = (group["y_pred"] - group["y_true"]).abs()
    scales = group["mase_scale"].replace(0, np.nan)
    scaled_errors = errors / scales
    finite_scaled_errors = scaled_errors[np.isfinite(scaled_errors)]
    if finite_scaled_errors.empty:
        return float("nan")
    return float(finite_scaled_errors.median())


def _baseline_mase(backtest: pd.DataFrame) -> float:
    baseline = backtest[backtest["model"] == "seasonal_naive_24h"]
    if baseline.empty:
        return float("nan")
    return _median_absolute_scaled_error(baseline)


def _relative_mase(mase: float, baseline_mase: float) -> float:
    if not np.isfinite(baseline_mase):
        return float("nan")
    if baseline_mase == 0:
        return 0.0 if mase == 0 else float("inf")
    return float(mase / baseline_mase)


def _metrics_for_group(
    group: pd.DataFrame,
    project: str,
    access_mode: str,
    model_name: str,
    baseline_mase: float,
) -> dict[str, Any]:
    errors = group["y_pred"] - group["y_true"]
    absolute_errors = errors.abs()
    denominator = group["y_true"].abs() + group["y_pred"].abs()
    smape_terms = np.where(denominator == 0, 0.0, 2 * absolute_errors / denominator)
    ape_terms = absolute_errors / group["y_true"].abs().replace(0, np.nan)
    finite_ape_terms = ape_terms[np.isfinite(ape_terms)]
    mase = _median_absolute_scaled_error(group)
    return {
        "project": project,
        "access_mode": access_mode,
        "model": model_name,
        "folds": int(group["fold_id"].nunique()),
        "observations": int(len(group)),
        "mdae": float(absolute_errors.median()),
        "mase": mase,
        "rmase": _relative_mase(mase, baseline_mase),
        "mdape": float(finite_ape_terms.median() * 100)
        if not finite_ape_terms.empty
        else float("nan"),
        "mdsmape": float(np.median(smape_terms) * 100),
    }


def rolling_backtest(
    features: pd.DataFrame,
    *,
    train_window_hours: int = 36,
    evaluation_horizon_hours: int = 12,
    step_hours: int = 12,
) -> pd.DataFrame:
    rows = []
    fold_counter = 0
    for (project, access_mode), group in features.groupby(["project", "access_mode"], sort=True):
        series = group.sort_values("ds").reset_index(drop=True)
        splits = make_rolling_splits(
            len(series),
            train_window_hours=train_window_hours,
            evaluation_horizon_hours=evaluation_horizon_hours,
            step_hours=step_hours,
        )
        if not splits:
            continue
        for train_idx, test_idx in splits:
            fold_counter += 1
            train = series.iloc[train_idx].reset_index(drop=True)
            test = series.iloc[test_idx].reset_index(drop=True)
            mase_scale = _seasonal_median_absolute_scale(train)
            for model_name in MODEL_NAMES:
                predictions = _predict(model_name, train, test["ds"])
                for horizon_step, (test_row, prediction) in enumerate(
                    zip(test.itertuples(index=False), predictions, strict=True),
                    start=1,
                ):
                    rows.append(
                        {
                            "fold_id": fold_counter,
                            "horizon_step": horizon_step,
                            "ds": pd.Timestamp(test_row.ds),
                            "project": project,
                            "access_mode": access_mode,
                            "model": model_name,
                            "y_true": float(test_row.y),
                            "y_pred": float(prediction),
                            "mase_scale": mase_scale,
                        }
                    )
    if not rows:
        msg = (
            "Not enough hourly observations for rolling backtesting. "
            "Reduce train_window_hours or evaluation_horizon_hours."
        )
        raise ValueError(msg)
    return pd.DataFrame(rows)


def forecast_future(features: pd.DataFrame, *, forecast_horizon_hours: int = 24) -> pd.DataFrame:
    if forecast_horizon_hours <= 0:
        msg = "forecast_horizon_hours must be positive"
        raise ValueError(msg)

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    rows = []
    for (project, access_mode), group in features.groupby(["project", "access_mode"], sort=True):
        series = group.sort_values("ds").reset_index(drop=True)
        last_ds = pd.Timestamp(series["ds"].iloc[-1])
        future_ds = pd.Series(
            [last_ds + timedelta(hours=step) for step in range(1, forecast_horizon_hours + 1)]
        )
        for model_name in MODEL_NAMES:
            predictions = _predict(model_name, series, future_ds)
            for horizon_step, (ds, prediction) in enumerate(
                zip(future_ds, predictions, strict=True),
                start=1,
            ):
                rows.append(
                    {
                        "generated_at_utc": generated_at,
                        "horizon_step": horizon_step,
                        "ds": pd.Timestamp(ds),
                        "project": project,
                        "access_mode": access_mode,
                        "model": model_name,
                        "yhat": float(prediction),
                    }
                )
    return pd.DataFrame(rows)


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def build_forecast_layer(
    *,
    gold_dir: Path,
    forecast_dir: Path,
    train_window_hours: int = 36,
    evaluation_horizon_hours: int = 12,
    step_hours: int = 12,
    forecast_horizon_hours: int = 24,
    overwrite: bool = False,
) -> ForecastSummary:
    target_dir = forecast_dir / TARGET_TABLE
    assert_forecast_writable(target_dir, overwrite)
    if overwrite and target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    features = build_forecasting_features(gold_dir)
    backtest = rolling_backtest(
        features,
        train_window_hours=train_window_hours,
        evaluation_horizon_hours=evaluation_horizon_hours,
        step_hours=step_hours,
    )
    metrics = _metric_frame(backtest)
    forecast = forecast_future(features, forecast_horizon_hours=forecast_horizon_hours)

    _write_parquet(features, target_dir / "features.parquet")
    _write_parquet(backtest, target_dir / "backtest_predictions.parquet")
    _write_parquet(metrics, target_dir / "metrics.parquet")
    _write_parquet(forecast, target_dir / "forecast.parquet")

    summary = ForecastSummary(
        forecast_dir=target_dir,
        feature_rows=len(features),
        series_count=int(features.groupby(["project", "access_mode"]).ngroups),
        fold_count=int(backtest["fold_id"].nunique()),
        backtest_rows=len(backtest),
        forecast_rows=len(forecast),
        metric_rows=len(metrics),
        train_window_hours=train_window_hours,
        evaluation_horizon_hours=evaluation_horizon_hours,
        step_hours=step_hours,
        forecast_horizon_hours=forecast_horizon_hours,
        overwrite=overwrite,
    )
    _write_json(
        target_dir / "forecast_manifest.json",
        {
            "manifest_version": 1,
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "source_table": TARGET_TABLE,
            "target": "total_views",
            "target_transform": "log1p(y / rolling_median_positive_series_scale)",
            "feature_normalization": (
                "lag, rolling median, and rolling std features include "
                "per-series scale-normalized variants"
            ),
            "grain": "hour, project, access_mode",
            "models": list(MODEL_NAMES),
            **summary.to_dict(),
        },
    )
    return summary
