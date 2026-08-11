"""Score the saved LightGBM model against the latest Gold forecast features.

This is deliberately separate from model training. It supports historical replay:
score an origin hour now, then rerun that same origin after the next-hour target has
been materialized to populate the comparison metrics.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds

try:
    from .forecast_evaluation import (
        CAT_COLUMNS,
        NUMERIC_COLUMNS,
        build_spark_session,
    )
except ImportError:
    from forecast_evaluation import (
        CAT_COLUMNS,
        NUMERIC_COLUMNS,
        build_spark_session,
    )
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf
from pyspark.storagelevel import StorageLevel

PREDICTION_COLUMNS = [
    "timestamp_hour",
    "forecast_hour",
    "forecast_date",
    "forecast_hour_of_day",
    "source_project",
    "project",
    "language",
    "project_family",
    "access_mode",
    "page_title",
    "normalized_title",
    "is_observed",
    "view_count",
    "lag_1h_views",
    "target_next_hour_views",
    "mase_scale",
    "lightgbm_predicted_views",
    "forecast_views",
    "predicted_growth_rate",
    "predicted_traffic_rank",
    "predicted_growth_rank",
    "forecast_method",
    "fallback_used",
    "fallback_reason",
    "feature_missing",
    "model_prediction_available",
    "actual_available",
    "model_version",
    "manifest_id",
    "scoring_run_id",
    "quality_gate_degraded",
    "quality_gate_metric",
    "quality_gate_model_value",
    "quality_gate_baseline_value",
    "quality_gate_history_rows",
    "quality_gate_history_origins",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score the saved LightGBM model on one Gold forecast origin."
    )
    parser.add_argument(
        "--forecast-features",
        type=Path,
        default=Path("data/gold/forecast_features"),
        help="Gold forecast_features Parquet directory.",
    )
    parser.add_argument(
        "--model-output",
        type=Path,
        default=Path("models/lightgbm"),
        help="Directory containing model.txt, metadata.json, and category_levels.json.",
    )
    parser.add_argument(
        "--predictions-output",
        type=Path,
        default=Path("data/gold/lightgbm_predictions"),
        help="Gold output directory for predictions and metrics.",
    )
    parser.add_argument(
        "--timestamp-hour",
        help="UTC origin hour in YYYY-MM-DDTHH:MM:SS format. Defaults to latest Gold hour.",
    )
    parser.add_argument(
        "--master",
        default="local[4]",
        help="Spark master URL; use spark://spark-master:7077 in Docker.",
    )
    parser.add_argument(
        "--quality-gate-window-hours",
        type=int,
        default=24,
        help="Historical scored-hour window used by the fallback quality gate.",
    )
    parser.add_argument(
        "--quality-gate-min-rows",
        type=int,
        default=1000,
        help="Minimum historical model rows required before enabling the quality gate.",
    )
    parser.add_argument(
        "--quality-gate-min-origins",
        type=int,
        default=24,
        help="Minimum distinct evaluated origins required before automatic fallback.",
    )
    parser.add_argument(
        "--ranking-cutoffs",
        default="10,50,100",
        help="Comma-separated K values for ranking metrics.",
    )
    parser.add_argument(
        "--quality-gate-metric",
        choices=("mase", "msmape"),
        default="msmape",
        help="Metric used to compare recent LightGBM predictions with lag-1.",
    )
    parser.add_argument(
        "--disable-quality-gate",
        action="store_true",
        help="Do not activate the historical quality-based lag-1 fallback.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Number of predicted-traffic and predicted-growth pages per project to publish.",
    )
    return parser.parse_args()


def _absolute_project_path(path: Path, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path


def configure_windows_spark_path(project_root: Path) -> tuple[Path, bool]:
    """Map the project to an ASCII drive for Hadoop's Windows native checks."""
    if os.name != "nt":
        return project_root, False
    root_text = str(project_root).rstrip("\\/")
    mapping = subprocess.run(
        ["subst"], capture_output=True, check=False
    ).stdout.decode(errors="replace")
    mapping_lower = mapping.lower().replace("/", "\\")
    root_lower = root_text.lower().replace("/", "\\")
    if "x:" in mapping_lower:
        if root_lower not in mapping_lower:
            raise RuntimeError("X: is mapped to a different folder; unmap it before scoring.")
        created = False
    else:
        subprocess.run(["subst", "X:", root_text], check=True)
        created = True
    os.environ["HADOOP_HOME"] = "X:\\.hadoop"
    os.environ["WIKITREND_HADOOP_BIN"] = "X:/.hadoop/bin"
    return Path("X:/"), created


def to_spark_path(path: Path, project_root: Path, spark_root: Path) -> Path:
    absolute = _absolute_project_path(path, project_root).resolve()
    relative = absolute.relative_to(project_root.resolve())
    return spark_root / relative


def read_origin_features(
    spark: SparkSession,
    forecast_features: Path,
    spark_forecast_features: Path,
    origin: datetime,
    baseline_hours: int,
) -> DataFrame:
    """Read the origin plus enough prior Gold history to derive MASE scaling."""
    origin_dates = sorted(
        {
            origin.date(),
            (origin - timedelta(hours=baseline_hours)).date(),
        }
    )
    local_paths = [forecast_features / f"date={value:%Y-%m-%d}" for value in origin_dates]
    missing_paths = [path for path in local_paths if not path.exists()]
    if missing_paths:
        raise ValueError(f"Gold is missing history partitions needed for scoring: {missing_paths}")
    spark_paths = [spark_forecast_features / path.name for path in local_paths]
    source = spark.read.option("basePath", str(spark_forecast_features)).parquet(
        *[str(path) for path in spark_paths]
    )
    if "mase_scale" not in source.columns:
        history = source.select(
            "timestamp_hour", "project", "normalized_title", "view_count", "lag_1h_views"
        ).filter(
            (
                F.col("timestamp_hour")
                >= F.to_timestamp(
                    F.lit(
                        (origin - timedelta(hours=baseline_hours)).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    )
                )
            )
            & (
                F.col("timestamp_hour")
                < F.to_timestamp(F.lit(origin.strftime("%Y-%m-%d %H:%M:%S")))
            )
            & F.col("view_count").isNotNull()
            & F.col("lag_1h_views").isNotNull()
        ).withColumn(
            "_naive_absolute_error",
            F.abs(F.col("view_count") - F.col("lag_1h_views")),
        )
        mase_scale = history.groupBy("project", "normalized_title").agg(
            F.avg("_naive_absolute_error").alias("mase_scale")
        )
        source = source.join(mase_scale, on=["project", "normalized_title"], how="left")
    return source.filter(
        F.col("timestamp_hour")
        == F.to_timestamp(F.lit(origin.strftime("%Y-%m-%d %H:%M:%S")))
    )


def parse_utc_hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed.replace(minute=0, second=0, microsecond=0)


def _as_utc_naive(value: object) -> datetime:
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        raise TypeError(f"Expected a timestamp value, got {type(value)!r}")
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(minute=0, second=0, microsecond=0)


def latest_gold_hour(path: Path) -> datetime:
    """Find the latest timestamp using Parquet statistics, with a scan fallback."""
    if not path.exists():
        raise FileNotFoundError(f"Gold forecast feature path does not exist: {path}")
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    field_index = dataset.schema.get_field_index("timestamp_hour")
    if field_index < 0:
        raise ValueError("Gold forecast_features is missing timestamp_hour")

    latest: datetime | None = None
    statistics_complete = True
    for fragment in dataset.get_fragments():
        metadata = fragment.metadata
        if metadata is None:
            statistics_complete = False
            break
        try:
            fragment_index = metadata.schema.names.index("timestamp_hour")
        except ValueError:
            statistics_complete = False
            break
        for row_group in range(metadata.num_row_groups):
            statistics = metadata.row_group(row_group).column(fragment_index).statistics
            if statistics is None or statistics.max is None:
                statistics_complete = False
                break
            candidate = _as_utc_naive(statistics.max)
            latest = candidate if latest is None else max(latest, candidate)
        if not statistics_complete:
            break

    if statistics_complete and latest is not None:
        return latest

    latest = None
    scanner = dataset.scanner(columns=["timestamp_hour"], batch_size=1_000_000)
    for batch in scanner.to_batches():
        values = batch.column(0)
        if len(values) == 0:
            continue
        candidate = _as_utc_naive(pc.max(values).as_py())
        latest = candidate if latest is None else max(latest, candidate)
    if latest is None:
        raise ValueError(f"Gold forecast_features contains no timestamps: {path}")
    return latest


def load_model_artifacts(model_output: Path) -> tuple[lgb.Booster, dict, dict, str]:
    current_path = model_output / "current.json"
    if current_path.is_file():
        current = json.loads(current_path.read_text(encoding="utf-8"))
        model_output = model_output / str(current["path"])
    model_path = model_output / "model.txt"
    metadata_path = model_output / "metadata.json"
    levels_path = model_output / "category_levels.json"
    for path in (model_path, metadata_path, levels_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required LightGBM artifact is missing: {path}")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    category_levels = json.loads(levels_path.read_text(encoding="utf-8"))
    feature_columns = metadata.get("feature_columns")
    expected_columns = NUMERIC_COLUMNS + CAT_COLUMNS
    if feature_columns != expected_columns:
        raise ValueError(
            "Saved feature order does not match the scoring contract: "
            f"expected {expected_columns}, got {feature_columns}"
        )
    if metadata.get("target") != "target_next_hour_views":
        raise ValueError("The saved model target is not target_next_hour_views")
    for column in CAT_COLUMNS:
        levels = category_levels.get(column)
        if not levels or "__unknown__" not in levels:
            raise ValueError(f"Saved category levels for {column!r} lack __unknown__")

    model_text = model_path.read_text(encoding="utf-8")
    booster = lgb.Booster(model_str=model_text)
    if booster.feature_name() != expected_columns:
        raise ValueError(
            "LightGBM feature names do not match metadata: "
            f"expected {expected_columns}, got {booster.feature_name()}"
        )
    model_version = metadata.get("model_version", metadata.get("manifest_id", "unknown"))
    return booster, metadata, category_levels, str(model_version)


def prepare_feature_matrix(
    frame: pd.DataFrame,
    category_levels: dict[str, list[str]],
    feature_columns: list[str],
) -> pd.DataFrame:
    work = frame.copy()
    for column in CAT_COLUMNS:
        values = work[column].fillna("__unknown__").astype(str)
        levels = list(category_levels[column])
        values = values.where(values.isin(levels), "__unknown__")
        work[column] = pd.Categorical(values, categories=levels)
    for column in NUMERIC_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0.0)
    return work[feature_columns]


def prediction_udf(
    spark: SparkSession,
    model_text: str,
    category_levels: dict[str, list[str]],
    feature_columns: list[str],
):
    broadcast_model = spark.sparkContext.broadcast(model_text)
    worker_state: dict[str, lgb.Booster] = {}

    @pandas_udf(T.DoubleType())
    def predict_batch(*series: pd.Series) -> pd.Series:
        booster = worker_state.get("booster")
        if booster is None:
            booster = lgb.Booster(model_str=broadcast_model.value)
            worker_state["booster"] = booster
        frame = pd.DataFrame(
            {column: values for column, values in zip(feature_columns, series, strict=True)}
        )
        features = prepare_feature_matrix(frame, category_levels, feature_columns)
        predictions = np.asarray(booster.predict(features), dtype="float64")
        predictions[~np.isfinite(predictions)] = np.nan
        predictions[predictions < 0] = np.nan
        return pd.Series(predictions)

    return predict_batch


def _feature_missing_expression() -> F.Column:
    conditions = [F.col(column).isNull() for column in NUMERIC_COLUMNS]
    conditions.extend(
        F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)
        for column in CAT_COLUMNS
    )
    conditions.append(F.col("lag_1h_views").isNull() | (F.col("lag_1h_views") < 0))
    expression = conditions[0]
    for condition in conditions[1:]:
        expression = expression | condition
    return expression


def add_scoring_time_features(frame: DataFrame) -> DataFrame:
    hour_angle = F.lit(2.0 * math.pi) * F.hour("timestamp_hour") / F.lit(24.0)
    return frame.withColumn("hour_sin", F.sin(hour_angle)).withColumn(
        "hour_cos", F.cos(hour_angle)
    )


def _metric_columns(
    actual: F.Column, prediction: F.Column, mase_scale: F.Column
) -> dict[str, F.Column]:
    absolute_error = F.abs(actual - prediction)
    denominator = F.abs(actual) + F.abs(prediction)
    return {
        "absolute_error": absolute_error,
        "mase_observation": F.when(mase_scale > 0, absolute_error / mase_scale),
        "smape_observation": F.when(
            denominator > 0, 2.0 * absolute_error / denominator
        ).otherwise(F.lit(0.0)),
        "msmape_observation": 2.0
        * absolute_error
        / F.greatest(denominator, F.lit(1.0)),
    }


def _method_metrics(predictions: DataFrame, method: str, prediction_column: str) -> DataFrame:
    scored = predictions.filter(
        F.col("target_next_hour_views").isNotNull() & F.col(prediction_column).isNotNull()
    )
    metrics = _metric_columns(
        F.col("target_next_hour_views").cast("double"),
        F.col(prediction_column).cast("double"),
        F.col("mase_scale").cast("double"),
    )
    scored = scored.select(
        "timestamp_hour",
        F.lit(method).alias("forecast_method"),
        F.col("target_next_hour_views").cast("double").alias("actual_views"),
        F.col("mase_scale").cast("double"),
        *[expression.alias(name) for name, expression in metrics.items()],
    )
    return scored.groupBy("forecast_method").agg(
        F.count("*").cast("long").alias("evaluated_rows"),
        F.count("mase_observation").cast("long").alias("mase_valid_rows"),
        F.sum("mase_observation").alias("mase_sum"),
        F.sum("absolute_error").alias("absolute_error_sum"),
        F.sum(F.abs("actual_views")).alias("actual_abs_sum"),
        F.sum("smape_observation").alias("smape_sum"),
        F.sum("msmape_observation").alias("msmape_sum"),
        F.min("timestamp_hour").alias("evaluation_start_hour"),
        F.max("timestamp_hour").alias("evaluation_end_hour"),
    ).withColumn(
        "mase",
        F.when(F.col("mase_valid_rows") > 0, F.col("mase_sum") / F.col("mase_valid_rows")),
    ).withColumn(
        "nd",
        F.when(F.col("actual_abs_sum") > 0, F.col("absolute_error_sum") / F.col("actual_abs_sum")),
    ).withColumn(
        "smape",
        F.when(F.col("evaluated_rows") > 0, F.col("smape_sum") / F.col("evaluated_rows")),
    ).withColumn(
        "msmape",
        F.when(F.col("evaluated_rows") > 0, F.col("msmape_sum") / F.col("evaluated_rows")),
    )


def quality_gate(
    spark: SparkSession,
    predictions_path: Path,
    origin: datetime,
    metric: str,
    window_hours: int,
    minimum_rows: int,
    minimum_origins: int,
) -> dict[str, object]:
    default = {
        "degraded": False,
        "metric": metric,
        "model_value": None,
        "baseline_value": None,
        "history_rows": 0,
        "history_origins": 0,
        "reason": "no_eligible_history",
    }
    if not predictions_path.exists() or not list(predictions_path.rglob("*.parquet")):
        return default
    history = spark.read.parquet(str(predictions_path)).filter(
        (F.col("timestamp_hour") < F.to_timestamp(F.lit(origin.strftime("%Y-%m-%d %H:%M:%S"))))
        & (
            F.col("timestamp_hour")
            >= F.to_timestamp(
                F.lit((origin - timedelta(hours=window_hours)).strftime("%Y-%m-%d %H:%M:%S"))
            )
        )
        & F.col("target_next_hour_views").isNotNull()
        & F.col("lightgbm_predicted_views").isNotNull()
        & F.col("lag_1h_views").isNotNull()
        & (~F.col("feature_missing"))
    )
    row_count = history.count()
    origin_count = history.select("timestamp_hour").distinct().count()
    if row_count < minimum_rows or origin_count < minimum_origins:
        default["history_rows"] = row_count
        default["history_origins"] = origin_count
        default["reason"] = "insufficient_eligible_history"
        return default
    model_metrics = _method_metrics(history, "lightgbm", "lightgbm_predicted_views").first()
    baseline_metrics = _method_metrics(history, "lag_1h", "lag_1h_views").first()
    if model_metrics is None or baseline_metrics is None:
        return default
    model_value = model_metrics[metric]
    baseline_value = baseline_metrics[metric]
    degraded = (
        model_value is not None
        and baseline_value is not None
        and float(model_value) > float(baseline_value)
    )
    return {
        "degraded": degraded,
        "metric": metric,
        "model_value": float(model_value) if model_value is not None else None,
        "baseline_value": float(baseline_value) if baseline_value is not None else None,
        "history_rows": row_count,
        "history_origins": origin_count,
        "reason": "model_metric_worse_than_lag_1h" if degraded else "model_beats_lag_1h",
    }


def build_predictions(
    source: DataFrame,
    predict,
    origin: datetime,
    model_version: str,
    manifest_id: str,
    scoring_run_id: str,
    gate: dict[str, object],
) -> DataFrame:
    source = source.withColumn("feature_missing", _feature_missing_expression())
    source = source.withColumn(
        "lightgbm_predicted_views_raw",
        predict(*[F.col(column) for column in NUMERIC_COLUMNS + CAT_COLUMNS]),
    )
    source = source.withColumn(
        "model_prediction_available",
        (~F.col("feature_missing"))
        & F.col("lightgbm_predicted_views_raw").isNotNull()
        & (~F.isnan(F.col("lightgbm_predicted_views_raw"))),
    ).withColumn(
        "lightgbm_predicted_views",
        F.when(F.col("model_prediction_available"), F.col("lightgbm_predicted_views_raw")),
    )
    quality_degraded = bool(gate["degraded"])
    lag_available = F.col("lag_1h_views").isNotNull() & (F.col("lag_1h_views") >= 0)
    use_fallback = (
        (
            F.col("feature_missing")
            | (~F.col("model_prediction_available"))
            | F.lit(quality_degraded)
        )
        & lag_available
    )
    source = source.withColumn("fallback_used", use_fallback).withColumn(
        "forecast_views",
        F.when(use_fallback, F.col("lag_1h_views").cast("double")).otherwise(
            F.col("lightgbm_predicted_views")
        ),
    ).withColumn(
        "forecast_method",
        F.when(use_fallback, F.lit("lag_1h_fallback"))
        .when(F.col("model_prediction_available"), F.lit("lightgbm"))
        .otherwise(F.lit("unavailable")),
    ).withColumn(
        "fallback_reason",
        F.when(~use_fallback, F.lit(None).cast("string"))
        .when(F.lit(quality_degraded), F.lit("quality_gate"))
        .when(F.col("feature_missing"), F.lit("missing_feature"))
        .otherwise(F.lit("invalid_model_prediction")),
    )
    source = source.withColumn(
        "predicted_growth_rate",
        F.when(
            F.col("forecast_views").isNotNull(),
            F.col("forecast_views")
            / F.greatest(F.col("view_count").cast("double"), F.lit(1.0))
            - F.lit(1.0),
        ),
    )
    traffic_rank = Window.partitionBy("timestamp_hour", "project", "access_mode").orderBy(
        F.desc_nulls_last("forecast_views"), F.asc("normalized_title")
    )
    growth_rank = Window.partitionBy("timestamp_hour", "project", "access_mode").orderBy(
        F.desc_nulls_last("predicted_growth_rate"),
        F.desc_nulls_last("forecast_views"),
        F.asc("normalized_title"),
    )
    return source.select(
        F.col("timestamp_hour"),
        (F.col("timestamp_hour") + F.expr("INTERVAL 1 HOUR")).alias("forecast_hour"),
        F.date_format(F.col("timestamp_hour") + F.expr("INTERVAL 1 HOUR"), "yyyy-MM-dd").alias(
            "forecast_date"
        ),
        F.hour(F.col("timestamp_hour") + F.expr("INTERVAL 1 HOUR")).alias("forecast_hour_of_day"),
        *[F.col(column) for column in [
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "page_title",
            "normalized_title",
            "is_observed",
            "view_count",
            "lag_1h_views",
            "target_next_hour_views",
            "mase_scale",
            "lightgbm_predicted_views",
            "forecast_views",
            "predicted_growth_rate",
            "forecast_method",
            "fallback_used",
            "fallback_reason",
            "feature_missing",
            "model_prediction_available",
        ]],
        F.col("target_next_hour_views").isNotNull().alias("actual_available"),
        F.lit(model_version).alias("model_version"),
        F.lit(manifest_id).alias("manifest_id"),
        F.lit(scoring_run_id).alias("scoring_run_id"),
        F.lit(quality_degraded).alias("quality_gate_degraded"),
        F.lit(gate["metric"]).alias("quality_gate_metric"),
        F.lit(gate["model_value"]).cast("double").alias("quality_gate_model_value"),
        F.lit(gate["baseline_value"]).cast("double").alias("quality_gate_baseline_value"),
        F.lit(int(gate["history_rows"])).alias("quality_gate_history_rows"),
        F.lit(int(gate["history_origins"])).alias("quality_gate_history_origins"),
    ).withColumn("predicted_traffic_rank", F.row_number().over(traffic_rank)).withColumn(
        "predicted_growth_rank", F.row_number().over(growth_rank)
    )


def write_predictions(predictions: DataFrame, output: Path, top_n: int) -> None:
    output.mkdir(parents=True, exist_ok=True)
    prediction_path = output / "predictions"
    predictions.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        predictions.write.mode("overwrite")
        .partitionBy("forecast_date", "forecast_hour_of_day", "project", "access_mode")
        .parquet(str(prediction_path))
    )
    top_pages = predictions.filter(F.col("forecast_views").isNotNull()).select(
        "timestamp_hour",
        "forecast_hour",
        "forecast_date",
        "forecast_hour_of_day",
        "source_project",
        "project",
        "language",
        "project_family",
        "access_mode",
        "page_title",
        "normalized_title",
        "view_count",
        "lag_1h_views",
        "lightgbm_predicted_views",
        "forecast_views",
        "predicted_growth_rate",
        "predicted_traffic_rank",
        "predicted_growth_rank",
        "forecast_method",
        "fallback_used",
        "model_version",
        "manifest_id",
        "scoring_run_id",
    )
    traffic = top_pages.filter(F.col("predicted_traffic_rank") <= top_n).withColumn(
        "ranking_type", F.lit("predicted_traffic")
    )
    growth = top_pages.filter(F.col("predicted_growth_rank") <= top_n).withColumn(
        "ranking_type", F.lit("predicted_growth")
    )
    (
        traffic.unionByName(growth)
        .write.mode("overwrite")
        .partitionBy(
            "forecast_date",
            "forecast_hour_of_day",
            "project",
            "access_mode",
            "ranking_type",
        )
        .parquet(str(output / "research_top_pages"))
    )


def build_ranking_metrics(predictions: DataFrame, cutoffs: list[int]) -> DataFrame:
    keys = ["timestamp_hour", "source_project", "project", "access_mode"]
    eligible = predictions.filter(F.col("target_next_hour_views").isNotNull())
    actual_rank_window = Window.partitionBy(*keys).orderBy(
        F.desc("target_next_hour_views"), F.asc("normalized_title")
    )
    ranked = eligible.withColumn(
        "actual_traffic_rank", F.row_number().over(actual_rank_window)
    )
    scored = ranked.filter(F.col("forecast_views").isNotNull())
    coverage = ranked.groupBy(*keys).agg(
        F.count("*").alias("eligible_rows")
    ).join(
        scored.groupBy(*keys).agg(F.count("*").alias("predicted_rows")),
        on=keys,
        how="left",
    ).fillna(0, subset=["predicted_rows"])
    correlation = scored.groupBy(*keys).agg(
        F.corr(
            F.col("actual_traffic_rank").cast("double"),
            F.col("predicted_traffic_rank").cast("double"),
        ).alias("spearman_rank_correlation")
    )

    frames: list[DataFrame] = []
    for cutoff in cutoffs:
        actual_top = ranked.filter(F.col("actual_traffic_rank") <= cutoff)
        predicted_top = scored.filter(F.col("predicted_traffic_rank") <= cutoff)
        ideal = actual_top.groupBy(*keys).agg(
            F.sum(
                F.log1p(F.col("target_next_hour_views").cast("double"))
                / F.log2(F.col("actual_traffic_rank") + F.lit(1.0))
            ).alias("idcg"),
            F.count("*").alias("actual_top_rows"),
        )
        predicted = predicted_top.groupBy(*keys).agg(
            F.sum(
                F.log1p(F.col("target_next_hour_views").cast("double"))
                / F.log2(F.col("predicted_traffic_rank") + F.lit(1.0))
            ).alias("dcg"),
            F.count("*").alias("predicted_top_rows"),
        )
        overlap = actual_top.select(*keys, "normalized_title").join(
            predicted_top.select(*keys, "normalized_title"),
            on=[*keys, "normalized_title"],
            how="inner",
        ).groupBy(*keys).agg(F.count("*").alias("top_k_overlap_count"))
        frames.append(
            coverage.join(ideal, on=keys, how="left")
            .join(predicted, on=keys, how="left")
            .join(overlap, on=keys, how="left")
            .join(correlation, on=keys, how="left")
            .fillna(
                0,
                subset=[
                    "dcg",
                    "idcg",
                    "actual_top_rows",
                    "predicted_top_rows",
                    "top_k_overlap_count",
                ],
            )
            .withColumn("k", F.lit(cutoff))
            .withColumn(
                "forecast_coverage",
                F.col("predicted_rows") / F.greatest(F.col("eligible_rows"), F.lit(1)),
            )
            .withColumn(
                "ndcg_at_k", F.when(F.col("idcg") > 0, F.col("dcg") / F.col("idcg"))
            )
            .withColumn(
                "recall_at_k",
                F.col("top_k_overlap_count")
                / F.greatest(F.col("actual_top_rows"), F.lit(1)),
            )
            .withColumn(
                "top_k_overlap",
                F.col("top_k_overlap_count")
                / F.greatest(
                    F.col("actual_top_rows")
                    + F.col("predicted_top_rows")
                    - F.col("top_k_overlap_count"),
                    F.lit(1),
                ),
            )
        )
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def write_metrics(
    predictions: DataFrame, output: Path, ranking_cutoffs: list[int]
) -> list[dict[str, object]]:
    paired = predictions.filter(
        F.col("target_next_hour_views").isNotNull()
        & F.col("lightgbm_predicted_views").isNotNull()
        & F.col("lag_1h_views").isNotNull()
        & (~F.col("feature_missing"))
    )
    method_frames = [
        _method_metrics(paired, "lightgbm_paired", "lightgbm_predicted_views"),
        _method_metrics(paired, "lag_1h_paired", "lag_1h_views"),
        _method_metrics(predictions, "lightgbm_operational", "lightgbm_predicted_views"),
        _method_metrics(predictions, "selected_forecast", "forecast_views"),
    ]
    metrics = method_frames[0]
    for frame in method_frames[1:]:
        metrics = metrics.unionByName(frame)
    eligible_rows = predictions.filter(F.col("target_next_hour_views").isNotNull()).count()
    metrics = (
        metrics.withColumn("eligible_rows", F.lit(eligible_rows))
        .withColumn(
            "forecast_coverage",
            F.col("evaluated_rows") / F.greatest(F.col("eligible_rows"), F.lit(1)),
        )
        .withColumn("evaluation_date", F.to_date("evaluation_start_hour"))
        .withColumn("evaluation_hour", F.hour("evaluation_start_hour"))
        .withColumn("scored_at_utc", F.current_timestamp())
    )
    metrics_path = output / "metrics"
    metrics.write.mode("overwrite").partitionBy("evaluation_date", "evaluation_hour").parquet(
        str(metrics_path)
    )
    ranking = build_ranking_metrics(predictions, ranking_cutoffs).withColumn(
        "evaluation_date", F.to_date("timestamp_hour")
    ).withColumn("evaluation_hour", F.hour("timestamp_hour"))
    ranking.write.mode("overwrite").partitionBy(
        "evaluation_date", "evaluation_hour", "project", "access_mode", "k"
    ).parquet(str(output / "ranking_metrics"))
    rows = [row.asDict() for row in metrics.collect()]
    return rows


def main() -> None:
    args = parse_args()
    ranking_cutoffs = sorted(
        {int(value.strip()) for value in args.ranking_cutoffs.split(",") if value.strip()}
    )
    if (
        args.quality_gate_window_hours <= 0
        or args.quality_gate_min_rows <= 0
        or args.quality_gate_min_origins <= 0
        or args.top_n <= 0
        or not ranking_cutoffs
        or any(value <= 0 for value in ranking_cutoffs)
    ):
        raise ValueError("Quality-gate, top-N, and ranking-cutoff values must be positive")
    project_root = Path(__file__).resolve().parents[1]
    forecast_features = _absolute_project_path(args.forecast_features, project_root)
    model_output = _absolute_project_path(args.model_output, project_root)
    predictions_output = _absolute_project_path(args.predictions_output, project_root)
    origin = (
        parse_utc_hour(args.timestamp_hour)
        if args.timestamp_hour
        else latest_gold_hour(forecast_features)
    )
    booster, metadata, category_levels, model_version = load_model_artifacts(model_output)
    feature_columns = list(metadata["feature_columns"])
    model_text = booster.model_to_string()
    spark_root, mapping_created = configure_windows_spark_path(project_root)
    spark_forecast_features = to_spark_path(forecast_features, project_root, spark_root)
    spark_predictions_output = to_spark_path(predictions_output, project_root, spark_root)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = build_spark_session("wikitrend-lightgbm-scoring", master=args.master)
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.default.parallelism", "8")
    try:
        baseline_hours = int(metadata.get("mase_baseline_hours", 24))
        source = read_origin_features(
            spark,
            forecast_features,
            spark_forecast_features,
            origin,
            baseline_hours,
        )
        source = add_scoring_time_features(source)
        source_count = source.count()
        if source_count == 0:
            raise ValueError(f"Gold has no forecast feature rows for origin {origin}")
        gate = {
            "degraded": False,
            "metric": args.quality_gate_metric,
            "model_value": None,
            "baseline_value": None,
            "history_rows": 0,
            "history_origins": 0,
            "reason": "disabled" if args.disable_quality_gate else "no_eligible_history",
        }
        if not args.disable_quality_gate:
            gate = quality_gate(
                spark,
                spark_predictions_output / "predictions",
                origin,
                args.quality_gate_metric,
                args.quality_gate_window_hours,
                args.quality_gate_min_rows,
                args.quality_gate_min_origins,
            )
        predict = prediction_udf(spark, model_text, category_levels, feature_columns)
        scoring_run_id = f"{origin:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
        predictions = build_predictions(
            source,
            predict,
            origin,
            model_version,
            str(metadata.get("manifest_id", "unknown")),
            scoring_run_id,
            gate,
        ).select(*PREDICTION_COLUMNS)
        predictions = predictions.persist(StorageLevel.DISK_ONLY)
        write_predictions(predictions, spark_predictions_output, args.top_n)
        metrics = write_metrics(predictions, spark_predictions_output, ranking_cutoffs)
        run_metadata = {
            "scoring_run_id": scoring_run_id,
            "origin_hour": origin.isoformat(),
            "forecast_hour": (origin + timedelta(hours=1)).isoformat(),
            "source_rows": source_count,
            "model_output": str(args.model_output),
            "model_version": model_version,
            "manifest_id": metadata.get("manifest_id"),
            "feature_columns": feature_columns,
            "quality_gate": gate,
            "metrics": metrics,
        }
        predictions_output.mkdir(parents=True, exist_ok=True)
        (predictions_output / "latest_run.json").write_text(
            json.dumps(run_metadata, indent=2, default=str) + "\n", encoding="utf-8"
        )
        print(f"Origin hour: {origin} UTC")
        print(f"Forecast hour: {origin + timedelta(hours=1)} UTC")
        print(f"Source rows: {source_count}")
        print(f"Quality gate: {gate['reason']}")
        print(f"Predictions: {predictions_output / 'predictions'}")
        print(f"Research top pages: {predictions_output / 'research_top_pages'}")
        if metrics:
            print("Evaluation metrics:")
            for row in metrics:
                print(row)
        else:
            print("Actual next-hour traffic is not available yet; metrics were not produced.")
    finally:
        spark.stop()
        if mapping_created:
            subprocess.run(["subst", "X:", "/D"], check=False)


if __name__ == "__main__":
    main()
