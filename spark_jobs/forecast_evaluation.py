from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CAT_COLUMNS = ["project", "language", "project_family", "access_mode"]
NUMERIC_COLUMNS = [
    "is_observed",
    "view_count",
    "lag_1h_views",
    "lag_24h_views",
    "rolling_forecast_avg",
    "forecast_history_elapsed_hours",
    "forecast_history_active_hours",
    "hour_sin",
    "hour_cos",
]
REQUIRED_COMMON_COLUMNS = [
    "timestamp_hour",
    "source_project",
    "project",
    "language",
    "project_family",
    "access_mode",
    "normalized_title",
    "is_observed",
    "view_count",
    "lag_1h_views",
    "lag_24h_views",
    "rolling_forecast_avg",
    "baseline_forecast",
    "forecast_history_elapsed_hours",
    "forecast_history_active_hours",
    "mase_scale",
    "target_next_hour_views",
]


@dataclass(frozen=True)
class Fold:
    fold_id: str
    origin_hour: datetime
    training_start_hour: datetime
    training_end_hour: datetime


def load_forecast_fold_manifest(path: Path) -> tuple[dict, list[Fold], list[datetime]]:
    """Load and validate the versioned fold schedule shared by all models."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "manifest_version",
        "manifest_id",
        "training_window_hours",
        "forecast_horizon_hours",
        "fold_stride_hours",
        "development_folds",
        "final_holdout",
    }
    missing = required - set(payload)
    if missing:
        raise ValueError(f"Fold manifest is missing required keys: {sorted(missing)}")

    holdout = payload["final_holdout"]
    for key in (
        "start_hour",
        "end_hour_exclusive",
        "origins",
        "model_training_start_hour",
        "model_training_end_hour",
    ):
        if key not in holdout:
            raise ValueError(f"Fold manifest final_holdout is missing {key!r}")

    training_window_hours = int(payload["training_window_hours"])
    horizon_hours = int(payload["forecast_horizon_hours"])
    stride_hours = int(payload["fold_stride_hours"])
    if training_window_hours <= 0 or horizon_hours <= 0 or stride_hours <= 0:
        raise ValueError("Fold manifest window, horizon, and stride must be positive")

    folds: list[Fold] = []
    for item in payload["development_folds"]:
        try:
            origin = datetime.fromisoformat(item["origin_hour"])
            training_start = datetime.fromisoformat(item["training_start_hour"])
            training_end = datetime.fromisoformat(item["training_end_hour"])
            target = datetime.fromisoformat(item["target_hour"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid development fold in {path}: {item!r}") from exc
        if training_start >= training_end:
            raise ValueError(f"Fold {item.get('fold_id')} has an invalid training window")
        if training_end - training_start != timedelta(hours=training_window_hours):
            raise ValueError(
                f"Fold {item.get('fold_id')} does not use the manifest training window"
            )
        if training_end != origin - timedelta(hours=horizon_hours):
            raise ValueError(f"Fold {item.get('fold_id')} violates the forecast embargo")
        if target != origin + timedelta(hours=horizon_hours):
            raise ValueError(f"Fold {item.get('fold_id')} has an invalid target hour")
        folds.append(
            Fold(
                fold_id=str(item["fold_id"]),
                origin_hour=origin,
                training_start_hour=training_start,
                training_end_hour=training_end,
            )
        )

    development_origins = [fold.origin_hour for fold in folds]
    if development_origins != sorted(development_origins):
        raise ValueError("Development fold origins must be sorted")
    if len(set(development_origins)) != len(development_origins):
        raise ValueError("Development fold origins must be unique")
    if any(
        right - left != timedelta(hours=stride_hours)
        for left, right in zip(development_origins, development_origins[1:], strict=False)
    ):
        raise ValueError("Development folds do not use the manifest stride")

    holdout_origins = [datetime.fromisoformat(value) for value in holdout["origins"]]
    if not holdout_origins:
        raise ValueError("Fold manifest final_holdout must contain at least one origin")
    if holdout_origins != sorted(holdout_origins):
        raise ValueError("Final holdout origins must be sorted")
    if len(set(holdout_origins)) != len(holdout_origins):
        raise ValueError("Final holdout origins must be unique")
    holdout_start = datetime.fromisoformat(holdout["start_hour"])
    holdout_end = datetime.fromisoformat(holdout["end_hour_exclusive"])
    if holdout_start != holdout_origins[0] or holdout_end <= holdout_origins[-1]:
        raise ValueError("Final holdout bounds do not match its origin list")
    if any(
        right - left != timedelta(hours=1)
        for left, right in zip(holdout_origins, holdout_origins[1:], strict=False)
    ):
        raise ValueError("Final holdout origins must be contiguous hourly origins")
    if holdout_end != holdout_origins[-1] + timedelta(hours=1):
        raise ValueError("Final holdout end must be one hour after its last origin")
    model_training_start = datetime.fromisoformat(holdout["model_training_start_hour"])
    model_training_end = datetime.fromisoformat(holdout["model_training_end_hour"])
    if model_training_end - model_training_start != timedelta(hours=training_window_hours):
        raise ValueError("Final holdout model does not use the manifest training window")
    if model_training_end != holdout_start - timedelta(hours=horizon_hours):
        raise ValueError("Final holdout model violates the forecast embargo")
    if set(development_origins) & set(holdout_origins):
        raise ValueError("A development fold overlaps the final holdout")
    return payload, folds, holdout_origins


def add_model_features(frame: DataFrame) -> DataFrame:
    hour_angle = F.lit(2.0 * math.pi) * F.hour("timestamp_hour") / F.lit(24.0)
    return (
        frame.withColumn("hour_sin", F.sin(hour_angle))
        .withColumn("hour_cos", F.cos(hour_angle))
        .withColumn("label", F.log1p(F.col("target_next_hour_views").cast("double")))
        .fillna("__unknown__", subset=CAT_COLUMNS)
    )


def resolve_parquet_files(path: Path) -> list[str]:
    if path.is_file():
        return [str(path)]
    files = sorted(str(file) for file in path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {path}")
    return files


def read_partitioned_parquet(spark: SparkSession, path: Path) -> DataFrame:
    files = resolve_parquet_files(path)
    frame = spark.read.option("basePath", str(path)).parquet(*files)
    if "project" not in frame.columns:
        frame = frame.withColumn(
            "project",
            F.regexp_extract(F.input_file_name(), r"project=([^/\\]+)", 1),
        )
    return frame


def ensure_mase_scale(frame: DataFrame, baseline_hours: int) -> DataFrame:
    if "mase_scale" in frame.columns:
        return frame
    history = (
        Window.partitionBy("project", "access_mode", "normalized_title")
        .orderBy(F.col("timestamp_hour").cast("long"))
        .rangeBetween(-baseline_hours * 60 * 60, -1)
    )
    return (
        frame.withColumn(
            "_naive_absolute_error",
            F.abs(F.col("view_count") - F.col("lag_1h_views")),
        )
        .withColumn("mase_scale", F.avg("_naive_absolute_error").over(history))
        .drop("_naive_absolute_error")
    )


def build_common_cohort(forecast_features: DataFrame) -> DataFrame:
    missing = set(REQUIRED_COMMON_COLUMNS) - set(forecast_features.columns)
    if missing:
        raise ValueError(f"forecast_features is missing required columns: {sorted(missing)}")
    common = forecast_features.select(*REQUIRED_COMMON_COLUMNS).filter(
        F.col("target_next_hour_views").isNotNull()
        & (F.col("target_next_hour_views") >= 0)
    )
    for column in (
        "view_count",
        "lag_1h_views",
        "lag_24h_views",
        "rolling_forecast_avg",
        "baseline_forecast",
        "forecast_history_elapsed_hours",
        "forecast_history_active_hours",
    ):
        common = common.filter(F.col(column).isNotNull())
    return add_model_features(common)


def make_folds(
    start_hour: datetime,
    end_hour: datetime,
    training_window_hours: int,
    horizon_hours: int,
    stride_hours: int,
) -> list[Fold]:
    first_origin = start_hour + timedelta(hours=training_window_hours + horizon_hours)
    last_origin = end_hour - timedelta(hours=horizon_hours)
    folds: list[Fold] = []
    origin = first_origin
    index = 0
    while origin <= last_origin:
        training_end = origin - timedelta(hours=horizon_hours)
        training_start = training_end - timedelta(hours=training_window_hours)
        folds.append(
            Fold(
                fold_id=f"fold_{index:03d}",
                origin_hour=origin,
                training_start_hour=training_start,
                training_end_hour=training_end,
            )
        )
        origin += timedelta(hours=stride_hours)
        index += 1
    return folds


def timestamp_literal(value: datetime) -> F.Column:
    return F.to_timestamp(F.lit(value.strftime("%Y-%m-%d %H:%M:%S")))


def deterministic_sample(frame: DataFrame, max_rows: int, seed: int) -> DataFrame:
    row_count = frame.count()
    if row_count <= max_rows:
        return frame
    return frame.sample(withReplacement=False, fraction=max_rows / row_count, seed=seed)


def with_fold_columns(frame: DataFrame, fold: Fold) -> DataFrame:
    return frame.withColumn("fold_id", F.lit(fold.fold_id)).withColumn(
        "origin_hour", timestamp_literal(fold.origin_hour)
    ).withColumn("training_start_hour", timestamp_literal(fold.training_start_hour)).withColumn(
        "training_end_hour", timestamp_literal(fold.training_end_hour)
    )


def build_spark_session(
    app_name: str = "wikitrend-forecast-evaluation", master: str | None = None
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if master:
        builder = builder.master(master)
    hadoop_bin = Path(__file__).resolve().parents[1] / ".hadoop" / "bin"
    if os.name == "nt" and hadoop_bin.exists():
        os.environ.setdefault("HADOOP_HOME", str(hadoop_bin.parent))
        library_path = os.environ.get("WIKITREND_HADOOP_BIN", hadoop_bin.as_posix())
        builder = (
            builder.config("spark.driver.extraJavaOptions", f"-Djava.library.path={library_path}")
            .config("spark.executor.extraJavaOptions", f"-Djava.library.path={library_path}")
        )
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
