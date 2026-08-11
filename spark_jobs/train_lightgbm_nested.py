"""Nested rolling-origin LightGBM objective selection and final model training."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import lightgbm as lgb
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

try:
    from .forecast_evaluation import (
        CAT_COLUMNS,
        NUMERIC_COLUMNS,
        build_common_cohort,
        build_spark_session,
        ensure_mase_scale,
        read_partitioned_parquet,
        timestamp_literal,
    )
    from .score_lightgbm import (
        _method_metrics,
        build_ranking_metrics,
        configure_windows_spark_path,
        prediction_udf,
        to_spark_path,
    )
except ImportError:
    from forecast_evaluation import (
        CAT_COLUMNS,
        NUMERIC_COLUMNS,
        build_common_cohort,
        build_spark_session,
        ensure_mase_scale,
        read_partitioned_parquet,
        timestamp_literal,
    )
    from score_lightgbm import (
        _method_metrics,
        build_ranking_metrics,
        configure_windows_spark_path,
        prediction_udf,
        to_spark_path,
    )

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.forecast_metrics import paired_block_bootstrap_ratio_difference

FEATURE_COLUMNS = NUMERIC_COLUMNS + CAT_COLUMNS
SCORE_COLUMNS = list(
    dict.fromkeys(
        [
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "timestamp_hour",
            "normalized_title",
            "target_next_hour_views",
            "mase_scale",
            "lag_1h_views",
            *FEATURE_COLUMNS,
        ]
    )
)
BASE_PARAMS: dict[str, Any] = {
    "n_estimators": 1600,
    "learning_rate": 0.015,
    "num_leaves": 31,
    "max_depth": -1,
    "min_child_samples": 100,
    "subsample": 1.0,
    "subsample_freq": 0,
    "colsample_bytree": 1.0,
    "reg_lambda": 5.0,
    "random_state": 42,
    "n_jobs": 4,
    "verbosity": -1,
}


def objective_params(objective: str) -> dict[str, Any]:
    params = {**BASE_PARAMS, "objective": objective}
    if objective == "tweedie":
        params["tweedie_variance_power"] = 1.3
    return params


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("configs/forecast_fold_manifest_v2.json"),
    )
    parser.add_argument(
        "--snapshot-manifest",
        type=Path,
        default=Path("artifacts/manifests/training_snapshot.json"),
    )
    parser.add_argument("--model-root", type=Path, default=Path("models/lightgbm"))
    parser.add_argument(
        "--evaluation-root",
        type=Path,
        default=Path("data/gold/lightgbm_nested_evaluation"),
    )
    parser.add_argument("--allow-dirty-snapshot", action="store_true")
    parser.add_argument("--allow-holdout-rerun", action="store_true")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_snapshot(root: Path, snapshot: dict[str, Any], manifest: dict[str, Any]) -> None:
    if snapshot.get("manifest_version") != 1:
        raise ValueError("Unsupported training snapshot manifest version")
    if snapshot.get("dataset") != manifest.get("dataset"):
        raise ValueError(
            "Training snapshot dataset does not match the fixed fold manifest: "
            f"{snapshot.get('dataset')} != {manifest.get('dataset')}"
        )
    required_end = datetime.fromisoformat(
        manifest["dataset_window"]["end_hour_exclusive"]
    ) - timedelta(hours=1)
    available_end = datetime.fromisoformat(snapshot["dataset_end_hour"])
    if available_end < required_end:
        raise ValueError(
            f"Training snapshot ends at {available_end}, before required origin {required_end}"
        )
    for record in snapshot.get("files", []):
        path = root / record["path"]
        if not path.is_file():
            raise FileNotFoundError(f"Snapshot file is missing: {path}")
        if path.stat().st_size != int(record["size_bytes"]):
            raise ValueError(f"Snapshot file size changed: {path}")
        if sha256_file(path) != record["sha256"]:
            raise ValueError(f"Snapshot file hash changed: {path}")


def _time_filter(frame: DataFrame, start: str, end_exclusive: str) -> DataFrame:
    return frame.filter(
        (F.col("timestamp_hour") >= timestamp_literal(datetime.fromisoformat(start)))
        & (F.col("timestamp_hour") < timestamp_literal(datetime.fromisoformat(end_exclusive)))
    )


def _volume_bin(column: F.Column, boundaries: list[float]) -> F.Column:
    expression = F.lit(len(boundaries) - 1)
    for index in reversed(range(len(boundaries) - 1)):
        expression = F.when(
            (column >= F.lit(boundaries[index]))
            & (column < F.lit(boundaries[index + 1])),
            F.lit(index),
        ).otherwise(expression)
    return expression


def stratified_training_sample(
    frame: DataFrame,
    max_rows: int,
    volume_bins: list[float],
    seed: int,
) -> DataFrame:
    strata = ["project", "access_mode", "_volume_bin"]
    prepared = frame.withColumn("_volume_bin", _volume_bin(F.col("view_count"), volume_bins))
    counts = prepared.groupBy(*strata).agg(F.count("*").alias("_stratum_rows"))
    stratum_count = counts.count()
    if stratum_count == 0:
        return prepared.limit(0)
    quota = max(1, math.floor(max_rows / stratum_count))
    order = Window.partitionBy(*strata).orderBy(
        F.xxhash64(
            "timestamp_hour",
            "normalized_title",
            "project",
            "access_mode",
            F.lit(seed),
        )
    )
    return (
        prepared.join(counts, on=strata, how="inner")
        .withColumn("_sample_rank", F.row_number().over(order))
        .filter(F.col("_sample_rank") <= quota)
        .withColumn(
            "sampling_weight",
            F.col("_stratum_rows") / F.least(F.col("_stratum_rows"), F.lit(quota)),
        )
        .drop("_sample_rank", "_stratum_rows", "_volume_bin")
    )


def prepare_feature_matrix(
    frame: pd.DataFrame,
    category_levels: dict[str, list[str]] | None = None,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    work = frame.copy()
    learned_levels: dict[str, list[str]] = {}
    for column in CAT_COLUMNS:
        values = work[column].fillna("__unknown__").astype(str)
        levels = (
            sorted(set(values.tolist()) | {"__unknown__"})
            if category_levels is None
            else list(category_levels[column])
        )
        values = values.where(values.isin(levels), "__unknown__")
        work[column] = pd.Categorical(values, categories=levels)
        learned_levels[column] = levels
    for column in NUMERIC_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0.0)
    return work[FEATURE_COLUMNS], learned_levels


def _collect_sample(
    frame: DataFrame,
    max_rows: int,
    volume_bins: list[float],
    seed: int,
) -> pd.DataFrame:
    return stratified_training_sample(
        frame.select(*SCORE_COLUMNS), max_rows, volume_bins, seed
    ).toPandas()


def train_candidate(
    training: DataFrame,
    training_end: datetime,
    objective: str,
    sampling: dict[str, Any],
    seed_offset: int,
) -> tuple[lgb.LGBMRegressor, dict[str, list[str]], dict[str, int]]:
    early_stop_start = training_end - timedelta(hours=24)
    fit_frame = training.filter(F.col("timestamp_hour") < timestamp_literal(early_stop_start))
    validation_frame = training.filter(
        (F.col("timestamp_hour") >= timestamp_literal(early_stop_start))
        & (F.col("timestamp_hour") < timestamp_literal(training_end))
    )
    max_rows = int(sampling["max_training_rows"])
    volume_bins = [float(value) for value in sampling["volume_bins"]]
    seed = int(sampling["seed"]) + seed_offset
    fit_pdf = _collect_sample(fit_frame, max_rows, volume_bins, seed)
    validation_pdf = _collect_sample(
        validation_frame, max(50_000, max_rows // 5), volume_bins, seed + 10_000
    )
    if fit_pdf.empty or validation_pdf.empty:
        raise ValueError("A nested training split produced an empty fit or early-stop sample")
    fit_features, category_levels = prepare_feature_matrix(fit_pdf)
    validation_features, _ = prepare_feature_matrix(validation_pdf, category_levels)
    params = objective_params(objective)
    model = lgb.LGBMRegressor(**params)
    model.fit(
        fit_features,
        fit_pdf["target_next_hour_views"].astype("float64"),
        sample_weight=fit_pdf["sampling_weight"].astype("float64"),
        eval_set=[
            (
                validation_features,
                validation_pdf["target_next_hour_views"].astype("float64"),
            )
        ],
        eval_sample_weight=[validation_pdf["sampling_weight"].astype("float64")],
        eval_metric="l1",
        categorical_feature=CAT_COLUMNS,
        callbacks=[
            lgb.early_stopping(75, first_metric_only=True, verbose=False),
            lgb.log_evaluation(period=0),
        ],
    )
    diagnostics = {
        "fit_rows": len(fit_pdf),
        "early_stop_rows": len(validation_pdf),
        "best_iteration": int(model.best_iteration_ or 0),
    }
    return model, category_levels, diagnostics


def score_candidate(
    spark: SparkSession,
    evaluation: DataFrame,
    model: lgb.LGBMRegressor,
    category_levels: dict[str, list[str]],
) -> DataFrame:
    model_text = model.booster_.model_to_string()
    predict = prediction_udf(spark, model_text, category_levels, FEATURE_COLUMNS)
    scored = evaluation.withColumn(
        "lightgbm_predicted_views",
        predict(*[F.col(column) for column in FEATURE_COLUMNS]),
    )
    rank = Window.partitionBy(
        "timestamp_hour", "project", "access_mode"
    ).orderBy(
        F.desc_nulls_last("lightgbm_predicted_views"), F.asc("normalized_title")
    )
    return scored.withColumn(
        "forecast_views", F.col("lightgbm_predicted_views")
    ).withColumn("predicted_traffic_rank", F.row_number().over(rank))


def collect_block_metrics(
    scored: DataFrame,
    block_id: str,
    objective: str,
    split: str,
) -> list[dict[str, Any]]:
    paired = scored.filter(
        F.col("target_next_hour_views").isNotNull()
        & F.col("lightgbm_predicted_views").isNotNull()
        & F.col("lag_1h_views").isNotNull()
    )
    rows = []
    for method, column in (
        ("lightgbm", "lightgbm_predicted_views"),
        ("lag_1h", "lag_1h_views"),
    ):
        metric = _method_metrics(paired, method, column).first()
        if metric is not None:
            rows.append(
                {
                    **metric.asDict(),
                    "block_id": block_id,
                    "objective": objective,
                    "split": split,
                }
            )
    return rows


def _objective_mase(rows: list[dict[str, Any]], objective: str) -> float:
    selected = [
        row
        for row in rows
        if row["objective"] == objective and row["forecast_method"] == "lightgbm"
    ]
    numerator = sum(float(row["mase_sum"] or 0.0) for row in selected)
    denominator = sum(int(row["mase_valid_rows"] or 0) for row in selected)
    return numerator / denominator if denominator else math.inf


def _model_version(manifest: dict[str, Any], snapshot: dict[str, Any], objective: str) -> str:
    payload = json.dumps(
        {
            "manifest_id": manifest["manifest_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "objective": objective,
            "features": FEATURE_COLUMNS,
            "params": objective_params(objective),
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _bootstrap_reports(metrics: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
    reports = {}
    specifications = {
        "mase": ("mase_sum", "mase_valid_rows"),
        "nd": ("absolute_error_sum", "actual_abs_sum"),
        "msmape": ("msmape_sum", "evaluated_rows"),
    }
    for metric, (numerator, denominator) in specifications.items():
        reports[metric] = paired_block_bootstrap_ratio_difference(
            metrics,
            block_column="block_id",
            method_column="forecast_method",
            numerator_column=numerator,
            denominator_column=denominator,
            challenger="lightgbm",
            baseline="lag_1h",
            resamples=int(config["resamples"]),
            confidence_level=float(config["confidence_level"]),
            seed=int(config["seed"]),
        )
    return reports


def ranking_metrics_for_method(
    scored: DataFrame,
    prediction_column: str,
    method: str,
    cutoffs: list[int],
) -> DataFrame:
    rank = Window.partitionBy(
        "timestamp_hour", "source_project", "project", "access_mode"
    ).orderBy(F.desc_nulls_last(prediction_column), F.asc("normalized_title"))
    prepared = scored.withColumn(
        "forecast_views", F.col(prediction_column).cast("double")
    ).withColumn("predicted_traffic_rank", F.row_number().over(rank))
    return build_ranking_metrics(prepared, cutoffs).withColumn(
        "forecast_method", F.lit(method)
    )


def _ranking_bootstrap_reports(
    metrics: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    metric_names = [
        "forecast_coverage",
        "ndcg_at_k",
        "recall_at_k",
        "top_k_overlap",
        "spearman_rank_correlation",
    ]
    for cutoff in sorted(metrics["k"].dropna().unique()):
        cutoff_frame = metrics.loc[metrics["k"] == cutoff].copy()
        reports[str(int(cutoff))] = {}
        for metric in metric_names:
            work = cutoff_frame.loc[cutoff_frame[metric].notna()].copy()
            work["metric_sum"] = work[metric].astype(float)
            work["metric_rows"] = 1
            reports[str(int(cutoff))][metric] = paired_block_bootstrap_ratio_difference(
                work,
                block_column="block_id",
                method_column="forecast_method",
                numerator_column="metric_sum",
                denominator_column="metric_rows",
                challenger="lightgbm",
                baseline="lag_1h",
                resamples=int(config["resamples"]),
                confidence_level=float(config["confidence_level"]),
                seed=int(config["seed"]),
                lower_is_better=False,
            )
    return reports


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / args.manifest).read_text(encoding="utf-8"))
    snapshot = json.loads((root / args.snapshot_manifest).read_text(encoding="utf-8"))
    if snapshot.get("git_dirty") and not args.allow_dirty_snapshot:
        raise ValueError("Training snapshot was created from a dirty Git worktree")
    if manifest.get("manifest_version") != 2:
        raise ValueError("Nested LightGBM training requires a version-2 fold manifest")
    verify_snapshot(root, snapshot, manifest)

    dataset = root / manifest["dataset"]
    model_root = root / args.model_root
    evaluation_root = root / args.evaluation_root
    spark_root, _mapping_created = configure_windows_spark_path(root)
    spark_dataset = to_spark_path(dataset, root, spark_root)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = build_spark_session("wikitrend-lightgbm-nested", master="local[4]")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    inner_metrics: list[dict[str, Any]] = []
    outer_metrics: list[dict[str, Any]] = []
    ranking_frames: list[pd.DataFrame] = []
    selected_objectives: list[str] = []
    try:
        features = read_partitioned_parquet(spark, spark_dataset)
        common = build_common_cohort(ensure_mase_scale(features, baseline_hours=24)).persist(
            StorageLevel.DISK_ONLY
        )
        for outer_index, outer in enumerate(manifest["outer_blocks"]):
            objective_rows: list[dict[str, Any]] = []
            for objective_index, objective in enumerate(manifest["objective_candidates"]):
                for inner_index, inner in enumerate(outer["inner_blocks"]):
                    training = _time_filter(
                        common,
                        inner["training_start_hour"],
                        inner["training_end_hour_exclusive"],
                    )
                    evaluation = _time_filter(
                        common,
                        inner["evaluation_start_hour"],
                        inner["evaluation_end_hour_exclusive"],
                    )
                    model, levels, _diagnostics = train_candidate(
                        training,
                        datetime.fromisoformat(inner["training_end_hour_exclusive"]),
                        objective,
                        manifest["sampling"],
                        outer_index * 100 + objective_index * 10 + inner_index,
                    )
                    scored = score_candidate(spark, evaluation, model, levels)
                    objective_rows.extend(
                        collect_block_metrics(
                            scored, inner["block_id"], objective, "inner_validation"
                        )
                    )
            inner_metrics.extend(objective_rows)
            selected = min(
                manifest["objective_candidates"],
                key=lambda candidate: _objective_mase(objective_rows, candidate),
            )
            selected_objectives.append(selected)
            outer_training = _time_filter(
                common,
                outer["training_start_hour"],
                outer["training_end_hour_exclusive"],
            )
            outer_evaluation = _time_filter(
                common,
                outer["evaluation_start_hour"],
                outer["evaluation_end_hour_exclusive"],
            )
            model, levels, _diagnostics = train_candidate(
                outer_training,
                datetime.fromisoformat(outer["training_end_hour_exclusive"]),
                selected,
                manifest["sampling"],
                10_000 + outer_index,
            )
            scored = score_candidate(spark, outer_evaluation, model, levels).persist(
                StorageLevel.DISK_ONLY
            )
            outer_metrics.extend(
                collect_block_metrics(scored, outer["block_id"], selected, "outer_validation")
            )
            ranking = ranking_metrics_for_method(
                scored,
                "lightgbm_predicted_views",
                "lightgbm",
                manifest["ranking_cutoffs"],
            ).unionByName(
                ranking_metrics_for_method(
                    scored,
                    "lag_1h_views",
                    "lag_1h",
                    manifest["ranking_cutoffs"],
                )
            ).withColumn("block_id", F.lit(outer["block_id"])).withColumn(
                "objective", F.lit(selected)
            )
            ranking_frames.append(ranking.toPandas())
            scored.unpersist()

        final_objective = min(
            manifest["objective_candidates"],
            key=lambda candidate: _objective_mase(inner_metrics, candidate),
        )
        holdout = manifest["final_holdout"]
        holdout_lock = model_root / "HOLDOUT_OPENED.json"
        if holdout_lock.exists() and not args.allow_holdout_rerun:
            raise RuntimeError(
                "Final holdout has already been opened; create a new future holdout before retuning"
            )
        final_training = _time_filter(
            common,
            holdout["training_start_hour"],
            holdout["training_end_hour_exclusive"],
        )
        final_evaluation = _time_filter(
            common,
            holdout["evaluation_start_hour"],
            holdout["evaluation_end_hour_exclusive"],
        ).filter(F.col("target_next_hour_views").isNotNull())
        if final_training.limit(1).count() == 0 or final_evaluation.limit(1).count() == 0:
            raise ValueError("The final training or untouched holdout split is empty")
        final_model, final_levels, diagnostics = train_candidate(
            final_training,
            datetime.fromisoformat(holdout["training_end_hour_exclusive"]),
            final_objective,
            manifest["sampling"],
            20_000,
        )
        version = _model_version(manifest, snapshot, final_objective)
        version_dir = model_root / version
        version_dir.mkdir(parents=True, exist_ok=True)
        final_model.booster_.save_model(str(version_dir / "model.txt"))
        (version_dir / "category_levels.json").write_text(
            json.dumps(final_levels, indent=2) + "\n", encoding="utf-8"
        )
        metadata = {
            "model_version": version,
            "model": "LightGBM LGBMRegressor",
            "objective": final_objective,
            "feature_columns": FEATURE_COLUMNS,
            "categorical_columns": CAT_COLUMNS,
            "target": "target_next_hour_views",
            "manifest_id": manifest["manifest_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "mase_baseline_hours": 24,
            "training_diagnostics": diagnostics,
            "outer_selected_objectives": selected_objectives,
            "params": objective_params(final_objective),
        }
        (version_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        holdout_lock.parent.mkdir(parents=True, exist_ok=True)
        holdout_lock.write_text(
            json.dumps(
                {
                    "manifest_id": manifest["manifest_id"],
                    "model_version": version,
                    "opened_at_utc": datetime.now(UTC).isoformat(),
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        final_scored = score_candidate(
            spark, final_evaluation, final_model, final_levels
        ).persist(StorageLevel.DISK_ONLY)
        holdout_metrics = collect_block_metrics(
            final_scored, "final_holdout", final_objective, "final_holdout"
        )
        holdout_ranking = ranking_metrics_for_method(
            final_scored,
            "lightgbm_predicted_views",
            "lightgbm",
            manifest["ranking_cutoffs"],
        ).unionByName(
            ranking_metrics_for_method(
                final_scored,
                "lag_1h_views",
                "lag_1h",
                manifest["ranking_cutoffs"],
            )
        ).withColumn("block_id", F.lit("final_holdout")).withColumn(
            "objective", F.lit(final_objective)
        ).toPandas()

        version_evaluation = evaluation_root / version
        version_evaluation.mkdir(parents=True, exist_ok=True)
        inner_pdf = pd.DataFrame(inner_metrics)
        outer_pdf = pd.DataFrame(outer_metrics)
        inner_pdf.to_parquet(version_evaluation / "inner_metrics.parquet", index=False)
        outer_pdf.to_parquet(version_evaluation / "outer_metrics.parquet", index=False)
        pd.DataFrame(holdout_metrics).to_parquet(
            version_evaluation / "holdout_metrics.parquet", index=False
        )
        outer_ranking_pdf = pd.concat(ranking_frames, ignore_index=True)
        outer_ranking_pdf.to_parquet(
            version_evaluation / "outer_ranking_metrics.parquet", index=False
        )
        holdout_ranking.to_parquet(
            version_evaluation / "holdout_ranking_metrics.parquet", index=False
        )
        bootstrap = _bootstrap_reports(outer_pdf, manifest["block_bootstrap"])
        bootstrap["ranking"] = _ranking_bootstrap_reports(
            outer_ranking_pdf, manifest["block_bootstrap"]
        )
        (version_evaluation / "block_bootstrap_confidence_intervals.json").write_text(
            json.dumps(bootstrap, indent=2) + "\n", encoding="utf-8"
        )
        current_temp = model_root / "current.json.part"
        current_temp.write_text(
            json.dumps({"model_version": version, "path": version}, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(current_temp, model_root / "current.json")
        print(f"Published LightGBM model version {version} with objective {final_objective}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
