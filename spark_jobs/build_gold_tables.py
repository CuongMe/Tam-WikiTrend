from __future__ import annotations

import argparse
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TOPIC_KEYS = ["project", "access_mode", "normalized_title"]
DIMENSION_COLUMNS = [
    "source_project",
    "project",
    "language",
    "project_family",
    "access_mode",
]
SYMBOL_ONLY_PATTERN = r"^[^\p{L}\p{N}]+$"


def timestamp_hour() -> F.Column:
    """Build a UTC hour timestamp from the Silver date and hour columns."""
    return F.to_timestamp(
        F.concat_ws(
            " ",
            F.col("date").cast("string"),
            F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00:00")),
        )
    )


def prepare_gold_input(silver: DataFrame, include_symbol_only: bool = False) -> DataFrame:
    """Keep valid topic candidates and retain the Silver dimensions needed by Gold."""
    valid = silver.filter(
        F.col("normalized_title").isNotNull()
        & (F.trim("normalized_title") != "")
        & F.col("view_count").isNotNull()
        & (F.col("view_count") >= 0)
        & F.col("response_size").isNotNull()
        & (F.col("response_size") >= 0)
    )
    if not include_symbol_only:
        valid = valid.filter(~F.col("normalized_title").rlike(SYMBOL_ONLY_PATTERN))
    return valid.select(
        "date",
        "hour",
        "source_project",
        "project",
        "language",
        "project_family",
        "access_mode",
        "page_title",
        "normalized_title",
        "view_count",
        "response_size",
    )


def build_page_hourly(silver: DataFrame) -> DataFrame:
    """Aggregate raw Silver observations to one row per topic, project, and hour."""
    return (
        silver.groupBy(
            "date",
            "hour",
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "normalized_title",
        )
        .agg(
            F.min("page_title").alias("page_title"),
            F.sum("view_count").cast("long").alias("view_count"),
            F.sum("response_size").cast("long").alias("response_size"),
            F.count("*").cast("long").alias("page_rows"),
        )
        .withColumn("timestamp_hour", timestamp_hour())
        .select(
            "timestamp_hour",
            "date",
            "hour",
            "project",
            "source_project",
            "language",
            "project_family",
            "access_mode",
            "page_title",
            "normalized_title",
            "view_count",
            "response_size",
            "page_rows",
        )
    )


def build_hourly_project_traffic(page_hourly: DataFrame) -> DataFrame:
    return (
        page_hourly.groupBy("timestamp_hour", "date", "hour", *DIMENSION_COLUMNS)
        .agg(
            F.sum("view_count").cast("long").alias("view_count"),
            F.sum("response_size").cast("long").alias("response_size"),
            F.sum("page_rows").cast("long").alias("page_rows"),
            F.count("*").cast("long").alias("topic_count"),
        )
        .select(
            "timestamp_hour",
            "date",
            "hour",
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "view_count",
            "response_size",
            "page_rows",
            "topic_count",
        )
    )


def build_top_pages_hourly(page_hourly: DataFrame, top_n: int) -> DataFrame:
    ranking = Window.partitionBy("date", "hour", "project", "access_mode").orderBy(
        F.desc("view_count"),
        F.asc("normalized_title"),
    )
    return (
        page_hourly.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= top_n)
        .select(
            "timestamp_hour",
            "date",
            "hour",
            "project",
            "source_project",
            "language",
            "project_family",
            "access_mode",
            "page_title",
            "normalized_title",
            "view_count",
            "response_size",
            "page_rows",
            "rank",
        )
    )


def build_complete_modeling_series(
    page_hourly: DataFrame,
    min_topic_views: int,
    min_history_hours: int,
) -> DataFrame:
    """Create a zero-complete hourly series for the bounded modeling universe."""
    topics = (
        page_hourly.groupBy(*TOPIC_KEYS)
        .agg(
            F.sum("view_count").alias("topic_total_views"),
            F.count("*").alias("topic_observed_hours"),
            F.min("page_title").alias("page_title"),
            F.first("source_project", ignorenulls=True).alias("source_project"),
            F.first("language", ignorenulls=True).alias("language"),
            F.first("project_family", ignorenulls=True).alias("project_family"),
        )
        .filter(
            (F.col("topic_total_views") >= min_topic_views)
            & (F.col("topic_observed_hours") >= min_history_hours)
        )
        .drop("topic_total_views", "topic_observed_hours")
    )
    hours = page_hourly.select("timestamp_hour", "date", "hour").distinct()
    observed = page_hourly.select(
        *TOPIC_KEYS,
        "timestamp_hour",
        F.lit(True).alias("is_observed"),
        "view_count",
        "response_size",
        "page_rows",
    )
    return (
        topics.crossJoin(F.broadcast(hours))
        .join(observed, on=[*TOPIC_KEYS, "timestamp_hour"], how="left")
        .fillna(False, subset=["is_observed"])
        .fillna(0, subset=["view_count", "response_size", "page_rows"])
        .withColumn("view_count", F.col("view_count").cast("long"))
        .withColumn("response_size", F.col("response_size").cast("long"))
        .withColumn("page_rows", F.col("page_rows").cast("long"))
    )


def add_past_only_eligibility(
    complete_page_hourly: DataFrame,
    min_topic_views: int,
    min_history_hours: int,
) -> DataFrame:
    """Mark eligibility from cumulative history strictly before each origin."""
    history = (
        Window.partitionBy(*TOPIC_KEYS)
        .orderBy(F.col("timestamp_hour").cast("long"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return (
        complete_page_hourly.withColumn(
            "eligibility_history_views", F.sum("view_count").over(history)
        )
        .withColumn(
            "eligibility_observed_hours",
            F.sum(F.col("is_observed").cast("long")).over(history),
        )
        .withColumn(
            "eligible_at_origin",
            (F.col("eligibility_history_views") >= min_topic_views)
            & (F.col("eligibility_observed_hours") >= min_history_hours),
        )
    )


def _history_window(start: int, end: int) -> Window:
    return (
        Window.partitionBy(*TOPIC_KEYS)
        .orderBy(F.col("timestamp_hour").cast("long"))
        .rangeBetween(start, end)
    )


def _array_median(values: F.Column) -> F.Column:
    ordered = F.array_sort(values)
    count = F.size(ordered)
    lower_index = F.floor((count + F.lit(1)) / F.lit(2)).cast("int")
    upper_index = F.floor((count + F.lit(2)) / F.lit(2)).cast("int")
    return F.when(
        count > 0,
        (F.element_at(ordered, lower_index) + F.element_at(ordered, upper_index)) / 2.0,
    )


def build_trends_and_anomalies(
    page_hourly: DataFrame,
    min_views: int,
    z_threshold: float,
    baseline_hours: int,
    min_baseline_observations: int,
) -> tuple[DataFrame, DataFrame]:
    """Create leakage-safe robust trend features and anomaly alerts."""
    hour_seconds = 60 * 60
    history = _history_window(-baseline_hours * hour_seconds, -1)
    previous_hour = _history_window(-hour_seconds, -hour_seconds)
    robust_z_scale = 0.6744897502
    enriched = (
        page_hourly.withColumn("previous_hour_views", F.max("view_count").over(previous_hour))
        .withColumn("rolling_baseline_avg", F.avg("view_count").over(history))
        .withColumn("rolling_baseline_stddev", F.stddev_pop("view_count").over(history))
        .withColumn("baseline_observed_hours", F.count("view_count").over(history))
        .withColumn("baseline_window_hours", F.lit(baseline_hours))
        .withColumn(
            "growth_rate",
            F.when(
                F.col("previous_hour_views") > 0,
                F.col("view_count") / F.col("previous_hour_views") - F.lit(1.0),
            ),
        )
        .withColumn("log1p_views", F.log1p(F.col("view_count").cast("double")))
        .withColumn(
            "_history_log_values", F.collect_list("log1p_views").over(history)
        )
        .withColumn(
            "rolling_baseline_log_median", _array_median(F.col("_history_log_values"))
        )
        .withColumn(
            "_history_log_deviations",
            F.transform(
                F.col("_history_log_values"),
                lambda value: F.abs(value - F.col("rolling_baseline_log_median")),
            ),
        )
        .withColumn(
            "rolling_baseline_log_mad", _array_median(F.col("_history_log_deviations"))
        )
        .withColumn(
            "robust_z_score",
            F.when(
                (F.col("rolling_baseline_log_mad") > 0)
                & (F.col("baseline_observed_hours") >= min_baseline_observations),
                F.lit(robust_z_scale)
                * (F.col("log1p_views") - F.col("rolling_baseline_log_median"))
                / F.col("rolling_baseline_log_mad"),
            ),
        )
        .withColumn(
            "trend_score",
            F.greatest(F.coalesce(F.col("robust_z_score"), F.lit(0.0)), F.lit(0.0))
            * F.col("log1p_views")
            * F.least(
                F.lit(1.0),
                F.col("baseline_observed_hours") / F.lit(float(min_baseline_observations)),
            ),
        )
        .filter(F.col("eligible_at_origin") & (F.col("view_count") > 0))
    )
    trend_rank = Window.partitionBy("date", "hour", "project", "access_mode").orderBy(
        F.desc("trend_score"),
        F.desc("view_count"),
        F.asc("normalized_title"),
    )
    enriched = enriched.withColumn("trend_rank", F.row_number().over(trend_rank))
    enriched = enriched.drop(
        "_history_log_values", "_history_log_deviations"
    ).persist(StorageLevel.DISK_ONLY)
    anomalies = (
        enriched.filter(F.col("view_count") >= min_views)
        .filter(F.col("robust_z_score") >= z_threshold)
        .withColumn("alert_type", F.lit("traffic_spike"))
        .withColumn(
            "alert_severity",
            F.when(F.col("robust_z_score") >= z_threshold * 2, "critical").otherwise("high"),
        )
        .select(
            "timestamp_hour",
            "date",
            "hour",
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "page_title",
            "normalized_title",
            "view_count",
            "rolling_baseline_avg",
            "rolling_baseline_stddev",
            "rolling_baseline_log_median",
            "rolling_baseline_log_mad",
            "baseline_observed_hours",
            "baseline_window_hours",
            "eligibility_history_views",
            "eligibility_observed_hours",
            "growth_rate",
            "robust_z_score",
            "trend_score",
            "alert_type",
            "alert_severity",
        )
    )
    return enriched, anomalies


def build_forecast_features(
    page_hourly: DataFrame,
    baseline_hours: int,
    forecast_average_hours: int,
) -> DataFrame:
    """Create next-hour baseline features using only observations before the current hour."""
    hour_seconds = 60 * 60
    history = _history_window(-baseline_hours * hour_seconds, -1)
    previous_hour = _history_window(-hour_seconds, -hour_seconds)
    previous_day = _history_window(-24 * hour_seconds, -24 * hour_seconds)
    next_hour = _history_window(hour_seconds, hour_seconds)
    rolling_window = _history_window(-forecast_average_hours * hour_seconds, -1)
    return (
        page_hourly.withColumn("lag_1h_views", F.max("view_count").over(previous_hour))
        .withColumn("lag_24h_views", F.max("view_count").over(previous_day))
        .withColumn("rolling_forecast_avg", F.avg("view_count").over(rolling_window))
        .withColumn("forecast_history_elapsed_hours", F.count("view_count").over(history))
        .withColumn(
            "forecast_history_active_hours",
            F.sum(F.col("is_observed").cast("long")).over(history),
        )
        .withColumn(
            "naive_absolute_error",
            F.abs(F.col("view_count") - F.col("lag_1h_views")),
        )
        .withColumn("mase_scale", F.avg("naive_absolute_error").over(history))
        .withColumn("target_next_hour_views", F.max("view_count").over(next_hour))
        .withColumn(
            "baseline_forecast",
            F.coalesce(
                F.col("lag_24h_views").cast("double"),
                F.col("rolling_forecast_avg"),
                F.col("lag_1h_views").cast("double"),
            ),
        )
        .withColumn("forecast_horizon_hours", F.lit(1))
        .withColumn("baseline_window_hours", F.lit(baseline_hours))
        .withColumn("forecast_average_window_hours", F.lit(forecast_average_hours))
        .withColumn("forecast_available", F.col("baseline_forecast").isNotNull())
        .filter(F.col("eligible_at_origin"))
        .select(
            "timestamp_hour",
            "date",
            "hour",
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
            "lag_24h_views",
            "rolling_forecast_avg",
            "forecast_history_elapsed_hours",
            "forecast_history_active_hours",
            "mase_scale",
            "baseline_forecast",
            "forecast_horizon_hours",
            "baseline_window_hours",
            "forecast_average_window_hours",
            "forecast_available",
            "target_next_hour_views",
            "eligibility_history_views",
            "eligibility_observed_hours",
        )
    )


def build_forecast_evaluation(
    forecast_features: DataFrame,
    smape_epsilon: float,
    baseline_hours: int = 24,
) -> DataFrame:
    """Evaluate baseline forecasts only where the next-hour target is observed."""
    if "mase_scale" not in forecast_features.columns:
        history = _history_window(-baseline_hours * 60 * 60, -1)
        forecast_features = (
            forecast_features.withColumn(
                "naive_absolute_error",
                F.abs(F.col("view_count") - F.col("lag_1h_views")),
            )
            .withColumn("mase_scale", F.avg("naive_absolute_error").over(history))
            .drop("naive_absolute_error")
        )
    dimensions = [
        "source_project",
        "project",
        "language",
        "project_family",
        "access_mode",
    ]
    methods = [
        ("baseline_forecast", "baseline_forecast"),
        ("lag_1h", "lag_1h_views"),
        ("lag_24h", "lag_24h_views"),
        ("rolling_average", "rolling_forecast_avg"),
    ]
    evaluations = []
    for method_name, column_name in methods:
        evaluations.append(
            forecast_features.select(
                *dimensions,
                "timestamp_hour",
                "target_next_hour_views",
                "mase_scale",
                F.lit(method_name).alias("forecast_method"),
                F.col(column_name).cast("double").alias("predicted_views"),
            )
        )
    long_form = evaluations[0]
    for evaluation in evaluations[1:]:
        long_form = long_form.unionByName(evaluation)
    scored = (
        long_form.filter(
            F.col("target_next_hour_views").isNotNull() & F.col("predicted_views").isNotNull()
        )
        .withColumn(
            "absolute_error",
            F.abs(F.col("target_next_hour_views") - F.col("predicted_views")),
        )
        .withColumn(
            "mase_observation",
            F.when(F.col("mase_scale") > 0, F.col("absolute_error") / F.col("mase_scale")),
        )
        .withColumn(
            "smape_observation",
            F.when(
                (F.abs(F.col("target_next_hour_views")) + F.abs(F.col("predicted_views"))) > 0,
                2.0
                * F.col("absolute_error")
                / (F.abs(F.col("target_next_hour_views")) + F.abs(F.col("predicted_views"))),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "msmape_observation",
            2.0
            * F.col("absolute_error")
            / F.greatest(
                F.abs(F.col("target_next_hour_views")) + F.abs(F.col("predicted_views")),
                F.lit(smape_epsilon),
            ),
        )
    )
    return scored.groupBy(*dimensions, "forecast_method").agg(
        F.count("*").cast("long").alias("evaluated_rows"),
        F.count("mase_observation").cast("long").alias("mase_valid_rows"),
        F.avg("mase_observation").alias("mase"),
        F.when(
            F.sum(F.abs(F.col("target_next_hour_views"))) > 0,
            F.sum("absolute_error") / F.sum(F.abs(F.col("target_next_hour_views"))),
        ).alias("nd"),
        F.avg("smape_observation").alias("smape"),
        F.avg("msmape_observation").alias("msmape"),
        F.min("timestamp_hour").alias("evaluation_start_hour"),
        F.max("timestamp_hour").alias("evaluation_end_hour"),
    )


def _write_table(
    frame: DataFrame,
    output: Path,
    mode: str,
    partition_columns: tuple[str, ...] = ("date", "project", "access_mode"),
) -> None:
    writer = frame.write.mode(mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(output))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build WikiTrend Gold analytics tables.")
    parser.add_argument("--silver", required=True, help="Input Silver pageviews Parquet directory.")
    parser.add_argument("--gold", required=True, help="Gold output directory.")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--min-topic-views", type=int, default=100)
    parser.add_argument("--min-history-hours", type=int, default=6)
    parser.add_argument("--baseline-hours", type=int, default=24)
    parser.add_argument("--forecast-average-hours", type=int, default=6)
    parser.add_argument("--min-anomaly-views", type=int, default=1000)
    parser.add_argument("--min-baseline-observations", type=int, default=6)
    parser.add_argument("--z-threshold", type=float, default=4.0)
    parser.add_argument(
        "--smape-epsilon",
        type=float,
        default=1.0,
        help="View-count epsilon used by modified sMAPE.",
    )
    parser.add_argument("--include-symbol-only", action="store_true")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args()
    if args.top_n <= 0:
        parser.error("--top-n must be positive")
    for name in (
        "min_topic_views",
        "min_history_hours",
        "baseline_hours",
        "forecast_average_hours",
        "min_anomaly_views",
        "min_baseline_observations",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.min_baseline_observations > args.baseline_hours:
        parser.error("--min-baseline-observations cannot exceed --baseline-hours")
    if args.z_threshold <= 0:
        parser.error("--z-threshold must be positive")
    if args.smape_epsilon <= 0:
        parser.error("--smape-epsilon must be positive")
    return args


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("wikitrend-build-gold-tables")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.read.parquet(args.silver)
        clean_silver = prepare_gold_input(silver, include_symbol_only=args.include_symbol_only)
        page_hourly = build_page_hourly(clean_silver).persist(StorageLevel.DISK_ONLY)

        gold_root = Path(args.gold)
        page_hourly_output = gold_root / "page_hourly"
        page_hourly_output.parent.mkdir(parents=True, exist_ok=True)
        _write_table(page_hourly, page_hourly_output, args.mode)

        hourly_project_traffic = build_hourly_project_traffic(page_hourly)
        top_pages_hourly = build_top_pages_hourly(page_hourly, args.top_n)
        complete_modeling_series = build_complete_modeling_series(
            page_hourly,
            min_topic_views=args.min_topic_views,
            min_history_hours=args.min_history_hours,
        ).persist(StorageLevel.DISK_ONLY)
        modeling_page_hourly = add_past_only_eligibility(
            complete_modeling_series,
            min_topic_views=args.min_topic_views,
            min_history_hours=args.min_history_hours,
        ).persist(StorageLevel.DISK_ONLY)
        trending_pages, anomaly_alerts = build_trends_and_anomalies(
            modeling_page_hourly,
            min_views=args.min_anomaly_views,
            z_threshold=args.z_threshold,
            baseline_hours=args.baseline_hours,
            min_baseline_observations=args.min_baseline_observations,
        )
        forecast_features = build_forecast_features(
            modeling_page_hourly,
            baseline_hours=args.baseline_hours,
            forecast_average_hours=args.forecast_average_hours,
        )
        forecast_evaluation = build_forecast_evaluation(
            forecast_features,
            smape_epsilon=args.smape_epsilon,
            baseline_hours=args.baseline_hours,
        )

        _write_table(hourly_project_traffic, gold_root / "hourly_project_traffic", args.mode)
        _write_table(top_pages_hourly, gold_root / "top_pages_hourly", args.mode)
        _write_table(modeling_page_hourly, gold_root / "modeling_page_hourly", args.mode)
        _write_table(trending_pages, gold_root / "trending_pages", args.mode)
        _write_table(anomaly_alerts, gold_root / "anomaly_alerts", args.mode)
        _write_table(forecast_features, gold_root / "forecast_features", args.mode)
        _write_table(
            forecast_evaluation,
            gold_root / "forecast_evaluation",
            args.mode,
            partition_columns=("project", "access_mode"),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
