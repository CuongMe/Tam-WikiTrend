from __future__ import annotations

import argparse
from pathlib import Path

from build_gold_tables import (
    _write_table,
    build_trends_and_anomalies,
    filter_eligible_topics,
)
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild WikiTrend robust trend and anomaly Gold tables."
    )
    parser.add_argument("--gold", type=Path, default=Path("data/gold"))
    parser.add_argument("--page-hourly", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--baseline-hours", type=int, default=24)
    parser.add_argument("--min-topic-views", type=int, default=100)
    parser.add_argument("--min-history-hours", type=int, default=6)
    parser.add_argument("--min-baseline-observations", type=int, default=6)
    parser.add_argument("--min-anomaly-views", type=int, default=1000)
    parser.add_argument("--z-threshold", type=float, default=4.0)
    args = parser.parse_args()
    for name in (
        "baseline_hours",
        "min_topic_views",
        "min_history_hours",
        "min_baseline_observations",
        "min_anomaly_views",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.min_baseline_observations > args.baseline_hours:
        parser.error("--min-baseline-observations cannot exceed --baseline-hours")
    if args.z_threshold <= 0:
        parser.error("--z-threshold must be positive")
    return args


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("wikitrend-rebuild-robust-trend-tables")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        page_hourly_path = args.page_hourly or args.gold / "page_hourly"
        output_root = args.output_root or args.gold
        page_hourly = spark.read.parquet(str(page_hourly_path))
        eligible_page_hourly = filter_eligible_topics(
            page_hourly,
            min_topic_views=args.min_topic_views,
            min_history_hours=args.min_history_hours,
        )
        trending_pages, anomaly_alerts = build_trends_and_anomalies(
            eligible_page_hourly,
            min_views=args.min_anomaly_views,
            z_threshold=args.z_threshold,
            baseline_hours=args.baseline_hours,
            min_baseline_observations=args.min_baseline_observations,
        )
        _write_table(
            trending_pages,
            output_root / "trending_pages",
            "overwrite",
        )
        _write_table(
            anomaly_alerts,
            output_root / "anomaly_alerts",
            "overwrite",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
