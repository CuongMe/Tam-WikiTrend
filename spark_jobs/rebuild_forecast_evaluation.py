from __future__ import annotations

import argparse
from pathlib import Path

from build_gold_tables import build_forecast_evaluation
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild Gold forecast evaluation metrics.")
    parser.add_argument("--gold", type=Path, default=Path("data/gold"))
    parser.add_argument("--forecast-features", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline-hours", type=int, default=24)
    parser.add_argument("--smape-epsilon", type=float, default=1.0)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    args = parser.parse_args()
    if args.baseline_hours <= 0:
        parser.error("--baseline-hours must be positive")
    if args.smape_epsilon <= 0:
        parser.error("--smape-epsilon must be positive")
    return args


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("wikitrend-rebuild-forecast-evaluation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        forecast_features_path = args.forecast_features or args.gold / "forecast_features"
        output_path = args.output or args.gold / "forecast_evaluation"
        forecast_features = spark.read.parquet(str(forecast_features_path))
        evaluation = build_forecast_evaluation(
            forecast_features,
            smape_epsilon=args.smape_epsilon,
            baseline_hours=args.baseline_hours,
        )
        evaluation.write.mode(args.mode).partitionBy("project").parquet(str(output_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
