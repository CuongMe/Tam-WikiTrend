from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter Silver Parquet to selected Wikimedia project codes.")
    parser.add_argument("--input", required=True, help="Input Silver Parquet directory.")
    parser.add_argument("--output", required=True, help="Output Silver Parquet directory.")
    parser.add_argument("--projects", required=True, help="Comma-separated exact project codes to keep.")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    projects = [item.strip() for item in args.projects.split(",") if item.strip()]
    spark = (
        SparkSession.builder.appName("wikitrend-filter-projects")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    silver = spark.read.parquet(args.input).filter(F.col("project").isin(projects))
    (
        silver.write.mode(args.mode)
        .partitionBy("date", "hour", "project")
        .parquet(args.output)
    )
    spark.stop()


if __name__ == "__main__":
    main()
