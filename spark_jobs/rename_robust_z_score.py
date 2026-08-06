from __future__ import annotations

import argparse
from pathlib import Path

from build_gold_tables import _write_table
from pyspark.sql import SparkSession

TABLES = ("trending_pages", "anomaly_alerts")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rename the verified Gold z-score column to robust_z_score."
    )
    parser.add_argument("--gold", type=Path, default=Path("data/gold"))
    parser.add_argument("--output-root", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("wikitrend-rename-robust-z-score")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        for table in TABLES:
            source = spark.read.parquet(str(args.gold / table))
            if "z_score" not in source.columns:
                raise ValueError(f"{table} does not contain the expected z_score column")
            renamed = source.withColumnRenamed("z_score", "robust_z_score")
            _write_table(renamed, args.output_root / table, "overwrite")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
