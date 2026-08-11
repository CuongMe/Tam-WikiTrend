"""Publish validated local Parquet staging tables as versioned Delta tables in MinIO."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_TABLES = (
    "page_hourly",
    "hourly_project_traffic",
    "top_pages_hourly",
    "modeling_page_hourly",
    "trending_pages",
    "anomaly_alerts",
    "forecast_features",
    "forecast_evaluation",
    "lightgbm_predictions/predictions",
    "lightgbm_predictions/research_top_pages",
    "lightgbm_predictions/metrics",
    "lightgbm_predictions/ranking_metrics",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", type=Path, required=True)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--snapshot-manifest", type=Path, required=True)
    parser.add_argument("--bucket", default=os.getenv("WIKITREND_MINIO_BUCKET", "wikitrend"))
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER_URL"))
    parser.add_argument(
        "--publication-output",
        type=Path,
        default=Path("artifacts/publications"),
    )
    return parser.parse_args()


def build_spark(master: str | None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("wikitrend-publish-delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def read_parquet(spark: SparkSession, path: Path):
    files = sorted(str(item) for item in path.rglob("*.parquet"))
    if not files:
        return None
    return spark.read.option("basePath", str(path)).parquet(*files)


def publish_table(frame, destination: str, snapshot_id: str) -> None:
    partition_candidates = [
        "date",
        "forecast_date",
        "hour",
        "forecast_hour_of_day",
        "project",
        "access_mode",
        "ranking_type",
        "k",
    ]
    partitions = [column for column in partition_candidates if column in frame.columns]
    writer = (
        frame.write.format("delta")
        .mode("errorifexists")
        .option("userMetadata", json.dumps({"snapshot_id": snapshot_id}))
    )
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(destination)


def main() -> None:
    args = parse_args()
    snapshot = json.loads(args.snapshot_manifest.read_text(encoding="utf-8"))
    snapshot_id = str(snapshot["snapshot_id"]).removeprefix("sha256:")
    snapshot_root = f"s3a://{args.bucket}/snapshots/{snapshot_id}"
    spark = build_spark(args.master)
    published: list[dict[str, str]] = []
    try:
        silver = read_parquet(spark, args.silver)
        if silver is None:
            raise FileNotFoundError(f"No Silver Parquet files found under {args.silver}")
        silver_destination = f"{snapshot_root}/silver/pageviews"
        publish_table(silver, silver_destination, snapshot_id)
        published.append({"table": "silver/pageviews", "location": silver_destination})

        for table in GOLD_TABLES:
            frame = read_parquet(spark, args.gold / table)
            if frame is None:
                continue
            destination = f"{snapshot_root}/gold/{table}"
            publish_table(frame, destination, snapshot_id)
            published.append({"table": f"gold/{table}", "location": destination})

        publication = {
            "publication_version": 1,
            "snapshot_id": snapshot["snapshot_id"],
            "manifest_id": snapshot.get("manifest_id"),
            "published_at_utc": datetime.now(UTC).isoformat(),
            "snapshot_root": snapshot_root,
            "tables": published,
        }
        marker = spark.createDataFrame(
            [(json.dumps(publication, sort_keys=True),)], ["publication_json"]
        ).withColumn("published_at_utc", F.current_timestamp())
        marker.write.format("delta").mode("errorifexists").save(
            f"{snapshot_root}/_publication"
        )
        output = args.publication_output / f"{snapshot_id}.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(publication, indent=2) + "\n", encoding="utf-8")
        print(f"Published {len(published)} Delta tables to {snapshot_root}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
