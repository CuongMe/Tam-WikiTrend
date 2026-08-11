"""Consume deterministic pageview events and publish idempotent Delta tables."""

from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

PAGEVIEW_SCHEMA = StructType(
    [
        StructField("date", StringType()),
        StructField("hour", IntegerType()),
        StructField("source_project", StringType()),
        StructField("project", StringType()),
        StructField("language", StringType()),
        StructField("project_family", StringType()),
        StructField("access_mode", StringType()),
        StructField("page_title", StringType()),
        StructField("normalized_title", StringType()),
        StructField("normalization_status", StringType()),
        StructField("view_count", LongType()),
        StructField("response_size", LongType()),
        StructField("source_file", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("event_id", StringType()),
        StructField("emitted_at_utc", StringType()),
        StructField("event", PAGEVIEW_SCHEMA),
    ]
)

HOURLY_KEYS = [
    "timestamp_hour",
    "source_project",
    "project",
    "access_mode",
    "normalized_title",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER_URL"))
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="wikitrend.pageviews")
    parser.add_argument("--output", default="s3a://wikitrend/streaming")
    parser.add_argument("--checkpoint", default="data/checkpoints/pageview_stream_v1")
    parser.add_argument("--watermark", default="45 days")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--available-now", action="store_true")
    return parser.parse_args()


def build_spark(master: str | None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("wikitrend-structured-streaming")
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


def parse_events(kafka_df: DataFrame, watermark: str) -> DataFrame:
    decoded = kafka_df.select(
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("message"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    return (
        decoded.select(
            "message.schema_version",
            "message.event_id",
            F.to_timestamp("message.emitted_at_utc").alias("emitted_at_utc"),
            "message.event.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .filter(
            (F.col("schema_version") == 1)
            & F.col("event_id").isNotNull()
            & F.col("date").isNotNull()
            & F.col("hour").between(0, 23)
            & F.col("source_project").isNotNull()
            & F.col("access_mode").isNotNull()
            & F.col("normalized_title").isNotNull()
            & F.col("view_count").isNotNull()
        )
        .withColumn(
            "event_time",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("date"),
                    F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00:00")),
                )
            ),
        )
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id"])
    )


def merge_events(spark: SparkSession, batch: DataFrame, events_path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, events_path):
        batch.write.format("delta").mode("overwrite").partitionBy("date", "hour").save(
            events_path
        )
        return
    target = DeltaTable.forPath(spark, events_path)
    target.alias("target").merge(
        batch.alias("source"),
        "target.event_id = source.event_id",
    ).whenNotMatchedInsertAll().execute()


def refresh_hourly(
    spark: SparkSession,
    batch: DataFrame,
    events_path: str,
    hourly_path: str,
) -> None:
    affected_hours = batch.select("date", "hour").distinct()
    events = spark.read.format("delta").load(events_path).join(
        F.broadcast(affected_hours), ["date", "hour"], "left_semi"
    )
    hourly = events.groupBy(
        "event_time",
        "date",
        "hour",
        "source_project",
        "project",
        "language",
        "project_family",
        "access_mode",
        "normalized_title",
    ).agg(
        F.min("page_title").alias("page_title"),
        F.sum("view_count").cast("long").alias("view_count"),
        F.sum("response_size").cast("long").alias("response_size"),
        F.count("event_id").cast("long").alias("source_event_count"),
    ).withColumnRenamed("event_time", "timestamp_hour")

    if not DeltaTable.isDeltaTable(spark, hourly_path):
        hourly.write.format("delta").mode("overwrite").partitionBy(
            "date", "hour", "project", "access_mode"
        ).save(hourly_path)
        return
    condition = " AND ".join(f"target.{key} <=> source.{key}" for key in HOURLY_KEYS)
    DeltaTable.forPath(spark, hourly_path).alias("target").merge(
        hourly.alias("source"), condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def main() -> None:
    args = parse_args()
    spark = build_spark(args.master)
    spark.sparkContext.setLogLevel("WARN")
    events_path = f"{args.output.rstrip('/')}/events"
    hourly_path = f"{args.output.rstrip('/')}/page_hourly"

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    events = parse_events(kafka_df, args.watermark)

    def publish_batch(batch: DataFrame, batch_id: int) -> None:
        del batch_id
        persisted = batch.persist()
        try:
            if persisted.limit(1).count() == 0:
                return
            merge_events(spark, persisted, events_path)
            refresh_hourly(spark, persisted, events_path, hourly_path)
        finally:
            persisted.unpersist()

    writer = (
        events.writeStream.foreachBatch(publish_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")
    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
