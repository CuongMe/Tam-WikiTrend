from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


PAGEVIEW_SCHEMA = StructType(
    [
        StructField("date", StringType()),
        StructField("hour", IntegerType()),
        StructField("project", StringType()),
        StructField("language", StringType()),
        StructField("project_family", StringType()),
        StructField("page_title", StringType()),
        StructField("normalized_title", StringType()),
        StructField("view_count", LongType()),
        StructField("response_size", LongType()),
        StructField("source_file", StringType()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka pageviews with Spark Structured Streaming.")
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="wikitrend.pageviews")
    parser.add_argument("--output", default="data/gold/streaming_top_pages")
    parser.add_argument("--checkpoint", default="data/checkpoints/streaming_top_pages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("wikitrend-streaming-trends").getOrCreate()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        kafka_df.select(F.from_json(F.col("value").cast("string"), PAGEVIEW_SCHEMA).alias("event"))
        .select("event.*")
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
    )

    top_pages = (
        events.withWatermark("event_time", "2 hours")
        .groupBy(F.window("event_time", "1 hour"), "project", "page_title")
        .agg(F.sum("view_count").alias("view_count"))
    )

    query = (
        top_pages.writeStream.format("parquet")
        .outputMode("append")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

