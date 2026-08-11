from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.pageviews import PROJECT_CODE_MAP, SUPPORTED_SOURCE_PROJECTS

DUPLICATE_KEY_COLUMNS = (
    "date",
    "hour",
    "source_project",
    "project",
    "access_mode",
    "page_title",
)


def parse_project_allowlist(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _project_dimension_map(attribute: str) -> F.Column:
    entries: list[F.Column] = []
    for source_project, dimensions in PROJECT_CODE_MAP.items():
        entries.extend(
            [F.lit(source_project), F.lit(getattr(dimensions, attribute)).cast("string")]
        )
    return F.create_map(*entries)


def resolve_input_paths(input_path: str) -> list[str]:
    if any(token in input_path for token in "*?["):
        paths = sorted(glob.glob(input_path, recursive=True))
    elif Path(input_path).is_dir():
        paths = sorted(str(path) for path in Path(input_path).rglob("*.gz"))
    else:
        paths = [input_path]
    if not paths:
        raise FileNotFoundError(f"No input `.gz` files found for {input_path}")
    return paths


def _build_parsed_rows(
    spark: SparkSession, input_path: str, project_allowlist: list[str]
) -> DataFrame:
    raw = spark.read.text(resolve_input_paths(input_path)).withColumn(
        "source_file", F.input_file_name()
    )
    parts = F.split(F.col("value"), " ", -1)

    parsed = (
        raw.select(
            F.col("value").alias("raw_line"),
            F.col("source_file"),
            F.size(parts).alias("part_count"),
            parts.getItem(0).alias("source_project"),
            parts.getItem(1).alias("page_title"),
            parts.getItem(2).alias("view_count_raw"),
            parts.getItem(3).alias("response_size_raw"),
        )
        .withColumn("view_count", F.col("view_count_raw").cast("long"))
        .withColumn("response_size", F.col("response_size_raw").cast("long"))
    )

    date_raw = F.regexp_extract("source_file", r"pageviews-(\d{8})-\d{6}\.gz", 1)
    hour_raw = F.regexp_extract("source_file", r"pageviews-\d{8}-(\d{2})0000\.gz", 1)
    parsed = (
        parsed.withColumn("_source_date_raw", date_raw)
        .withColumn("_source_hour_raw", hour_raw)
        .withColumn("date", F.to_date(date_raw, "yyyyMMdd").cast("string"))
        .withColumn("hour", hour_raw.cast("int"))
    )

    invalid_view_count = (
        (F.col("part_count") == 4)
        & F.col("view_count_raw").isNotNull()
        & F.col("view_count").isNull()
    )
    invalid_response_size = (
        (F.col("part_count") == 4)
        & F.col("response_size_raw").isNotNull()
        & F.col("response_size").isNull()
    )
    invalid_source_filename = (
        F.col("date").isNull() | F.col("hour").isNull() | (F.col("hour") < 0) | (F.col("hour") > 23)
    )
    quality_reasons = [
        F.when(F.col("part_count") != 4, F.lit("wrong_field_count")),
        F.when(
            (F.col("part_count") == 4)
            & (F.col("source_project").isNull() | (F.trim("source_project") == "")),
            F.lit("missing_project"),
        ),
        F.when(
            (F.col("part_count") == 4)
            & (F.col("page_title").isNull() | (F.trim("page_title") == "")),
            F.lit("missing_page_title"),
        ),
        F.when(invalid_view_count, F.lit("invalid_view_count")),
        F.when(invalid_response_size, F.lit("invalid_response_size")),
        F.when(F.col("view_count") < 0, F.lit("negative_view_count")),
        F.when(F.col("response_size") < 0, F.lit("negative_response_size")),
        F.when(invalid_source_filename, F.lit("invalid_source_filename")),
    ]
    parsed = (
        parsed.withColumn("_quality_reject_reason_raw", F.concat_ws(";", *quality_reasons))
        .withColumn(
            "_quality_reject_reason",
            F.when(
                F.length(F.col("_quality_reject_reason_raw")) > 0,
                F.col("_quality_reject_reason_raw"),
            ),
        )
        .drop("_quality_reject_reason_raw")
    )

    allowed_projects = project_allowlist or list(SUPPORTED_SOURCE_PROJECTS)
    parsed = parsed.withColumn(
        "_scope_reject_reason",
        F.when(
            F.col("_quality_reject_reason").isNull()
            & F.col("source_project").isNotNull()
            & (F.trim("source_project") != "")
            & ~F.col("source_project").isin(allowed_projects),
            F.lit("out_of_scope_project"),
        ),
    )

    parsed = (
        parsed.withColumn("project", _project_dimension_map("project")[F.col("source_project")])
        .withColumn("language", _project_dimension_map("language")[F.col("source_project")])
        .withColumn(
            "project_family", _project_dimension_map("project_family")[F.col("source_project")]
        )
        .withColumn("access_mode", _project_dimension_map("access_mode")[F.col("source_project")])
    )

    malformed_percent_escape = F.col("page_title").rlike(r"%(?![0-9A-Fa-f]{2})")
    safe_page_title = F.regexp_replace(
        F.col("page_title"),
        r"%(?![0-9A-Fa-f]{2})",
        "%25",
    )
    parsed = parsed.withColumn("_safe_page_title", safe_page_title)
    parsed = parsed.withColumn(
        "normalized_title",
        F.regexp_replace(F.expr("url_decode(_safe_page_title)"), "_", " "),
    )
    parsed = parsed.withColumn(
        "normalization_status",
        F.when(
            F.col("page_title").isNull() | (F.trim("page_title") == ""),
            F.lit("missing_raw_title"),
        )
        .when(malformed_percent_escape, F.lit("malformed_percent_escape_recovered"))
        .when(
            F.col("normalized_title").isNull() | (F.trim("normalized_title") == ""),
            F.lit("blank_normalized_title"),
        )
        .otherwise(F.lit("normalized")),
    )
    return parsed


def _silver_columns(parsed: DataFrame) -> DataFrame:
    return parsed.select(
        "date",
        "hour",
        "source_project",
        "project",
        "language",
        "project_family",
        "access_mode",
        "page_title",
        "normalized_title",
        "normalization_status",
        "view_count",
        "response_size",
        "source_file",
    )


def build_silver_pageviews(
    spark: SparkSession, input_path: str, project_allowlist: list[str]
) -> DataFrame:
    parsed = _build_parsed_rows(spark, input_path, project_allowlist)
    return _silver_columns(
        parsed.filter(F.col("_quality_reject_reason").isNull()).filter(
            F.col("_scope_reject_reason").isNull()
        )
    )


def build_quality_quarantine(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("_quality_reject_reason").isNotNull()).select(
        "raw_line",
        "source_file",
        "date",
        "hour",
        "source_project",
        "project",
        "access_mode",
        "page_title",
        "view_count_raw",
        "response_size_raw",
        F.col("_quality_reject_reason").alias("reject_reason"),
    )


def build_rejection_summary(parsed: DataFrame) -> DataFrame:
    quality = (
        parsed.filter(F.col("_quality_reject_reason").isNotNull())
        .select(
            F.lit("quality").alias("rejection_type"),
            F.col("_quality_reject_reason").alias("reject_reason"),
            "date",
            "hour",
            "source_project",
            "project",
            "access_mode",
            "source_file",
        )
        .groupBy(
            "rejection_type",
            "reject_reason",
            "date",
            "hour",
            "source_project",
            "project",
            "access_mode",
            "source_file",
        )
        .count()
        .withColumnRenamed("count", "row_count")
    )
    out_of_scope = (
        parsed.filter(F.col("_quality_reject_reason").isNull())
        .filter(F.col("_scope_reject_reason").isNotNull())
        .select(
            F.lit("scope").alias("rejection_type"),
            F.col("_scope_reject_reason").alias("reject_reason"),
            "date",
            "hour",
            "source_project",
            "project",
            "access_mode",
            "source_file",
        )
        .groupBy(
            "rejection_type",
            "reject_reason",
            "date",
            "hour",
            "source_project",
            "project",
            "access_mode",
            "source_file",
        )
        .count()
        .withColumnRenamed("count", "row_count")
    )
    return quality.unionByName(out_of_scope)


def assert_no_duplicate_keys(silver: DataFrame, quarantine_output: str, mode: str) -> None:
    duplicate_keys = silver.groupBy(*DUPLICATE_KEY_COLUMNS).count().filter(F.col("count") > 1)
    if not duplicate_keys.limit(1).count():
        return

    duplicate_rows = silver.join(
        duplicate_keys.select(*DUPLICATE_KEY_COLUMNS),
        on=list(DUPLICATE_KEY_COLUMNS),
        how="inner",
    ).select(
        F.lit(None).cast("string").alias("raw_line"),
        "source_file",
        "date",
        "hour",
        "source_project",
        "project",
        "access_mode",
        "page_title",
        F.col("view_count").cast("string").alias("view_count_raw"),
        F.col("response_size").cast("string").alias("response_size_raw"),
        F.lit("duplicate_natural_key").alias("reject_reason"),
    )
    duplicate_rows.write.mode("append").partitionBy("reject_reason").parquet(quarantine_output)
    raise ValueError(
        "Silver duplicate natural keys found for "
        f"{duplicate_keys.count():,} keys; duplicate rows were written to {quarantine_output}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Wikimedia pageviews dumps to Silver Parquet."
    )
    parser.add_argument("--input", required=True, help="Input `.gz` glob or directory.")
    parser.add_argument("--output", required=True, help="Output Silver Parquet directory.")
    parser.add_argument(
        "--project-allowlist",
        help="Comma-separated Wikimedia source domain codes to keep.",
    )
    parser.add_argument(
        "--quarantine-output",
        default="data/quarantine/pageviews",
        help="Parquet output for malformed and invalid raw records.",
    )
    parser.add_argument(
        "--rejection-summary-output",
        default="data/quarantine/pageviews_rejection_summary",
        help="Parquet output for rejection counts, including out-of-scope projects.",
    )
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    args = parser.parse_args()
    if args.project_allowlist:
        requested = set(parse_project_allowlist(args.project_allowlist))
        unsupported = requested - set(SUPPORTED_SOURCE_PROJECTS)
        if unsupported:
            parser.error(
                "--project-allowlist contains unsupported source codes: "
                + ", ".join(sorted(unsupported))
            )
    return args


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("wikitrend-parse-pageviews")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    project_allowlist = parse_project_allowlist(args.project_allowlist)
    parsed = _build_parsed_rows(spark, args.input, project_allowlist)
    quality_quarantine = build_quality_quarantine(parsed)
    rejection_summary = build_rejection_summary(parsed)
    silver = _silver_columns(
        parsed.filter(F.col("_quality_reject_reason").isNull()).filter(
            F.col("_scope_reject_reason").isNull()
        )
    ).persist(StorageLevel.DISK_ONLY)

    quality_quarantine.write.mode(args.mode).partitionBy("reject_reason").parquet(
        args.quarantine_output
    )
    rejection_summary.write.mode(args.mode).partitionBy("rejection_type").parquet(
        args.rejection_summary_output
    )
    try:
        assert_no_duplicate_keys(silver, args.quarantine_output, args.mode)
        silver.write.mode(args.mode).partitionBy(
            "date", "hour", "project", "access_mode"
        ).parquet(args.output)
    finally:
        silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
