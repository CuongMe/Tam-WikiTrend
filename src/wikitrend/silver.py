from __future__ import annotations

import gzip
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wikitrend.pageviews import PROJECT_CODE_MAP, parse_dump_filename, parse_pageview_line
from wikitrend.storage import ensure_spark_path


@dataclass(frozen=True)
class BronzeInputFile:
    filename: str
    relative_path: str
    path: Path
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class SilverBuildSummary:
    bronze_files: int
    silver_dir: Path
    quarantine_dir: Path
    overwrite: bool
    engine: str = "spark"


def path_has_payload(path: Path) -> bool:
    return path.exists() and any(item.is_file() for item in path.rglob("*"))


def assert_output_writable(*, silver_dir: Path, quarantine_dir: Path, overwrite: bool) -> None:
    if overwrite:
        return

    existing = [
        str(path)
        for path in (silver_dir, quarantine_dir)
        if path_has_payload(path)
    ]
    if existing:
        raise FileExistsError(
            "Refusing to write Silver outputs because data already exists. "
            "Use --overwrite only when you intentionally want to replace it: "
            + ", ".join(existing)
        )


def remove_existing_outputs(*, silver_dir: Path, quarantine_dir: Path) -> None:
    for path in (silver_dir, quarantine_dir):
        if path.exists():
            shutil.rmtree(path)


def _resolve_manifest_path(raw_dir: Path, relative_path: str) -> Path:
    raw_root = raw_dir.resolve()
    candidate = (raw_root / relative_path).resolve()
    try:
        candidate.relative_to(raw_root)
    except ValueError as exc:
        raise ValueError(f"Manifest path escapes raw directory: {relative_path}") from exc
    return candidate


def load_bronze_manifest_files(
    manifest_path: Path,
    raw_dir: Path,
    limit_files: int | None = None,
) -> list[BronzeInputFile]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = payload.get("files", [])
    if limit_files is not None:
        if limit_files < 1:
            raise ValueError("limit_files must be positive")
        records = records[:limit_files]

    seen: set[str] = set()
    files: list[BronzeInputFile] = []
    for record in records:
        filename = str(record["filename"])
        if filename in seen:
            raise ValueError(f"Duplicate manifest filename: {filename}")
        seen.add(filename)

        path = _resolve_manifest_path(raw_dir, str(record["relative_path"]))
        if not path.exists():
            raise FileNotFoundError(f"Manifest file is missing locally: {path}")

        files.append(
            BronzeInputFile(
                filename=filename,
                relative_path=str(record["relative_path"]),
                path=path,
                size_bytes=int(record["size_bytes"]),
                sha256=str(record["sha256"]),
            )
        )
    return files


def project_dimension_rows(source_projects: tuple[str, ...]) -> list[dict[str, Any]]:
    unknown = sorted(set(source_projects) - set(PROJECT_CODE_MAP))
    if unknown:
        raise ValueError(f"Unsupported source project codes: {unknown}")

    rows = []
    for source_project in source_projects:
        dimensions = PROJECT_CODE_MAP[source_project]
        rows.append(
            {
                "source_project": source_project,
                "project": dimensions.project,
                "language": dimensions.language,
                "project_family": dimensions.project_family,
                "access_mode": dimensions.access_mode,
            }
        )
    return rows


def create_spark_session(app_name: str = "WikiTrend Bronze to Silver"):
    from pyspark.sql import SparkSession

    python_executable = sys.executable
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.dfs.permissions.enabled", "false")
        .getOrCreate()
    )


def create_project_dimensions_dataframe(spark, source_projects: tuple[str, ...]):
    from pyspark.sql import functions as F

    rows = project_dimension_rows(source_projects)
    if not rows:
        raise ValueError("At least one source project is required")

    row_columns = [
        F.struct(
            F.lit(row["source_project"]).alias("source_project"),
            F.lit(row["project"]).alias("project"),
            F.lit(row["language"]).cast("string").alias("language"),
            F.lit(row["project_family"]).alias("project_family"),
            F.lit(row["access_mode"]).alias("access_mode"),
        )
        for row in rows
    ]
    return spark.range(1).select(F.explode(F.array(*row_columns)).alias("row")).select("row.*")


def build_silver_dataframes(
    spark,
    input_files: list[BronzeInputFile],
    source_projects: tuple[str, ...],
):
    from pyspark.sql import functions as F

    if not input_files:
        raise ValueError("No Bronze input files were provided")

    input_paths = [ensure_spark_path(item.path) for item in input_files]
    dimensions_df = create_project_dimensions_dataframe(spark, source_projects)
    filename_re = r"(pageviews-\d{8}-\d{6}\.gz)$"
    date_hour_re = r"pageviews-(\d{4})(\d{2})(\d{2})-(\d{2})0000\.gz$"

    raw_df = (
        spark.read.text(input_paths)
        .withColumn("source_file", F.input_file_name())
        .withColumn("source_filename", F.regexp_extract("source_file", filename_re, 1))
        .withColumn("parts", F.split(F.col("value"), " ", 4))
    )

    parsed_df = (
        raw_df.withColumn("date", F.to_date(
            F.concat_ws(
                "-",
                F.regexp_extract("source_filename", date_hour_re, 1),
                F.regexp_extract("source_filename", date_hour_re, 2),
                F.regexp_extract("source_filename", date_hour_re, 3),
            )
        ))
        .withColumn("hour", F.regexp_extract("source_filename", date_hour_re, 4).cast("int"))
        .withColumn("source_project", F.col("parts").getItem(0))
        .withColumn("page_title", F.col("parts").getItem(1))
        .withColumn("view_count_raw", F.col("parts").getItem(2))
        .withColumn("response_size_raw", F.col("parts").getItem(3))
        .withColumn("view_count", F.expr("try_cast(view_count_raw as bigint)"))
        .withColumn("response_size", F.expr("try_cast(response_size_raw as bigint)"))
    )

    joined_df = parsed_df.join(dimensions_df, on="source_project", how="left")
    invalid_reason = (
        F.when(F.col("source_filename") == "", F.lit("invalid_filename"))
        .when(F.size("parts") != 4, F.lit("malformed_line"))
        .when(
            (F.col("source_project").isNull())
            | (F.col("source_project") == "")
            | (F.col("page_title").isNull())
            | (F.col("page_title") == ""),
            F.lit("missing_required_value"),
        )
        .when(
            F.col("view_count").isNull() | F.col("response_size").isNull(),
            F.lit("non_numeric_metric"),
        )
        .when(
            (F.col("view_count") < 0) | (F.col("response_size") < 0),
            F.lit("negative_metric"),
        )
        .when(F.col("project").isNull(), F.lit("unsupported_source_project"))
    )

    classified_df = joined_df.withColumn("invalid_reason", invalid_reason)
    decoded_title = F.coalesce(F.expr("try_url_decode(page_title)"), F.col("page_title"))
    normalized_title = F.regexp_replace(decoded_title, "_", " ")

    silver_df = (
        classified_df.where(F.col("invalid_reason").isNull())
        .select(
            "date",
            "hour",
            "source_project",
            "project",
            "language",
            "project_family",
            "access_mode",
            "page_title",
            normalized_title.alias("normalized_title"),
            "view_count",
            "response_size",
            "source_filename",
        )
    )

    quarantine_df = (
        classified_df.where(F.col("invalid_reason").isNotNull())
        .select(
            "date",
            "hour",
            "source_project",
            "page_title",
            "view_count_raw",
            "response_size_raw",
            "invalid_reason",
            F.col("value").alias("raw_line"),
            "source_file",
            "source_filename",
        )
    )
    return silver_df, quarantine_df


def write_silver_outputs(
    silver_df,
    quarantine_df,
    *,
    silver_dir: Path,
    quarantine_dir: Path,
    overwrite: bool,
) -> None:
    assert_output_writable(
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=overwrite,
    )
    if overwrite:
        remove_existing_outputs(silver_dir=silver_dir, quarantine_dir=quarantine_dir)

    silver_df.write.mode("errorifexists").partitionBy(
        "date", "hour", "project", "access_mode"
    ).parquet(ensure_spark_path(silver_dir))
    quarantine_df.write.mode("errorifexists").partitionBy(
        "date", "hour", "invalid_reason"
    ).parquet(ensure_spark_path(quarantine_dir))


def _write_pyarrow_rows(
    *,
    base_dir: Path,
    partition_values: tuple[str, ...],
    partition_keys: tuple[str, ...],
    rows: list[dict[str, Any]],
    schema,
    file_counters: dict[tuple[str, ...], int],
) -> None:
    if not rows:
        return

    import pyarrow as pa
    import pyarrow.parquet as pq

    partition_dir = base_dir
    for key, value in zip(partition_keys, partition_values, strict=True):
        partition_dir = partition_dir / f"{key}={value}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_index = file_counters.get(partition_values, 0)
    file_counters[partition_values] = file_index + 1
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, partition_dir / f"part-{file_index:05d}.parquet")


def build_silver_layer_python(
    *,
    manifest_path: Path,
    raw_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    source_projects: tuple[str, ...],
    overwrite: bool = False,
    limit_files: int | None = None,
    batch_size: int = 100_000,
) -> SilverBuildSummary:
    import pyarrow as pa

    bronze_files = load_bronze_manifest_files(manifest_path, raw_dir, limit_files)
    assert_output_writable(
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=overwrite,
    )
    if overwrite:
        remove_existing_outputs(silver_dir=silver_dir, quarantine_dir=quarantine_dir)

    source_project_set = set(source_projects)
    project_dimension_rows(source_projects)
    silver_schema = pa.schema(
        [
            ("source_project", pa.string()),
            ("language", pa.string()),
            ("project_family", pa.string()),
            ("page_title", pa.string()),
            ("normalized_title", pa.string()),
            ("view_count", pa.int64()),
            ("response_size", pa.int64()),
            ("source_filename", pa.string()),
        ]
    )
    quarantine_schema = pa.schema(
        [
            ("source_project", pa.string()),
            ("page_title", pa.string()),
            ("view_count_raw", pa.string()),
            ("response_size_raw", pa.string()),
            ("raw_line", pa.string()),
            ("source_file", pa.string()),
            ("source_filename", pa.string()),
        ]
    )
    silver_partition_keys = ("date", "hour", "project", "access_mode")
    quarantine_partition_keys = ("date", "hour", "invalid_reason")
    silver_batches: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    quarantine_batches: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    silver_file_counters: dict[tuple[str, ...], int] = {}
    quarantine_file_counters: dict[tuple[str, ...], int] = {}
    buffered_rows = 0

    def flush() -> None:
        nonlocal buffered_rows
        for partition, rows in list(silver_batches.items()):
            _write_pyarrow_rows(
                base_dir=silver_dir,
                partition_values=partition,
                partition_keys=silver_partition_keys,
                rows=rows,
                schema=silver_schema,
                file_counters=silver_file_counters,
            )
        for partition, rows in list(quarantine_batches.items()):
            _write_pyarrow_rows(
                base_dir=quarantine_dir,
                partition_values=partition,
                partition_keys=quarantine_partition_keys,
                rows=rows,
                schema=quarantine_schema,
                file_counters=quarantine_file_counters,
            )
        silver_batches.clear()
        quarantine_batches.clear()
        buffered_rows = 0

    for bronze_file in bronze_files:
        date_value, hour = parse_dump_filename(bronze_file.filename)
        hour_value = str(hour)
        with gzip.open(bronze_file.path, "rt", encoding="utf-8") as handle:
            for line in handle:
                raw_line = line.rstrip("\n")
                parts = raw_line.split(" ")
                source_project = parts[0] if parts else ""
                if source_project not in source_project_set:
                    continue

                record = parse_pageview_line(raw_line, date_value, hour)
                if record is None:
                    page_title = parts[1] if len(parts) > 1 else None
                    view_count_raw = parts[2] if len(parts) > 2 else None
                    response_size_raw = parts[3] if len(parts) > 3 else None
                    invalid_reason = "malformed_supported_project_line"
                    partition = (date_value, hour_value, invalid_reason)
                    quarantine_batches.setdefault(partition, []).append(
                        {
                            "source_project": source_project,
                            "page_title": page_title,
                            "view_count_raw": view_count_raw,
                            "response_size_raw": response_size_raw,
                            "raw_line": raw_line,
                            "source_file": str(bronze_file.path),
                            "source_filename": bronze_file.filename,
                        }
                    )
                    buffered_rows += 1
                    if buffered_rows >= batch_size:
                        flush()
                    continue

                partition = (
                    record.date,
                    str(record.hour),
                    record.project,
                    record.access_mode,
                )
                silver_batches.setdefault(partition, []).append(
                    {
                        "source_project": record.source_project,
                        "language": record.language,
                        "project_family": record.project_family,
                        "page_title": record.page_title,
                        "normalized_title": record.normalized_title,
                        "view_count": record.view_count,
                        "response_size": record.response_size,
                        "source_filename": bronze_file.filename,
                    }
                )
                buffered_rows += 1
                if buffered_rows >= batch_size:
                    flush()

    flush()
    return SilverBuildSummary(
        bronze_files=len(bronze_files),
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=overwrite,
        engine="python",
    )


def build_silver_layer(
    *,
    spark,
    manifest_path: Path,
    raw_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    source_projects: tuple[str, ...],
    overwrite: bool = False,
    limit_files: int | None = None,
) -> SilverBuildSummary:
    bronze_files = load_bronze_manifest_files(manifest_path, raw_dir, limit_files)
    silver_df, quarantine_df = build_silver_dataframes(spark, bronze_files, source_projects)
    write_silver_outputs(
        silver_df,
        quarantine_df,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=overwrite,
    )
    return SilverBuildSummary(
        bronze_files=len(bronze_files),
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=overwrite,
        engine="spark",
    )
