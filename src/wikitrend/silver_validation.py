from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from wikitrend.pageviews import DEFAULT_SOURCE_PROJECTS, PROJECT_CODE_MAP

PARTITION_RE = re.compile(
    r"date=(?P<date>[^/\\]+)[/\\]hour=(?P<hour>[^/\\]+)[/\\]"
    r"project=(?P<project>[^/\\]+)[/\\]access_mode=(?P<access_mode>[^/\\]+)"
)
SIDECAR_PATTERNS = ("_SUCCESS", ".crc")
REQUIRED_COLUMNS = {
    "date",
    "hour",
    "source_project",
    "project",
    "project_family",
    "access_mode",
    "page_title",
    "normalized_title",
    "view_count",
    "response_size",
}
SOURCE_FILE_COLUMNS = {"source_file", "source_filename"}


@dataclass(frozen=True)
class SilverPartition:
    date: str
    hour: int
    project: str
    access_mode: str

    @property
    def key(self) -> tuple[str, int, str, str]:
        return self.date, self.hour, self.project, self.access_mode


@dataclass(frozen=True)
class SilverValidationReport:
    generated_at_utc: str
    silver_dir: str
    quarantine_dir: str
    manifest_path: str
    status: str
    errors: list[str]
    warnings: list[str]
    metrics: dict[str, Any]
    cleanup_candidates: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_silver_partition(path: Path, silver_dir: Path) -> SilverPartition | None:
    relative = path.relative_to(silver_dir).as_posix()
    match = PARTITION_RE.search(relative)
    if not match:
        return None
    payload = match.groupdict()
    try:
        hour = int(payload["hour"])
    except ValueError:
        return None
    return SilverPartition(
        date=payload["date"],
        hour=hour,
        project=payload["project"],
        access_mode=payload["access_mode"],
    )


def find_parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(item for item in path.rglob("*.parquet") if item.is_file())


def find_cleanup_candidates(path: Path) -> list[Path]:
    if not path.exists():
        return []
    candidates = []
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        if item.name == "_SUCCESS" or item.name.endswith(".crc"):
            candidates.append(item)
    return sorted(candidates)


def expected_project_access_pairs(source_projects: tuple[str, ...]) -> set[tuple[str, str]]:
    pairs = set()
    for source_project in source_projects:
        dimensions = PROJECT_CODE_MAP[source_project]
        pairs.add((dimensions.project, dimensions.access_mode))
    return pairs


def manifest_expected_hours(manifest_path: Path) -> set[tuple[str, int]]:
    if not manifest_path.exists():
        return set()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected: set[tuple[str, int]] = set()
    for item in payload.get("files", []):
        timestamp = item.get("timestamp_hour")
        if not timestamp:
            continue
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        expected.add((parsed.date().isoformat(), parsed.hour))
    return expected


def _dataset_schema_names(silver_dir: Path) -> set[str]:
    import pyarrow.dataset as ds

    dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
    return set(dataset.schema.names)


def _count_dataset_rows(silver_dir: Path) -> int:
    import pyarrow.dataset as ds

    dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
    return dataset.count_rows()


def _count_invalid_metric_rows(silver_dir: Path) -> dict[str, int]:
    import pyarrow.dataset as ds

    dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
    return {
        "null_page_title_rows": dataset.count_rows(
            filter=ds.field("page_title").is_null()
        ),
        "null_normalized_title_rows": dataset.count_rows(
            filter=ds.field("normalized_title").is_null()
        ),
        "negative_view_count_rows": dataset.count_rows(
            filter=ds.field("view_count") < 0
        ),
        "negative_response_size_rows": dataset.count_rows(
            filter=ds.field("response_size") < 0
        ),
    }


def validate_silver_layer(
    *,
    silver_dir: Path,
    quarantine_dir: Path,
    manifest_path: Path,
    source_projects: tuple[str, ...] = DEFAULT_SOURCE_PROJECTS,
    full_scan: bool = False,
) -> SilverValidationReport:
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {}

    parquet_files = find_parquet_files(silver_dir)
    quarantine_files = find_parquet_files(quarantine_dir)
    cleanup_candidates = [
        str(path)
        for path in (
            find_cleanup_candidates(silver_dir) + find_cleanup_candidates(quarantine_dir)
        )
    ]

    metrics["silver_parquet_files"] = len(parquet_files)
    metrics["silver_size_bytes"] = sum(path.stat().st_size for path in parquet_files)
    metrics["quarantine_parquet_files"] = len(quarantine_files)
    metrics["quarantine_size_bytes"] = sum(path.stat().st_size for path in quarantine_files)
    metrics["cleanup_candidate_files"] = len(cleanup_candidates)

    if not silver_dir.exists():
        errors.append(f"Silver directory does not exist: {silver_dir}")
    if not parquet_files:
        errors.append(f"No Silver Parquet files found under: {silver_dir}")

    partitions = [
        partition
        for partition in (
            parse_silver_partition(path, silver_dir)
            for path in parquet_files
        )
        if partition is not None
    ]
    malformed_partition_files = len(parquet_files) - len(partitions)
    if malformed_partition_files:
        errors.append(
            f"{malformed_partition_files} Silver Parquet files are outside expected partitions"
        )

    partition_keys = {partition.key for partition in partitions}
    silver_hours = {(partition.date, partition.hour) for partition in partitions}
    expected_hours = manifest_expected_hours(manifest_path)
    expected_pairs = expected_project_access_pairs(source_projects)
    expected_partition_keys = {
        (date_value, hour, project, access_mode)
        for date_value, hour in expected_hours
        for project, access_mode in expected_pairs
    }

    metrics["partition_hours"] = len(silver_hours)
    metrics["partition_combinations"] = len(partition_keys)
    metrics["expected_manifest_hours"] = len(expected_hours)
    metrics["expected_project_access_pairs"] = len(expected_pairs)

    missing_hours = sorted(expected_hours - silver_hours)
    extra_hours = sorted(silver_hours - expected_hours)
    missing_partitions = sorted(expected_partition_keys - partition_keys)
    extra_partitions = sorted(partition_keys - expected_partition_keys) if expected_hours else []
    metrics["missing_hours"] = len(missing_hours)
    metrics["extra_hours"] = len(extra_hours)
    metrics["missing_partitions"] = len(missing_partitions)
    metrics["extra_partitions"] = len(extra_partitions)

    if missing_hours:
        errors.append(f"Missing Silver partitions for {len(missing_hours)} manifest hours")
    if extra_hours:
        warnings.append(f"Silver contains {len(extra_hours)} hours outside the manifest")
    if missing_partitions:
        errors.append(f"Missing {len(missing_partitions)} expected project/access partitions")
    if extra_partitions:
        warnings.append(
            f"Silver contains {len(extra_partitions)} unexpected project/access partitions"
        )

    if parquet_files:
        schema_names = _dataset_schema_names(silver_dir)
        missing_columns = sorted(REQUIRED_COLUMNS - schema_names)
        if missing_columns:
            errors.append(f"Silver schema is missing required columns: {missing_columns}")
        if not (schema_names & SOURCE_FILE_COLUMNS):
            warnings.append(
                "Silver schema has neither source_file nor source_filename lineage column"
            )
        metrics["schema_columns"] = sorted(schema_names)

    if cleanup_candidates:
        warnings.append(
            f"Found {len(cleanup_candidates)} Spark sidecar files; "
            "they are optional cleanup candidates"
        )

    if full_scan and parquet_files:
        row_count = _count_dataset_rows(silver_dir)
        invalid_metrics = _count_invalid_metric_rows(silver_dir)
        metrics["silver_rows"] = row_count
        metrics.update(invalid_metrics)
        for name, count in invalid_metrics.items():
            if count:
                errors.append(f"{name}={count}")
    else:
        warnings.append("Full row-quality scan was skipped; use --full-scan before building Gold")

    status = "pass" if not errors else "fail"
    return SilverValidationReport(
        generated_at_utc=datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        silver_dir=str(silver_dir),
        quarantine_dir=str(quarantine_dir),
        manifest_path=str(manifest_path),
        status=status,
        errors=errors,
        warnings=warnings,
        metrics=metrics,
        cleanup_candidates=cleanup_candidates,
    )


def write_validation_report(report: SilverValidationReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8")


def clean_sidecar_files(paths: list[Path]) -> int:
    removed = 0
    for path in paths:
        if path.name != "_SUCCESS" and not path.name.endswith(".crc"):
            raise ValueError(f"Refusing to remove non-sidecar file: {path}")
        path.unlink()
        removed += 1
    return removed
