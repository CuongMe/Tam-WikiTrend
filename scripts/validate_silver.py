from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.pageviews import DEFAULT_SOURCE_PROJECTS, PROJECT_CODE_MAP

RAW_PATTERN = re.compile(r"pageviews-(\d{8})-(\d{2})0000\.gz$")
PARTITION_PATTERN = re.compile(
    r"date=(\d{4}-\d{2}-\d{2})/hour=(\d+)/project=([^/]+)/access_mode=([^/]+)$"
)
REQUIRED_COLUMNS = {
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
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate WikiTrend Silver Parquet and raw manifest integrity."
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/pageviews"))
    parser.add_argument("--silver-dir", type=Path, default=Path("data/silver/pageviews"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("data/quarantine/pageviews"))
    parser.add_argument(
        "--rejection-summary-dir",
        type=Path,
        default=Path("data/quarantine/pageviews_rejection_summary"),
    )
    parser.add_argument(
        "--project-allowlist",
        default=",".join(DEFAULT_SOURCE_PROJECTS),
        help="Comma-separated required Wikimedia source domain codes.",
    )
    parser.add_argument("--report-output", type=Path)
    parser.add_argument("--memory-limit", default="6GB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("data/temp/duckdb_validation/silver"),
    )
    return parser.parse_args()


def parse_allowlist(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def discover_raw_manifest(raw_dir: Path) -> tuple[dict[str, tuple[str, int]], list[str]]:
    manifest: dict[str, tuple[str, int]] = {}
    invalid_paths: list[str] = []
    for path in sorted(raw_dir.rglob("*.gz")):
        match = RAW_PATTERN.search(path.name)
        if not match:
            invalid_paths.append(str(path))
            continue
        raw_date, raw_hour = match.groups()
        try:
            date_value = date.fromisoformat(f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}")
            hour_value = int(raw_hour)
            if hour_value > 23:
                raise ValueError
        except ValueError:
            invalid_paths.append(str(path))
            continue
        if path.name in manifest:
            invalid_paths.append(str(path))
            continue
        manifest[path.name] = (date_value.isoformat(), hour_value)
    return manifest, invalid_paths


def discover_partitions(silver_dir: Path) -> set[tuple[str, int, str, str]]:
    partitions: set[tuple[str, int, str, str]] = set()
    for path in silver_dir.rglob("*"):
        if not path.is_dir():
            continue
        relative = path.relative_to(silver_dir).as_posix()
        match = PARTITION_PATTERN.fullmatch(relative)
        if match:
            partitions.add(
                (match.group(1), int(match.group(2)), match.group(3), match.group(4))
            )
    return partitions


def source_basename(source_file: str) -> str:
    return re.split(r"[/\\]", source_file)[-1]


def main() -> None:
    args = parse_args()
    expected_source_projects = parse_allowlist(args.project_allowlist)
    unsupported = expected_source_projects - set(PROJECT_CODE_MAP)
    if unsupported:
        raise ValueError(f"Unknown Wikimedia source project codes: {sorted(unsupported)}")
    expected_projects = {
        PROJECT_CODE_MAP[source_project].project
        for source_project in expected_source_projects
    }
    raw_manifest, invalid_raw_paths = discover_raw_manifest(args.raw_dir)
    raw_files = set(raw_manifest)
    raw_hours = set(raw_manifest.values())
    partitions = discover_partitions(args.silver_dir)
    partition_hours = {(date_value, hour) for date_value, hour, _, _ in partitions}
    partition_projects = {project for _, _, project, _ in partitions}
    partition_access_modes = {access_mode for _, _, _, access_mode in partitions}

    silver_glob = (args.silver_dir / "**" / "*.parquet").as_posix()
    source = f"read_parquet('{silver_glob}', hive_partitioning=true)"
    args.temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")
    escaped_temp = args.temp_dir.resolve().as_posix().replace("'", "''")
    con.execute(f"SET temp_directory='{escaped_temp}'")
    schema_rows = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    silver_columns = {row[0] for row in schema_rows}
    metrics = con.execute(
        f"""
        SELECT
            count(*) AS rows,
            count(DISTINCT date) AS dates,
            count(DISTINCT hour) AS hours,
            count(DISTINCT project) AS projects,
            count(DISTINCT source_project) AS source_projects,
            sum(CASE WHEN view_count IS NULL OR view_count < 0 THEN 1 ELSE 0 END) AS invalid_views,
            sum(
                CASE WHEN response_size IS NULL OR response_size < 0 THEN 1 ELSE 0 END
            ) AS invalid_response_sizes,
            sum(
                CASE WHEN page_title IS NULL OR trim(page_title) = '' THEN 1 ELSE 0 END
            ) AS missing_page_titles,
            sum(
                CASE WHEN normalized_title IS NULL OR trim(normalized_title) = '' THEN 1 ELSE 0 END
            ) AS blank_normalized_titles,
            sum(
                CASE WHEN source_file IS NULL OR trim(source_file) = '' THEN 1 ELSE 0 END
            ) AS missing_source_files
        FROM {source}
        """
    ).fetchone()
    project_rows = con.execute(
        f"""
        SELECT source_project, project, access_mode, count(*) AS rows,
               min(date) AS first_date, max(date) AS last_date
        FROM {source}
        GROUP BY source_project, project, access_mode
        ORDER BY source_project
        """
    ).fetchall()
    normalization_rows = con.execute(
        f"""
        SELECT normalization_status, count(*) AS rows
        FROM {source}
        GROUP BY normalization_status
        ORDER BY normalization_status
        """
    ).fetchall()
    duplicate_metrics = con.execute(
        f"""
        WITH duplicate_keys AS (
            SELECT date, hour, source_project, project, access_mode, page_title,
                   count(*) AS rows_for_key
            FROM {source}
            GROUP BY date, hour, source_project, project, access_mode, page_title
            HAVING count(*) > 1
        )
        SELECT
            count(*) AS duplicate_keys,
            coalesce(sum(rows_for_key), 0) AS rows_in_duplicate_keys,
            coalesce(sum(rows_for_key - 1), 0) AS excess_rows
        FROM duplicate_keys
        """
    ).fetchone()
    lineage_rows = con.execute(f"SELECT DISTINCT source_file, date, hour FROM {source}").fetchall()
    source_projects_found = {
        str(row[0])
        for row in con.execute(f"SELECT DISTINCT source_project FROM {source}").fetchall()
    }
    source_project_hours = {
        (str(row[0]), int(row[1]), str(row[2]))
        for row in con.execute(
            f"SELECT DISTINCT date, hour, source_project FROM {source}"
        ).fetchall()
    }
    con.close()

    expected_source_project_hours = {
        (date_value, hour_value, source_project)
        for date_value, hour_value in raw_hours
        for source_project in expected_source_projects
    }
    missing_source_project_hours = expected_source_project_hours - source_project_hours
    unexpected_source_project_hours = source_project_hours - expected_source_project_hours

    source_file_names = {source_basename(str(row[0])) for row in lineage_rows}
    lineage_mismatches = []
    for source_file, date_value, hour_value in lineage_rows:
        basename = source_basename(str(source_file))
        match = RAW_PATTERN.search(basename)
        expected = raw_manifest.get(basename)
        actual = (str(date_value), int(hour_value))
        if not match or expected != actual:
            lineage_mismatches.append(
                {
                    "source_file": str(source_file),
                    "basename": basename,
                    "silver_date": str(date_value),
                    "silver_hour": int(hour_value),
                    "manifest_value": expected,
                }
            )

    metric_names = [
        "rows",
        "dates",
        "hours",
        "projects",
        "source_projects",
        "invalid_views",
        "invalid_response_sizes",
        "missing_page_titles",
        "blank_normalized_titles",
        "missing_source_files",
    ]
    metric_values = dict(zip(metric_names, metrics, strict=True))
    failures: list[str] = []
    if invalid_raw_paths:
        failures.append("invalid raw manifest filenames found")
    if raw_hours - partition_hours:
        failures.append("raw date-hours missing from Silver partitions")
    if partition_hours - raw_hours:
        failures.append("Silver partitions have no raw manifest date-hour")
    if partition_projects - expected_projects:
        failures.append("unexpected project partitions found")
    if expected_projects - partition_projects:
        failures.append("required canonical project partitions are missing")
    if expected_source_projects - source_projects_found:
        failures.append("required source projects are missing from Silver")
    if missing_source_project_hours:
        failures.append("required source projects are missing from one or more raw hours")
    if unexpected_source_project_hours:
        failures.append("unexpected source project/hour combinations found")
    if partition_access_modes - {"desktop", "mobile"}:
        failures.append("invalid access-mode partitions found")
    if raw_files - source_file_names:
        failures.append("raw manifest files missing from Silver source lineage")
    if source_file_names - raw_files:
        failures.append("Silver source lineage contains unknown raw files")
    if lineage_mismatches:
        failures.append("Silver source-file date/hour does not match the raw manifest")
    if REQUIRED_COLUMNS - silver_columns:
        failures.append("required Silver columns are missing")
    if any(
        metric_values[name]
        for name in [
            "invalid_views",
            "invalid_response_sizes",
            "missing_page_titles",
            "missing_source_files",
        ]
    ):
        failures.append("Silver contains invalid required values")
    if duplicate_metrics[0]:
        failures.append("duplicate Silver natural keys found")

    result = {
        "raw_gz_files": len(raw_files),
        "raw_date_hours": len(raw_hours),
        "invalid_raw_manifest_paths": invalid_raw_paths,
        "silver_parquet_files": len(list(args.silver_dir.rglob("*.parquet"))),
        "silver_partitions": len(partitions),
        "silver_date_hours": len(partition_hours),
        "missing_date_hours": sorted(raw_hours - partition_hours),
        "unexpected_date_hours": sorted(partition_hours - raw_hours),
        "raw_files_missing_from_silver": sorted(raw_files - source_file_names),
        "silver_files_missing_from_raw": sorted(source_file_names - raw_files),
        "lineage_mismatches": lineage_mismatches,
        "silver_projects": sorted(partition_projects),
        "silver_source_projects": sorted(source_projects_found),
        "silver_access_modes": sorted(partition_access_modes),
        "unexpected_projects": sorted(partition_projects - expected_projects),
        "missing_requested_projects": sorted(expected_projects - partition_projects),
        "missing_requested_source_projects": sorted(
            expected_source_projects - source_projects_found
        ),
        "missing_source_project_hours": sorted(missing_source_project_hours),
        "unexpected_source_project_hours": sorted(unexpected_source_project_hours),
        "missing_required_columns": sorted(REQUIRED_COLUMNS - silver_columns),
        "metrics": metric_values,
        "duplicate_metrics": dict(
            zip(
                ["duplicate_keys", "rows_in_duplicate_keys", "excess_rows"],
                duplicate_metrics,
                strict=True,
            )
        ),
        "normalization_status": [
            {"status": status, "rows": rows} for status, rows in normalization_rows
        ],
        "rows_by_project": [
            {
                "source_project": source_project,
                "project": project,
                "access_mode": access_mode,
                "rows": rows,
                "first_date": first_date,
                "last_date": last_date,
            }
            for source_project, project, access_mode, rows, first_date, last_date in project_rows
        ],
        "quarantine_parquet_files": len(list(args.quarantine_dir.rglob("*.parquet"))),
        "rejection_summary_parquet_files": len(list(args.rejection_summary_dir.rglob("*.parquet"))),
        "failures": failures,
    }
    rendered = json.dumps(result, indent=2, default=str) + "\n"
    if args.report_output:
        args.report_output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.report_output.with_suffix(args.report_output.suffix + ".part")
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, args.report_output)
    print(rendered, end="")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
