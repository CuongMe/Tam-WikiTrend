from __future__ import annotations

import argparse
import gzip
import re
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

RAW_FILENAME = re.compile(r"pageviews-(\d{8})-(\d{2})0000\.gz$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect WikiTrend raw and Silver data.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/pageviews"))
    parser.add_argument(
        "--raw-file", type=Path, help="Specific .gz file; defaults to the first sorted file."
    )
    parser.add_argument("--raw-lines", type=int, default=10)
    parser.add_argument("--project", help="Exact raw project code to filter, for example en.m.")
    parser.add_argument("--silver-dir", type=Path, default=Path("data/silver/pageviews"))
    parser.add_argument("--silver-rows", type=int, default=10)
    parser.add_argument(
        "--silver-date", help="Filter Silver rows by UTC date, for example 2026-08-01."
    )
    parser.add_argument("--silver-hour", type=int, help="Filter Silver rows by UTC hour.")
    parser.add_argument("--silver-project", help="Filter Silver rows by exact project code.")
    return parser.parse_args()


def raw_manifest(raw_dir: Path) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for path in sorted(raw_dir.rglob("*.gz")):
        match = RAW_FILENAME.search(path.name)
        if not match:
            continue
        date_value = datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()
        entries.append(
            {
                "file": str(path),
                "date": date_value,
                "hour": int(match.group(2)),
                "compressed_bytes": path.stat().st_size,
            }
        )
    return entries


def sample_raw(path: Path, line_limit: int, project: str | None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            fields = line.rstrip("\n").split(" ")
            if len(fields) != 4:
                continue
            if project and fields[0] != project:
                continue
            rows.append(
                {
                    "project": fields[0],
                    "page_title": fields[1],
                    "view_count": int(fields[2]) if fields[2].lstrip("-").isdigit() else fields[2],
                    "response_size": int(fields[3])
                    if fields[3].lstrip("-").isdigit()
                    else fields[3],
                }
            )
            if len(rows) >= line_limit:
                break
    return rows


def main() -> None:
    args = parse_args()
    manifest = raw_manifest(args.raw_dir)
    if not manifest:
        raise SystemExit(f"No matching .gz files found under {args.raw_dir}")

    raw_file = args.raw_file or Path(str(manifest[0]["file"]))
    silver = ds.dataset(args.silver_dir, format="parquet", partitioning="hive")
    silver_columns = [
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "normalization_status",
        "view_count",
        "response_size",
        "source_file",
    ]
    silver_filter = None
    for condition in (
        ds.field("date") == args.silver_date if args.silver_date else None,
        ds.field("hour") == args.silver_hour if args.silver_hour is not None else None,
        ds.field("project") == args.silver_project if args.silver_project else None,
    ):
        if condition is not None:
            silver_filter = condition if silver_filter is None else silver_filter & condition

    print("RAW MANIFEST")
    print(f"files={len(manifest)}")
    print(f"compressed_bytes={sum(int(item['compressed_bytes']) for item in manifest)}")
    print(f"first={manifest[0]}")
    print(f"last={manifest[-1]}")

    print("\nRAW SAMPLE")
    print(f"file={raw_file}")
    for row in sample_raw(raw_file, args.raw_lines, args.project):
        print(row)

    print("\nSILVER SCHEMA")
    print(silver.schema)
    print("\nSILVER SUMMARY")
    print(f"parquet_files={len(silver.files)}")
    print(f"rows={silver.count_rows()}")
    if silver_filter is not None:
        print(f"filtered_rows={silver.count_rows(filter=silver_filter)}")
    print("\nSILVER SAMPLE")
    for row in silver.head(
        args.silver_rows, columns=silver_columns, filter=silver_filter
    ).to_pylist():
        print(row)


if __name__ == "__main__":
    main()
