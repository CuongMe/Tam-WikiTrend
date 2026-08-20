from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.silver import (
    assert_output_writable,
    build_silver_layer,
    build_silver_layer_python,
    create_spark_session,
    load_bronze_manifest_files,
    path_has_payload,
)

LOGGER = logging.getLogger("wikitrend.cli.build_silver_pageviews")


def parse_source_projects(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the Silver pageviews layer from Bronze Wikimedia gzip dumps."
    )
    parser.add_argument("--raw-dir", type=Path, help="Bronze pageview root directory.")
    parser.add_argument("--manifest", type=Path, help="Bronze manifest path.")
    parser.add_argument("--silver-dir", type=Path, help="Silver Parquet output directory.")
    parser.add_argument(
        "--quarantine-dir",
        type=Path,
        help="Malformed/out-of-scope output directory.",
    )
    parser.add_argument(
        "--source-projects",
        help="Comma-separated Wikimedia source projects to retain.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        help="Process only the first N manifest files; use with a non-canonical output path.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Silver outputs.")
    parser.add_argument(
        "--engine",
        choices=("python", "spark"),
        default="python",
        help="Processing engine. The Python/PyArrow engine avoids local Spark/Hadoop setup.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print a plan.")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    raw_dir = args.raw_dir or settings.raw_dir
    manifest_path = args.manifest or Path("data/raw/pageviews_manifest.json")
    silver_dir = args.silver_dir or settings.silver_dir
    quarantine_dir = args.quarantine_dir or Path("data/processed/quarantine/pageviews")
    source_projects = (
        parse_source_projects(args.source_projects)
        if args.source_projects
        else settings.source_project_allowlist
    )

    bronze_files = load_bronze_manifest_files(manifest_path, raw_dir, args.limit_files)
    if args.dry_run:
        print(
            json.dumps(
                {
                    "manifest_path": str(manifest_path),
                    "raw_dir": str(raw_dir),
                    "bronze_files": len(bronze_files),
                    "silver_dir": str(silver_dir),
                    "quarantine_dir": str(quarantine_dir),
                    "source_projects": list(source_projects),
                    "overwrite": args.overwrite,
                    "engine": args.engine,
                    "limit_files": args.limit_files,
                    "silver_output_exists": path_has_payload(silver_dir),
                    "quarantine_output_exists": path_has_payload(quarantine_dir),
                    "would_refuse_without_overwrite": (
                        not args.overwrite
                        and (
                            path_has_payload(silver_dir)
                            or path_has_payload(quarantine_dir)
                        )
                    ),
                },
                indent=2,
            )
        )
        return 0

    assert_output_writable(
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=args.overwrite,
    )

    if args.engine == "python":
        summary = build_silver_layer_python(
            manifest_path=manifest_path,
            raw_dir=raw_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            source_projects=source_projects,
            overwrite=args.overwrite,
            limit_files=args.limit_files,
        )
        LOGGER.info(
            "silver build complete engine=%s bronze_files=%s "
            "silver_dir=%s quarantine_dir=%s overwrite=%s",
            summary.engine,
            summary.bronze_files,
            summary.silver_dir,
            summary.quarantine_dir,
            summary.overwrite,
        )
        return 0

    spark = create_spark_session()
    try:
        summary = build_silver_layer(
            spark=spark,
            manifest_path=manifest_path,
            raw_dir=raw_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            source_projects=source_projects,
            overwrite=args.overwrite,
            limit_files=args.limit_files,
        )
        LOGGER.info(
            "silver build complete engine=%s bronze_files=%s "
            "silver_dir=%s quarantine_dir=%s overwrite=%s",
            summary.engine,
            summary.bronze_files,
            summary.silver_dir,
            summary.quarantine_dir,
            summary.overwrite,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
