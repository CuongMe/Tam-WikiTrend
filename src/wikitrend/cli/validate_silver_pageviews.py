from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.silver_validation import (
    clean_sidecar_files,
    find_cleanup_candidates,
    validate_silver_layer,
    write_validation_report,
)

LOGGER = logging.getLogger("wikitrend.cli.validate_silver_pageviews")


def parse_source_projects(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Silver pageviews layer before building Gold."
    )
    parser.add_argument("--silver-dir", type=Path, help="Silver Parquet input directory.")
    parser.add_argument("--quarantine-dir", type=Path, help="Silver quarantine directory.")
    parser.add_argument("--manifest", type=Path, help="Bronze manifest path.")
    parser.add_argument(
        "--source-projects",
        help="Comma-separated Wikimedia source projects expected in Silver.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="Optional JSON report path.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Scan row values for null titles and negative metrics.",
    )
    parser.add_argument(
        "--clean-sidecars",
        action="store_true",
        help="Remove Spark _SUCCESS and .crc sidecar files after validation.",
    )
    parser.add_argument(
        "--show-cleanup-candidates",
        action="store_true",
        help="Print every cleanup candidate path to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    silver_dir = args.silver_dir or settings.silver_dir
    quarantine_dir = args.quarantine_dir or Path("data/processed/quarantine/pageviews")
    manifest_path = args.manifest or Path("data/raw/pageviews_manifest.json")
    source_projects = (
        parse_source_projects(args.source_projects)
        if args.source_projects
        else settings.source_project_allowlist
    )

    report = validate_silver_layer(
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        manifest_path=manifest_path,
        source_projects=source_projects,
        full_scan=args.full_scan,
    )
    output = report.to_dict()
    if not args.show_cleanup_candidates:
        output["cleanup_candidates"] = output["cleanup_candidates"][:10]
        output["cleanup_candidates_truncated"] = (
            len(report.cleanup_candidates) > len(output["cleanup_candidates"])
        )
    print(json.dumps(output, indent=2))

    if args.report:
        write_validation_report(report, args.report)
        LOGGER.info("wrote validation report path=%s", args.report)

    if args.clean_sidecars:
        candidates = find_cleanup_candidates(silver_dir) + find_cleanup_candidates(quarantine_dir)
        removed = clean_sidecar_files(candidates)
        LOGGER.info("removed sidecar files count=%s", removed)

    return 0 if report.status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
