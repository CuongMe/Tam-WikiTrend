from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.gold_validation import (
    clean_sidecar_files,
    find_cleanup_candidates,
    validate_gold_layer,
    write_validation_report,
)
from wikitrend.logging_utils import configure_logging

LOGGER = logging.getLogger("wikitrend.cli.validate_gold_pageviews")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate compact Gold pageview aggregate tables."
    )
    parser.add_argument("--gold-dir", type=Path, help="Gold Parquet directory.")
    parser.add_argument("--manifest", type=Path, help="Gold manifest path.")
    parser.add_argument(
        "--silver-validation-report",
        type=Path,
        help="Optional Silver validation report used as upstream context.",
    )
    parser.add_argument("--report", type=Path, help="Optional JSON report path.")
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

    gold_dir = args.gold_dir or settings.gold_dir
    manifest_path = args.manifest or gold_dir / "gold_manifest.json"

    report = validate_gold_layer(
        gold_dir=gold_dir,
        manifest_path=manifest_path,
        silver_validation_report_path=args.silver_validation_report,
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
        LOGGER.info("wrote Gold validation report path=%s", args.report)

    if args.clean_sidecars:
        candidates = find_cleanup_candidates(gold_dir)
        removed = clean_sidecar_files(candidates)
        LOGGER.info("removed Gold sidecar files count=%s", removed)

    return 0 if report.status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
