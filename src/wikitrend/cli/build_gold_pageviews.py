from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.gold import assert_gold_writable, build_gold_layer
from wikitrend.logging_utils import configure_logging
from wikitrend.silver import path_has_payload

LOGGER = logging.getLogger("wikitrend.cli.build_gold_pageviews")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build compact Gold pageview aggregates from the Silver layer."
    )
    parser.add_argument("--silver-dir", type=Path, help="Silver Parquet input directory.")
    parser.add_argument("--gold-dir", type=Path, help="Gold output directory.")
    parser.add_argument(
        "--top-n-pages",
        type=int,
        default=100,
        help="Top pages to retain per date/hour/project/access partition.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Gold outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print a plan.")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    silver_dir = args.silver_dir or settings.silver_dir
    gold_dir = args.gold_dir or settings.gold_dir
    silver_files = sorted(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []

    if args.dry_run:
        would_refuse = path_has_payload(gold_dir) and not args.overwrite
        print(
            json.dumps(
                {
                    "silver_dir": str(silver_dir),
                    "gold_dir": str(gold_dir),
                    "silver_parquet_files": len(silver_files),
                    "top_n_pages": args.top_n_pages,
                    "overwrite": args.overwrite,
                    "gold_output_exists": path_has_payload(gold_dir),
                    "would_refuse_without_overwrite": would_refuse,
                    "tables": [
                        "hourly_project_access",
                        "daily_project_access",
                        "top_pages_hourly",
                    ],
                },
                indent=2,
            )
        )
        return 0

    assert_gold_writable(gold_dir, args.overwrite)
    summary = build_gold_layer(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        overwrite=args.overwrite,
        top_n_pages=args.top_n_pages,
    )
    LOGGER.info(
        "gold build complete hourly_rows=%s daily_rows=%s top_page_rows=%s gold_dir=%s",
        summary.hourly_rows,
        summary.daily_rows,
        summary.top_page_rows,
        summary.gold_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
