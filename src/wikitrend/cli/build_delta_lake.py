from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.delta_lake import build_gold_delta_lake
from wikitrend.gold_validation import GOLD_TABLE_CONTRACTS
from wikitrend.logging_utils import configure_logging
from wikitrend.silver import path_has_payload

LOGGER = logging.getLogger("wikitrend.cli.build_delta_lake")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build local Delta Lake tables from compact Gold Parquet tables."
    )
    parser.add_argument("--gold-dir", type=Path, help="Gold Parquet input directory.")
    parser.add_argument("--delta-dir", type=Path, help="Delta Lake output root directory.")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Delta outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print a plan.")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    gold_dir = args.gold_dir or settings.gold_dir
    delta_dir = args.delta_dir or settings.delta_dir
    target_delta_dir = delta_dir / "gold"

    if args.dry_run:
        would_refuse = path_has_payload(target_delta_dir) and not args.overwrite
        print(
            json.dumps(
                {
                    "gold_dir": str(gold_dir),
                    "delta_dir": str(delta_dir),
                    "target_delta_dir": str(target_delta_dir),
                    "overwrite": args.overwrite,
                    "delta_output_exists": path_has_payload(target_delta_dir),
                    "would_refuse_without_overwrite": would_refuse,
                    "tables": list(GOLD_TABLE_CONTRACTS),
                    "engine": "delta-rs",
                    "storage_mode": "local_delta_tables_from_compact_gold",
                },
                indent=2,
            )
        )
        return 0

    summary = build_gold_delta_lake(
        gold_dir=gold_dir,
        delta_dir=delta_dir,
        overwrite=args.overwrite,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    LOGGER.info(
        "Delta Lake build complete delta_dir=%s tables=%s",
        summary.delta_dir,
        ",".join(table.table_name for table in summary.tables),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
