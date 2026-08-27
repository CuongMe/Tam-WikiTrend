from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.serving import assert_serving_writable, build_serving_database

LOGGER = logging.getLogger("wikitrend.cli.build_serving_db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a compact DuckDB serving database over validated Gold tables."
    )
    parser.add_argument("--gold-dir", type=Path, help="Gold Parquet directory.")
    parser.add_argument("--database", type=Path, help="DuckDB serving database path.")
    parser.add_argument("--forecast-dir", type=Path, help="Forecast Parquet output directory.")
    parser.add_argument(
        "--gold-validation-report",
        type=Path,
        default=Path("data/processed/validation/gold_pageviews_validation.json"),
        help="Gold validation report that must have status=pass.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Build without requiring a passing Gold validation report.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing database.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print a plan.")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    gold_dir = args.gold_dir or settings.gold_dir
    database_path = args.database or settings.serving_db
    forecast_dir = args.forecast_dir or settings.forecast_dir
    validation_report_path = None if args.skip_validation else args.gold_validation_report

    if args.dry_run:
        would_refuse = database_path.exists() and not args.overwrite
        print(
            json.dumps(
                {
                    "gold_dir": str(gold_dir),
                    "database": str(database_path),
                    "forecast_dir": str(forecast_dir),
                    "forecast_outputs_exist": (forecast_dir / "hourly_project_access").exists(),
                    "gold_validation_report": (
                        str(validation_report_path) if validation_report_path else None
                    ),
                    "require_validation": not args.skip_validation,
                    "overwrite": args.overwrite,
                    "database_exists": database_path.exists(),
                    "would_refuse_without_overwrite": would_refuse,
                    "views": [
                        "gold.hourly_project_access",
                        "gold.daily_project_access",
                        "gold.top_pages_hourly",
                        "forecast.forecast_metrics",
                        "forecast.forecast_backtest_predictions",
                        "forecast.forecast_future",
                    ],
                    "storage_mode": "views_over_gold_parquet",
                },
                indent=2,
            )
        )
        return 0

    assert_serving_writable(database_path, args.overwrite)
    summary = build_serving_database(
        gold_dir=gold_dir,
        database_path=database_path,
        forecast_dir=forecast_dir,
        validation_report_path=validation_report_path,
        overwrite=args.overwrite,
        require_validation=not args.skip_validation,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    LOGGER.info(
        "serving database build complete database=%s views=%s",
        summary.database_path,
        ",".join(summary.views),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
