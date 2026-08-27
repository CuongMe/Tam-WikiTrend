from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from wikitrend.config import get_settings
from wikitrend.forecasting import (
    MODEL_NAMES,
    TARGET_TABLE,
    assert_forecast_writable,
    build_forecast_layer,
)
from wikitrend.logging_utils import configure_logging
from wikitrend.silver import path_has_payload

LOGGER = logging.getLogger("wikitrend.cli.build_forecast_pageviews")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build rolling-window forecasts from Gold hourly pageview aggregates."
    )
    parser.add_argument("--gold-dir", type=Path, help="Gold Parquet input directory.")
    parser.add_argument("--forecast-dir", type=Path, help="Forecast output root directory.")
    parser.add_argument(
        "--train-window-hours",
        type=int,
        default=36,
        help="Fixed rolling training window size in hours.",
    )
    parser.add_argument(
        "--evaluation-horizon-hours",
        type=int,
        default=12,
        help="Backtest forecast horizon in hours for each rolling split.",
    )
    parser.add_argument(
        "--step-hours",
        type=int,
        default=12,
        help="Number of hours to move the rolling window between backtest folds.",
    )
    parser.add_argument(
        "--forecast-horizon-hours",
        type=int,
        default=24,
        help="Number of future hours to forecast from the latest Gold observation.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing forecasts.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print a plan.")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    gold_dir = args.gold_dir or settings.gold_dir
    forecast_dir = args.forecast_dir or settings.forecast_dir
    target_dir = forecast_dir / TARGET_TABLE

    if args.dry_run:
        hourly_dir = gold_dir / TARGET_TABLE
        parquet_files = sorted(hourly_dir.rglob("*.parquet")) if hourly_dir.exists() else []
        forecast_output_exists = path_has_payload(target_dir)
        print(
            json.dumps(
                {
                    "gold_dir": str(gold_dir),
                    "forecast_dir": str(forecast_dir),
                    "target_dir": str(target_dir),
                    "source_table": TARGET_TABLE,
                    "source_parquet_files": len(parquet_files),
                    "train_window_hours": args.train_window_hours,
                    "evaluation_horizon_hours": args.evaluation_horizon_hours,
                    "step_hours": args.step_hours,
                    "forecast_horizon_hours": args.forecast_horizon_hours,
                    "overwrite": args.overwrite,
                    "forecast_output_exists": forecast_output_exists,
                    "would_refuse_without_overwrite": forecast_output_exists and not args.overwrite,
                    "models": list(MODEL_NAMES),
                },
                indent=2,
            )
        )
        return 0

    assert_forecast_writable(target_dir, args.overwrite)
    summary = build_forecast_layer(
        gold_dir=gold_dir,
        forecast_dir=forecast_dir,
        train_window_hours=args.train_window_hours,
        evaluation_horizon_hours=args.evaluation_horizon_hours,
        step_hours=args.step_hours,
        forecast_horizon_hours=args.forecast_horizon_hours,
        overwrite=args.overwrite,
    )
    print(json.dumps(summary.to_dict(), indent=2))
    LOGGER.info(
        "forecast build complete forecast_dir=%s series=%s folds=%s models=%s",
        summary.forecast_dir,
        summary.series_count,
        summary.fold_count,
        ",".join(MODEL_NAMES),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())