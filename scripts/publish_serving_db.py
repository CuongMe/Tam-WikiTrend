from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

SERVING_TABLES = {
    "top_pages": ("top_pages_hourly", "timestamp_hour DESC, rank ASC", 100_000),
    "trending": ("trending_pages", "timestamp_hour DESC, trend_score DESC", 250_000),
    "anomalies": ("anomaly_alerts", "timestamp_hour DESC, robust_z_score DESC", 250_000),
    "predictions": (
        "lightgbm_predictions/predictions",
        "timestamp_hour DESC, predicted_traffic_rank ASC",
        1_000_000,
    ),
    "prediction_rankings": (
        "lightgbm_predictions/research_top_pages",
        "timestamp_hour DESC, predicted_traffic_rank ASC",
        250_000,
    ),
    "forecast_metrics": (
        "lightgbm_predictions/metrics",
        "evaluation_start_hour DESC",
        100_000,
    ),
    "ranking_metrics": (
        "lightgbm_predictions/ranking_metrics",
        "timestamp_hour DESC, k ASC",
        100_000,
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the atomic DuckDB serving snapshot.")
    parser.add_argument("--gold", type=Path, default=Path("data/gold"))
    parser.add_argument(
        "--output", type=Path, default=Path("data/serving/wikitrend.duckdb")
    )
    return parser.parse_args()


def parquet_glob(path: Path) -> str:
    return str(path / "**" / "*.parquet").replace("\\", "/")


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    if temporary.exists():
        temporary.unlink()
    con = duckdb.connect(str(temporary))
    published: list[str] = []
    try:
        for serving_table, (gold_table, order_by, limit) in SERVING_TABLES.items():
            source = args.gold / gold_table
            if not source.exists() or not any(source.rglob("*.parquet")):
                continue
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {serving_table} AS
                SELECT * FROM read_parquet('{parquet_glob(source)}', hive_partitioning=true)
                ORDER BY {order_by}
                LIMIT {limit}
                """
            )
            published.append(serving_table)
        con.execute(
            """
            CREATE OR REPLACE TABLE serving_metadata AS
            SELECT current_timestamp AS published_at_utc, ? AS tables
            """,
            [",".join(published)],
        )
        for table in published:
            columns = {
                row[1]
                for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()
            }
            index_columns = [
                column
                for column in ("timestamp_hour", "project", "access_mode")
                if column in columns
            ]
            if index_columns:
                con.execute(
                    f"CREATE INDEX idx_{table}_serving ON {table} ({', '.join(index_columns)})"
                )
        con.execute("CHECKPOINT")
    finally:
        con.close()
    os.replace(temporary, args.output)
    print(f"Published serving database {args.output} with tables: {published}")


if __name__ == "__main__":
    main()
