from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

REQUIRED_COLUMNS = {
    "page_hourly": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "view_count",
        "response_size",
        "page_rows",
    },
    "hourly_project_traffic": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "view_count",
        "response_size",
        "page_rows",
        "topic_count",
    },
    "top_pages_hourly": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "view_count",
        "rank",
    },
    "trending_pages": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "view_count",
        "previous_hour_views",
        "rolling_baseline_avg",
        "rolling_baseline_stddev",
        "rolling_baseline_log_median",
        "rolling_baseline_log_mad",
        "baseline_observed_hours",
        "baseline_window_hours",
        "growth_rate",
        "robust_z_score",
        "log1p_views",
        "trend_score",
        "trend_rank",
    },
    "anomaly_alerts": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "view_count",
        "rolling_baseline_avg",
        "rolling_baseline_stddev",
        "rolling_baseline_log_median",
        "rolling_baseline_log_mad",
        "baseline_observed_hours",
        "baseline_window_hours",
        "growth_rate",
        "robust_z_score",
        "trend_score",
        "alert_type",
        "alert_severity",
    },
    "forecast_features": {
        "timestamp_hour",
        "date",
        "hour",
        "project",
        "language",
        "project_family",
        "page_title",
        "normalized_title",
        "view_count",
        "lag_1h_views",
        "lag_24h_views",
        "rolling_forecast_avg",
        "forecast_history_observed_hours",
        "baseline_forecast",
        "forecast_horizon_hours",
        "baseline_window_hours",
        "forecast_average_window_hours",
        "forecast_available",
        "target_next_hour_views",
    },
    "forecast_evaluation": {
        "project",
        "language",
        "project_family",
        "forecast_method",
        "evaluated_rows",
        "mase_valid_rows",
        "mase",
        "nd",
        "smape",
        "msmape",
        "evaluation_start_hour",
        "evaluation_end_hour",
    },
}

OPTIONALLY_EMPTY = {"anomaly_alerts"}


def table_glob(gold_dir: Path, table: str) -> str:
    return (gold_dir / table / "**" / "*.parquet").as_posix()


def parquet_files(gold_dir: Path, table: str) -> list[Path]:
    return sorted((gold_dir / table).rglob("*.parquet"))


def describe_columns(con: duckdb.DuckDBPyConnection, source: str) -> set[str]:
    return {
        row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate WikiTrend Gold Parquet tables.")
    parser.add_argument("--gold-dir", type=Path, default=Path("data/gold"))
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--baseline-hours", type=int, default=24)
    parser.add_argument("--allowed-projects", default="en,en.m,vi,vi.m,commons.m,commons.m.m")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    failures: list[str] = []
    table_files = {table: parquet_files(args.gold_dir, table) for table in REQUIRED_COLUMNS}
    missing_tables = [
        table for table, files in table_files.items() if not files and table not in OPTIONALLY_EMPTY
    ]
    failures.extend(f"missing Gold table or Parquet files: {table}" for table in missing_tables)

    report: dict[str, object] = {
        "gold_dir": str(args.gold_dir),
        "tables": {table: len(files) for table, files in table_files.items()},
        "failures": failures,
    }
    if any(table_files[table] for table in REQUIRED_COLUMNS if table not in OPTIONALLY_EMPTY):
        con = duckdb.connect()
        try:
            schema_report: dict[str, object] = {}
            for table, files in table_files.items():
                if not files:
                    schema_report[table] = {"missing_columns": sorted(REQUIRED_COLUMNS[table])}
                    continue
                source = table_glob(args.gold_dir, table)
                columns = describe_columns(con, source)
                missing = REQUIRED_COLUMNS[table] - columns
                schema_report[table] = {
                    "columns": sorted(columns),
                    "missing_columns": sorted(missing),
                }
                if missing:
                    failures.append(f"{table} is missing required columns")
            report["schemas"] = schema_report

            page_source = table_glob(args.gold_dir, "page_hourly")
            traffic_source = table_glob(args.gold_dir, "hourly_project_traffic")
            page_metrics = con.execute(
                f"""
                SELECT
                    count(*) AS rows,
                    count(DISTINCT date) AS dates,
                    count(DISTINCT hour) AS hours,
                    count(DISTINCT project) AS projects,
                    sum(
                        CASE
                            WHEN normalized_title IS NULL OR trim(normalized_title) = ''
                            THEN 1 ELSE 0
                        END
                    ) AS blank_titles,
                    sum(
                        CASE
                            WHEN regexp_matches(normalized_title, '^[^\\p{{L}}\\p{{N}}]+$')
                            THEN 1 ELSE 0
                        END
                    ) AS symbol_only_titles,
                    sum(
                        CASE WHEN view_count IS NULL OR view_count < 0 THEN 1 ELSE 0 END
                    ) AS invalid_views,
                    sum(
                        CASE WHEN response_size IS NULL OR response_size < 0 THEN 1 ELSE 0 END
                    ) AS invalid_response_sizes
                FROM read_parquet('{page_source}')
                """
            ).fetchone()
            duplicate_page_keys = con.execute(
                f"""
                SELECT count(*)
                FROM (
                    SELECT date, hour, project, normalized_title, count(*) AS rows
                    FROM read_parquet('{page_source}')
                    GROUP BY 1, 2, 3, 4
                    HAVING count(*) > 1
                )
                """
            ).fetchone()[0]
            traffic_reconciliation = con.execute(
                f"""
                WITH page_totals AS (
                    SELECT date, hour, project,
                           sum(view_count) AS view_count,
                           sum(response_size) AS response_size
                    FROM read_parquet('{page_source}')
                    GROUP BY 1, 2, 3
                ), traffic AS (
                    SELECT date, hour, project,
                           sum(view_count) AS view_count,
                           sum(response_size) AS response_size
                    FROM read_parquet('{traffic_source}')
                    GROUP BY 1, 2, 3
                )
                SELECT count(*)
                FROM page_totals p
                FULL OUTER JOIN traffic t USING (date, hour, project)
                WHERE p.view_count IS NULL OR t.view_count IS NULL
                   OR p.view_count <> t.view_count
                   OR p.response_size <> t.response_size
                """
            ).fetchone()[0]
            allowed_projects = {
                value.strip() for value in args.allowed_projects.split(",") if value.strip()
            }
            project_values = {
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT project FROM read_parquet('{page_source}')"
                ).fetchall()
            }
            report["page_hourly_metrics"] = {
                "rows": page_metrics[0],
                "dates": page_metrics[1],
                "hours": page_metrics[2],
                "projects": page_metrics[3],
                "blank_titles": page_metrics[4],
                "symbol_only_titles": page_metrics[5],
                "invalid_views": page_metrics[6],
                "invalid_response_sizes": page_metrics[7],
                "duplicate_topic_hour_keys": duplicate_page_keys,
                "traffic_reconciliation_mismatches": traffic_reconciliation,
                "projects_found": sorted(project_values),
            }
            if page_metrics[4] or page_metrics[5] or page_metrics[6] or page_metrics[7]:
                failures.append("page_hourly contains invalid topic or metric values")
            if duplicate_page_keys:
                failures.append("page_hourly contains duplicate topic-hour keys")
            if traffic_reconciliation:
                failures.append("hourly_project_traffic does not reconcile to page_hourly")
            if not project_values.issubset(allowed_projects):
                failures.append("Gold contains projects outside the configured allowlist")

            top_source = table_glob(args.gold_dir, "top_pages_hourly")
            top_metrics = con.execute(
                f"""
                SELECT
                    count(*) AS rows,
                    sum(
                        CASE WHEN rank < 1 OR rank > {args.top_n} THEN 1 ELSE 0 END
                    ) AS invalid_ranks,
                    count(*) - count(
                        DISTINCT concat(
                            cast(date AS VARCHAR), '|', cast(hour AS VARCHAR), '|',
                            project, '|', normalized_title
                        )
                    ) AS duplicate_keys
                FROM read_parquet('{top_source}')
                """
            ).fetchone()
            report["top_pages_metrics"] = {
                "rows": top_metrics[0],
                "invalid_ranks": top_metrics[1],
                "duplicate_keys": top_metrics[2],
            }
            if top_metrics[1] or top_metrics[2]:
                failures.append("top_pages_hourly contains invalid ranks or duplicate keys")

            trends_source = table_glob(args.gold_dir, "trending_pages")
            trend_metrics = con.execute(
                f"""
                SELECT
                    count(*) AS rows,
                    sum(
                        CASE WHEN trend_score IS NULL OR trend_score < 0 THEN 1 ELSE 0 END
                    ) AS invalid_scores,
                    sum(
                        CASE
                            WHEN baseline_observed_hours < 0
                              OR baseline_observed_hours > {args.baseline_hours}
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_baselines,
                    sum(
                        CASE
                            WHEN rolling_baseline_log_median IS NOT NULL
                              AND rolling_baseline_log_median < 0
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_log_medians,
                    sum(
                        CASE
                            WHEN rolling_baseline_log_mad IS NOT NULL
                              AND rolling_baseline_log_mad < 0
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_log_mads,
                    sum(CASE WHEN trend_rank < 1 THEN 1 ELSE 0 END) AS invalid_ranks
                FROM read_parquet('{trends_source}')
                """
            ).fetchone()
            report["trending_metrics"] = {
                "rows": trend_metrics[0],
                "invalid_scores": trend_metrics[1],
                "invalid_baselines": trend_metrics[2],
                "invalid_log_medians": trend_metrics[3],
                "invalid_log_mads": trend_metrics[4],
                "invalid_ranks": trend_metrics[5],
            }
            if any(trend_metrics[1:]):
                failures.append("trending_pages contains invalid feature values")

            forecast_source = table_glob(args.gold_dir, "forecast_features")
            forecast_metrics = con.execute(
                f"""
                SELECT
                    count(*) AS rows,
                    sum(
                        CASE WHEN forecast_horizon_hours <> 1 THEN 1 ELSE 0 END
                    ) AS invalid_horizon,
                    sum(
                        CASE
                            WHEN baseline_forecast IS NOT NULL AND baseline_forecast < 0
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_forecasts,
                    sum(
                        CASE
                            WHEN target_next_hour_views IS NOT NULL AND target_next_hour_views < 0
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_targets
                FROM read_parquet('{forecast_source}')
                """
            ).fetchone()
            report["forecast_metrics"] = {
                "rows": forecast_metrics[0],
                "invalid_horizon": forecast_metrics[1],
                "invalid_forecasts": forecast_metrics[2],
                "invalid_targets": forecast_metrics[3],
            }
            if forecast_metrics[1] or forecast_metrics[2] or forecast_metrics[3]:
                failures.append("forecast_features contains invalid values")

            evaluation_source = table_glob(args.gold_dir, "forecast_evaluation")
            evaluation_metrics = con.execute(
                f"""
                SELECT
                    count(*) AS rows,
                    sum(
                        CASE
                            WHEN evaluated_rows < 1
                              OR mase_valid_rows < 0
                              OR mase_valid_rows > evaluated_rows
                              OR (mase IS NOT NULL AND mase < 0)
                              OR (nd IS NOT NULL AND nd < 0)
                              OR smape < 0
                              OR msmape < 0
                            THEN 1 ELSE 0
                        END
                    ) AS invalid_metrics,
                    count(DISTINCT forecast_method) AS methods
                FROM read_parquet('{evaluation_source}')
                """
            ).fetchone()
            report["forecast_evaluation_metrics"] = {
                "rows": evaluation_metrics[0],
                "invalid_metrics": evaluation_metrics[1],
                "methods": evaluation_metrics[2],
            }
            if evaluation_metrics[1]:
                failures.append("forecast_evaluation contains invalid metrics")
        finally:
            con.close()

    report["failures"] = failures
    print(json.dumps(report, indent=2, default=str))
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
