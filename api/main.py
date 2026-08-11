from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, Query

app = FastAPI(title="WikiTrend API", version="0.1.0")
SERVING_TABLES = {
    "top_pages",
    "trending",
    "anomalies",
    "predictions",
    "prediction_rankings",
    "forecast_metrics",
    "ranking_metrics",
    "serving_metadata",
}


def gold_dir() -> Path:
    return Path(os.getenv("WIKITREND_GOLD_DIR", "data/gold"))


def serving_db() -> Path:
    return Path(os.getenv("WIKITREND_SERVING_DB", "data/serving/wikitrend.duckdb"))


def serving_table_exists(table_name: str) -> bool:
    if table_name not in SERVING_TABLES or not serving_db().is_file():
        return False
    with duckdb.connect(str(serving_db()), read_only=True) as conn:
        return bool(
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
        )


def read_serving_table(
    table_name: str,
    limit: int,
    order_by: str,
    project: str | None = None,
    access_mode: str | None = None,
) -> list[dict[str, Any]]:
    if not serving_table_exists(table_name):
        return []
    filters = []
    parameters: list[Any] = []
    if project:
        filters.append("project = ?")
        parameters.append(project)
    if access_mode:
        filters.append("access_mode = ?")
        parameters.append(access_mode)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    parameters.append(limit)
    sql = f"SELECT * FROM {table_name}{where} ORDER BY {order_by} LIMIT ?"
    with duckdb.connect(str(serving_db()), read_only=True) as conn:
        return conn.execute(sql, parameters).fetch_df().to_dict(orient="records")


def table_path(table_name: str) -> Path:
    return gold_dir() / table_name


def table_exists(table_name: str) -> bool:
    path = table_path(table_name)
    return path.exists() and any(path.rglob("*.parquet"))


def read_table(table_name: str, limit: int, order_by: str | None = None) -> list[dict[str, Any]]:
    if not table_exists(table_name):
        return []

    path_glob = str(table_path(table_name) / "**" / "*.parquet").replace("\\", "/")
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    sql = f"SELECT * FROM read_parquet('{path_glob}'){order_clause} LIMIT ?"
    with duckdb.connect() as conn:
        return conn.execute(sql, [limit]).fetch_df().to_dict(orient="records")


@app.get("/health")
def health() -> dict[str, Any]:
    serving_ready = serving_db().is_file()
    return {
        "status": "ok" if serving_ready or gold_dir().exists() else "degraded",
        "gold_dir": str(gold_dir()),
        "gold_dir_exists": gold_dir().exists(),
        "serving_db": str(serving_db()),
        "serving_db_exists": serving_ready,
    }


@app.get("/top-pages")
def top_pages(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    if serving_table_exists("top_pages"):
        return read_serving_table(
            "top_pages", limit, "timestamp_hour DESC, rank ASC"
        )
    return read_table("top_pages_hourly", limit=limit, order_by="timestamp_hour DESC, rank ASC")


@app.get("/trending")
def trending(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    if serving_table_exists("trending"):
        return read_serving_table(
            "trending", limit, "timestamp_hour DESC, trend_score DESC"
        )
    return read_table(
        "trending_pages", limit=limit, order_by="timestamp_hour DESC, trend_score DESC"
    )


@app.get("/anomalies")
def anomalies(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    if serving_table_exists("anomalies"):
        return read_serving_table(
            "anomalies", limit, "timestamp_hour DESC, robust_z_score DESC"
        )
    return read_table(
        "anomaly_alerts", limit=limit, order_by="timestamp_hour DESC, robust_z_score DESC"
    )


@app.get("/predictions")
def predictions(
    limit: int = Query(default=100, ge=1, le=1000),
    project: str | None = None,
    access_mode: str | None = Query(default=None, pattern="^(desktop|mobile)$"),
) -> list[dict[str, Any]]:
    return read_serving_table(
        "predictions",
        limit,
        "timestamp_hour DESC, predicted_traffic_rank ASC",
        project,
        access_mode,
    )


@app.get("/prediction-rankings")
def prediction_rankings(
    limit: int = Query(default=100, ge=1, le=1000),
    project: str | None = None,
    access_mode: str | None = Query(default=None, pattern="^(desktop|mobile)$"),
) -> list[dict[str, Any]]:
    return read_serving_table(
        "prediction_rankings",
        limit,
        "timestamp_hour DESC, predicted_traffic_rank ASC",
        project,
        access_mode,
    )


@app.get("/ranking-metrics")
def ranking_metrics(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return read_serving_table("ranking_metrics", limit, "timestamp_hour DESC, k ASC")


@app.get("/forecast")
def forecast(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    if serving_table_exists("predictions"):
        return read_serving_table(
            "predictions", limit, "timestamp_hour DESC, predicted_traffic_rank ASC"
        )
    return read_table("forecast_features", limit=limit, order_by="timestamp_hour DESC")


@app.get("/pipeline-status")
def pipeline_status() -> dict[str, Any]:
    tables = [
        "page_hourly",
        "hourly_project_traffic",
        "top_pages_hourly",
        "trending_pages",
        "anomaly_alerts",
        "forecast_features",
        "forecast_evaluation",
    ]
    return {
        "tables": {table: table_exists(table) for table in tables},
        "serving_tables": {
            table: serving_table_exists(table) for table in sorted(SERVING_TABLES)
        },
    }
