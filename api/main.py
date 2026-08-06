from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, Query

app = FastAPI(title="WikiTrend API", version="0.1.0")


def gold_dir() -> Path:
    return Path(os.getenv("WIKITREND_GOLD_DIR", "data/gold"))


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
    return {
        "status": "ok",
        "gold_dir": str(gold_dir()),
        "gold_dir_exists": gold_dir().exists(),
    }


@app.get("/top-pages")
def top_pages(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return read_table("top_pages_hourly", limit=limit, order_by="timestamp_hour DESC, rank ASC")


@app.get("/trending")
def trending(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return read_table(
        "trending_pages", limit=limit, order_by="timestamp_hour DESC, trend_score DESC"
    )


@app.get("/anomalies")
def anomalies(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return read_table(
        "anomaly_alerts", limit=limit, order_by="timestamp_hour DESC, robust_z_score DESC"
    )


@app.get("/forecast")
def forecast(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
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
    }
