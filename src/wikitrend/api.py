from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query

from wikitrend.config import Settings, get_settings


@dataclass(frozen=True)
class ApiSettings:
    serving_db: Path
    gold_validation_report: Path


def get_api_settings() -> ApiSettings:
    settings = get_settings()
    return ApiSettings(
        serving_db=settings.serving_db,
        gold_validation_report=Path(
            os.getenv(
                "WIKITREND_GOLD_VALIDATION_REPORT",
                "data/processed/validation/gold_pageviews_validation.json",
            )
        ),
    )


def _connect(settings: ApiSettings):
    if not settings.serving_db.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Serving database is missing: {settings.serving_db}",
        )
    return duckdb.connect(str(settings.serving_db), read_only=True)


def _read_validation(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "missing", "errors": [f"Missing validation report: {path}"]}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _rows(con, sql: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
    result = con.execute(sql, parameters).fetchdf()
    return result.to_dict(orient="records")


def create_app(api_settings: ApiSettings | None = None) -> FastAPI:
    app = FastAPI(
        title="WikiTrend API",
        version="0.1.0",
        description="Read API for WikiTrend DuckDB serving views.",
    )

    def settings_dependency() -> ApiSettings:
        return api_settings or get_api_settings()

    settings_depends = Depends(settings_dependency)

    @app.get("/health")
    def health(settings: ApiSettings = settings_depends) -> dict[str, Any]:
        validation = _read_validation(settings.gold_validation_report)
        return {
            "status": "ok" if settings.serving_db.exists() else "degraded",
            "serving_db_exists": settings.serving_db.exists(),
            "gold_validation_status": validation.get("status"),
        }

    @app.get("/v1/quality")
    def quality(settings: ApiSettings = settings_depends) -> dict[str, Any]:
        return _read_validation(settings.gold_validation_report)

    @app.get("/v1/metadata")
    def metadata(settings: ApiSettings = settings_depends) -> dict[str, Any]:
        con = _connect(settings)
        try:
            inventory = _rows(
                con,
                """
                select table_name, view_name, grain, row_count
                from metadata.gold_table_inventory
                order by table_name
                """,
            )
            serving_build = _rows(con, "select * from metadata.serving_build")
            return {"tables": inventory, "serving_build": serving_build}
        finally:
            con.close()

    @app.get("/v1/projects")
    def projects(settings: ApiSettings = settings_depends) -> dict[str, Any]:
        con = _connect(settings)
        try:
            rows = _rows(
                con,
                """
                select project, access_mode, sum(total_views)::bigint as total_views
                from gold.daily_project_access
                group by 1, 2
                order by total_views desc
                """,
            )
            return {"projects": rows}
        finally:
            con.close()

    @app.get("/v1/trends/hourly")
    def hourly_trends(
        project: str | None = None,
        access_mode: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = Query(default=1_000, ge=1, le=10_000),
        settings: ApiSettings = settings_depends,
    ) -> dict[str, Any]:
        filters = []
        params: list[Any] = []
        if project:
            filters.append("project = ?")
            params.append(project)
        if access_mode:
            filters.append("access_mode = ?")
            params.append(access_mode)
        if start_date:
            filters.append("date >= cast(? as date)")
            params.append(start_date)
        if end_date:
            filters.append("date <= cast(? as date)")
            params.append(end_date)
        where_sql = "where " + " and ".join(filters) if filters else ""
        params.append(limit)
        con = _connect(settings)
        try:
            rows = _rows(
                con,
                f"""
                select
                    cast(date as varchar) as date,
                    hour,
                    project,
                    access_mode,
                    page_rows,
                    total_views,
                    total_response_size,
                    max_page_views,
                    avg_page_views,
                    approx_distinct_pages
                from gold.hourly_project_access
                {where_sql}
                order by date, hour, project, access_mode
                limit ?
                """,
                params,
            )
            return {"rows": rows, "count": len(rows)}
        finally:
            con.close()

    @app.get("/v1/top-pages")
    def top_pages(
        project: str | None = None,
        access_mode: str | None = None,
        rank_cap: int = Query(default=25, ge=1, le=100),
        limit: int = Query(default=100, ge=1, le=1_000),
        settings: ApiSettings = settings_depends,
    ) -> dict[str, Any]:
        filters = ["rank_in_hour <= ?"]
        params: list[Any] = [rank_cap]
        if project:
            filters.append("project = ?")
            params.append(project)
        if access_mode:
            filters.append("access_mode = ?")
            params.append(access_mode)
        params.append(limit)
        con = _connect(settings)
        try:
            rows = _rows(
                con,
                f"""
                select
                    project,
                    access_mode,
                    normalized_title,
                    page_title,
                    count(*)::bigint as appearances,
                    sum(view_count)::bigint as total_top_views,
                    min(rank_in_hour) as best_rank,
                    max(view_count)::bigint as max_hourly_views
                from gold.top_pages_hourly
                where {' and '.join(filters)}
                group by 1, 2, 3, 4
                order by total_top_views desc, max_hourly_views desc
                limit ?
                """,
                params,
            )
            return {"rows": rows, "count": len(rows)}
        finally:
            con.close()

    return app


app = create_app()

__all__ = ["ApiSettings", "Settings", "app", "create_app", "get_api_settings"]