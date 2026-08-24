from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from wikitrend.silver import path_has_payload


@dataclass(frozen=True)
class GoldBuildSummary:
    gold_dir: Path
    silver_dir: Path
    top_n_pages: int
    hourly_rows: int
    daily_rows: int
    top_page_rows: int
    manifest_path: Path
    overwrite: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("gold_dir", "silver_dir", "manifest_path"):
            payload[key] = str(payload[key])
        return payload


def assert_gold_writable(gold_dir: Path, overwrite: bool) -> None:
    if overwrite:
        return
    if path_has_payload(gold_dir):
        raise FileExistsError(
            "Refusing to write Gold outputs because data already exists. "
            f"Use --overwrite only when you intentionally want to replace it: {gold_dir}"
        )


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _count_parquet_rows(con, path: Path) -> int:
    parquet_glob = _sql_path(path / "**" / "*.parquet")
    return int(
        con.execute(
            f"select count(*) from read_parquet('{parquet_glob}', hive_partitioning=true)"
        ).fetchone()[0]
    )


def build_gold_layer(
    *,
    silver_dir: Path,
    gold_dir: Path,
    overwrite: bool = False,
    top_n_pages: int = 100,
) -> GoldBuildSummary:
    if top_n_pages < 1:
        raise ValueError("top_n_pages must be positive")
    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver directory does not exist: {silver_dir}")
    if not any(silver_dir.rglob("*.parquet")):
        raise FileNotFoundError(f"No Silver Parquet files found under: {silver_dir}")

    assert_gold_writable(gold_dir, overwrite)
    if overwrite and gold_dir.exists():
        shutil.rmtree(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)

    import duckdb

    con = duckdb.connect()
    silver_glob = _sql_path(silver_dir / "**" / "*.parquet")
    hourly_dir = gold_dir / "hourly_project_access"
    daily_dir = gold_dir / "daily_project_access"
    top_pages_dir = gold_dir / "top_pages_hourly"

    con.execute(
        f"""
        copy (
            select
                cast(date as date) as date,
                cast(hour as integer) as hour,
                project,
                access_mode,
                count(*) as page_rows,
                cast(sum(view_count) as bigint) as total_views,
                cast(sum(response_size) as bigint) as total_response_size,
                max(view_count) as max_page_views,
                avg(view_count) as avg_page_views,
                approx_count_distinct(normalized_title) as approx_distinct_pages
            from read_parquet('{silver_glob}', hive_partitioning=true)
            group by 1, 2, 3, 4
            order by 1, 2, 3, 4
        )
        to '{_sql_path(hourly_dir)}'
        (format parquet, partition_by (date))
        """
    )

    con.execute(
        f"""
        copy (
            select
                cast(date as date) as date,
                project,
                access_mode,
                count(*) as page_rows,
                cast(sum(view_count) as bigint) as total_views,
                cast(sum(response_size) as bigint) as total_response_size,
                max(view_count) as max_page_views,
                avg(view_count) as avg_page_views,
                approx_count_distinct(normalized_title) as approx_distinct_pages,
                count(distinct hour) as observed_hours
            from read_parquet('{silver_glob}', hive_partitioning=true)
            group by 1, 2, 3
            order by 1, 2, 3
        )
        to '{_sql_path(daily_dir)}'
        (format parquet, partition_by (date))
        """
    )

    con.execute(
        f"""
        copy (
            with ranked as (
                select
                    cast(date as date) as date,
                    cast(hour as integer) as hour,
                    project,
                    access_mode,
                    source_project,
                    normalized_title,
                    page_title,
                    view_count,
                    response_size,
                    row_number() over (
                        partition by date, hour, project, access_mode
                        order by view_count desc, normalized_title asc
                    ) as rank_in_hour
                from read_parquet('{silver_glob}', hive_partitioning=true)
            )
            select *
            from ranked
            where rank_in_hour <= {top_n_pages}
            order by date, hour, project, access_mode, rank_in_hour
        )
        to '{_sql_path(top_pages_dir)}'
        (format parquet, partition_by (date))
        """
    )

    summary = GoldBuildSummary(
        gold_dir=gold_dir,
        silver_dir=silver_dir,
        top_n_pages=top_n_pages,
        hourly_rows=_count_parquet_rows(con, hourly_dir),
        daily_rows=_count_parquet_rows(con, daily_dir),
        top_page_rows=_count_parquet_rows(con, top_pages_dir),
        manifest_path=gold_dir / "gold_manifest.json",
        overwrite=overwrite,
    )
    _write_json(
        summary.manifest_path,
        {
            "manifest_version": 1,
            "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat()
            + "Z",
            "source_layer": "silver",
            "source_dir": str(silver_dir),
            "gold_dir": str(gold_dir),
            "top_n_pages": top_n_pages,
            "tables": {
                "hourly_project_access": {
                    "path": "hourly_project_access",
                    "grain": "date, hour, project, access_mode",
                    "rows": summary.hourly_rows,
                },
                "daily_project_access": {
                    "path": "daily_project_access",
                    "grain": "date, project, access_mode",
                    "rows": summary.daily_rows,
                },
                "top_pages_hourly": {
                    "path": "top_pages_hourly",
                    "grain": "date, hour, project, access_mode, rank_in_hour",
                    "rows": summary.top_page_rows,
                },
            },
        },
    )
    return summary
