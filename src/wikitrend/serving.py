from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from wikitrend.gold_validation import GOLD_TABLE_CONTRACTS, validate_gold_layer

FORECAST_TARGET_TABLE = "hourly_project_access"
FORECAST_TABLE_CONTRACTS: dict[str, dict[str, str]] = {
    "forecast_metrics": {
        "file_name": "metrics.parquet",
        "view_name": "forecast.forecast_metrics",
        "grain": "model, project, access_mode",
    },
    "forecast_backtest_predictions": {
        "file_name": "backtest_predictions.parquet",
        "view_name": "forecast.forecast_backtest_predictions",
        "grain": "fold, horizon_step, hour, model, project, access_mode",
    },
    "forecast_future": {
        "file_name": "forecast.parquet",
        "view_name": "forecast.forecast_future",
        "grain": "generated_at_utc, horizon_step, hour, model, project, access_mode",
    },
}


@dataclass(frozen=True)
class ServingBuildSummary:
    database_path: Path
    gold_dir: Path
    forecast_dir: Path | None
    validation_report_path: Path | None
    views: tuple[str, ...]
    row_counts: dict[str, int]
    overwrite: bool
    generated_at_utc: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["database_path"] = str(self.database_path)
        payload["gold_dir"] = str(self.gold_dir)
        payload["forecast_dir"] = str(self.forecast_dir) if self.forecast_dir else None
        payload["validation_report_path"] = (
            str(self.validation_report_path) if self.validation_report_path else None
        )
        return payload


def assert_serving_writable(database_path: Path, overwrite: bool) -> None:
    if overwrite:
        return
    if database_path.exists():
        raise FileExistsError(
            "Refusing to overwrite existing serving database. "
            f"Use --overwrite only when intentionally replacing it: {database_path}"
        )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_path(path: Path) -> str:
    return _sql_literal(path.resolve().as_posix())


def _parquet_glob(path: Path) -> str:
    return _sql_literal((path.resolve() / "**" / "*.parquet").as_posix())


def _forecast_table_dir(forecast_dir: Path) -> Path:
    return forecast_dir / FORECAST_TARGET_TABLE


def _forecast_table_path(forecast_dir: Path, table_name: str) -> Path:
    return _forecast_table_dir(forecast_dir) / FORECAST_TABLE_CONTRACTS[table_name]["file_name"]


def _forecast_outputs_available(forecast_dir: Path | None) -> bool:
    if forecast_dir is None:
        return False
    return all(
        _forecast_table_path(forecast_dir, table_name).exists()
        for table_name in FORECAST_TABLE_CONTRACTS
    )


def _read_validation_report(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _assert_gold_valid(
    *,
    gold_dir: Path,
    validation_report_path: Path | None,
    require_validation: bool,
) -> None:
    if not require_validation:
        return

    if validation_report_path is None:
        report = validate_gold_layer(gold_dir=gold_dir)
        if report.status != "pass":
            raise ValueError(f"Gold validation failed: {report.errors}")
        return

    if not validation_report_path.exists():
        raise FileNotFoundError(
            "Gold validation report is required before building the serving database: "
            f"{validation_report_path}"
        )
    report = _read_validation_report(validation_report_path)
    if report.get("status") != "pass":
        raise ValueError(
            "Gold validation report must pass before building the serving database: "
            f"status={report.get('status')}"
        )


def _create_gold_views(con, gold_dir: Path) -> tuple[str, ...]:
    con.execute("create schema if not exists gold")
    con.execute("create schema if not exists metadata")

    con.execute(
        f"""
        create or replace view gold.hourly_project_access as
        select
            cast(date as date) as date,
            cast(hour as smallint) as hour,
            project,
            access_mode,
            cast(page_rows as bigint) as page_rows,
            cast(total_views as bigint) as total_views,
            cast(total_response_size as bigint) as total_response_size,
            cast(max_page_views as bigint) as max_page_views,
            cast(avg_page_views as double) as avg_page_views,
            cast(approx_distinct_pages as bigint) as approx_distinct_pages
        from read_parquet(
            {_parquet_glob(gold_dir / GOLD_TABLE_CONTRACTS["hourly_project_access"].path)},
            hive_partitioning=true
        )
        """
    )
    con.execute(
        f"""
        create or replace view gold.daily_project_access as
        select
            cast(date as date) as date,
            project,
            access_mode,
            cast(page_rows as bigint) as page_rows,
            cast(total_views as bigint) as total_views,
            cast(total_response_size as bigint) as total_response_size,
            cast(max_page_views as bigint) as max_page_views,
            cast(avg_page_views as double) as avg_page_views,
            cast(approx_distinct_pages as bigint) as approx_distinct_pages,
            cast(observed_hours as smallint) as observed_hours
        from read_parquet(
            {_parquet_glob(gold_dir / GOLD_TABLE_CONTRACTS["daily_project_access"].path)},
            hive_partitioning=true
        )
        """
    )
    con.execute(
        f"""
        create or replace view gold.top_pages_hourly as
        select
            cast(date as date) as date,
            cast(hour as smallint) as hour,
            project,
            access_mode,
            source_project,
            normalized_title,
            page_title,
            cast(view_count as bigint) as view_count,
            cast(response_size as bigint) as response_size,
            cast(rank_in_hour as integer) as rank_in_hour
        from read_parquet(
            {_parquet_glob(gold_dir / GOLD_TABLE_CONTRACTS["top_pages_hourly"].path)},
            hive_partitioning=true
        )
        """
    )
    return tuple(f"gold.{table_name}" for table_name in GOLD_TABLE_CONTRACTS)


def _create_forecast_views(con, forecast_dir: Path | None) -> tuple[str, ...]:
    if not _forecast_outputs_available(forecast_dir):
        return ()

    assert forecast_dir is not None
    con.execute("create schema if not exists forecast")
    con.execute(
        f"""
        create or replace view forecast.forecast_metrics as
        select
            project,
            access_mode,
            model,
            cast(folds as integer) as folds,
            cast(observations as integer) as observations,
            cast(mdae as double) as mdae,
            cast(mase as double) as mase,
            cast(rmase as double) as rmase,
            cast(mdape as double) as mdape,
            cast(mdsmape as double) as mdsmape
        from read_parquet({_sql_path(_forecast_table_path(forecast_dir, "forecast_metrics"))})
        """
    )
    con.execute(
        f"""
        create or replace view forecast.forecast_backtest_predictions as
        select
            cast(fold_id as integer) as fold_id,
            cast(horizon_step as integer) as horizon_step,
            cast(ds as timestamp) as timestamp_utc,
            project,
            access_mode,
            model,
            cast(y_true as double) as y_true,
            cast(y_pred as double) as y_pred,
            cast(mase_scale as double) as mase_scale
        from read_parquet(
            {_sql_path(_forecast_table_path(forecast_dir, "forecast_backtest_predictions"))}
        )
        """
    )
    con.execute(
        f"""
        create or replace view forecast.forecast_future as
        select
            cast(generated_at_utc as varchar) as generated_at_utc,
            cast(horizon_step as integer) as horizon_step,
            cast(ds as timestamp) as timestamp_utc,
            project,
            access_mode,
            model,
            cast(yhat as double) as yhat
        from read_parquet({_sql_path(_forecast_table_path(forecast_dir, "forecast_future"))})
        """
    )
    return tuple(contract["view_name"] for contract in FORECAST_TABLE_CONTRACTS.values())


def _create_metadata(
    *,
    con,
    gold_dir: Path,
    forecast_dir: Path | None,
    database_path: Path,
    validation_report_path: Path | None,
    generated_at_utc: str,
    row_counts: dict[str, int],
) -> None:
    con.execute("drop table if exists metadata.serving_build")
    con.execute("drop table if exists metadata.gold_table_inventory")
    con.execute("drop table if exists metadata.forecast_table_inventory")
    con.execute(
        f"""
        create table metadata.serving_build as
        select
            {_sql_literal(generated_at_utc)} as generated_at_utc,
            {_sql_path(database_path)} as database_path,
            {_sql_path(gold_dir)} as gold_dir,
            {_sql_literal(str(forecast_dir) if forecast_dir else "")} as forecast_dir,
            {_sql_literal(str(validation_report_path) if validation_report_path else "")}
                as validation_report_path,
            'view' as storage_mode
        """
    )

    rows_sql = []
    for table_name, row_count in row_counts.items():
        if table_name not in GOLD_TABLE_CONTRACTS:
            continue
        contract = GOLD_TABLE_CONTRACTS[table_name]
        rows_sql.append(
            "select "
            f"{_sql_literal(table_name)} as table_name, "
            f"{_sql_literal('gold.' + table_name)} as view_name, "
            f"{_sql_literal(contract.grain)} as grain, "
            f"{int(row_count)}::bigint as row_count, "
            f"{_parquet_glob(gold_dir / contract.path)} as parquet_glob"
        )
    con.execute("create table metadata.gold_table_inventory as\n" + "\nunion all\n".join(rows_sql))

    forecast_rows_sql = []
    if forecast_dir is not None:
        for table_name, contract in FORECAST_TABLE_CONTRACTS.items():
            if table_name not in row_counts:
                continue
            forecast_rows_sql.append(
                "select "
                f"{_sql_literal(table_name)} as table_name, "
                f"{_sql_literal(contract['view_name'])} as view_name, "
                f"{_sql_literal(contract['grain'])} as grain, "
                f"{int(row_counts[table_name])}::bigint as row_count, "
                f"{_sql_path(_forecast_table_path(forecast_dir, table_name))} as parquet_path"
            )
    if forecast_rows_sql:
        con.execute(
            "create table metadata.forecast_table_inventory as\n"
            + "\nunion all\n".join(forecast_rows_sql)
        )
    else:
        con.execute(
            """
            create table metadata.forecast_table_inventory as
            select
                cast(null as varchar) as table_name,
                cast(null as varchar) as view_name,
                cast(null as varchar) as grain,
                cast(null as bigint) as row_count,
                cast(null as varchar) as parquet_path
            where false
            """
        )


def build_serving_database(
    *,
    gold_dir: Path,
    database_path: Path,
    forecast_dir: Path | None = None,
    validation_report_path: Path | None = None,
    overwrite: bool = False,
    require_validation: bool = True,
) -> ServingBuildSummary:
    if not gold_dir.exists():
        raise FileNotFoundError(f"Gold directory does not exist: {gold_dir}")
    assert_serving_writable(database_path, overwrite)
    _assert_gold_valid(
        gold_dir=gold_dir,
        validation_report_path=validation_report_path,
        require_validation=require_validation,
    )

    if overwrite and database_path.exists():
        database_path.unlink()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    import duckdb

    generated_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    con = duckdb.connect(str(database_path))
    try:
        gold_views = _create_gold_views(con, gold_dir)
        forecast_views = _create_forecast_views(con, forecast_dir)
        row_counts = {
            table_name: int(con.execute(f"select count(*) from gold.{table_name}").fetchone()[0])
            for table_name in GOLD_TABLE_CONTRACTS
        }
        for table_name, contract in FORECAST_TABLE_CONTRACTS.items():
            if contract["view_name"] in forecast_views:
                row_counts[table_name] = int(
                    con.execute(f"select count(*) from {contract['view_name']}").fetchone()[0]
                )
        _create_metadata(
            con=con,
            gold_dir=gold_dir,
            forecast_dir=forecast_dir if forecast_views else None,
            database_path=database_path,
            validation_report_path=validation_report_path,
            generated_at_utc=generated_at_utc,
            row_counts=row_counts,
        )
        con.execute("checkpoint")
    finally:
        con.close()

    return ServingBuildSummary(
        database_path=database_path,
        gold_dir=gold_dir,
        forecast_dir=forecast_dir if forecast_views else None,
        validation_report_path=validation_report_path,
        views=(*gold_views, *forecast_views),
        row_counts=row_counts,
        overwrite=overwrite,
        generated_at_utc=generated_at_utc,
    )


__all__ = [
    "FORECAST_TABLE_CONTRACTS",
    "ServingBuildSummary",
    "assert_serving_writable",
    "build_serving_database",
]
