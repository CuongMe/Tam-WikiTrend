from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from wikitrend.silver_validation import (
    clean_sidecar_files,
    find_cleanup_candidates,
    find_parquet_files,
)


@dataclass(frozen=True)
class GoldTableContract:
    path: str
    grain: str
    key_columns: tuple[str, ...]
    required_columns: set[str]


@dataclass(frozen=True)
class GoldValidationReport:
    generated_at_utc: str
    gold_dir: str
    manifest_path: str
    silver_validation_report_path: str | None
    status: str
    errors: list[str]
    warnings: list[str]
    metrics: dict[str, Any]
    cleanup_candidates: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


GOLD_TABLE_CONTRACTS = {
    "hourly_project_access": GoldTableContract(
        path="hourly_project_access",
        grain="date, hour, project, access_mode",
        key_columns=("date", "hour", "project", "access_mode"),
        required_columns={
            "date",
            "hour",
            "project",
            "access_mode",
            "page_rows",
            "total_views",
            "total_response_size",
            "max_page_views",
            "avg_page_views",
            "approx_distinct_pages",
        },
    ),
    "daily_project_access": GoldTableContract(
        path="daily_project_access",
        grain="date, project, access_mode",
        key_columns=("date", "project", "access_mode"),
        required_columns={
            "date",
            "project",
            "access_mode",
            "page_rows",
            "total_views",
            "total_response_size",
            "max_page_views",
            "avg_page_views",
            "approx_distinct_pages",
            "observed_hours",
        },
    ),
    "top_pages_hourly": GoldTableContract(
        path="top_pages_hourly",
        grain="date, hour, project, access_mode, rank_in_hour",
        key_columns=("date", "hour", "project", "access_mode", "rank_in_hour"),
        required_columns={
            "date",
            "hour",
            "project",
            "access_mode",
            "source_project",
            "normalized_title",
            "page_title",
            "view_count",
            "response_size",
            "rank_in_hour",
        },
    ),
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_validation_report(report: GoldValidationReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8")


def _load_gold_table(path: Path) -> pd.DataFrame:
    import pyarrow.dataset as ds

    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    frame = dataset.to_table().to_pandas()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"]).dt.date.astype(str)
    if "hour" in frame.columns:
        frame["hour"] = pd.to_numeric(frame["hour"], errors="coerce").astype("Int16")
    return frame


def _schema_names(path: Path) -> set[str]:
    import pyarrow.dataset as ds

    return set(ds.dataset(path, format="parquet", partitioning="hive").schema.names)


def _table_row_count(path: Path) -> int:
    import pyarrow.dataset as ds

    return int(ds.dataset(path, format="parquet", partitioning="hive").count_rows())


def _validate_table_contracts(
    *,
    gold_dir: Path,
    manifest: dict[str, Any] | None,
    errors: list[str],
    metrics: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    loaded_tables: dict[str, pd.DataFrame] = {}
    manifest_tables = manifest.get("tables", {}) if manifest else {}
    table_metrics: dict[str, dict[str, Any]] = {}

    for table_name, contract in GOLD_TABLE_CONTRACTS.items():
        table_path = gold_dir / contract.path
        parquet_files = find_parquet_files(table_path)
        table_metrics[table_name] = {
            "parquet_files": len(parquet_files),
            "size_bytes": sum(path.stat().st_size for path in parquet_files),
        }

        if not table_path.exists():
            errors.append(f"Gold table directory does not exist: {table_path}")
            continue
        if not parquet_files:
            errors.append(f"No Gold Parquet files found for table: {table_name}")
            continue
        if table_name not in manifest_tables:
            errors.append(f"Gold manifest is missing table entry: {table_name}")

        schema_names = _schema_names(table_path)
        missing_columns = sorted(contract.required_columns - schema_names)
        table_metrics[table_name]["schema_columns"] = sorted(schema_names)
        table_metrics[table_name]["missing_columns"] = missing_columns
        if missing_columns:
            errors.append(f"{table_name} schema is missing required columns: {missing_columns}")

        actual_rows = _table_row_count(table_path)
        table_metrics[table_name]["rows"] = actual_rows
        manifest_rows = manifest_tables.get(table_name, {}).get("rows")
        table_metrics[table_name]["manifest_rows"] = manifest_rows
        if manifest_rows is not None and int(manifest_rows) != actual_rows:
            errors.append(
                f"{table_name} row count mismatch: manifest={manifest_rows} actual={actual_rows}"
            )

        loaded_tables[table_name] = _load_gold_table(table_path)

    metrics["tables"] = table_metrics
    return loaded_tables


def _validate_grains(
    *,
    tables: dict[str, pd.DataFrame],
    errors: list[str],
    metrics: dict[str, Any],
) -> None:
    grain_metrics = {}
    for table_name, frame in tables.items():
        key_columns = list(GOLD_TABLE_CONTRACTS[table_name].key_columns)
        if not set(key_columns).issubset(frame.columns):
            continue
        duplicate_mask = frame.duplicated(key_columns, keep=False)
        duplicate_rows = int(duplicate_mask.sum())
        duplicate_keys = int(frame.loc[duplicate_mask, key_columns].drop_duplicates().shape[0])
        null_key_rows = int(frame[key_columns].isna().any(axis=1).sum())
        grain_metrics[table_name] = {
            "key_columns": key_columns,
            "duplicate_rows": duplicate_rows,
            "duplicate_keys": duplicate_keys,
            "null_key_rows": null_key_rows,
        }
        if duplicate_rows:
            errors.append(f"{table_name} has {duplicate_keys} duplicate grain keys")
        if null_key_rows:
            errors.append(f"{table_name} has {null_key_rows} rows with null grain values")
    metrics["grain_checks"] = grain_metrics


def _validate_metric_domains(
    *,
    tables: dict[str, pd.DataFrame],
    errors: list[str],
    metrics: dict[str, Any],
) -> None:
    checks = {
        "hourly_project_access": {
            "positive": ("page_rows", "avg_page_views", "approx_distinct_pages"),
            "non_negative": ("total_views", "total_response_size", "max_page_views"),
        },
        "daily_project_access": {
            "positive": ("page_rows", "avg_page_views", "approx_distinct_pages", "observed_hours"),
            "non_negative": ("total_views", "total_response_size", "max_page_views"),
        },
        "top_pages_hourly": {
            "positive": ("rank_in_hour",),
            "non_negative": ("view_count", "response_size"),
        },
    }
    domain_metrics: dict[str, dict[str, int]] = {}
    for table_name, frame in tables.items():
        table_metrics: dict[str, int] = {}
        for column in checks[table_name]["positive"]:
            if column not in frame.columns:
                continue
            invalid_rows = int((pd.to_numeric(frame[column], errors="coerce") <= 0).sum())
            table_metrics[f"non_positive_{column}_rows"] = invalid_rows
            if invalid_rows:
                errors.append(f"{table_name}.{column} has {invalid_rows} non-positive rows")
        for column in checks[table_name]["non_negative"]:
            if column not in frame.columns:
                continue
            invalid_rows = int((pd.to_numeric(frame[column], errors="coerce") < 0).sum())
            table_metrics[f"negative_{column}_rows"] = invalid_rows
            if invalid_rows:
                errors.append(f"{table_name}.{column} has {invalid_rows} negative rows")
        domain_metrics[table_name] = table_metrics
    metrics["domain_checks"] = domain_metrics


def _validate_hourly_daily_reconciliation(
    *,
    tables: dict[str, pd.DataFrame],
    errors: list[str],
    metrics: dict[str, Any],
) -> None:
    hourly = tables.get("hourly_project_access")
    daily = tables.get("daily_project_access")
    if hourly is None or daily is None:
        return

    key = ["date", "project", "access_mode"]
    hourly_to_daily = (
        hourly.groupby(key, as_index=False)
        .agg(
            page_rows_from_hourly=("page_rows", "sum"),
            total_views_from_hourly=("total_views", "sum"),
            total_response_size_from_hourly=("total_response_size", "sum"),
            max_page_views_from_hourly=("max_page_views", "max"),
            observed_hours_from_hourly=("hour", "nunique"),
        )
    )
    comparison = daily.merge(hourly_to_daily, on=key, how="outer", indicator=True)
    for column in (
        "page_rows",
        "total_views",
        "total_response_size",
        "max_page_views",
        "observed_hours",
    ):
        comparison[f"{column}_delta"] = (
            comparison[column] - comparison[f"{column}_from_hourly"]
        )
    delta_columns = [column for column in comparison.columns if column.endswith("_delta")]
    failures = comparison.loc[
        (comparison["_merge"] != "both") | comparison[delta_columns].ne(0).any(axis=1)
    ]
    metrics["hourly_daily_reconciliation"] = {
        "compared_keys": int(len(comparison)),
        "failed_keys": int(len(failures)),
        "max_abs_total_views_delta": int(comparison["total_views_delta"].abs().max() or 0),
    }
    if not failures.empty:
        errors.append(f"Daily Gold metrics do not reconcile for {len(failures)} keys")


def _validate_top_page_ranks(
    *,
    tables: dict[str, pd.DataFrame],
    manifest: dict[str, Any] | None,
    errors: list[str],
    metrics: dict[str, Any],
) -> None:
    top_pages = tables.get("top_pages_hourly")
    if top_pages is None:
        return

    top_n_pages = int((manifest or {}).get("top_n_pages", 100))
    key = ["date", "hour", "project", "access_mode"]
    rank_summary = (
        top_pages.groupby(key, as_index=False)
        .agg(
            rows=("rank_in_hour", "count"),
            min_rank=("rank_in_hour", "min"),
            max_rank=("rank_in_hour", "max"),
            unique_ranks=("rank_in_hour", "nunique"),
        )
    )
    invalid_groups = rank_summary.loc[
        (rank_summary["min_rank"] != 1)
        | (rank_summary["max_rank"] > top_n_pages)
        | (rank_summary["rows"] != rank_summary["unique_ranks"])
    ]

    ordered = top_pages.sort_values(key + ["rank_in_hour"]).copy()
    ordered["previous_view_count"] = ordered.groupby(key)["view_count"].shift(1)
    rank_order_violations = ordered.loc[
        ordered["previous_view_count"].notna()
        & (ordered["view_count"] > ordered["previous_view_count"])
    ]
    null_title_rows = int(
        top_pages[["source_project", "normalized_title", "page_title"]].isna().any(axis=1).sum()
    )

    metrics["top_page_rank_checks"] = {
        "top_n_pages": top_n_pages,
        "groups": int(len(rank_summary)),
        "invalid_rank_groups": int(len(invalid_groups)),
        "rank_order_violations": int(len(rank_order_violations)),
        "null_title_rows": null_title_rows,
    }
    if not invalid_groups.empty:
        errors.append(f"top_pages_hourly has {len(invalid_groups)} groups with invalid ranks")
    if not rank_order_violations.empty:
        errors.append(
            f"top_pages_hourly has {len(rank_order_violations)} view-count rank order violations"
        )
    if null_title_rows:
        errors.append(f"top_pages_hourly has {null_title_rows} rows with null page identity")


def _attach_silver_validation_context(
    *,
    silver_validation_report_path: Path | None,
    warnings: list[str],
    metrics: dict[str, Any],
) -> None:
    if silver_validation_report_path is None:
        warnings.append("No Silver validation report was supplied for Gold context")
        return
    if not silver_validation_report_path.exists():
        warnings.append(f"Silver validation report does not exist: {silver_validation_report_path}")
        return

    report = read_json(silver_validation_report_path)
    metrics["silver_validation"] = {
        "status": report.get("status"),
        "partition_combinations": report.get("metrics", {}).get("partition_combinations"),
        "silver_rows": report.get("metrics", {}).get("silver_rows"),
        "warnings": len(report.get("warnings", [])),
        "errors": len(report.get("errors", [])),
    }
    if report.get("status") != "pass":
        warnings.append(
            f"Silver validation report status is {report.get('status')}; "
            "Gold can be internally valid while upstream Silver still needs attention"
        )


def validate_gold_layer(
    *,
    gold_dir: Path,
    manifest_path: Path | None = None,
    silver_validation_report_path: Path | None = None,
) -> GoldValidationReport:
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {}

    manifest_path = manifest_path or gold_dir / "gold_manifest.json"
    cleanup_candidates = [str(path) for path in find_cleanup_candidates(gold_dir)]
    metrics["cleanup_candidate_files"] = len(cleanup_candidates)

    if cleanup_candidates:
        warnings.append(f"Found {len(cleanup_candidates)} optional Gold sidecar cleanup candidates")
    if not gold_dir.exists():
        errors.append(f"Gold directory does not exist: {gold_dir}")

    manifest: dict[str, Any] | None = None
    if not manifest_path.exists():
        errors.append(f"Gold manifest does not exist: {manifest_path}")
    else:
        manifest = read_json(manifest_path)
        metrics["manifest_version"] = manifest.get("manifest_version")
        metrics["top_n_pages"] = manifest.get("top_n_pages")

    tables = _validate_table_contracts(
        gold_dir=gold_dir,
        manifest=manifest,
        errors=errors,
        metrics=metrics,
    )
    _validate_grains(tables=tables, errors=errors, metrics=metrics)
    _validate_metric_domains(tables=tables, errors=errors, metrics=metrics)
    _validate_hourly_daily_reconciliation(tables=tables, errors=errors, metrics=metrics)
    _validate_top_page_ranks(tables=tables, manifest=manifest, errors=errors, metrics=metrics)
    _attach_silver_validation_context(
        silver_validation_report_path=silver_validation_report_path,
        warnings=warnings,
        metrics=metrics,
    )

    return GoldValidationReport(
        generated_at_utc=datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        gold_dir=str(gold_dir),
        manifest_path=str(manifest_path),
        silver_validation_report_path=(
            str(silver_validation_report_path) if silver_validation_report_path else None
        ),
        status="pass" if not errors else "fail",
        errors=errors,
        warnings=warnings,
        metrics=metrics,
        cleanup_candidates=cleanup_candidates,
    )


__all__ = [
    "GOLD_TABLE_CONTRACTS",
    "GoldValidationReport",
    "clean_sidecar_files",
    "find_cleanup_candidates",
    "validate_gold_layer",
    "write_validation_report",
]
