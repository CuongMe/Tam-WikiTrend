from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from wikitrend.gold import build_gold_layer
from wikitrend.gold_validation import validate_gold_layer, write_validation_report


def write_silver_partition(
    base_dir,
    *,
    date: str,
    hour: int,
    project: str,
    access_mode: str,
    rows: list[dict],
) -> None:
    partition_dir = (
        base_dir
        / f"date={date}"
        / f"hour={hour}"
        / f"project={project}"
        / f"access_mode={access_mode}"
    )
    partition_dir.mkdir(parents=True)
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("source_project", pa.string()),
                ("language", pa.string()),
                ("project_family", pa.string()),
                ("page_title", pa.string()),
                ("normalized_title", pa.string()),
                ("view_count", pa.int64()),
                ("response_size", pa.int64()),
                ("source_filename", pa.string()),
            ]
        ),
    )
    pq.write_table(table, partition_dir / "part-00000.parquet")


def build_sample_gold(tmp_path):
    silver_dir = tmp_path / "silver" / "pageviews"
    write_silver_partition(
        silver_dir,
        date="2026-01-01",
        hour=0,
        project="en",
        access_mode="desktop",
        rows=[
            {
                "source_project": "en",
                "language": "en",
                "project_family": "wikipedia",
                "page_title": "Main_Page",
                "normalized_title": "Main Page",
                "view_count": 10,
                "response_size": 100,
                "source_filename": "pageviews-20260101-000000.gz",
            },
            {
                "source_project": "en",
                "language": "en",
                "project_family": "wikipedia",
                "page_title": "Python_(programming_language)",
                "normalized_title": "Python (programming language)",
                "view_count": 5,
                "response_size": 50,
                "source_filename": "pageviews-20260101-000000.gz",
            },
        ],
    )
    gold_dir = tmp_path / "gold"
    build_gold_layer(silver_dir=silver_dir, gold_dir=gold_dir, top_n_pages=2)
    return gold_dir


def test_validate_gold_layer_passes_compact_aggregate_contracts(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)

    report = validate_gold_layer(gold_dir=gold_dir)

    assert report.status == "pass"
    assert not report.errors
    assert report.metrics["tables"]["hourly_project_access"]["rows"] == 1
    assert report.metrics["tables"]["daily_project_access"]["rows"] == 1
    assert report.metrics["tables"]["top_pages_hourly"]["rows"] == 2
    assert report.metrics["hourly_daily_reconciliation"]["failed_keys"] == 0
    assert report.metrics["top_page_rank_checks"]["invalid_rank_groups"] == 0


def test_validate_gold_layer_fails_manifest_row_mismatch(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    manifest_path = gold_dir / "gold_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["tables"]["hourly_project_access"]["rows"] = 999
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    report = validate_gold_layer(gold_dir=gold_dir)

    assert report.status == "fail"
    assert any("hourly_project_access row count mismatch" in error for error in report.errors)


def test_validate_gold_layer_fails_missing_table(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    for path in (gold_dir / "top_pages_hourly").rglob("*.parquet"):
        path.unlink()

    report = validate_gold_layer(gold_dir=gold_dir)

    assert report.status == "fail"
    assert any(
        "No Gold Parquet files found for table: top_pages_hourly" in error
        for error in report.errors
    )


def test_write_validation_report_writes_json(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    report = validate_gold_layer(gold_dir=gold_dir)
    report_path = tmp_path / "validation" / "gold.json"

    write_validation_report(report, report_path)

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
