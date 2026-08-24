from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wikitrend.gold import build_gold_layer
from wikitrend.gold_validation import validate_gold_layer, write_validation_report
from wikitrend.serving import assert_serving_writable, build_serving_database


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


def build_validated_gold(tmp_path):
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
    report = validate_gold_layer(gold_dir=gold_dir)
    report_path = tmp_path / "validation" / "gold_pageviews_validation.json"
    write_validation_report(report, report_path)
    return gold_dir, report_path


def test_assert_serving_writable_refuses_existing_database(tmp_path) -> None:
    database_path = tmp_path / "serving.duckdb"
    database_path.write_bytes(b"existing")

    with pytest.raises(FileExistsError, match="Refusing to overwrite existing serving database"):
        assert_serving_writable(database_path, overwrite=False)

    assert_serving_writable(database_path, overwrite=True)


def test_build_serving_database_creates_gold_views_and_metadata(tmp_path) -> None:
    gold_dir, report_path = build_validated_gold(tmp_path)
    database_path = tmp_path / "serving" / "wikitrend.duckdb"

    summary = build_serving_database(
        gold_dir=gold_dir,
        database_path=database_path,
        validation_report_path=report_path,
    )

    assert summary.row_counts == {
        "hourly_project_access": 1,
        "daily_project_access": 1,
        "top_pages_hourly": 2,
    }
    assert database_path.exists()

    import duckdb

    con = duckdb.connect(str(database_path), read_only=True)
    try:
        assert con.execute("select count(*) from gold.hourly_project_access").fetchone()[0] == 1
        daily_views = con.execute(
            "select sum(total_views) from gold.daily_project_access"
        ).fetchone()[0]
        storage_mode = con.execute(
            "select storage_mode from metadata.serving_build"
        ).fetchone()[0]
        assert daily_views == 15
        assert con.execute("select count(*) from gold.top_pages_hourly").fetchone()[0] == 2
        assert storage_mode == "view"
        assert (
            con.execute("select count(*) from metadata.gold_table_inventory").fetchone()[0]
            == 3
        )
    finally:
        con.close()


def test_build_serving_database_rejects_failed_validation_report(tmp_path) -> None:
    gold_dir, _ = build_validated_gold(tmp_path)
    failed_report_path = tmp_path / "validation" / "failed_gold.json"
    failed_report_path.parent.mkdir(exist_ok=True)
    failed_report_path.write_text(json.dumps({"status": "fail"}), encoding="utf-8")

    with pytest.raises(ValueError, match="Gold validation report must pass"):
        build_serving_database(
            gold_dir=gold_dir,
            database_path=tmp_path / "serving.duckdb",
            validation_report_path=failed_report_path,
        )
