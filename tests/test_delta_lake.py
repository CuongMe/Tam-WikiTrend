from __future__ import annotations

import json

import pytest
from deltalake import DeltaTable

from tests.test_gold import write_silver_partition
from wikitrend.delta_lake import assert_delta_writable, build_gold_delta_lake
from wikitrend.gold import build_gold_layer


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


def test_build_gold_delta_lake_writes_readable_delta_tables(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    delta_dir = tmp_path / "delta"

    summary = build_gold_delta_lake(gold_dir=gold_dir, delta_dir=delta_dir)

    rows_by_table = {table.table_name: table.rows for table in summary.tables}
    assert rows_by_table == {
        "hourly_project_access": 1,
        "daily_project_access": 1,
        "top_pages_hourly": 2,
    }
    assert summary.manifest_path == delta_dir / "delta_manifest.json"

    manifest = json.loads(summary.manifest_path.read_text(encoding="utf-8"))
    assert manifest["storage_format"] == "delta"
    assert manifest["engine"] == "delta-rs"
    assert len(manifest["tables"]) == 3

    top_pages = DeltaTable(str(delta_dir / "gold" / "top_pages_hourly")).to_pyarrow_table()
    assert top_pages.num_rows == 2


def test_delta_lake_refuses_existing_payload_without_overwrite(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    delta_dir = tmp_path / "delta"
    build_gold_delta_lake(gold_dir=gold_dir, delta_dir=delta_dir)

    with pytest.raises(FileExistsError, match="Refusing to write Delta outputs"):
        assert_delta_writable(delta_dir / "gold", overwrite=False)

    with pytest.raises(FileExistsError, match="Refusing to write Delta outputs"):
        build_gold_delta_lake(gold_dir=gold_dir, delta_dir=delta_dir)


def test_delta_lake_overwrite_replaces_existing_tables(tmp_path) -> None:
    gold_dir = build_sample_gold(tmp_path)
    delta_dir = tmp_path / "delta"
    build_gold_delta_lake(gold_dir=gold_dir, delta_dir=delta_dir)

    summary = build_gold_delta_lake(gold_dir=gold_dir, delta_dir=delta_dir, overwrite=True)

    assert {table.table_name for table in summary.tables} == {
        "hourly_project_access",
        "daily_project_access",
        "top_pages_hourly",
    }
