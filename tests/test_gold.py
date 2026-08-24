from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wikitrend.gold import assert_gold_writable, build_gold_layer


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


def test_assert_gold_writable_refuses_existing_payload(tmp_path) -> None:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    (gold_dir / "old.parquet").write_bytes(b"old")

    with pytest.raises(FileExistsError, match="Refusing to write Gold outputs"):
        assert_gold_writable(gold_dir, overwrite=False)

    assert_gold_writable(gold_dir, overwrite=True)


def test_build_gold_layer_writes_compact_aggregates(tmp_path) -> None:
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

    summary = build_gold_layer(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        top_n_pages=1,
    )

    assert summary.hourly_rows == 1
    assert summary.daily_rows == 1
    assert summary.top_page_rows == 1
    assert (gold_dir / "hourly_project_access").exists()
    assert (gold_dir / "daily_project_access").exists()
    assert (gold_dir / "top_pages_hourly").exists()

    manifest = json.loads((gold_dir / "gold_manifest.json").read_text(encoding="utf-8"))
    assert manifest["tables"]["hourly_project_access"]["rows"] == 1
    assert manifest["tables"]["daily_project_access"]["rows"] == 1
    assert manifest["tables"]["top_pages_hourly"]["rows"] == 1
