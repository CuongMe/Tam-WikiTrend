from __future__ import annotations

import json

import pytest

from wikitrend.silver_validation import (
    clean_sidecar_files,
    expected_project_access_pairs,
    find_cleanup_candidates,
    manifest_expected_hours,
    parse_silver_partition,
)


def test_parse_silver_partition_extracts_hive_keys(tmp_path) -> None:
    silver_dir = tmp_path / "silver" / "pageviews"
    path = (
        silver_dir
        / "date=2026-01-01"
        / "hour=0"
        / "project=en"
        / "access_mode=desktop"
        / "part-000.parquet"
    )

    partition = parse_silver_partition(path, silver_dir)

    assert partition is not None
    assert partition.key == ("2026-01-01", 0, "en", "desktop")


def test_expected_project_access_pairs_deduplicates_source_codes() -> None:
    assert expected_project_access_pairs(("en", "en.m", "www.wd")) == {
        ("en", "desktop"),
        ("en", "mobile"),
        ("wikidata", "desktop"),
    }


def test_manifest_expected_hours_reads_manifest_timestamps(tmp_path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "files": [
                    {"timestamp_hour": "2026-01-01T00:00:00+00:00"},
                    {"timestamp_hour": "2026-01-01T01:00:00+00:00"},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert manifest_expected_hours(manifest_path) == {
        ("2026-01-01", 0),
        ("2026-01-01", 1),
    }


def test_find_cleanup_candidates_only_returns_spark_sidecars(tmp_path) -> None:
    output_dir = tmp_path / "silver"
    output_dir.mkdir()
    (output_dir / "_SUCCESS").write_text("", encoding="utf-8")
    (output_dir / ".part-000.parquet.crc").write_text("", encoding="utf-8")
    (output_dir / "part-000.parquet").write_text("", encoding="utf-8")

    assert [path.name for path in find_cleanup_candidates(output_dir)] == [
        ".part-000.parquet.crc",
        "_SUCCESS",
    ]


def test_clean_sidecar_files_refuses_payload_files(tmp_path) -> None:
    payload = tmp_path / "part-000.parquet"
    payload.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="non-sidecar"):
        clean_sidecar_files([payload])


def test_clean_sidecar_files_removes_only_sidecars(tmp_path) -> None:
    success = tmp_path / "_SUCCESS"
    crc = tmp_path / ".part-000.parquet.crc"
    success.write_text("", encoding="utf-8")
    crc.write_text("", encoding="utf-8")

    assert clean_sidecar_files([success, crc]) == 2
    assert not success.exists()
    assert not crc.exists()
