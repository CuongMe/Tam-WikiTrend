from __future__ import annotations

import gzip
import json

import pytest

from wikitrend.silver import (
    assert_output_writable,
    build_silver_layer_python,
    load_bronze_manifest_files,
    path_has_payload,
    project_dimension_rows,
)


def test_project_dimension_rows_respects_allowlist() -> None:
    rows = project_dimension_rows(("en", "vi.m"))

    assert rows == [
        {
            "source_project": "en",
            "project": "en",
            "language": "en",
            "project_family": "wikipedia",
            "access_mode": "desktop",
        },
        {
            "source_project": "vi.m",
            "project": "vi",
            "language": "vi",
            "project_family": "wikipedia",
            "access_mode": "mobile",
        },
    ]


def test_project_dimension_rows_rejects_unknown_codes() -> None:
    with pytest.raises(ValueError, match="Unsupported source project"):
        project_dimension_rows(("en", "missing.project"))


def test_load_bronze_manifest_files_rejects_path_escape(tmp_path) -> None:
    raw_dir = tmp_path / "raw" / "pageviews"
    raw_dir.mkdir(parents=True)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "filename": "escape.gz",
                        "relative_path": "../escape.gz",
                        "size_bytes": 1,
                        "sha256": "abc",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="escapes raw directory"):
        load_bronze_manifest_files(manifest_path, raw_dir)


def test_load_bronze_manifest_files_resolves_local_files(tmp_path) -> None:
    raw_dir = tmp_path / "raw" / "pageviews"
    bronze_path = raw_dir / "2026" / "2026-01" / "pageviews-20260101-000000.gz"
    bronze_path.parent.mkdir(parents=True)
    bronze_path.write_bytes(b"test")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "filename": bronze_path.name,
                        "relative_path": "2026/2026-01/pageviews-20260101-000000.gz",
                        "size_bytes": 4,
                        "sha256": "abc",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    files = load_bronze_manifest_files(manifest_path, raw_dir)

    assert len(files) == 1
    assert files[0].path == bronze_path.resolve()
    assert files[0].size_bytes == 4


def test_path_has_payload_detects_nested_files(tmp_path) -> None:
    output_dir = tmp_path / "silver" / "pageviews"
    assert not path_has_payload(output_dir)

    nested_file = output_dir / "date=2026-01-01" / "part.parquet"
    nested_file.parent.mkdir(parents=True)
    nested_file.write_bytes(b"parquet")

    assert path_has_payload(output_dir)


def test_assert_output_writable_refuses_existing_payload(tmp_path) -> None:
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    silver_dir.mkdir()
    (silver_dir / "_SUCCESS").write_text("", encoding="utf-8")

    with pytest.raises(FileExistsError, match="Refusing to write Silver outputs"):
        assert_output_writable(
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            overwrite=False,
        )

    assert_output_writable(
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        overwrite=True,
    )


def test_build_silver_layer_python_writes_partitioned_parquet(tmp_path) -> None:
    import pyarrow.dataset as ds

    raw_dir = tmp_path / "raw" / "pageviews"
    bronze_path = raw_dir / "2026" / "2026-01" / "pageviews-20260101-000000.gz"
    bronze_path.parent.mkdir(parents=True)
    with gzip.open(bronze_path, "wt", encoding="utf-8") as handle:
        handle.write("en Main_Page 10 100\n")
        handle.write("fr Page_d_accueil 5 50\n")

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "filename": bronze_path.name,
                        "relative_path": "2026/2026-01/pageviews-20260101-000000.gz",
                        "size_bytes": bronze_path.stat().st_size,
                        "sha256": "abc",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    silver_dir = tmp_path / "processed" / "silver" / "pageviews"
    quarantine_dir = tmp_path / "processed" / "quarantine" / "pageviews"

    summary = build_silver_layer_python(
        manifest_path=manifest_path,
        raw_dir=raw_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        source_projects=("en",),
    )

    dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
    assert summary.engine == "python"
    assert dataset.count_rows() == 1
    assert (
        silver_dir
        / "date=2026-01-01"
        / "hour=0"
        / "project=en"
        / "access_mode=desktop"
    ).exists()
