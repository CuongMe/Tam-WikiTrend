from __future__ import annotations

import gzip

from scripts.build_bronze_manifest import build_manifest


def test_build_manifest_hashes_and_optionally_checks_gzip(tmp_path) -> None:
    project = tmp_path / "project"
    raw = project / "data" / "raw" / "pageviews"
    raw.mkdir(parents=True)
    source = raw / "pageviews-20260801-000000.gz"
    with gzip.open(source, "wb") as handle:
        handle.write(b"en Main_Page 1 10\n")
    manifest = build_manifest(raw, verify_crc=True, project_root=project)

    assert manifest["file_count"] == 1
    assert manifest["gzip_crc_verified"] is True
    assert manifest["files"][0]["date"] == "2026-08-01"
    assert len(manifest["files"][0]["sha256"]) == 64
