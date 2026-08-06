from __future__ import annotations

from scripts.validate_silver import discover_raw_manifest, parse_allowlist, source_basename


def test_discover_raw_manifest_rejects_invalid_filenames(tmp_path) -> None:
    valid = tmp_path / "pageviews-20260801-030000.gz"
    invalid_hour = tmp_path / "pageviews-20260801-240000.gz"
    invalid_date = tmp_path / "pageviews-20260230-030000.gz"
    valid.touch()
    invalid_hour.touch()
    invalid_date.touch()

    manifest, invalid_paths = discover_raw_manifest(tmp_path)

    assert manifest == {valid.name: ("2026-08-01", 3)}
    assert set(invalid_paths) == {str(invalid_hour), str(invalid_date)}


def test_source_basename_handles_docker_and_windows_paths() -> None:
    assert source_basename("file:///opt/wikitrend/pageviews-20260801-030000.gz") == (
        "pageviews-20260801-030000.gz"
    )
    assert source_basename(r"D:\data\pageviews-20260801-030000.gz") == (
        "pageviews-20260801-030000.gz"
    )


def test_parse_allowlist_strips_empty_values() -> None:
    assert parse_allowlist("en, en.m,,vi") == {"en", "en.m", "vi"}
