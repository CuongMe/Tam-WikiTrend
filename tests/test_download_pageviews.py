from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from wikitrend.cli.download_pageviews import (
    download_file,
    download_with_retries,
    iter_hours,
    load_download_plan,
    load_manifest,
    resolve_base_urls,
    resolve_download_attempts,
    resolve_download_timeout,
    resolve_download_workers,
    retain_manifest_scope,
    rotate_base_urls,
    validate_plan_overrides,
)


def test_iter_hours_covers_inclusive_utc_date_range() -> None:
    timestamps = list(iter_hours(date(2026, 1, 1), date(2026, 1, 7)))

    assert len(timestamps) == 168
    assert timestamps[0].isoformat() == "2026-01-01T00:00:00+00:00"
    assert timestamps[-1].isoformat() == "2026-01-07T23:00:00+00:00"


def test_load_download_plan_rejects_incorrect_expected_hours(tmp_path) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "plan_id": "test-plan",
                "start_date": "2026-08-01",
                "end_date": "2026-08-01",
                "expected_hours": 23,
                "output_dir": "data/raw/pageviews",
                "manifest_path": "data/raw/pageviews_manifest.json",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="expected_hours"):
        load_download_plan(plan_path)


def test_load_manifest_accepts_utf8_bom(tmp_path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "filename": "pageviews-20260101-000000.gz",
                        "sha256": "a" * 64,
                    }
                ]
            }
        ),
        encoding="utf-8-sig",
    )

    manifest = load_manifest(manifest_path)

    assert set(manifest) == {"pageviews-20260101-000000.gz"}


def test_download_file_hashes_existing_bronze_without_network(tmp_path) -> None:
    destination = tmp_path / "pageviews-20260801-000000.gz"
    with gzip.open(destination, "wb") as handle:
        handle.write(b"en Main_Page 1 10\n")

    was_downloaded, size_bytes, sha256 = download_file(
        "https://example.invalid/not-requested.gz", destination
    )

    assert was_downloaded is False
    assert size_bytes == destination.stat().st_size
    assert len(sha256) == 64


def test_download_file_can_trust_existing_manifest_without_hashing(
    monkeypatch, tmp_path
) -> None:
    destination = tmp_path / "pageviews-20260801-000000.gz"
    with gzip.open(destination, "wb") as handle:
        handle.write(b"en Main_Page 1 10\n")

    def fail_hash(_path):
        raise AssertionError("existing file should not be hashed")

    monkeypatch.setattr("wikitrend.cli.download_pageviews.sha256_file", fail_hash)
    expected_sha256 = "a" * 64

    was_downloaded, size_bytes, sha256 = download_file(
        "https://example.invalid/not-requested.gz",
        destination,
        expected_size_bytes=destination.stat().st_size,
        expected_sha256=expected_sha256,
        trust_existing_manifest=True,
    )

    assert was_downloaded is False
    assert size_bytes == destination.stat().st_size
    assert sha256 == expected_sha256


def test_retain_manifest_scope_removes_old_plan_entries() -> None:
    manifest = {
        "keep.gz": {"filename": "keep.gz"},
        "old.gz": {"filename": "old.gz"},
    }

    assert retain_manifest_scope(manifest, {"keep.gz"}) == {
        "keep.gz": {"filename": "keep.gz"}
    }


def test_versioned_plan_rejects_scope_overrides() -> None:
    with pytest.raises(ValueError, match="complete acquisition scope"):
        validate_plan_overrides(
            plan=Path("configs/pageview_download_plan.json"),
            start_date=date(2026, 8, 1),
            end_date=None,
            output_dir=None,
            hours=None,
        )


def test_ad_hoc_download_allows_explicit_scope() -> None:
    validate_plan_overrides(
        plan=None,
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 1),
        output_dir=Path("data/raw/pageviews"),
        hours="0",
    )


def test_download_worker_count_is_bounded() -> None:
    assert resolve_download_workers({"download_workers": 4}, None) == 4
    assert resolve_download_workers({}, 2) == 2
    with pytest.raises(ValueError, match="between 1 and 8"):
        resolve_download_workers({}, 9)


def test_download_attempt_count_is_bounded() -> None:
    assert resolve_download_attempts({"download_attempts": 3}, None) == 3
    with pytest.raises(ValueError, match="between 1 and 5"):
        resolve_download_attempts({}, 6)


def test_download_timeout_is_bounded() -> None:
    assert resolve_download_timeout({"download_timeout_seconds": 30}, None) == 30
    with pytest.raises(ValueError, match="between 5 and 300"):
        resolve_download_timeout({}, 1)


def test_download_base_urls_are_https_and_deduplicated() -> None:
    plan = {"base_urls": ["https://mirror.example/", "https://mirror.example"]}
    assert resolve_base_urls(plan) == ["https://mirror.example"]
    with pytest.raises(ValueError, match="HTTPS"):
        resolve_base_urls({"base_urls": ["http://mirror.example"]})


def test_download_base_urls_rotate_deterministically() -> None:
    urls = ["https://one", "https://two", "https://three"]
    assert rotate_base_urls(urls, 1) == ["https://two", "https://three", "https://one"]
    assert rotate_base_urls(urls, 3) == urls


def test_download_retries_transient_failure(monkeypatch, tmp_path) -> None:
    calls = 0
    called_urls = []

    def flaky_download(
        url,
        _destination,
        _overwrite,
        _timeout_seconds,
        _expected_size_bytes=None,
        _expected_sha256=None,
        _trust_existing_manifest=False,
    ):
        nonlocal calls
        calls += 1
        called_urls.append(url)
        if calls == 1:
            raise TimeoutError("temporary")
        return True, 10, "abc"

    monkeypatch.setattr("wikitrend.cli.download_pageviews.download_file", flaky_download)

    assert download_with_retries(
        [
            "https://primary.example/file.gz",
            "https://backup.example/file.gz",
        ],
        tmp_path / "file.gz",
        False,
        max_attempts=2,
        backoff_seconds=0,
        timeout_seconds=30,
    ) == (True, 10, "abc", "https://backup.example/file.gz")
    assert calls == 2
    assert called_urls == [
        "https://primary.example/file.gz",
        "https://backup.example/file.gz",
    ]
