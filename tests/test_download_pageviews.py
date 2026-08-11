from __future__ import annotations

import gzip
import json
from datetime import date

import pytest

from scripts.download_pageviews import download_file, iter_hours, load_download_plan


def test_iter_hours_covers_inclusive_utc_date_range() -> None:
    timestamps = list(iter_hours(date(2026, 7, 8), date(2026, 8, 5)))

    assert len(timestamps) == 696
    assert timestamps[0].isoformat() == "2026-07-08T00:00:00+00:00"
    assert timestamps[-1].isoformat() == "2026-08-05T23:00:00+00:00"


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
