from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from wikitrend.storage import raw_file_path


def test_raw_file_path_matches_immutable_bronze_layout() -> None:
    path = raw_file_path(Path("data/raw/pageviews"), datetime(2026, 8, 1, 3, tzinfo=UTC))
    assert path.as_posix().endswith(
        "data/raw/pageviews/2026/2026-08/pageviews-20260801-030000.gz"
    )
