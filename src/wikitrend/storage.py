from __future__ import annotations

from datetime import datetime
from pathlib import Path


def raw_file_path(base_dir: Path, timestamp_utc: datetime) -> Path:
    filename = timestamp_utc.strftime("pageviews-%Y%m%d-%H0000.gz")
    return (
        base_dir
        / f"date={timestamp_utc:%Y-%m-%d}"
        / f"hour={timestamp_utc:%H}"
        / filename
    )


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

