from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote


def raw_file_path(base_dir: Path, timestamp_utc: datetime) -> Path:
    filename = timestamp_utc.strftime("pageviews-%Y%m%d-%H0000.gz")
    return (
        base_dir
        / f"{timestamp_utc:%Y}"
        / f"{timestamp_utc:%Y-%m}"
        / filename
    )


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_spark_path(local_path: Path | str) -> str:
    """
    Convert local file path to Spark-compatible URI.

    Spark on Windows requires file:///C:/path format, while Unix uses /path.
    This function ensures cross-platform compatibility.

    Args:
        local_path: Local filesystem path (Path or str)

    Returns:
        Spark-compatible path URI
    """
    path = Path(local_path).resolve()

    if sys.platform == "win32":
        return f"file:///{path.as_posix()}"
    return str(path)


def ensure_local_path(spark_uri: str) -> Path:
    """Convert Spark URI back to local Path."""
    if spark_uri.startswith("file:///"):
        local = unquote(spark_uri.replace("file:///", ""))
        if sys.platform != "win32" and not local.startswith("/"):
            local = "/" + local
        return Path(local)
    return Path(spark_uri)
