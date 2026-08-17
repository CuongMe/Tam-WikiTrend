from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


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
    path = Path(local_path)

    if sys.platform == "win32":
        # Windows: convert to file:/// URI with forward slashes
        posix_path = path.as_posix()
        if posix_path.startswith("/"):
            # Already absolute Unix-style
            return f"file:///{posix_path[1:]}"
        elif len(posix_path) > 1 and posix_path[1] == ":":
            # Windows absolute path (C:/...)
            return f"file:///{posix_path}"
        else:
            # Relative path
            return f"file:///{path.resolve().as_posix()}"
    else:
        # Unix: return absolute path as string
        return str(path.resolve())


def ensure_local_path(spark_uri: str) -> Path:
    """Convert Spark URI back to local Path."""
    if spark_uri.startswith("file:///"):
        # Remove file:// prefix
        local = spark_uri.replace("file:///", "")
        if sys.platform != "win32" and not local.startswith("/"):
            local = "/" + local
        return Path(local)
    else:
        return Path(spark_uri)
