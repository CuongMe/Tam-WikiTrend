from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.pageviews import pageviews_url
from wikitrend.storage import ensure_parent, raw_file_path

LOGGER = logging.getLogger("wikitrend.download_pageviews")


def iter_hours(
    start_date: date, end_date: date, hours: set[int] | None = None
) -> Iterable[datetime]:
    current = datetime.combine(start_date, time(0), tzinfo=UTC)
    end = datetime.combine(end_date, time(23), tzinfo=UTC)
    while current <= end:
        if hours is None or current.hour in hours:
            yield current
        current += timedelta(hours=1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_gzip(path: Path) -> None:
    with gzip.open(path, "rb") as handle:
        while handle.read(1024 * 1024):
            pass


def download_file(url: str, destination: Path, overwrite: bool = False) -> tuple[bool, int, str]:
    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        LOGGER.info("skip existing file path=%s size=%s", destination, destination.stat().st_size)
        return False, destination.stat().st_size, sha256_file(destination)

    ensure_parent(destination)
    partial = destination.with_suffix(destination.suffix + ".part")
    request = Request(url, headers={"User-Agent": "WikiTrend local data engineering project"})
    LOGGER.info("download url=%s path=%s", url, destination)
    with urlopen(request, timeout=120) as response:
        expected_size = int(response.headers.get("Content-Length", 0))
        digest = hashlib.sha256()
        bytes_written = 0
        with partial.open("wb") as handle:
            while chunk := response.read(1024 * 1024):
                handle.write(chunk)
                digest.update(chunk)
                bytes_written += len(chunk)

    if bytes_written == 0:
        msg = f"Downloaded empty file: {partial}"
        raise RuntimeError(msg)
    if expected_size and bytes_written != expected_size:
        raise RuntimeError(
            f"Content-Length mismatch for {partial}: "
            f"expected={expected_size} actual={bytes_written}"
        )
    validate_gzip(partial)
    os.replace(partial, destination)
    return True, bytes_written, digest.hexdigest()


def load_download_plan(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {"plan_id", "start_date", "end_date", "output_dir", "manifest_path"}
    missing = required - set(payload)
    if missing:
        raise ValueError(f"Download plan is missing keys: {sorted(missing)}")
    if date.fromisoformat(payload["end_date"]) < date.fromisoformat(payload["start_date"]):
        raise ValueError("Download plan end_date precedes start_date")
    expected_hours = len(
        list(
            iter_hours(
                date.fromisoformat(payload["start_date"]),
                date.fromisoformat(payload["end_date"]),
            )
        )
    )
    if payload.get("expected_hours") not in (None, expected_hours):
        raise ValueError(
            "Download plan expected_hours does not match its inclusive date range: "
            f"expected={expected_hours} configured={payload['expected_hours']}"
        )
    return payload


def load_manifest(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {item["filename"]: item for item in payload.get("files", [])}


def write_manifest(path: Path, plan_id: str, files: dict[str, dict[str, Any]]) -> None:
    ensure_parent(path)
    temporary = path.with_suffix(path.suffix + ".part")
    payload = {
        "manifest_version": 1,
        "plan_id": plan_id,
        "algorithm": "sha256",
        "files": [files[name] for name in sorted(files)],
    }
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official Wikimedia hourly pageview dumps."
    )
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--plan", type=Path, help="Versioned multi-week acquisition plan.")
    parser.add_argument("--manifest", type=Path, help="Override the Bronze SHA-256 manifest path.")
    parser.add_argument("--hours", help="Optional comma-separated UTC hours, for example 0,1,2.")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    plan = load_download_plan(args.plan) if args.plan else {}
    start_date = args.start_date or (
        date.fromisoformat(plan["start_date"]) if plan else settings.start_date
    )
    end_date = args.end_date or (
        date.fromisoformat(plan["end_date"]) if plan else settings.end_date
    )
    output_dir = args.output_dir or (
        Path(plan["output_dir"]) if plan else settings.raw_dir
    )
    manifest_path = args.manifest or Path(
        plan.get("manifest_path", "data/raw/pageviews_manifest.json")
    )
    plan_id = str(plan.get("plan_id", f"manual-{start_date}-{end_date}"))
    hours = {int(item) for item in args.hours.split(",")} if args.hours else None
    if hours is not None and not hours.issubset(set(range(24))):
        raise ValueError("--hours must contain UTC hour values from 0 through 23")
    timestamps = list(iter_hours(start_date, end_date, hours))
    if args.dry_run:
        existing = sum(raw_file_path(output_dir, item).exists() for item in timestamps)
        print(
            json.dumps(
                {
                    "plan_id": plan_id,
                    "hours": len(timestamps),
                    "existing_files": existing,
                    "files_to_download": len(timestamps) - existing,
                    "output_dir": str(output_dir),
                    "manifest_path": str(manifest_path),
                },
                indent=2,
            )
        )
        return 0

    downloaded = 0
    skipped = 0
    failed = 0
    manifest = load_manifest(manifest_path)
    for timestamp_utc in timestamps:
        url = pageviews_url(timestamp_utc)
        destination = raw_file_path(output_dir, timestamp_utc)
        try:
            was_downloaded, size_bytes, sha256 = download_file(
                url, destination, overwrite=args.overwrite
            )
            previous = manifest.get(destination.name)
            if previous and not args.overwrite:
                if previous.get("size_bytes") != size_bytes or previous.get("sha256") != sha256:
                    raise RuntimeError(
                        f"Immutable Bronze file changed since manifest publication: {destination}"
                    )
            manifest[destination.name] = {
                "filename": destination.name,
                "timestamp_hour": timestamp_utc.isoformat(),
                "relative_path": destination.relative_to(output_dir).as_posix(),
                "source_url": url,
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
            write_manifest(manifest_path, plan_id, manifest)
            if was_downloaded:
                downloaded += 1
            else:
                skipped += 1
        except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
            failed += 1
            LOGGER.exception("download failed url=%s error=%s", url, exc)

    LOGGER.info(
        "summary plan=%s downloaded=%s skipped=%s failed=%s manifest=%s",
        plan_id,
        downloaded,
        skipped,
        failed,
        manifest_path,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
