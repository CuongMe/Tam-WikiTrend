from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.pageviews import pageviews_url
from wikitrend.storage import ensure_parent, raw_file_path

LOGGER = logging.getLogger("wikitrend.cli.download_pageviews")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


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


def download_file(
    url: str,
    destination: Path,
    overwrite: bool = False,
    timeout_seconds: float = 120,
    expected_size_bytes: int | None = None,
    expected_sha256: str | None = None,
    trust_existing_manifest: bool = False,
) -> tuple[bool, int, str]:
    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        size_bytes = destination.stat().st_size
        if (
            trust_existing_manifest
            and expected_size_bytes == size_bytes
            and expected_sha256
        ):
            LOGGER.info(
                "skip existing manifest-trusted file path=%s size=%s",
                destination,
                size_bytes,
            )
            return False, size_bytes, expected_sha256
        LOGGER.info("skip existing file path=%s size=%s", destination, size_bytes)
        return False, size_bytes, sha256_file(destination)

    ensure_parent(destination)
    partial = destination.with_suffix(destination.suffix + ".part")
    request = Request(url, headers={"User-Agent": "WikiTrend local data engineering project"})
    LOGGER.info("download url=%s path=%s", url, destination)
    with urlopen(request, timeout=timeout_seconds) as response:
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


def download_with_retries(
    urls: list[str],
    destination: Path,
    overwrite: bool,
    max_attempts: int,
    backoff_seconds: float,
    timeout_seconds: float,
    expected_size_bytes: int | None = None,
    expected_sha256: str | None = None,
    trust_existing_manifest: bool = False,
) -> tuple[bool, int, str, str]:
    for attempt in range(1, max_attempts + 1):
        url = urls[(attempt - 1) % len(urls)]
        try:
            downloaded, size_bytes, sha256 = download_file(
                url,
                destination,
                overwrite,
                timeout_seconds,
                expected_size_bytes,
                expected_sha256,
                trust_existing_manifest,
            )
            return downloaded, size_bytes, sha256, url
        except (HTTPError, URLError, TimeoutError, RuntimeError, OSError):
            if attempt == max_attempts:
                raise
            delay = backoff_seconds * attempt
            LOGGER.warning(
                "download retry url=%s attempt=%s/%s delay_seconds=%s",
                url,
                attempt + 1,
                max_attempts,
                delay,
            )
            sleep(delay)
    raise AssertionError("unreachable")


def load_download_plan(path: Path) -> dict[str, Any]:
    payload = load_json(path)
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
    payload = load_json(path)
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


def retain_manifest_scope(
    manifest: dict[str, dict[str, Any]], expected_filenames: set[str]
) -> dict[str, dict[str, Any]]:
    """Remove entries outside the active acquisition plan."""
    return {
        filename: record
        for filename, record in manifest.items()
        if filename in expected_filenames
    }


def validate_plan_overrides(
    *,
    plan: Path | None,
    start_date: date | None,
    end_date: date | None,
    output_dir: Path | None,
    hours: str | None,
) -> None:
    """Keep a versioned acquisition plan immutable at the command line."""
    if plan is None:
        return
    overrides = [
        name
        for name, value in (
            ("--start-date", start_date),
            ("--end-date", end_date),
            ("--output-dir", output_dir),
            ("--hours", hours),
        )
        if value is not None
    ]
    if overrides:
        raise ValueError(
            "--plan defines the complete acquisition scope; remove these overrides: "
            + ", ".join(overrides)
        )


def resolve_download_workers(plan: dict[str, Any], requested: int | None) -> int:
    workers = requested if requested is not None else int(plan.get("download_workers", 1))
    if not 1 <= workers <= 8:
        raise ValueError("Download workers must be between 1 and 8")
    return workers


def resolve_download_attempts(plan: dict[str, Any], requested: int | None) -> int:
    attempts = requested if requested is not None else int(plan.get("download_attempts", 3))
    if not 1 <= attempts <= 5:
        raise ValueError("Download attempts must be between 1 and 5")
    return attempts


def resolve_download_timeout(plan: dict[str, Any], requested: float | None) -> float:
    timeout = (
        requested
        if requested is not None
        else float(plan.get("download_timeout_seconds", 30))
    )
    if not 5 <= timeout <= 300:
        raise ValueError("Download timeout must be between 5 and 300 seconds")
    return timeout


def resolve_base_urls(plan: dict[str, Any]) -> list[str]:
    configured = plan.get("base_urls")
    if configured is None:
        configured = [plan["base_url"]] if plan.get("base_url") else []
    if not isinstance(configured, list):
        raise ValueError("Download base_urls must be a list")
    base_urls = list(dict.fromkeys(str(value).rstrip("/") for value in configured))
    if any(not url.startswith("https://") for url in base_urls):
        raise ValueError("Every download base URL must use HTTPS")
    return base_urls


def rotate_base_urls(base_urls: list[str], offset_seed: int) -> list[str]:
    if not base_urls:
        return []
    offset = offset_seed % len(base_urls)
    return base_urls[offset:] + base_urls[:offset]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official Wikimedia hourly pageview dumps."
    )
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--plan", type=Path, help="Versioned bounded acquisition plan.")
    parser.add_argument("--manifest", type=Path, help="Override the Bronze SHA-256 manifest path.")
    parser.add_argument("--hours", help="Optional comma-separated UTC hours, for example 0,1,2.")
    parser.add_argument("--workers", type=int, help="Concurrent downloads; defaults to the plan.")
    parser.add_argument("--attempts", type=int, help="Attempts per file; defaults to the plan.")
    parser.add_argument(
        "--timeout-seconds", type=float, help="Socket inactivity timeout; defaults to the plan."
    )
    parser.add_argument(
        "--verify-existing",
        action="store_true",
        help="Hash existing files even when the manifest already records matching size and hash.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()
    validate_plan_overrides(
        plan=args.plan,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        hours=args.hours,
    )

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
    base_urls = resolve_base_urls(plan)
    workers = resolve_download_workers(plan, args.workers)
    attempts = resolve_download_attempts(plan, args.attempts)
    timeout_seconds = resolve_download_timeout(plan, args.timeout_seconds)
    trust_existing_manifest = (
        bool(plan.get("trust_manifest_on_resume", False))
        and not args.verify_existing
        and not args.overwrite
    )
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
                    "download_workers": workers,
                    "download_attempts": attempts,
                    "download_timeout_seconds": timeout_seconds,
                    "trust_manifest_on_resume": trust_existing_manifest,
                    "base_urls": base_urls,
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
    expected_filenames = {
        raw_file_path(output_dir, timestamp_utc).name
        for timestamp_utc in timestamps
    }
    manifest = retain_manifest_scope(load_manifest(manifest_path), expected_filenames)
    jobs: dict[Future[tuple[bool, int, str, str]], tuple[datetime, list[str], Path]] = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bronze-download") as pool:
        for index, timestamp_utc in enumerate(timestamps):
            ordered_base_urls = rotate_base_urls(base_urls, index)
            urls = (
                [
                    pageviews_url(timestamp_utc, base_url)
                    for base_url in ordered_base_urls
                ]
                if ordered_base_urls
                else [pageviews_url(timestamp_utc)]
            )
            destination = raw_file_path(output_dir, timestamp_utc)
            previous = manifest.get(destination.name)
            future = pool.submit(
                download_with_retries,
                urls,
                destination,
                args.overwrite,
                attempts,
                2.0,
                timeout_seconds,
                (
                    int(previous["size_bytes"])
                    if previous and "size_bytes" in previous
                    else None
                ),
                (
                    str(previous["sha256"])
                    if previous and "sha256" in previous
                    else None
                ),
                trust_existing_manifest,
            )
            jobs[future] = (timestamp_utc, urls, destination)

        for future in as_completed(jobs):
            timestamp_utc, urls, destination = jobs[future]
            try:
                was_downloaded, size_bytes, sha256, source_url = future.result()
                previous = manifest.get(destination.name)
                if previous and not args.overwrite:
                    if (
                        previous.get("size_bytes") != size_bytes
                        or previous.get("sha256") != sha256
                    ):
                        raise RuntimeError(
                            "Immutable Bronze file changed since manifest publication: "
                            f"{destination}"
                        )
                manifest[destination.name] = {
                    "filename": destination.name,
                    "timestamp_hour": timestamp_utc.isoformat(),
                    "relative_path": destination.relative_to(output_dir).as_posix(),
                    "source_url": (
                        previous.get("source_url", source_url)
                        if previous and not was_downloaded
                        else source_url
                    ),
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                }
                write_manifest(manifest_path, plan_id, manifest)
                if was_downloaded:
                    downloaded += 1
                else:
                    skipped += 1
            except (HTTPError, URLError, TimeoutError, RuntimeError, OSError) as exc:
                failed += 1
                LOGGER.exception("download failed urls=%s error=%s", urls, exc)

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
