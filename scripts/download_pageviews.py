from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.config import get_settings
from wikitrend.logging_utils import configure_logging
from wikitrend.pageviews import pageviews_url
from wikitrend.storage import ensure_parent, raw_file_path

LOGGER = logging.getLogger("wikitrend.download_pageviews")


def iter_hours(start_date: date, end_date: date, hours: set[int] | None = None) -> Iterable[datetime]:
    current = datetime.combine(start_date, time(0), tzinfo=timezone.utc)
    end = datetime.combine(end_date, time(23), tzinfo=timezone.utc)
    while current <= end:
        if hours is None or current.hour in hours:
            yield current
        current += timedelta(hours=1)


def download_file(url: str, destination: Path, overwrite: bool = False) -> bool:
    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        LOGGER.info("skip existing file path=%s size=%s", destination, destination.stat().st_size)
        return False

    ensure_parent(destination)
    request = Request(url, headers={"User-Agent": "WikiTrend local data engineering project"})
    LOGGER.info("download url=%s path=%s", url, destination)
    with urlopen(request, timeout=120) as response:
        with destination.open("wb") as handle:
            while chunk := response.read(1024 * 1024):
                handle.write(chunk)

    if destination.stat().st_size == 0:
        msg = f"Downloaded empty file: {destination}"
        raise RuntimeError(msg)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download official Wikimedia hourly pageview dumps.")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--hours", help="Optional comma-separated UTC hours, for example 0,1,2.")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    settings = get_settings()
    args = parse_args()

    start_date = args.start_date or settings.start_date
    end_date = args.end_date or settings.end_date
    output_dir = args.output_dir or settings.raw_dir
    hours = {int(item) for item in args.hours.split(",")} if args.hours else None

    downloaded = 0
    skipped = 0
    failed = 0
    for timestamp_utc in iter_hours(start_date, end_date, hours):
        url = pageviews_url(timestamp_utc)
        destination = raw_file_path(output_dir, timestamp_utc)
        try:
            if download_file(url, destination, overwrite=args.overwrite):
                downloaded += 1
            else:
                skipped += 1
        except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
            failed += 1
            LOGGER.exception("download failed url=%s error=%s", url, exc)

    LOGGER.info("summary downloaded=%s skipped=%s failed=%s", downloaded, skipped, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
