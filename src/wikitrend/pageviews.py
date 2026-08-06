from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

PAGEVIEW_FILENAME_RE = re.compile(r"pageviews-(?P<date>\d{8})-(?P<hour>\d{2})0000\.gz$")
PAGEVIEWS_BASE_URL = "https://dumps.wikimedia.org/other/pageviews"


@dataclass(frozen=True)
class PageviewRecord:
    date: str
    hour: int
    project: str
    language: str | None
    project_family: str
    page_title: str
    normalized_title: str
    view_count: int
    response_size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_dump_filename(filename: str) -> tuple[str, int]:
    match = PAGEVIEW_FILENAME_RE.search(filename)
    if not match:
        msg = f"Invalid Wikimedia pageviews filename: {filename}"
        raise ValueError(msg)
    raw_date = match.group("date")
    return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}", int(match.group("hour"))


def pageviews_url(timestamp_utc: datetime) -> str:
    if timestamp_utc.tzinfo is None:
        timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
    timestamp_utc = timestamp_utc.astimezone(timezone.utc)
    year = timestamp_utc.strftime("%Y")
    month = timestamp_utc.strftime("%Y-%m")
    filename = timestamp_utc.strftime("pageviews-%Y%m%d-%H0000.gz")
    return f"{PAGEVIEWS_BASE_URL}/{year}/{month}/{filename}"


def normalize_title(raw_title: str) -> str:
    return unquote(raw_title).replace("_", " ")


def infer_language_and_family(project: str) -> tuple[str | None, str]:
    base = project.split(".")[0]
    special_families = {
        "commons": "commons",
        "meta": "meta",
        "species": "wikispecies",
        "wikidata": "wikidata",
        "wikimedia": "wikimedia",
    }
    if base in special_families:
        return None, special_families[base]
    if re.fullmatch(r"[a-z][a-z0-9-]{1,11}", base):
        return base, "wikipedia"
    return None, "other"


def parse_pageview_line(line: str, date_value: str, hour: int) -> PageviewRecord | None:
    parts = line.rstrip("\n").split(" ")
    if len(parts) != 4:
        return None

    project, page_title, view_count_raw, response_size_raw = parts
    try:
        view_count = int(view_count_raw)
        response_size = int(response_size_raw)
    except ValueError:
        return None

    if view_count < 0 or response_size < 0 or not project or not page_title:
        return None

    language, project_family = infer_language_and_family(project)
    return PageviewRecord(
        date=date_value,
        hour=hour,
        project=project,
        language=language,
        project_family=project_family,
        page_title=page_title,
        normalized_title=normalize_title(page_title),
        view_count=view_count,
        response_size=response_size,
    )

