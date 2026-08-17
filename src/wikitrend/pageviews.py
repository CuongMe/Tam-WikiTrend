from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

PAGEVIEW_FILENAME_RE = re.compile(r"pageviews-(?P<date>\d{8})-(?P<hour>\d{2})0000\.gz$")
PAGEVIEWS_BASE_URL = "https://dumps.wikimedia.org/other/pageviews"


@dataclass(frozen=True)
class ProjectDimensions:
    project: str
    language: str | None
    project_family: str
    access_mode: str


# Wikimedia's dump codes are transport-level domain codes, not analytical dimensions.
# Keep this map explicit so special projects are not guessed from string suffixes.
PROJECT_CODE_MAP = {
    "en": ProjectDimensions("en", "en", "wikipedia", "desktop"),
    "en.m": ProjectDimensions("en", "en", "wikipedia", "mobile"),
    "vi": ProjectDimensions("vi", "vi", "wikipedia", "desktop"),
    "vi.m": ProjectDimensions("vi", "vi", "wikipedia", "mobile"),
    "commons.m": ProjectDimensions("commons", None, "commons", "desktop"),
    "commons.m.m": ProjectDimensions("commons", None, "commons", "mobile"),
    "www.wd": ProjectDimensions("wikidata", None, "wikidata", "desktop"),
    "www.wd.m": ProjectDimensions("wikidata", None, "wikidata", "mobile"),
}
SUPPORTED_SOURCE_PROJECTS = tuple(PROJECT_CODE_MAP)
DEFAULT_SOURCE_PROJECTS = tuple(
    code for code in SUPPORTED_SOURCE_PROJECTS if code != "www.wd.m"
)


@dataclass(frozen=True)
class PageviewRecord:
    date: str
    hour: int
    source_project: str
    project: str
    language: str | None
    project_family: str
    access_mode: str
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


def pageviews_url(
    timestamp_utc: datetime, base_url: str = PAGEVIEWS_BASE_URL
) -> str:
    if timestamp_utc.tzinfo is None:
        timestamp_utc = timestamp_utc.replace(tzinfo=UTC)
    timestamp_utc = timestamp_utc.astimezone(UTC)
    year = timestamp_utc.strftime("%Y")
    month = timestamp_utc.strftime("%Y-%m")
    filename = timestamp_utc.strftime("pageviews-%Y%m%d-%H0000.gz")
    return f"{base_url.rstrip('/')}/{year}/{month}/{filename}"


def normalize_title(raw_title: str) -> str:
    return unquote(raw_title).replace("_", " ")


def canonicalize_project(source_project: str) -> ProjectDimensions | None:
    return PROJECT_CODE_MAP.get(source_project)


def infer_language_and_family(source_project: str) -> tuple[str | None, str]:
    dimensions = canonicalize_project(source_project)
    if dimensions is None:
        return None, "other"
    return dimensions.language, dimensions.project_family


def parse_pageview_line(line: str, date_value: str, hour: int) -> PageviewRecord | None:
    parts = line.rstrip("\n").split(" ")
    if len(parts) != 4:
        return None

    source_project, page_title, view_count_raw, response_size_raw = parts
    try:
        view_count = int(view_count_raw)
        response_size = int(response_size_raw)
    except ValueError:
        return None

    dimensions = canonicalize_project(source_project)
    if (
        view_count < 0
        or response_size < 0
        or not source_project
        or not page_title
        or dimensions is None
    ):
        return None

    return PageviewRecord(
        date=date_value,
        hour=hour,
        source_project=source_project,
        project=dimensions.project,
        language=dimensions.language,
        project_family=dimensions.project_family,
        access_mode=dimensions.access_mode,
        page_title=page_title,
        normalized_title=normalize_title(page_title),
        view_count=view_count,
        response_size=response_size,
    )
