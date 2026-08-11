from __future__ import annotations

from datetime import UTC, datetime

from wikitrend.pageviews import (
    canonicalize_project,
    infer_language_and_family,
    pageviews_url,
    parse_dump_filename,
    parse_pageview_line,
)


def test_parse_dump_filename() -> None:
    assert parse_dump_filename("pageviews-20260101-000000.gz") == ("2026-01-01", 0)


def test_pageviews_url() -> None:
    url = pageviews_url(datetime(2026, 1, 1, 0, tzinfo=UTC))
    assert url.endswith("/2026/2026-01/pageviews-20260101-000000.gz")


def test_parse_valid_line() -> None:
    record = parse_pageview_line("en Main_Page 123 4567", "2026-01-01", 0)
    assert record is not None
    assert record.source_project == "en"
    assert record.project == "en"
    assert record.access_mode == "desktop"
    assert record.language == "en"
    assert record.project_family == "wikipedia"
    assert record.normalized_title == "Main Page"
    assert record.view_count == 123


def test_parse_invalid_line() -> None:
    assert parse_pageview_line("bad row", "2026-01-01", 0) is None
    assert parse_pageview_line("en Main_Page -1 10", "2026-01-01", 0) is None


def test_infer_special_family() -> None:
    assert infer_language_and_family("www.wd") == (None, "wikidata")


def test_canonicalize_mobile_and_special_projects() -> None:
    assert canonicalize_project("en.m") is not None
    assert canonicalize_project("en.m").access_mode == "mobile"
    assert canonicalize_project("commons.m").project == "commons"
    assert canonicalize_project("www.wd").project == "wikidata"
    assert canonicalize_project("wikidata") is None
