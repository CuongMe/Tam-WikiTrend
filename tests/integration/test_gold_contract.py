from __future__ import annotations

import gzip
import math
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs.build_gold_tables import (
    add_past_only_eligibility,
    build_complete_modeling_series,
    build_forecast_features,
    build_trends_and_anomalies,
)
from spark_jobs.parse_pageviews import _build_parsed_rows

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    if os.name == "nt":
        conda_java = Path(sys.prefix) / "Library"
        if (conda_java / "bin" / "java.exe").is_file():
            os.environ["JAVA_HOME"] = str(conda_java)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("wikitrend-contract-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _page_row(hour: int, title: str, views: int) -> tuple[object, ...]:
    return (
        datetime(2026, 8, 1, hour),
        "2026-08-01",
        hour,
        "en",
        "en",
        "en",
        "wikipedia",
        "desktop",
        title.replace(" ", "_"),
        title,
        views,
        views * 10,
        1,
    )


PAGE_COLUMNS = [
    "timestamp_hour",
    "date",
    "hour",
    "source_project",
    "project",
    "language",
    "project_family",
    "access_mode",
    "page_title",
    "normalized_title",
    "view_count",
    "response_size",
    "page_rows",
]


def test_spark_parser_maps_special_projects_and_access_modes(spark, tmp_path) -> None:
    source = tmp_path / "pageviews-20260801-000000.gz"
    with gzip.open(source, "wt", encoding="utf-8") as handle:
        handle.write("en.m Main_Page 3 30\n")
        handle.write("commons.m File:Example.jpg 4 40\n")
        handle.write("commons.m.m File:Mobile.jpg 5 50\n")
        handle.write("www.wd Q42 6 60\n")
    parsed = _build_parsed_rows(
        spark,
        str(source),
        ["en.m", "commons.m", "commons.m.m", "www.wd"],
    )
    rows = {
        row.source_project: (row.project, row.project_family, row.access_mode)
        for row in parsed.select(
            "source_project", "project", "project_family", "access_mode"
        ).collect()
    }
    assert rows == {
        "en.m": ("en", "wikipedia", "mobile"),
        "commons.m": ("commons", "commons", "desktop"),
        "commons.m.m": ("commons", "commons", "mobile"),
        "www.wd": ("wikidata", "wikidata", "desktop"),
    }


def test_sparse_series_is_zero_complete_and_eligibility_is_past_only(spark) -> None:
    page_hourly = spark.createDataFrame(
        [
            _page_row(0, "Topic A", 10),
            _page_row(1, "Small topic", 1),
            _page_row(2, "Topic A", 30),
        ],
        PAGE_COLUMNS,
    )
    complete = build_complete_modeling_series(page_hourly, min_topic_views=5, min_history_hours=1)
    eligible = add_past_only_eligibility(complete, min_topic_views=10, min_history_hours=1)
    rows = eligible.orderBy("timestamp_hour").collect()

    assert [row.view_count for row in rows] == [10, 0, 30]
    assert [row.is_observed for row in rows] == [True, False, True]
    assert rows[0].eligible_at_origin is None or rows[0].eligible_at_origin is False
    assert rows[1].eligible_at_origin is True
    assert rows[1].eligibility_history_views == 10

    forecasts = build_forecast_features(eligible, baseline_hours=24, forecast_average_hours=6)
    origin = forecasts.filter("hour = 1").first()
    assert origin.lag_1h_views == 10
    assert origin.target_next_hour_views == 30
    assert origin.forecast_history_elapsed_hours == 1
    assert origin.forecast_history_active_hours == 1


def test_robust_history_median_uses_only_prior_hours(spark) -> None:
    views = [1, 2, 3, 100, 50]
    page_hourly = spark.createDataFrame(
        [_page_row(hour, "Topic A", value) for hour, value in enumerate(views)],
        PAGE_COLUMNS,
    )
    complete = build_complete_modeling_series(page_hourly, min_topic_views=1, min_history_hours=1)
    eligible = add_past_only_eligibility(complete, min_topic_views=1, min_history_hours=1)
    trends, _ = build_trends_and_anomalies(
        eligible,
        min_views=1,
        z_threshold=3.5,
        baseline_hours=4,
        min_baseline_observations=4,
    )
    try:
        current = trends.filter("hour = 4").first()
        expected = (math.log1p(2) + math.log1p(3)) / 2
        assert current.rolling_baseline_log_median == pytest.approx(expected)
        assert current.baseline_observed_hours == 4
        assert current.robust_z_score is not None
    finally:
        trends.unpersist()
