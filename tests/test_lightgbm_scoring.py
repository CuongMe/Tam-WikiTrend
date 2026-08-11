from datetime import datetime

import pandas as pd

from spark_jobs.score_lightgbm import (
    CAT_COLUMNS,
    NUMERIC_COLUMNS,
    parse_utc_hour,
    prepare_feature_matrix,
)


def test_parse_utc_hour_normalizes_timezone_and_truncates_minutes() -> None:
    assert parse_utc_hour("2026-08-04T10:45:30+00:00") == datetime(2026, 8, 4, 10)


def test_prepare_feature_matrix_uses_saved_categories_and_numeric_fill() -> None:
    feature_columns = NUMERIC_COLUMNS + CAT_COLUMNS
    levels = {
        "project": ["en", "__unknown__"],
        "language": ["en", "__unknown__"],
        "project_family": ["wikipedia", "__unknown__"],
        "access_mode": ["desktop", "mobile", "__unknown__"],
    }
    frame = pd.DataFrame(
        {
            **{column: [None] for column in NUMERIC_COLUMNS},
            "project": ["new-project"],
            "language": ["en"],
            "project_family": ["wikipedia"],
            "access_mode": ["desktop"],
        }
    )

    prepared = prepare_feature_matrix(frame, levels, feature_columns)

    assert list(prepared.columns) == feature_columns
    assert prepared.loc[0, "project"] == "__unknown__"
    assert prepared[NUMERIC_COLUMNS].iloc[0].eq(0.0).all()
