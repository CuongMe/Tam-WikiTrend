from __future__ import annotations

import duckdb

from api.main import predictions


def test_predictions_use_serving_database(tmp_path, monkeypatch) -> None:
    database = tmp_path / "wikitrend.duckdb"
    with duckdb.connect(str(database)) as conn:
        conn.execute(
            """
            CREATE TABLE predictions AS
            SELECT timestamp '2026-08-04 10:00:00' AS timestamp_hour,
                   'en' AS project, 'mobile' AS access_mode,
                   'Main Page' AS normalized_title,
                   10.0 AS forecast_views, 1 AS predicted_traffic_rank
            """
        )
    monkeypatch.setenv("WIKITREND_SERVING_DB", str(database))

    payload = predictions(limit=100, project="en", access_mode="mobile")

    assert len(payload) == 1
    assert payload[0]["normalized_title"] == "Main Page"
