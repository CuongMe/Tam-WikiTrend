from __future__ import annotations

from fastapi.testclient import TestClient

from tests.test_serving import build_validated_gold
from wikitrend.api import ApiSettings, create_app
from wikitrend.serving import build_serving_database


def build_test_api(tmp_path) -> TestClient:
    gold_dir, report_path = build_validated_gold(tmp_path)
    database_path = tmp_path / "serving" / "wikitrend.duckdb"
    build_serving_database(
        gold_dir=gold_dir,
        database_path=database_path,
        validation_report_path=report_path,
    )
    app = create_app(
        ApiSettings(
            serving_db=database_path,
            gold_validation_report=report_path,
        )
    )
    return TestClient(app)


def test_api_health_and_quality(tmp_path) -> None:
    client = build_test_api(tmp_path)

    health = client.get("/health")
    quality = client.get("/v1/quality")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["gold_validation_status"] == "pass"
    assert quality.status_code == 200
    assert quality.json()["status"] == "pass"


def test_api_metadata_projects_and_trends(tmp_path) -> None:
    client = build_test_api(tmp_path)

    metadata = client.get("/v1/metadata")
    projects = client.get("/v1/projects")
    hourly = client.get("/v1/trends/hourly", params={"project": "en", "limit": 10})
    top_pages = client.get("/v1/top-pages", params={"rank_cap": 2, "limit": 10})

    assert metadata.status_code == 200
    assert len(metadata.json()["tables"]) == 3
    assert projects.status_code == 200
    assert projects.json()["projects"][0]["project"] == "en"
    assert hourly.status_code == 200
    assert hourly.json()["count"] == 1
    assert top_pages.status_code == 200
    assert top_pages.json()["count"] == 2