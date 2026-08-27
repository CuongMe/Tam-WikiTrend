from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "airflow" / "dags" / "wikitrend_batch_lakehouse.py"
AIRFLOW_REQUIREMENTS = ROOT / "requirements-airflow.in"


def test_airflow_dag_orchestrates_existing_batch_cli_modules() -> None:
    dag = DAG_PATH.read_text(encoding="utf-8")

    expected_modules = [
        "wikitrend.cli.download_pageviews",
        "wikitrend.cli.build_silver_pageviews",
        "wikitrend.cli.validate_silver_pageviews",
        "wikitrend.cli.build_gold_pageviews",
        "wikitrend.cli.validate_gold_pageviews",
        "wikitrend.cli.build_forecast_pageviews",
        "wikitrend.cli.build_delta_lake",
        "wikitrend.cli.build_serving_db",
    ]
    for module in expected_modules:
        assert module in dag

    assert 'dag_id="wikitrend_batch_lakehouse"' in dag
    assert "schedule=None" in dag
    assert 'params={"overwrite": False}' in dag
    assert "download_bronze" in dag
    assert ">> build_forecast" in dag
    assert ">> build_serving_db" in dag
    assert "docker" not in dag.lower()
    assert "kafka" not in dag.lower()


def test_airflow_install_file_uses_versioned_constraints() -> None:
    requirements = AIRFLOW_REQUIREMENTS.read_text(encoding="utf-8")

    assert "apache-airflow==2.10.5" in requirements
    assert "constraints-2.10.5/constraints-3.11.txt" in requirements
