from __future__ import annotations

import os
import shlex
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_DIR = Path(__file__).resolve().parent
PROJECT_DIR = Path(os.getenv("WIKITREND_AIRFLOW_PROJECT_DIR", DAG_DIR.parents[1]))
PYTHON_BIN = os.getenv("WIKITREND_AIRFLOW_PYTHON", "python")

SILVER_VALIDATION_REPORT = "data/processed/validation/silver_pageviews_validation.json"
GOLD_VALIDATION_REPORT = "data/processed/validation/gold_pageviews_validation.json"

COMMON_ENV = {
    "PYTHONPATH": str(PROJECT_DIR / "src"),
    "WIKITREND_ENV": os.getenv("WIKITREND_ENV", "airflow-local"),
}


def module_command(module: str, *args: str) -> str:
    quoted_project_dir = shlex.quote(str(PROJECT_DIR))
    quoted_python = shlex.quote(PYTHON_BIN)
    quoted_args = " ".join(
        arg if arg.startswith("{{") and arg.endswith("}}") else shlex.quote(arg)
        for arg in args
        if arg
    )
    return f"cd {quoted_project_dir} && {quoted_python} -m {module} {quoted_args}".strip()


def overwrite_arg() -> str:
    return '{{ "--overwrite" if params.overwrite else "" }}'


with DAG(
    dag_id="wikitrend_batch_lakehouse",
    description=(
        "Run the local WikiTrend Bronze, Silver, Gold, forecast, Delta, and serving pipeline."
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "wikitrend", "retries": 0},
    params={"overwrite": False},
    tags=["wikitrend", "batch", "lakehouse"],
) as dag:
    download_bronze = BashOperator(
        task_id="download_bronze",
        bash_command=module_command(
            "wikitrend.cli.download_pageviews",
            "--plan",
            "configs/pageview_download_plan.json",
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=module_command(
            "wikitrend.cli.build_silver_pageviews",
            overwrite_arg(),
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=module_command(
            "wikitrend.cli.validate_silver_pageviews",
            "--full-scan",
            "--report",
            SILVER_VALIDATION_REPORT,
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=module_command(
            "wikitrend.cli.build_gold_pageviews",
            overwrite_arg(),
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=module_command(
            "wikitrend.cli.validate_gold_pageviews",
            "--silver-validation-report",
            SILVER_VALIDATION_REPORT,
            "--report",
            GOLD_VALIDATION_REPORT,
        ),
        env=COMMON_ENV,
        append_env=True,
    )


    build_forecast = BashOperator(
        task_id="build_forecast",
        bash_command=module_command(
            "wikitrend.cli.build_forecast_pageviews",
            overwrite_arg(),
        ),
        env=COMMON_ENV,
        append_env=True,
    )
    build_delta = BashOperator(
        task_id="build_delta",
        bash_command=module_command(
            "wikitrend.cli.build_delta_lake",
            overwrite_arg(),
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    build_serving_db = BashOperator(
        task_id="build_serving_db",
        bash_command=module_command(
            "wikitrend.cli.build_serving_db",
            overwrite_arg(),
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    (
        download_bronze
        >> build_silver
        >> validate_silver
        >> build_gold
        >> validate_gold
        >> build_forecast
        >> build_delta
        >> build_serving_db
    )
