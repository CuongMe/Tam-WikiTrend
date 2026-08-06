from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = os.getenv("WIKITREND_PROJECT_HOME", "/opt/wikitrend")
START_DATE = os.getenv("WIKITREND_START_DATE", "2026-01-01")
END_DATE = os.getenv("WIKITREND_END_DATE", "2026-01-07")
PROJECT_ALLOWLIST = os.getenv(
    "WIKITREND_PROJECT_ALLOWLIST",
    "en,en.m,vi,vi.m,wikidata,commons,commons.m,commons.m.m",
)

default_args = {
    "owner": "wikitrend",
    "retries": 1,
}

with DAG(
    dag_id="wikitrend_batch_lakehouse",
    description="Download Wikimedia dumps and publish Silver and Gold lakehouse tables.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["wikitrend", "lakehouse"],
) as dag:
    download_raw = BashOperator(
        task_id="download_raw_pageviews",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"python scripts/download_pageviews.py "
            f"--start-date {START_DATE} --end-date {END_DATE}"
        ),
    )

    parse_silver = BashOperator(
        task_id="parse_silver_pageviews",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"spark-submit spark_jobs/parse_pageviews.py "
            f"--input data/raw/pageviews "
            f"--output data/silver/pageviews "
            f"--project-allowlist '{PROJECT_ALLOWLIST}' "
            f"--quarantine-output data/quarantine/pageviews "
            f"--rejection-summary-output data/quarantine/pageviews_rejection_summary "
            f"--mode overwrite"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold_tables",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"spark-submit spark_jobs/build_gold_tables.py "
            f"--silver data/silver/pageviews "
            f"--gold data/gold "
            f"--mode overwrite"
        ),
    )

    download_raw >> parse_silver >> build_gold
