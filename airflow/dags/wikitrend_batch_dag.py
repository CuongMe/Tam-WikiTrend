from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT_HOME = os.environ.get("WIKITREND_PROJECT_HOME", "/opt/wikitrend")
DOWNLOAD_PLAN = os.environ.get(
    "WIKITREND_DOWNLOAD_PLAN", "configs/pageview_download_plan.json"
)
SOURCE_PROJECT_ALLOWLIST = os.environ.get(
    "WIKITREND_SOURCE_PROJECT_ALLOWLIST",
    "en,en.m,vi,vi.m,commons.m,commons.m.m,www.wd",
)
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
STAGING_ROOT = f"{PROJECT_HOME}/data/staging/{{{{ ts_nodash }}}}"
STAGED_SILVER = f"{STAGING_ROOT}/silver/pageviews"
STAGED_QUARANTINE = f"{STAGING_ROOT}/quarantine/pageviews"
STAGED_REJECTIONS = f"{STAGING_ROOT}/quarantine/pageviews_rejection_summary"
STAGED_GOLD = f"{STAGING_ROOT}/gold"
SNAPSHOT_MANIFEST = f"{STAGING_ROOT}/training_snapshot.json"
DELTA_PACKAGES = (
    "io.delta:delta-spark_2.13:4.0.0,"
    "org.apache.hadoop:hadoop-aws:3.4.0"
)

default_args = {
    "owner": "wikitrend",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wikitrend_batch_lakehouse",
    description="Validate a Wikimedia snapshot and publish versioned lakehouse tables.",
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
            f"--plan {DOWNLOAD_PLAN}"
        ),
    )

    parse_silver = BashOperator(
        task_id="parse_silver_pageviews",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"spark-submit --master {SPARK_MASTER} spark_jobs/parse_pageviews.py "
            f"--input data/raw/pageviews "
            f"--output {STAGED_SILVER} "
            f"--project-allowlist '{SOURCE_PROJECT_ALLOWLIST}' "
            f"--quarantine-output {STAGED_QUARANTINE} "
            f"--rejection-summary-output {STAGED_REJECTIONS} "
            f"--mode overwrite"
        ),
    )

    validate_silver = BashOperator(
        task_id="validate_silver_contract",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"python scripts/validate_silver.py "
            f"--raw-dir data/raw/pageviews "
            f"--silver-dir {STAGED_SILVER} "
            f"--quarantine-dir {STAGED_QUARANTINE} "
            f"--rejection-summary-dir {STAGED_REJECTIONS} "
            f"--project-allowlist '{SOURCE_PROJECT_ALLOWLIST}'"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold_tables",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"spark-submit --master {SPARK_MASTER} spark_jobs/build_gold_tables.py "
            f"--silver {STAGED_SILVER} "
            f"--gold {STAGED_GOLD} "
            f"--mode overwrite"
        ),
    )

    validate_gold = BashOperator(
        task_id="validate_gold_contract",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"python scripts/validate_gold.py --gold-dir {STAGED_GOLD} "
            f"--allowed-projects en,vi,commons,wikidata"
        ),
    )

    score_predictions = BashOperator(
        task_id="score_lightgbm_predictions",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            "if [ -f models/lightgbm/current.json ]; then "
            f"python spark_jobs/score_lightgbm.py "
            f"--master {SPARK_MASTER} "
            f"--forecast-features {STAGED_GOLD}/forecast_features "
            f"--predictions-output {STAGED_GOLD}/lightgbm_predictions; "
            "else echo 'No contract-compatible LightGBM model; skipping scoring'; fi"
        ),
    )

    build_snapshot_manifest = BashOperator(
        task_id="build_snapshot_manifest",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"python scripts/build_snapshot_manifest.py "
            f"--dataset {STAGED_GOLD}/forecast_features "
            f"--output {SNAPSHOT_MANIFEST}"
        ),
    )

    publish_delta = BashOperator(
        task_id="publish_delta_to_minio",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"spark-submit --master {SPARK_MASTER} --packages {DELTA_PACKAGES} "
            f"spark_jobs/publish_delta_lakehouse.py "
            f"--silver {STAGED_SILVER} --gold {STAGED_GOLD} "
            f"--snapshot-manifest {SNAPSHOT_MANIFEST} --master {SPARK_MASTER}"
        ),
    )

    publish_serving = BashOperator(
        task_id="publish_duckdb_serving_snapshot",
        bash_command=(
            f"cd {PROJECT_HOME} && "
            f"python scripts/publish_serving_db.py --gold {STAGED_GOLD} "
            f"--output data/serving/wikitrend.duckdb"
        ),
    )

    (
        download_raw
        >> parse_silver
        >> validate_silver
        >> build_gold
        >> validate_gold
        >> score_predictions
        >> build_snapshot_manifest
        >> publish_delta
        >> publish_serving
    )
