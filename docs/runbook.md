# Runbook

## Local Checks

```powershell
conda activate wikitrend
pip install -e ".[dev]"
pytest
ruff check .
```

## Download Data

Start with one hour before running the full 7-day scope:

```powershell
python scripts/download_pageviews.py --start-date 2026-01-01 --end-date 2026-01-01 --hours 0
```

Then expand to the full configured range:

```powershell
python scripts/download_pageviews.py
```

## Build Tables

```powershell
spark-submit spark_jobs/parse_pageviews.py `
  --input data/raw/pageviews `
  --output data/silver/pageviews `
  --project-allowlist en,en.m,vi,vi.m,wikidata,commons,commons.m,commons.m.m `
  --quarantine-output data/quarantine/pageviews `
  --rejection-summary-output data/quarantine/pageviews_rejection_summary `
  --mode overwrite
```

The parser writes structurally invalid rows to the raw-record quarantine and writes
compact counts for both quality rejections and out-of-scope projects to the rejection
summary. Valid Silver rows include `normalization_status` for title-quality analysis.

Validate the completed Silver write before building Gold:

```powershell
python scripts/validate_silver.py `
  --raw-dir data/raw/pageviews `
  --silver-dir data/silver/pageviews `
  --quarantine-dir data/quarantine/pageviews `
  --rejection-summary-dir data/quarantine/pageviews_rejection_summary
```

```powershell
spark-submit spark_jobs/build_gold_tables.py `
  --silver data/silver/pageviews `
  --gold data/gold
```

Validate Gold before starting the API or dashboard. Forecast evaluation reports
`MASE`, `ND`, `sMAPE`, and `msMAPE`; it does not use RMSE or MAE:

```powershell
python scripts/validate_gold.py `
  --gold-dir data/gold `
  --top-n 100 `
  --baseline-hours 24
```

## Start Services

```powershell
docker compose -f infrastructure/docker-compose.yml up -d postgres minio kafka spark-master spark-worker
```

```powershell
docker compose -f infrastructure/docker-compose.yml up airflow-init
docker compose -f infrastructure/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

## Common Issues

- If `spark-submit` is not found locally, install dependencies from `environment.yml`
  or run Spark inside Docker.
- If Airflow containers cannot write logs, set `AIRFLOW_UID=50000` in `.env`.
- If Docker reports access denied reading Docker config, fix permissions on
  `C:\Users\cuong\.docker\config.json` or run Docker from a shell with access.
