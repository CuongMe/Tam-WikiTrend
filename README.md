# TAM-WikiTrend (Ongoing): Wikimedia Demand Lakehouse

TAM-WikiTrend is a resume-scale local data engineering and forecasting project built
from official Wikimedia hourly pageview dumps. It demonstrates a reproducible path from
immutable Bronze files to validated Silver and Gold datasets, trend and anomaly
detection, one-hour-ahead forecasting, deterministic streaming replay, and analytical
serving.

This repository is a production-style local research system, not a production Wikimedia
traffic service. Its scope is deliberately fixed to seven complete UTC days so the full
workflow remains inspectable and runnable on one laptop.

## Canonical Scope

| Contract | Value |
| --- | --- |
| Window | `2026-01-01 00:00` through `2026-01-07 23:00` UTC |
| Bronze files | 168 hourly `.gz` dumps, 8.67 GiB |
| Source projects | `en`, `en.m`, `vi`, `vi.m`, `commons.m`, `commons.m.m`, `www.wd` |
| Canonical projects | English Wikipedia, Vietnamese Wikipedia, Commons, Wikidata |
| Access modes | Desktop and mobile |
| Silver rows | 405,393,771 validated rows |
| Gold page-hour rows | 405,366,097 reconciled rows |
| Zero-completed modeling rows | 75,992,448 rows |
| Eligible forecast rows | 38,084,887 rows |
| Forecast horizon | One hour |

The download plan, Airflow defaults, Docker Compose environment, local E2E wrapper, and
forecast protocol all use this same window. Files outside the plan are not retained in
the Bronze manifest.

## What It Demonstrates

- Immutable Bronze acquisition with gzip validation and SHA-256 manifests.
- PySpark parsing into partitioned Silver Parquet with explicit data contracts.
- Wikimedia source-code mapping into project, family, language, and access dimensions.
- Malformed-row quarantine, scope-rejection summaries, duplicate assertions, and lineage
  validation.
- Gold page-hour facts, sparse-series zero completion, past-only eligibility, robust
  median/MAD trend scoring, and anomaly alerts.
- A global LightGBM model with fixed rolling-origin folds, temporal preprocessing,
  versioned feature/category metadata, and lag-1 fallback.
- Traffic metrics (`MASE`, `ND`, `msMAPE`) and trend-ranking metrics (`NDCG@K`,
  `Recall@K`, top-K overlap, and Spearman correlation).
- Airflow orchestration, MinIO/Delta publication, bounded Kafka replay, deterministic
  Structured Streaming, DuckDB serving, FastAPI, and Streamlit.
- Docker Compose integration, dependency locks, pytest coverage, and GitHub Actions.

## Architecture

```text
Official Wikimedia hourly dumps
        |
        v
Bronze: immutable gzip files + SHA-256 manifest
        |
        v
Silver: canonical dimensions + partitioned Parquet + quarantine
        |
        v
Gold: facts + completed series + trends + anomalies + forecast features
        |
        +--> LightGBM rolling-origin evaluation and next-hour scoring
        |
        +--> DuckDB --> FastAPI / Streamlit
        |
        +--> Delta snapshots in MinIO

Bounded Parquet replay --> Kafka --> Structured Streaming --> idempotent Delta outputs
```

| Layer | Technology |
| --- | --- |
| Orchestration | Apache Airflow |
| Batch processing | PySpark |
| Streaming | Kafka and Spark Structured Streaming |
| Lakehouse storage | Parquet, Delta Lake, and MinIO |
| Forecasting | LightGBM |
| Analytical serving | DuckDB and FastAPI |
| Inspection UI | Streamlit and Jupyter |
| Runtime and CI | Docker Compose and GitHub Actions |

## Data Contract

Wikimedia dump codes are transport identifiers. Silver preserves each source code and
adds explicit analytical dimensions.

| Source code | Canonical project | Family | Access mode |
| --- | --- | --- | --- |
| `en` | `en` | `wikipedia` | desktop |
| `en.m` | `en` | `wikipedia` | mobile |
| `vi` | `vi` | `wikipedia` | desktop |
| `vi.m` | `vi` | `wikipedia` | mobile |
| `commons.m` | `commons` | `commons` | desktop |
| `commons.m.m` | `commons` | `commons` | mobile |
| `www.wd` | `wikidata` | `wikidata` | desktop |

Validation fails when a required source project is absent from any ingested hour, a
partition is missing, Bronze lineage does not reconcile, duplicate natural keys exist,
or a published Gold contract is violated.

## Repository Layout

| Path | Responsibility |
| --- | --- |
| `scripts/download_pageviews.py` | Resumable bounded Bronze acquisition and manifest publication. |
| `spark_jobs/parse_pageviews.py` | Silver parsing, dimensions, normalization, quarantine, and partitioning. |
| `scripts/validate_silver.py` | Silver coverage, schema, lineage, duplicate, and partition checks. |
| `spark_jobs/build_gold_tables.py` | Atomic construction of all canonical Gold tables. |
| `scripts/validate_gold.py` | Gold reconciliation, completeness, and feature-contract checks. |
| `spark_jobs/train_lightgbm_nested.py` | Nested rolling-origin objective selection and model training. |
| `spark_jobs/score_lightgbm.py` | Contract-enforced next-hour scoring and fallback logic. |
| `streaming/` | Kafka replay and deterministic Structured Streaming jobs. |
| `airflow/` | Batch lakehouse DAG. |
| `api/`, `dashboard/` | FastAPI and Streamlit serving applications. |
| `notebooks/` | Raw, Silver, Gold, prediction, and model inspection workflows. |
| `artifacts/` | Compact manifests, validation evidence, and evaluation summaries. |

## Environment

The supported local environment is Python 3.11, Java 17, Spark 4.0.1, pandas 2.2.3,
NumPy 2.2.6, and LightGBM 4.5.0.

```powershell
conda env update -n wikitrend -f environment.yml --prune
conda activate wikitrend
$env:HADOOP_HOME=(Resolve-Path ".hadoop").Path
${env:hadoop.home.dir}=$env:HADOOP_HOME
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"
```

## Bronze Acquisition

Inspect the fixed plan without network access:

```powershell
python scripts/download_pageviews.py `
  --plan configs/pageview_download_plan.json `
  --dry-run
```

Download or verify only the 168 planned files:

```powershell
python scripts/download_pageviews.py `
  --plan configs/pageview_download_plan.json
```

Existing files are hash-checked and skipped. The manifest is restricted to the active
plan, so an older acquisition window cannot remain advertised after the project is
downsized.

## Local E2E Run

The wrapper creates an isolated staging run and stops at the first failed contract:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_local_e2e.ps1 `
  -Run resume-20260101-20260108 `
  -StartHour 2026-01-01T00:00:00 `
  -EndHourExclusive 2026-01-08T00:00:00 `
  -PythonExe D:\Anaconda\envs\wikitrend\python.exe `
  -DriverMemory 8g `
  -LocalCores 4 `
  -ShufflePartitions 384 `
  -PublishCanonical
```

Add `-TrainModel` only when a new model artifact is required. Training snapshots record
the fixed folds, Parquet hashes, code/config contracts, feature order, category levels,
selected objective, evaluation results, and model version.

`-PublishCanonical` moves validated staged Silver and Gold into their canonical paths
before optional training and scoring. The move is atomic on the local volume and removes
the replaced copy, so a successful run does not retain duplicate lake tables.

## Forecast Evaluation

The canonical seven-day protocol uses six non-overlapping 12-hour outer evaluation
blocks, nested objective selection, a 24-hour final holdout, and block bootstrap
intervals. Poisson, Tweedie, and L1 objectives share the same fixed manifest.

The current development LightGBM selected Tweedie and beat lag-1 on the opened holdout:

| Method | MASE | ND | msMAPE |
| --- | ---: | ---: | ---: |
| LightGBM | 0.9624 | 0.2941 | 0.4674 |
| Lag-1 | 1.1463 | 0.3674 | 0.5740 |

These results demonstrate the evaluation workflow. Seven days cannot establish broad
seasonal generalization, and the opened holdout must not be reused for model tuning.

## Local Platform

On Windows, map the Unicode repository path to an ASCII drive before Docker builds:

```powershell
$root=(Resolve-Path .).Path
subst X: $root
Set-Location X:\
docker compose -f infrastructure/docker-compose.yml up -d `
  postgres minio minio-init kafka spark-master spark-worker
docker compose -f infrastructure/docker-compose.yml up airflow-init
docker compose -f infrastructure/docker-compose.yml up -d `
  airflow-webserver airflow-scheduler
docker compose -f infrastructure/docker-compose.yml --profile app up -d api dashboard
```

Use Kafka for a bounded replay sample rather than replaying the entire 405M-row batch.
That demonstrates event IDs, deduplication, checkpointing, and aggregate reconciliation
without duplicating the batch workload.

Local endpoints:

- Airflow: `http://localhost:8080`
- Spark: `http://localhost:8081`
- MinIO: `http://localhost:9001`
- FastAPI: `http://localhost:8000/docs`
- Streamlit: `http://localhost:8501`

## Verification

```powershell
ruff check .
pytest
python -m pip check
docker compose -f infrastructure/docker-compose.yml config --quiet
```

Spark, Kafka, API, dashboard, and Delta tests are opt-in integration tests because they
require local services or Java:

```powershell
pytest --run-integration
pytest --run-kafka-integration
```

## Retention Policy

- Keep only the 168 canonical Bronze gzip files.
- Keep the trusted seven-day Silver, Gold, model, and serving artifacts locally.
- Keep compact hashes, validation reports, and evaluation summaries in Git.
- Keep raw data, Parquet/Delta tables, DuckDB files, model binaries, logs, checkpoints,
  and temporary Spark output out of Git.
- Create a new isolated staging directory for rebuilds; never overwrite trusted outputs
  before validation succeeds.

## Limitations

- The dataset covers one week and cannot characterize longer seasonal cycles.
- Results are research evidence, not production service guarantees.
- Wikimedia dumps provide hourly aggregates without user or session context.
- Sparse zero completion is bounded to the past-eligible modeling universe.
- The local platform uses one Kafka broker and one Spark worker and is not highly
  available.
