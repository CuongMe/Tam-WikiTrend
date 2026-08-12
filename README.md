# TAM-WikiTrend: Ongoing Wikimedia Demand Lakehouse Research

TAM-WikiTrend is an ongoing local data-engineering and applied-forecasting research
project built from official Wikimedia hourly pageview dumps. It provides a reproducible
path from immutable source data to validated Silver and Gold datasets, robust traffic
trend detection, anomaly alerts, one-hour-ahead forecasting, and research-oriented
ranking analysis.

The system is designed as a serious local lakehouse exercise rather than a production
traffic service. The current downstream contract-v2 Silver and Gold snapshot covers
83 UTC hours from `2026-08-01 00:00` through `2026-08-04 10:00`. Bronze acquisition is
ongoing. The committed acquisition plan covers 696 UTC hours from `2026-07-08 00:00`
through `2026-08-05 23:00`, which provides the multi-week window required by the
rolling-origin experiment and its final holdout.

## Project Status

| Area | Current state |
| --- | --- |
| Bronze acquisition | Ongoing and resumable; missing planned dumps are downloaded incrementally. |
| Silver contract-v2 | Implemented and validated on the initial 83-hour snapshot. |
| Gold contract-v2 | Implemented and validated on the initial 83-hour snapshot. |
| Trend and anomaly logic | Robust median/MAD scoring is implemented in the canonical Gold build. |
| Forecast model | LightGBM training and scoring pipeline is ready; a compatible contract-v2 model is not yet published. |
| Model evaluation | Fixed nested rolling-origin protocol, untouched holdout, and block-bootstrap reporting are implemented. |
| Platform path | Airflow, MinIO/Delta, Kafka, Structured Streaming, DuckDB, FastAPI, and Streamlit paths are scaffolded and tested where local services are available. |

The model is intentionally not trained from the incomplete multi-week window. Existing
83-hour model artifacts are not considered compatible with the corrected project/access
contract and zero-complete modeling series.

## Research Objective

The project investigates whether a global LightGBM model can improve one-hour-ahead
pageview forecasting and trend discovery over simple historical baselines across
Wikipedia, Wikimedia Commons, and Wikidata access streams.

The evaluation separates two objectives:

- Traffic forecasting: MASE, normalized deviation (ND), msMAPE, and forecast coverage.
- Trend discovery: NDCG@K, Recall@K, top-K Jaccard overlap, and Spearman rank correlation.

All model comparisons use the same fixed nested rolling-origin manifest. Objective
selection is performed only inside development folds. The final multi-day holdout is
untouched until the model and feature decisions are frozen. Confidence intervals use
paired whole-UTC-day block bootstrap samples.

## Architecture

```text
Official Wikimedia hourly dumps
        |
        v
Bronze: immutable .gz files + SHA-256 manifest
        |
        v
Silver: parsed rows, canonical dimensions, deduplication, quarantine
        |
        v
Gold: page-hour facts, completed time series, trends, anomalies, features
        |
        +--> LightGBM training, evaluation, and next-hour scoring
        |
        v
Airflow validation and atomic publication
        |
        +--> Delta tables in MinIO
        +--> DuckDB serving snapshot
        +--> FastAPI and Streamlit research views

Kafka replay --> Structured Streaming --> idempotent Delta event and aggregate tables
```

The intended platform stack is:

| Layer | Technology | Purpose |
| --- | --- | --- |
| Orchestration | Apache Airflow | Run acquisition, parsing, validation, publication, and serving tasks. |
| Batch processing | PySpark | Parse large compressed dumps and construct Silver/Gold tables. |
| Storage | Parquet and Delta Lake | Reproducible local datasets and versioned publication snapshots. |
| Object storage | MinIO | Local S3-compatible storage for published Delta data. |
| Event transport | Kafka | Deterministic replay of pageview events. |
| Streaming | Spark Structured Streaming | Deduplicate events and merge affected aggregates. |
| Analytical serving | DuckDB | Atomic local serving database for research outputs. |
| API/UI | FastAPI and Streamlit | Query and inspect predictions, trends, and evaluations. |
| Quality | Native validators and pytest | Enforce data contracts, manifests, metrics, and integration behavior. |

## Data Contract

Wikimedia dump codes are transport identifiers. Silver maps them to explicit analytical
dimensions and retains both the source code and canonical dimensions:

| Source code | Canonical project | Project family | Access mode |
| --- | --- | --- | --- |
| `en` | `en` | `wikipedia` | desktop |
| `en.m` | `en` | `wikipedia` | mobile |
| `vi` | `vi` | `wikipedia` | desktop |
| `vi.m` | `vi` | `wikipedia` | mobile |
| `commons.m` | `commons` | `commons` | desktop |
| `commons.m.m` | `commons` | `commons` | mobile |
| `www.wd` | `wikidata` | `wikidata` | desktop |

`www.wd.m` is parser-supported but optional. Validation fails when any required source
project is absent for an ingested UTC hour. Silver retains valid rows with missing or
non-useful title-normalization status; Gold applies past-only eligibility rules so
current or future traffic cannot determine model inclusion.

## Repository Layout

| Path | Responsibility |
| --- | --- |
| `scripts/download_pageviews.py` | Resumable Bronze acquisition with gzip, size, hash, and manifest checks. |
| `spark_jobs/parse_pageviews.py` | Canonical Silver parsing, normalization, deduplication, and quarantine. |
| `scripts/validate_silver.py` | Silver schema, lineage, coverage, partition, and duplicate validation. |
| `spark_jobs/build_gold_tables.py` | Single supported Gold construction path. |
| `scripts/validate_gold.py` | Gold reconciliation, completeness, eligibility, feature, and metric validation. |
| `spark_jobs/train_lightgbm_nested.py` | Nested rolling-origin LightGBM training and objective selection. |
| `spark_jobs/score_lightgbm.py` | Versioned model loading, next-hour predictions, ranking, and fallback logic. |
| `spark_jobs/forecast_evaluation.py` | Shared fold, cohort, feature, and Spark evaluation helpers. |
| `configs/` | Acquisition plan, experiment protocol, and fixed fold manifest. |
| `artifacts/` | Committed Bronze and validated Silver/Gold snapshot evidence. |
| `notebooks/` | Raw, Silver, Gold, prediction, and model inspection notebooks. |
| `airflow/`, `infrastructure/`, `streaming/` | Orchestration, local services, Kafka replay, and Structured Streaming. |
| `tests/` | Unit, notebook, Spark, API, and optional Kafka integration tests. |

## Environment

The supported local runtime is Python 3.11, Java 17, Spark 4.0.1, pandas 2.2.3, NumPy
2.2.6, and LightGBM 4.5.0. Java 26 and Python 3.13 are outside this project contract.
The current laptop environment should be rebuilt if it does not match these versions.

```powershell
conda env update -n wikitrend -f environment.yml --prune
conda activate wikitrend
java -version
python --version
python -c "import pyspark, pandas, numpy; print(pyspark.__version__, pandas.__version__, numpy.__version__)"
```

Dependency declarations are kept in `pyproject.toml`. Runtime pins are in
`requirements.lock`; development pins are in `requirements-dev.lock`. CI checks that
the direct declarations and locks agree, then runs `pip check`.

## Acquire Bronze Data

Inspect the committed multi-week plan without making network requests:

```powershell
python scripts/download_pageviews.py `
  --plan configs/pageview_download_plan.json `
  --dry-run
```

Start or resume acquisition intentionally:

```powershell
python scripts/download_pageviews.py `
  --plan configs/pageview_download_plan.json
```

The downloader is resumable at the file level. Completed `.gz` files are skipped,
the manifest is preserved, and an interrupted `.part` file is retried. It does not
resume bytes within a partial file. Do not use `--overwrite` during normal acquisition.

Bronze files are immutable source inputs. Build or refresh a local inventory with:

```powershell
python scripts/build_bronze_manifest.py `
  --output artifacts/manifests/bronze_snapshot.json
```

Add `--verify-gzip` when a full decompression and CRC audit is required.

## Build And Validate Silver/Gold

Use a new staging root for every rebuild. Do not overwrite trusted Silver or Gold in
place. The canonical Gold builder writes all related tables together:

```powershell
$run = "contract-v2"
$sources = "en,en.m,vi,vi.m,commons.m,commons.m.m,www.wd"

python spark_jobs/parse_pageviews.py `
  --input data/raw/pageviews `
  --output "data/staging/$run/silver/pageviews" `
  --project-allowlist $sources `
  --quarantine-output "data/staging/$run/quarantine/pageviews" `
  --rejection-summary-output "data/staging/$run/quarantine/pageviews_rejection_summary"

python scripts/validate_silver.py `
  --raw-dir data/raw/pageviews `
  --silver-dir "data/staging/$run/silver/pageviews" `
  --quarantine-dir "data/staging/$run/quarantine/pageviews" `
  --rejection-summary-dir "data/staging/$run/quarantine/pageviews_rejection_summary" `
  --project-allowlist $sources

python spark_jobs/build_gold_tables.py `
  --silver "data/staging/$run/silver/pageviews" `
  --gold "data/staging/$run/gold"

python scripts/validate_gold.py `
  --gold-dir "data/staging/$run/gold"
```

Publish a staged snapshot only after both validators pass. Individual trend, anomaly,
feature, or evaluation tables must not be rebuilt directly over trusted Gold.

## Forecast Workflow

After the planned Bronze window is complete and Silver/Gold have been rebuilt:

```powershell
python scripts/generate_forecast_manifest.py
python scripts/build_snapshot_manifest.py
python spark_jobs/train_lightgbm_nested.py
```

The training job records the dataset snapshot, code/config contracts, feature order,
category levels, selected objective, fold results, holdout results, bootstrap intervals,
and model version. Scoring requires the versioned model pointer and exact feature
metadata:

```powershell
python spark_jobs/score_lightgbm.py `
  --top-n 100 `
  --ranking-cutoffs 10,50,100
python scripts/publish_serving_db.py
```

LightGBM candidates are evaluated through nested rolling validation using Poisson,
Tweedie, and L1 objectives. The `lag_1h_views` forecast is retained as the operational
fallback when features are unavailable or the model quality gate is not satisfied.

## Local Platform

Validate the Compose file first:

```powershell
docker compose -f infrastructure/docker-compose.yml config --quiet
```

Start the core services:

```powershell
docker compose -f infrastructure/docker-compose.yml up -d `
  postgres minio minio-init kafka spark-master spark-worker
docker compose -f infrastructure/docker-compose.yml up airflow-init
docker compose -f infrastructure/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

Optional research application and streaming profiles:

```powershell
docker compose -f infrastructure/docker-compose.yml --profile app up -d api dashboard
docker compose -f infrastructure/docker-compose.yml --profile stream up structured-streaming
docker compose -f infrastructure/docker-compose.yml --profile stream run --rm kafka-replay
```

Local endpoints are Airflow at `http://localhost:8080`, MinIO at
`http://localhost:9001`, FastAPI at `http://localhost:8000`, and Streamlit at
`http://localhost:8501`.

The local topology uses one Kafka broker and one Spark worker. It demonstrates data
contracts, deterministic replay, idempotent merges, and serving behavior; it is not an
HA production deployment.

## Verification

```powershell
ruff check .
pytest
python -m pip check
pytest --run-integration `
  tests/integration/test_gold_contract.py `
  tests/integration/test_api_serving.py
docker compose -f infrastructure/docker-compose.yml config --quiet
```

The Kafka transport test requires a running broker and is enabled separately:

```powershell
pytest --run-integration --run-kafka-integration `
  tests/integration/test_kafka_transport.py
```

Operational recovery rules are documented in [docs/runbook.md](docs/runbook.md).
Definitions and field-level semantics are documented in
[docs/data_dictionary.md](docs/data_dictionary.md). The research evaluation design is
documented in [docs/forecast_evaluation_protocol.md](docs/forecast_evaluation_protocol.md).
