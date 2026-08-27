# WikiTrend

WikiTrend is a local Python 3.11 data engineering project for Wikimedia hourly pageview data. It implements a bounded batch lakehouse that downloads raw Wikimedia dumps, cleans them into Silver Parquet, builds compact Gold analytics tables, trains baseline forecasting models, and serves the results through DuckDB, Streamlit, FastAPI, Delta Lake, and optional Apache Airflow orchestration.

The project is intentionally scoped to a small 72-hour dataset so it can run on a local machine without Docker or cloud storage.

## What This Project Builds

| Layer | Purpose | Output |
| --- | --- | --- |
| Bronze | Download official Wikimedia hourly gzip dumps and track file hashes. | `data/raw/pageviews` |
| Silver | Parse, clean, validate, and partition row-level pageview records. | `data/processed/silver/pageviews` |
| Quarantine | Store malformed or unsupported rows for inspection. | `data/processed/quarantine/pageviews` |
| Gold | Build compact analytical aggregates. | `data/processed/gold` |
| Forecast | Compare time-series models and generate future forecasts. | `data/processed/forecast` |
| Delta | Convert compact Gold tables to local Delta Lake format. | `data/processed/delta` |
| Serving | Expose Gold and forecast outputs as DuckDB views. | `data/processed/serving/wikitrend.duckdb` |
| Dashboard | Explore trends, top pages, quality, and forecasts. | `src/wikitrend/app.py` |
| API | Read Gold analytics from FastAPI. | `src/wikitrend/api.py` |
| Orchestration | Run the batch pipeline through Airflow. | `airflow/dags/wikitrend_batch_lakehouse.py` |

## Current Scope

| Item | Current value |
| --- | --- |
| Python | `>=3.11,<3.12` |
| Data window | `2026-01-01` through `2026-01-03` UTC |
| Expected Bronze files | `72` hourly files |
| Acquisition plan | `configs/pageview_download_plan.json` |
| Source allowlist | `en`, `en.m`, `vi`, `vi.m`, `commons.m`, `commons.m.m`, `www.wd` |
| Hash algorithm | SHA-256 |
| Forecast models | seasonal naive, Ridge, ElasticNet, LightGBM |
| Forecast evaluation | rolling-window backtest with median-grounded metrics |
| Orchestration | optional local Airflow, manual DAG schedule |

Supported Wikimedia source project codes are defined in `src/wikitrend/pageviews.py`. The configured default excludes `www.wd.m` to keep the local data footprint bounded.

## Architecture

```text
Wikimedia hourly dumps
        |
        v
Bronze raw gzip + manifest
        |
        v
Silver Parquet + quarantine + validation
        |
        v
Gold aggregate Parquet + validation
        |
        +--> Forecast features, backtests, metrics, future forecasts
        |
        +--> Delta Lake copies of Gold tables
        |
        v
DuckDB serving database views
        |
        +--> Streamlit dashboard
        +--> FastAPI read API
        +--> Airflow batch orchestration
```

## Repository Layout

```text
.
|-- .env.example
|-- .streamlit/
|   `-- config.toml
|-- airflow/
|   `-- dags/
|       `-- wikitrend_batch_lakehouse.py
|-- configs/
|   `-- pageview_download_plan.json
|-- notebooks/
|   |-- analyze_gold_trends.ipynb
|   |-- inspect_bronze_data.ipynb
|   |-- inspect_forecast_data.ipynb
|   |-- inspect_gold_data.ipynb
|   `-- inspect_silver_data.ipynb
|-- src/
|   `-- wikitrend/
|       |-- app.py
|       |-- api.py
|       |-- cli/
|       |   |-- build_delta_lake.py
|       |   |-- build_forecast_pageviews.py
|       |   |-- build_gold_pageviews.py
|       |   |-- build_serving_db.py
|       |   |-- build_silver_pageviews.py
|       |   |-- download_pageviews.py
|       |   |-- validate_gold_pageviews.py
|       |   `-- validate_silver_pageviews.py
|       |-- config.py
|       |-- delta_lake.py
|       |-- forecasting.py
|       |-- gold.py
|       |-- gold_validation.py
|       |-- pageviews.py
|       |-- serving.py
|       |-- silver.py
|       |-- silver_validation.py
|       `-- storage.py
|-- tests/
|-- pyproject.toml
|-- requirements-airflow.in
|-- requirements-dev.in
`-- requirements.in
```

Local data, databases, logs, and runtime artifacts are intentionally ignored by git.

## Setup

Create and activate a Python 3.11 environment. Then install the pinned project dependencies:

```bash
pip install -r requirements.in
pip install -r requirements-dev.in
pip install -e ".[dev]"
```

For the existing Windows conda environment used during development:

```powershell
& D:\Anaconda\envs\wikitrend\python.exe -m pip install -r requirements.in
& D:\Anaconda\envs\wikitrend\python.exe -m pip install -r requirements-dev.in
& D:\Anaconda\envs\wikitrend\python.exe -m pip install -e ".[dev]"
```

Airflow is optional and should be installed separately from the core project environment:

```bash
pip install -r requirements-airflow.in
```

For this project, Airflow is intended to run from Linux or WSL.

## End-to-End Pipeline

Run these commands from the repository root.

### 1. Download Bronze

```bash
python -m wikitrend.cli.download_pageviews --plan configs/pageview_download_plan.json
```

### 2. Build Silver

```bash
python -m wikitrend.cli.build_silver_pageviews
```

### 3. Validate Silver

```bash
python -m wikitrend.cli.validate_silver_pageviews --full-scan --report data/processed/validation/silver_pageviews_validation.json
```

### 4. Build Gold

```bash
python -m wikitrend.cli.build_gold_pageviews
```

### 5. Validate Gold

```bash
python -m wikitrend.cli.validate_gold_pageviews --silver-validation-report data/processed/validation/silver_pageviews_validation.json --report data/processed/validation/gold_pageviews_validation.json
```

### 6. Build Forecasts

```bash
python -m wikitrend.cli.build_forecast_pageviews
```

The forecasting layer compares:

| Model | Role |
| --- | --- |
| `seasonal_naive_24h` | Baseline using the same hour from the previous day. |
| `ridge_lag` | Linear lag model with scaled features. |
| `elasticnet_lag` | Regularized linear lag model with feature selection pressure. |
| `lightgbm_lag` | Tree-based lag model for nonlinear relationships. |

Forecasting uses rolling-window splitting, normalized lag and rolling median features, and a transformed target: `log1p(y / rolling_median_positive_series_scale)`.

Forecast evaluation uses median-grounded metrics:

| Metric | Meaning |
| --- | --- |
| `mdae` | Median absolute error in pageview units. |
| `mase` | Median absolute scaled error using a seasonal training-window scale. |
| `rmase` | Relative MASE versus `seasonal_naive_24h`; values below `1.0` beat naive. |
| `mdape` | Median absolute percentage error. |
| `mdsmape` | Median symmetric absolute percentage error. |

### 7. Build Delta Lake

```bash
python -m wikitrend.cli.build_delta_lake
```

### 8. Build Serving Database

```bash
python -m wikitrend.cli.build_serving_db
```

Use `--dry-run` to inspect a command without writing outputs. Use `--overwrite` only when intentionally replacing existing generated data.

## Data Contracts

### Gold Tables

| Table | Grain |
| --- | --- |
| `gold.hourly_project_access` | date, hour, project, access mode |
| `gold.daily_project_access` | date, project, access mode |
| `gold.top_pages_hourly` | date, hour, project, access mode, rank |

### Forecast Tables

| Table | Grain |
| --- | --- |
| `forecast.forecast_metrics` | model, project, access mode |
| `forecast.forecast_backtest_predictions` | fold, horizon step, hour, model, project, access mode |
| `forecast.forecast_future` | generated timestamp, horizon step, hour, model, project, access mode |

DuckDB serves these as views over Parquet files, so the serving layer does not duplicate Silver, Gold, or forecast data.

## Dashboard

Start the Streamlit dashboard:

```bash
streamlit run src/wikitrend/app.py
```

The dashboard includes:

| Page | What it shows |
| --- | --- |
| Overview | Total demand, hourly trend, and segment contribution. |
| Segments | Project/access-mode comparisons and volatility. |
| Top Pages | High-demand pages and concentration by rank. |
| Forecasting | Model leaderboard by `rmase`, beat-naive highlighting, backtest chart, and future forecast chart. |
| Quality | Validation status, table inventory, and serving metadata. |

## FastAPI

Start the API:

```bash
uvicorn wikitrend.api:app --reload
```

Open the docs at:

```text
http://127.0.0.1:8000/docs
```

Current API endpoints expose health, quality, metadata, project summaries, hourly trends, and top pages.

## Airflow

The Airflow DAG is `wikitrend_batch_lakehouse`. It orchestrates:

```text
download_bronze
  -> build_silver
  -> validate_silver
  -> build_gold
  -> validate_gold
  -> build_forecast
  -> build_delta
  -> build_serving_db
```

Set up Airflow with a repo-local home:

```bash
export AIRFLOW_HOME="$PWD/airflow"
export WIKITREND_AIRFLOW_PROJECT_DIR="$PWD"
export WIKITREND_AIRFLOW_PYTHON="$(which python)"
```

Initialize Airflow and create a local admin user:

```bash
airflow db migrate
airflow users create \
  --username admin \
  --firstname Wiki \
  --lastname Trend \
  --role Admin \
  --email admin@example.com \
  --password admin
```

Run the scheduler and webserver in separate terminals:

```bash
airflow scheduler
airflow webserver --port 8080
```

The DAG is manual by default. Trigger a full rebuild with this DAG config:

```json
{"overwrite": true}
```

## Notebooks

Read-only inspection and analysis notebooks live in `notebooks/`:

| Notebook | Purpose |
| --- | --- |
| `inspect_bronze_data.ipynb` | Inspect raw downloads and manifest coverage. |
| `inspect_silver_data.ipynb` | Validate parsed row-level Silver data. |
| `inspect_gold_data.ipynb` | Inspect Gold aggregate tables. |
| `inspect_forecast_data.ipynb` | Review forecast features, metrics, backtests, and future predictions. |
| `analyze_gold_trends.ipynb` | Analyze demand patterns from Gold tables. |

## Configuration

Default settings come from `src/wikitrend/config.py` and can be overridden with environment variables.

| Variable | Default |
| --- | --- |
| `WIKITREND_ENV` | `local` |
| `WIKITREND_START_DATE` | `2026-01-01` |
| `WIKITREND_END_DATE` | `2026-01-03` |
| `WIKITREND_SOURCE_PROJECT_ALLOWLIST` | default source project allowlist |
| `WIKITREND_RAW_DIR` | `data/raw/pageviews` |
| `WIKITREND_SILVER_DIR` | `data/processed/silver/pageviews` |
| `WIKITREND_GOLD_DIR` | `data/processed/gold` |
| `WIKITREND_FORECAST_DIR` | `data/processed/forecast` |
| `WIKITREND_DELTA_DIR` | `data/processed/delta` |
| `WIKITREND_SERVING_DB` | `data/processed/serving/wikitrend.duckdb` |
| `WIKITREND_GOLD_VALIDATION_REPORT` | `data/processed/validation/gold_pageviews_validation.json` |
| `WIKITREND_AIRFLOW_PROJECT_DIR` | current repository directory |
| `WIKITREND_AIRFLOW_PYTHON` | `python` |

## Testing

Run the full test suite:

```bash
pytest
```

Run linting:

```bash
ruff check .
```

The tests currently cover downloader planning, retries, manifest scoping, gzip handling, mirror URL validation, bounded configuration values, Silver build guardrails, Silver validation, Gold aggregation, Gold validation, forecasting model comparison, Delta conversion, DuckDB serving views, FastAPI endpoints, Airflow DAG structure, and local configuration defaults.

## Current Limitations

| Area | Limitation |
| --- | --- |
| Forecast horizon | The model is trained on only 72 hours, so the seasonal naive baseline is difficult to beat consistently. |
| API coverage | FastAPI currently exposes Gold analytics, but forecast endpoints are not yet exposed. |
| Delta coverage | Delta conversion currently covers compact Gold tables, not forecast tables. |
| Airflow runtime | Airflow is configured for local Linux/WSL usage, not Docker. |

## Recommended Next Improvements

1. Add forecast endpoints to FastAPI.
2. Add strict validation for forecast output completeness before serving DB rebuilds.
3. Add optional Bronze checksum verification before Silver processing.
4. Add Delta conversion for forecast tables if forecasts become a first-class data product.
5. Add a stronger Airflow DAG parse test using `DagBag`.