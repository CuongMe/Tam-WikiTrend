# WikiTrend

WikiTrend is a Python 3.11 project for working with Wikimedia hourly pageview dumps locally. It implements a bounded batch lakehouse workflow: Bronze acquisition, Silver cleaning and validation, compact Gold aggregates, local Delta Lake conversion, a DuckDB serving database, a Streamlit dashboard, and a FastAPI read API.


## Current Scope

| Item | Current value |
| --- | --- |
| Python version | `>=3.11,<3.12` |
| Package layout | `src/wikitrend` |
| Acquisition plan | `configs/pageview_download_plan.json` |
| Planned window | `2026-01-01` through `2026-01-03` UTC |
| Expected hourly files | `72` |
| Raw output path | `data/raw/pageviews` |
| Manifest path | `data/raw/pageviews_manifest.json` |
| Silver output path | `data/processed/silver/pageviews` |
| Quarantine output path | `data/processed/quarantine/pageviews` |
| Silver validation report | `data/processed/validation/silver_pageviews_validation.json` |
| Gold output path | `data/processed/gold` |
| Gold validation report | `data/processed/validation/gold_pageviews_validation.json` |
| Delta output path | `data/processed/delta` |
| Serving database path | `data/processed/serving/wikitrend.duckdb` |
| Hash algorithm | SHA-256 |

Supported Wikimedia source project codes are defined in `src/wikitrend/pageviews.py`: `en`, `en.m`, `vi`, `vi.m`, `commons.m`, `commons.m.m`, `www.wd`, and `www.wd.m`. The default allowlist excludes `www.wd.m`.

## Repository Layout

```text
.
|-- .env.example
|-- .streamlit/
|   `-- config.toml
|-- configs/
|   `-- pageview_download_plan.json
|-- data/
|   |-- raw/
|   |   |-- pageviews/
|   |   `-- pageviews_manifest.json
|   `-- processed/
|       |-- delta/
|       |-- gold/
|       |-- quarantine/
|       |-- serving/
|       |-- silver/
|       `-- validation/
|-- notebooks/
|   |-- analyze_gold_trends.ipynb
|   |-- inspect_bronze_data.ipynb
|   |-- inspect_gold_data.ipynb
|   `-- inspect_silver_data.ipynb
|-- src/
|   `-- wikitrend/
|       |-- app.py
|       |-- api.py
|       |-- cli/
|       |   |-- build_delta_lake.py
|       |   |-- build_gold_pageviews.py
|       |   |-- build_serving_db.py
|       |   |-- build_silver_pageviews.py
|       |   |-- download_pageviews.py
|       |   |-- validate_gold_pageviews.py
|       |   `-- validate_silver_pageviews.py
|       |-- config.py
|       |-- delta_lake.py
|       |-- gold.py
|       |-- gold_validation.py
|       |-- pageviews.py
|       |-- serving.py
|       |-- silver.py
|       |-- silver_validation.py
|       `-- storage.py
|-- tests/
|-- pyproject.toml
|-- requirements.in
`-- requirements-dev.in
```

## Setup

Create and activate a Python 3.11 environment, then install the pinned runtime and development dependencies:

```bash
pip install -r requirements.in
pip install -r requirements-dev.in
```

The package can also be installed in editable mode with development extras:

```bash
pip install -e ".[dev]"
```

## Pipeline

Download or verify the Bronze files in the configured plan:

```bash
python -m wikitrend.cli.download_pageviews --plan configs/pageview_download_plan.json
```

Build Silver Parquet from Bronze:

```bash
python -m wikitrend.cli.build_silver_pageviews
```

Validate Silver before building Gold:

```bash
python -m wikitrend.cli.validate_silver_pageviews --full-scan --report data/processed/validation/silver_pageviews_validation.json
```

Build compact Gold aggregates:

```bash
python -m wikitrend.cli.build_gold_pageviews
```

Validate Gold:

```bash
python -m wikitrend.cli.validate_gold_pageviews --silver-validation-report data/processed/validation/silver_pageviews_validation.json --report data/processed/validation/gold_pageviews_validation.json
```

Build local Delta tables from compact Gold:

```bash
python -m wikitrend.cli.build_delta_lake
```

Build the DuckDB serving database:

```bash
python -m wikitrend.cli.build_serving_db
```

Use `--dry-run` on build commands to inspect plans without writing data. Use `--overwrite` only when intentionally replacing existing processed outputs.

## Data Layers

Bronze stores raw Wikimedia gzip files and a SHA-256 manifest. Silver stores cleaned row-level Parquet partitioned by date, hour, project, and access mode. Gold stores compact aggregate Parquet tables: `hourly_project_access`, `daily_project_access`, and `top_pages_hourly`. Delta stores local Delta Lake copies of the compact Gold tables under `data/processed/delta/gold`. DuckDB serves validated Gold through views and metadata tables without duplicating Silver data.

## Notebooks, Dashboard, And API

Read-only inspection notebooks live in `notebooks/`:

```bash
jupyter lab notebooks/inspect_bronze_data.ipynb
jupyter lab notebooks/inspect_silver_data.ipynb
jupyter lab notebooks/inspect_gold_data.ipynb
jupyter lab notebooks/analyze_gold_trends.ipynb
```

Start the Streamlit dashboard from the DuckDB serving database:

```bash
streamlit run src/wikitrend/app.py
```

Start the local FastAPI read API:

```bash
uvicorn wikitrend.api:app --reload
```

API docs are available at `http://127.0.0.1:8000/docs` when the API is running.

## Configuration

Default settings come from `src/wikitrend/config.py` and can be overridden with environment variables:

| Variable | Default |
| --- | --- |
| `WIKITREND_ENV` | `local` |
| `WIKITREND_START_DATE` | `2026-01-01` |
| `WIKITREND_END_DATE` | `2026-01-03` |
| `WIKITREND_SOURCE_PROJECT_ALLOWLIST` | default source project allowlist |
| `WIKITREND_RAW_DIR` | `data/raw/pageviews` |
| `WIKITREND_SILVER_DIR` | `data/processed/silver/pageviews` |
| `WIKITREND_GOLD_DIR` | `data/processed/gold` |
| `WIKITREND_DELTA_DIR` | `data/processed/delta` |
| `WIKITREND_SERVING_DB` | `data/processed/serving/wikitrend.duckdb` |
| `WIKITREND_GOLD_VALIDATION_REPORT` | `data/processed/validation/gold_pageviews_validation.json` |

## Testing

Run the current test suite with:

```bash
pytest
```

The checked-in tests cover downloader planning, retries, manifest scoping, gzip handling, mirror URL validation, bounded configuration values, Silver output guardrails, Silver validation helpers, Gold aggregate builders, Gold validation, local Delta Lake conversion, DuckDB serving database creation, FastAPI endpoints, and local configuration defaults.