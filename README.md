# WikiTrend

WikiTrend is a Python 3.11 project for working with Wikimedia hourly pageview
dumps locally. The repository currently implements bounded Bronze acquisition,
Bronze-to-Silver pageview processing, Silver validation and cleanup, compact
Gold aggregate table building, Gold-backed DuckDB serving views, local MinIO
object-store configuration, pageview parsing helpers, configuration helpers,
storage path utilities, and unit tests for the implemented behavior.

The `paper/` directory is reserved for the project paper, written in LaTeX.

The project metadata also pins the intended local analytics stack in
`pyproject.toml` and `requirements.in`, including PySpark, Delta Lake, DuckDB,
Kafka client support, FastAPI, Streamlit, LightGBM, Prophet, pandas, NumPy, and
PyArrow. Streaming and forecasting layers are not currently implemented as
package modules in this repository.

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
| Serving database path | `data/processed/serving/wikitrend.duckdb` |
| MinIO endpoint | `http://localhost:9000` |
| MinIO bucket | `wikitrend` |
| Hash algorithm | SHA-256 |

Supported Wikimedia source project codes are defined in
`src/wikitrend/pageviews.py`: `en`, `en.m`, `vi`, `vi.m`, `commons.m`,
`commons.m.m`, `www.wd`, and `www.wd.m`. The default allowlist excludes
`www.wd.m`.

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
|       |-- gold/
|       |-- quarantine/
|       `-- silver/
|-- notebooks/
|   |-- inspect_bronze_data.ipynb
|   |-- inspect_gold_data.ipynb
|   `-- inspect_silver_data.ipynb
|-- paper/
|   |-- arxiv-style-LICENSE.txt
|   |-- arxiv.sty
|   |-- main.tex
|   `-- research_questions/
|       `-- research_questions.md
|-- src/
|   `-- wikitrend/
|       |-- __init__.py
|       |-- app.py
|       |-- cli/
|       |   |-- build_gold_pageviews.py
|       |   |-- build_serving_db.py
|       |   |-- build_silver_pageviews.py
|       |   |-- download_pageviews.py
|       |   |-- validate_gold_pageviews.py
|       |   `-- validate_silver_pageviews.py
|       |-- config.py
|       |-- gold.py
|       |-- gold_validation.py
|       |-- logging_utils.py
|       |-- pageviews.py
|       |-- serving.py
|       |-- silver.py
|       |-- silver_validation.py
|       `-- storage.py
|-- tests/
|   |-- conftest.py
|   |-- test_download_pageviews.py
|   |-- test_silver.py
|   |-- test_gold.py
|   |-- test_gold_validation.py
|   |-- test_serving.py
|   |-- test_silver_validation.py
|   `-- test_storage.py
|-- docker-compose.minio.yml
|-- pyproject.toml
|-- requirements.in
`-- requirements-dev.in
```

## Local MinIO Object Store

Start the local S3-compatible object store:

```bash
docker compose -f docker-compose.minio.yml up -d
```

MinIO endpoints:

| Service | URL |
| --- | --- |
| S3 API | `http://localhost:9000` |
| Console | `http://localhost:9001` |

Default local credentials are defined in `.env.example`:

| Field | Value |
| --- | --- |
| Access key | `wikitrend` |
| Secret key | `wikitrend-local-password` |
| Bucket | `wikitrend` |

The `minio-init` service creates the bucket and these lakehouse prefixes:

```text
s3://wikitrend/bronze/pageviews/
s3://wikitrend/silver/pageviews/
s3://wikitrend/gold/pageviews/
s3://wikitrend/validation/
s3://wikitrend/serving/
```

This does not migrate the current local filesystem data yet. It establishes the
object-store target for the upcoming Delta Lake and Spark work.

Stop MinIO with:

```bash
docker compose -f docker-compose.minio.yml down
```

## Setup

Create and activate a Python 3.11 environment, then install the pinned runtime
and development dependencies:

```bash
pip install -r requirements.in
pip install -r requirements-dev.in
```

The package can also be installed in editable mode with development extras:

```bash
pip install -e ".[dev]"
```

## Download Pageviews

Inspect the configured acquisition plan without downloading files:

```bash
python -m wikitrend.cli.download_pageviews --plan configs/pageview_download_plan.json --dry-run
```

Download or verify the files in the configured plan:

```bash
python -m wikitrend.cli.download_pageviews --plan configs/pageview_download_plan.json
```

The downloader:

- builds hourly Wikimedia pageview dump URLs,
- supports a plan-defined mirror list,
- skips existing non-empty files quickly when they match the manifest,
- supports `--verify-existing` when you want to re-hash existing Bronze files,
- validates downloaded gzip files,
- writes a SHA-256 manifest,
- trims manifest entries outside the active plan,
- bounds workers, attempts, and socket timeout values.

When a plan is supplied, the acquisition scope comes from the plan file. Command
line overrides for start date, end date, output directory, and hour subset are
rejected so the versioned plan remains the source of truth.

## Build Silver Pageviews

Inspect the Bronze-to-Silver plan without writing data:

```bash
python -m wikitrend.cli.build_silver_pageviews --dry-run
```

Build the Silver Parquet layer:

```bash
python -m wikitrend.cli.build_silver_pageviews
```

The Silver builder reads only files listed in the Bronze manifest, parses valid
Wikimedia pageview rows, writes partitioned Parquet to
`data/processed/silver/pageviews`, and writes malformed or out-of-scope rows to
`data/processed/quarantine/pageviews`.

The default builder engine is `python`, which uses PyArrow and avoids local
Spark/Hadoop setup on Windows. Use `--engine spark` only when Spark, Java, and
the Windows Hadoop utilities are configured.

By default, the builder refuses to write if either output already contains files.
This prevents accidentally appending or duplicating Silver data. Use
`--overwrite` only when you intentionally want to replace the existing processed
outputs.

## Validate Silver Pageviews

Run the structural validation gate:

```bash
python -m wikitrend.cli.validate_silver_pageviews \
  --report data/processed/validation/silver_pageviews_validation.json
```

Run the full validation gate before building Gold:

```bash
python -m wikitrend.cli.validate_silver_pageviews \
  --full-scan \
  --report data/processed/validation/silver_pageviews_validation.json
```

The validator checks manifest hour coverage, project/access partitions, required
schema columns, null title fields, and negative metrics. It can also remove
Spark sidecar files such as `.crc` files:

```bash
python -m wikitrend.cli.validate_silver_pageviews --clean-sidecars
```

It does not rewrite or duplicate Silver Parquet data.

## Build Gold Pageviews

Inspect the Silver-to-Gold plan without writing data:

```bash
python -m wikitrend.cli.build_gold_pageviews --dry-run
```

Build the compact Gold aggregate layer:

```bash
python -m wikitrend.cli.build_gold_pageviews
```

The Gold builder reads Silver Parquet data and writes small aggregate tables to
`data/processed/gold`:

- `hourly_project_access`: one row per date, hour, project, and access mode.
- `daily_project_access`: one row per date, project, and access mode.
- `top_pages_hourly`: top pages per date, hour, project, and access mode.

Gold is intentionally aggregated so it does not duplicate Silver row-level data.
The build writes `data/processed/gold/gold_manifest.json` with table grains and
row counts. Use `--overwrite` only when intentionally replacing Gold outputs.

Inspect the Gold layer with the read-only EDA notebook:

```bash
jupyter lab notebooks/inspect_gold_data.ipynb
```

## Validate Gold Pageviews

Run the Gold aggregate validation gate:

```bash
python -m wikitrend.cli.validate_gold_pageviews \
  --silver-validation-report data/processed/validation/silver_pageviews_validation.json \
  --report data/processed/validation/gold_pageviews_validation.json
```

The Gold validator checks manifest row counts, required schemas, table grains,
metric domains, hourly-to-daily reconciliation, top-page rank rules, and optional
sidecar cleanup candidates.

## Build Serving DuckDB

Build a compact DuckDB database over validated Gold Parquet:

```bash
python -m wikitrend.cli.build_serving_db
```

The serving database writes DuckDB views under the `gold` schema:

- `gold.hourly_project_access`
- `gold.daily_project_access`
- `gold.top_pages_hourly`

The database also stores small metadata tables under the `metadata` schema. It
does not copy Silver data or duplicate Gold row payloads into DuckDB tables.
By default, the builder requires
`data/processed/validation/gold_pageviews_validation.json` to exist with
`status=pass`, and it refuses to overwrite an existing database unless
`--overwrite` is supplied.

## Run Dashboard

Start the Streamlit dashboard from the DuckDB serving database:

```bash
streamlit run src/wikitrend/app.py
```

The dashboard reads `data/processed/serving/wikitrend.duckdb` in read-only mode
and uses the passing Gold validation report as its quality context.

## Configuration

Default settings come from `src/wikitrend/config.py` and can be overridden with
environment variables:

| Variable | Default |
| --- | --- |
| `WIKITREND_ENV` | `local` |
| `WIKITREND_START_DATE` | `2026-01-01` |
| `WIKITREND_END_DATE` | `2026-01-03` |
| `WIKITREND_SOURCE_PROJECT_ALLOWLIST` | default source project allowlist |
| `WIKITREND_RAW_DIR` | `data/raw/pageviews` |
| `WIKITREND_SILVER_DIR` | `data/processed/silver/pageviews` |
| `WIKITREND_GOLD_DIR` | `data/processed/gold` |
| `WIKITREND_SERVING_DB` | `data/processed/serving/wikitrend.duckdb` |
| `WIKITREND_S3_ENDPOINT_URL` | `http://localhost:9000` |
| `WIKITREND_S3_REGION` | `us-east-1` |
| `WIKITREND_S3_BUCKET` | `wikitrend` |
| `WIKITREND_S3_ACCESS_KEY_ID` | `wikitrend` |
| `WIKITREND_S3_SECRET_ACCESS_KEY` | `wikitrend-local-password` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` |
| `KAFKA_PAGEVIEWS_TOPIC` | `wikitrend.pageviews` |

## Testing

Run the current test suite with:

```bash
pytest
```

The declared pytest markers are:

- `integration`: local service or Spark integration coverage.
- `kafka_integration`: requires a Kafka broker on `localhost:9094`.

The checked-in tests currently cover downloader planning, retries, manifest
scoping, gzip handling, mirror URL validation, bounded configuration values,
Silver output guardrails, Silver validation helpers, Gold aggregate builders,
Gold validation, DuckDB serving database creation, and MinIO configuration defaults.
