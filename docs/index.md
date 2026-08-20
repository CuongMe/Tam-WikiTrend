# WikiTrend

WikiTrend is a local Python project for acquiring and preparing Wikimedia
hourly pageview dumps. The current repository focuses on the Bronze acquisition
step plus reusable utilities for pageview parsing, configuration, logging, and
storage paths.

!!! info "Current implementation"
    The checked-in package currently implements downloader and utility code.
    The pinned dependency stack includes PySpark, Delta Lake, DuckDB, Kafka,
    FastAPI, Streamlit, LightGBM, Prophet, pandas, NumPy, and PyArrow for later
    processing and serving work.

## Project Snapshot

| Area | Current state |
| --- | --- |
| Python | `>=3.11,<3.12` |
| Package layout | `src/wikitrend` |
| Acquisition plan | `configs/pageview_download_plan.json` |
| Planned window | `2026-01-01` through `2026-01-07` UTC |
| Expected hourly files | `168` |
| Raw data | `data/raw/pageviews` |
| Processed data | `data/processed` |
| Manifest | `data/raw/pageviews_manifest.json` |
| Hashing | SHA-256 |

## Data Layout

```text
data/
|-- raw/
|   |-- pageviews/
|   `-- pageviews_manifest.json
`-- processed/
    |-- gold/
    |-- quarantine/
    `-- silver/
```

!!! note "Repository policy"
    `data/` is ignored by Git. It is local runtime state, not source code.

## Repository Map

```text
.
|-- configs/
|   `-- pageview_download_plan.json
|-- docs/
|   `-- index.md
|-- notebooks/
|-- scripts/
|   `-- download_pageviews.py
|-- src/
|   `-- wikitrend/
|       |-- __init__.py
|       |-- config.py
|       |-- logging_utils.py
|       |-- pageviews.py
|       `-- storage.py
|-- tests/
|   |-- conftest.py
|   `-- test_download_pageviews.py
|-- mkdocs.yml
|-- pyproject.toml
|-- requirements.in
`-- requirements-dev.in
```

## Setup

=== "Runtime"

    ```bash
    pip install -r requirements.in
    ```

=== "Development"

    ```bash
    pip install -r requirements-dev.in
    ```

=== "Editable Package"

    ```bash
    pip install -e ".[dev]"
    ```

## Download Pageviews

Inspect the configured plan without network access:

```bash
python scripts/download_pageviews.py --plan configs/pageview_download_plan.json --dry-run
```

Run the bounded downloader:

```bash
python scripts/download_pageviews.py --plan configs/pageview_download_plan.json
```

The downloader:

- builds canonical hourly Wikimedia dump URLs,
- rotates through configured HTTPS mirrors,
- skips existing non-empty files after hashing them,
- validates downloaded gzip files,
- writes a SHA-256 Bronze manifest,
- removes manifest entries outside the active acquisition plan,
- bounds worker, retry, and timeout settings.

!!! warning "Plan immutability"
    When `--plan` is supplied, the plan controls the acquisition scope.
    Command-line overrides for start date, end date, output directory, and hour
    subset are rejected.

## Configuration

| Variable | Default |
| --- | --- |
| `WIKITREND_ENV` | `local` |
| `WIKITREND_START_DATE` | `2026-01-01` |
| `WIKITREND_END_DATE` | `2026-01-07` |
| `WIKITREND_SOURCE_PROJECT_ALLOWLIST` | default source project allowlist |
| `WIKITREND_RAW_DIR` | `data/raw/pageviews` |
| `WIKITREND_SILVER_DIR` | `data/processed/silver/pageviews` |
| `WIKITREND_GOLD_DIR` | `data/processed/gold` |
| `WIKITREND_SERVING_DB` | `data/processed/serving/wikitrend.duckdb` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` |
| `KAFKA_PAGEVIEWS_TOPIC` | `wikitrend.pageviews` |

## Tests

```bash
pytest
```

The current test suite covers downloader planning, retries, manifest scoping,
gzip handling, mirror URL validation, and bounded configuration values.

## Documentation

Preview this Material for MkDocs site locally:

```bash
mkdocs serve
```
