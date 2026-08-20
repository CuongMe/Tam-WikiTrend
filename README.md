# WikiTrend

WikiTrend is a Python 3.11 project for working with Wikimedia hourly pageview
dumps locally. The repository currently implements the bounded Bronze acquisition
step, pageview parsing helpers, configuration helpers, storage path utilities,
and unit tests for the downloader behavior.

The formatted documentation site uses Material for MkDocs. Preview it locally
with `mkdocs serve`.

The project metadata also pins the intended local analytics stack in
`pyproject.toml` and `requirements.in`, including PySpark, Delta Lake, DuckDB,
Kafka client support, FastAPI, Streamlit, LightGBM, Prophet, pandas, NumPy, and
PyArrow. Those higher-level processing and serving layers are not currently
implemented as package modules in this repository.

## Current Scope

| Item | Current value |
| --- | --- |
| Python version | `>=3.11,<3.12` |
| Package layout | `src/wikitrend` |
| Acquisition plan | `configs/pageview_download_plan.json` |
| Planned window | `2026-01-01` through `2026-01-07` UTC |
| Expected hourly files | `168` |
| Raw output path | `data/raw/pageviews` |
| Manifest path | `data/raw/pageviews_manifest.json` |
| Processed output root | `data/processed` |
| Hash algorithm | SHA-256 |

Supported Wikimedia source project codes are defined in
`src/wikitrend/pageviews.py`: `en`, `en.m`, `vi`, `vi.m`, `commons.m`,
`commons.m.m`, `www.wd`, and `www.wd.m`. The default allowlist excludes
`www.wd.m`.

## Repository Layout

```text
.
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

Preview the Material documentation site:

```bash
mkdocs serve
```

## Download Pageviews

Inspect the configured acquisition plan without downloading files:

```bash
python scripts/download_pageviews.py --plan configs/pageview_download_plan.json --dry-run
```

Download or verify the files in the configured plan:

```bash
python scripts/download_pageviews.py --plan configs/pageview_download_plan.json
```

The downloader:

- builds hourly Wikimedia pageview dump URLs,
- supports a plan-defined mirror list,
- skips existing non-empty files after hashing them,
- validates downloaded gzip files,
- writes a SHA-256 manifest,
- trims manifest entries outside the active plan,
- bounds workers, attempts, and socket timeout values.

When a plan is supplied, the acquisition scope comes from the plan file. Command
line overrides for start date, end date, output directory, and hour subset are
rejected so the versioned plan remains the source of truth.

## Configuration

Default settings come from `src/wikitrend/config.py` and can be overridden with
environment variables:

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

## Testing

Run the current test suite with:

```bash
pytest
```

The declared pytest markers are:

- `integration`: local service or Spark integration coverage.
- `kafka_integration`: requires a Kafka broker on `localhost:9094`.

The checked-in tests currently cover downloader planning, retries, manifest
scoping, gzip handling, mirror URL validation, and bounded configuration values.
