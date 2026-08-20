# WikiTrend

WikiTrend is a Python 3.11 project for working with Wikimedia hourly pageview
dumps locally. The repository currently implements bounded Bronze acquisition,
Bronze-to-Silver pageview processing, Silver validation and cleanup, pageview
parsing helpers, configuration helpers, storage path utilities, and unit tests
for the implemented behavior.

The `paper/` directory is reserved for the project paper, written in LaTeX.

The project metadata also pins the intended local analytics stack in
`pyproject.toml` and `requirements.in`, including PySpark, Delta Lake, DuckDB,
Kafka client support, FastAPI, Streamlit, LightGBM, Prophet, pandas, NumPy, and
PyArrow. Gold processing, serving, streaming, and forecasting layers are not
currently implemented as package modules in this repository.

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
|-- notebooks/
|   |-- inspect_bronze_data.ipynb
|   `-- inspect_silver_data.ipynb
|-- paper/
|   |-- arxiv-style-LICENSE.txt
|   |-- arxiv.sty
|   `-- main.tex
|-- src/
|   `-- wikitrend/
|       |-- __init__.py
|       |-- cli/
|       |   |-- build_silver_pageviews.py
|       |   |-- download_pageviews.py
|       |   `-- validate_silver_pageviews.py
|       |-- config.py
|       |-- logging_utils.py
|       |-- pageviews.py
|       |-- silver.py
|       |-- silver_validation.py
|       `-- storage.py
|-- tests/
|   |-- conftest.py
|   |-- test_download_pageviews.py
|   |-- test_silver.py
|   `-- test_silver_validation.py
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
Silver output guardrails, and Silver validation helpers.
