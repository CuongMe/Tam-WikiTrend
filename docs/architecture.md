# WikiTrend Architecture

## Goal

WikiTrend turns official Wikimedia hourly pageview dumps into a local demand
intelligence platform. The design emphasizes reproducibility, idempotent processing,
resource control, and analytics outputs that are useful to editorial, product, and
infrastructure teams.

## Data Flow

```text
Wikimedia hourly .gz dumps
  -> Bronze raw storage
  -> Silver parsed Parquet
  -> Gold analytics Parquet
  -> DuckDB, FastAPI, Streamlit
```

The streaming path replays Silver records into Kafka, consumes them with Spark
Structured Streaming, and writes streaming outputs to Gold-compatible Parquet.

## Layers

Bronze:

- Original `.gz` files.
- Date and hour partitioned local paths.
- Files are never fully uncompressed and retained.

Silver:

- Parsed and validated pageview rows.
- Partitioned by `date`, `hour`, and `project`.
- Corrupt rows are dropped by the parsing job and can be counted in a later audit job.

Gold:

- `page_hourly`
- `hourly_project_traffic`
- `top_pages_hourly`
- `trending_pages`
- `anomaly_alerts`
- `forecast_features`
- `forecast_evaluation`
- `streaming_top_pages`

## Local Execution Strategy

Airflow, Kafka, Spark, MinIO, and PostgreSQL run in Docker Compose. Python utility
scripts, tests, FastAPI, and Streamlit can run from the `wikitrend` Conda environment.

Processing should be performed hour-by-hour or day-by-day, with optional project
allowlists to keep laptop resource usage predictable.
