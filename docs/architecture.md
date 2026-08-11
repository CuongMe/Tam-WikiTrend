# Architecture

## Batch Publication

```text
Wikimedia hourly dumps
  -> immutable gzip Bronze + SHA-256 manifest
  -> Spark parse, canonical project mapping, deduplication, quarantine
  -> Silver contract and source-hour coverage validation
  -> zero-complete Gold modeling grid and leakage-safe features
  -> Gold reconciliation and semantic validation
  -> optional LightGBM scoring
  -> versioned Delta snapshot in MinIO
  -> atomic DuckDB serving snapshot
  -> FastAPI and Streamlit
```

Airflow writes each run to `data/staging/<run-id>`. No staged table is published until
the Silver and Gold validators pass. Delta destinations include the training snapshot
hash, use `errorIfExists`, and receive a publication marker only after all tables have
been written. MinIO is configured for one Spark driver; this satisfies Delta's local
single-cluster S3 consistency requirement but is not a multi-writer production design.

## Table Semantics

Bronze keeps source bytes unchanged. Silver has one row per accepted source page-hour
and retains `source_file`, `source_project`, canonical `project`, `project_family`,
`language`, and `access_mode`. Quality failures are quarantined; out-of-scope records
are counted separately.

Gold first aggregates source rows to canonical page-hour facts. The bounded modeling
universe is crossed with every source hour and absent page-hours become zero with
`is_observed=false`. Eligibility is calculated from cumulative history strictly before
the forecast origin. This prevents the current label or future traffic from deciding
whether a row enters an experiment.

## Forecasting

The target is next-hour pageviews. A global LightGBM model uses traffic lags, rolling
history, observed/elapsed history indicators, cyclical hour features, project, family,
language, and access mode. A fixed nested rolling manifest controls objective selection
and an untouched seven-day holdout. Model artifacts are immutable version directories;
`models/lightgbm/current.json` is the only mutable pointer.

Scoring preserves the exact feature order and categorical levels from metadata. Invalid
features or model predictions use lag-1 traffic when available. Historical quality
fallback requires at least 24 evaluated origins and cannot be activated by one hour.

## Streaming

Silver replay emits a versioned envelope with a SHA-256 event ID derived from immutable
source identity. Files and Parquet fragments are replayed in path order; Kafka uses the
event ID as its key and requires all replicas to acknowledge sends.

Structured Streaming starts at the earliest retained offset, validates envelope version,
deduplicates event IDs, and uses `foreachBatch` Delta merges for both the event ledger and
affected page-hour aggregates. Event IDs also protect against duplicates after a
checkpoint reset. A 45-day watermark covers the planned historical replay window.

## Serving

`scripts/publish_serving_db.py` compacts selected research outputs into a temporary
DuckDB database, checkpoints it, and atomically replaces the serving file. FastAPI and
Streamlit only read the published database when it exists; local Gold Parquet remains a
development fallback for legacy analytical views.

## Constraints

- The current laptop snapshot is 83 hours, not sufficient for the final experiment.
- Local Docker uses one Spark worker and one Kafka broker; it demonstrates contracts and
  replay behavior, not high availability.
- Delta on MinIO is single-writer in this topology.
- The system uses dump hour as event time; Wikimedia publication latency is not modeled.
