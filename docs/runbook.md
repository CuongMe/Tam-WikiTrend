# Runbook

## Verify Environment

```powershell
conda activate wikitrend
java -version
python -c "import pyspark, pandas, numpy; print(pyspark.__version__, pandas.__version__, numpy.__version__)"
ruff check .
pytest
python -m pip check
```

Use Java 17 from the conda environment. If system Java 26 wins on Windows:

```powershell
$env:JAVA_HOME="$env:CONDA_PREFIX\Library"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON="$env:CONDA_PREFIX\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$env:CONDA_PREFIX\python.exe"
```

## Acquire Bronze

Preview only; this performs no network requests:

```powershell
python scripts/download_pageviews.py --plan configs/pageview_download_plan.json --dry-run
```

Run the same command without `--dry-run` only when intentionally acquiring the
multi-week experiment. Each file is written to `.part`, checked, hashed, then renamed.
Do not use `--overwrite` unless a source correction has been verified independently.

Inventory an existing manual snapshot:

```powershell
python scripts/build_bronze_manifest.py `
  --output artifacts/manifests/bronze_83h_snapshot.json
```

Add `--verify-gzip` for a full decompression/CRC audit; it is much slower than hashing.

## Stage Silver And Gold

Never write a new contract directly over trusted data. Use a unique staging root:

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
python scripts/validate_gold.py --gold-dir "data/staging/$run/gold"
```

Silver validation checks raw lineage, duplicate natural keys, required project coverage
for every source hour, dimensions, partitions, and invalid values. Gold validation checks
schemas, reconciliation, complete page-hour series, past-only eligibility, robust trend
features, one-hour targets, and evaluation summaries.

`spark_jobs/build_gold_tables.py` is the only supported Gold construction path. Rebuild
the complete staged Gold snapshot and validate it; do not overwrite individual trend,
anomaly, feature, or evaluation tables in place.

## Train LightGBM

Training is blocked until the full planned window exists and staged contract-v2 Gold is
published as the experiment snapshot.

```powershell
python scripts/generate_forecast_manifest.py
python scripts/build_snapshot_manifest.py
python spark_jobs/train_lightgbm_nested.py
```

Do not use `--allow-dirty-snapshot` for reportable results. Do not use
`--allow-holdout-rerun` to tune against the same holdout. Collect new future data and
move the holdout instead.

## Score And Serve

After a compatible versioned model exists:

```powershell
python spark_jobs/score_lightgbm.py --top-n 100 --ranking-cutoffs 10,50,100
python scripts/publish_serving_db.py
```

Scoring loads `models/lightgbm/current.json`, checks exact feature order/category levels,
writes model and lag-1 fallback predictions, and reports paired traffic/ranking metrics
when labels exist. The serving publisher creates a temporary DuckDB file and atomically
replaces `data/serving/wikitrend.duckdb`.

## Platform

```powershell
docker compose -f infrastructure/docker-compose.yml config --quiet
docker compose -f infrastructure/docker-compose.yml up -d `
  postgres minio minio-init kafka spark-master spark-worker
docker compose -f infrastructure/docker-compose.yml up airflow-init
docker compose -f infrastructure/docker-compose.yml up -d airflow-webserver airflow-scheduler
```

Streaming replay:

```powershell
docker compose -f infrastructure/docker-compose.yml --profile stream up structured-streaming
docker compose -f infrastructure/docker-compose.yml --profile stream run --rm kafka-replay
```

The replay is deterministic and idempotent, but the local topology is one broker and one
Spark worker. It is an integration demonstration, not an HA deployment.

## Recovery Rules

- **Failed download:** keep the final `.gz` immutable; delete only the failed `.part` and
  rerun the downloader.
- **Failed staged validation:** inspect quarantine/report, fix code or source contract,
  and rebuild a new staging run. Do not publish it.
- **Failed Delta publication:** a versioned snapshot has no publication marker; leave it
  for audit or remove it explicitly after confirming no reader points to it.
- **Streaming checkpoint loss:** replay from earliest. Delta event-ID merges prevent
  double counting.
- **Holdout accidentally opened:** record the event and define a new future holdout.
- **Docker config access denied:** repair access to
  `C:\Users\cuong\.docker\config.json`; do not run the stack under an unrelated account.
