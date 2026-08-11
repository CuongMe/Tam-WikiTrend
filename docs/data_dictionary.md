# WikiTrend Data Dictionary

All timestamps are UTC. `source_project` is the Wikimedia dump domain code; `project`
is the canonical analytical entity. `access_mode` is always `desktop` or `mobile`.

## Snapshot Scope

The current Bronze development snapshot contains 83 hourly dumps from
`2026-08-01 00:00` through `2026-08-04 10:00`. The planned research snapshot contains
696 hours from `2026-07-08 00:00` through `2026-08-05 23:00`. The experiment uses
origins through August 4 and retains August 5 traffic for labels. Row counts are
recorded in validator outputs and snapshot manifests rather than hard-coded here.

## Canonical Dimensions

| `source_project` | `project` | `language` | `project_family` | `access_mode` |
| --- | --- | --- | --- | --- |
| `en` | `en` | `en` | `wikipedia` | `desktop` |
| `en.m` | `en` | `en` | `wikipedia` | `mobile` |
| `vi` | `vi` | `vi` | `wikipedia` | `desktop` |
| `vi.m` | `vi` | `vi` | `wikipedia` | `mobile` |
| `commons.m` | `commons` | null | `commons` | `desktop` |
| `commons.m.m` | `commons` | null | `commons` | `mobile` |
| `www.wd` | `wikidata` | null | `wikidata` | `desktop` |
| `www.wd.m` | `wikidata` | null | `wikidata` | `mobile` (supported, optional) |

## Bronze

Bronze files retain the official `pageviews-YYYYMMDD-HH0000.gz` bytes under
`data/raw/pageviews/YYYY/YYYY-MM/`. The tracked Bronze snapshot manifest records path,
date, hour, byte size, and SHA-256. The downloader's operational manifest additionally
records source URL.

## Silver `pageviews`

Grain: one accepted source project/title/hour record. Natural key:
`date, hour, source_project, project, access_mode, page_title`.

Partitioning: `date`, `hour`, canonical `project`, `access_mode`.

| Column | Type | Contract |
| --- | --- | --- |
| `date` | string | Source UTC date, `YYYY-MM-DD`. |
| `hour` | int | Source UTC hour, 0 through 23. |
| `source_project` | string | Exact dump code from the canonical mapping. |
| `project` | string | `en`, `vi`, `commons`, or `wikidata`. |
| `language` | string/null | Language where meaningful. |
| `project_family` | string | `wikipedia`, `commons`, or `wikidata`. |
| `access_mode` | string | `desktop` or `mobile`. |
| `page_title` | string | Nonblank raw title token. |
| `normalized_title` | string | URL-decoded title with underscores replaced by spaces. |
| `normalization_status` | string | `normalized`, recovered malformed escape, or title-quality state. |
| `view_count` | long | Nonnegative hourly views. |
| `response_size` | long | Nonnegative response bytes. |
| `source_file` | string | Bronze lineage URI/path. |

Structurally or numerically invalid rows go to `data/quarantine/pageviews` with a
`reject_reason`. Out-of-scope rows and quality counts go to
`pageviews_rejection_summary`; out-of-scope rows are not mislabeled as malformed data.

## Gold Tables

Every topic table retains `source_project`, `project`, `language`, `project_family`, and
`access_mode`.

| Table | Grain and purpose |
| --- | --- |
| `page_hourly` | Canonical project/access/normalized-title/source hour. |
| `hourly_project_traffic` | Canonical project/access/source hour totals. |
| `top_pages_hourly` | Top `N` observed pages per project/access/source hour. |
| `modeling_page_hourly` | Bounded topic universe crossed with every source hour. |
| `trending_pages` | Past-eligible active topic/origin with robust trend features. |
| `anomaly_alerts` | High-volume robust-score threshold subset. |
| `forecast_features` | Past-eligible topic/forecast origin and next-hour label. |
| `forecast_evaluation` | Baseline traffic summaries by project/access/method. |

### `modeling_page_hourly`

Absent sparse page-hours are explicit zero rows. `is_observed=false` means no source
row existed for that topic-hour; it does not mean the dump file was missing. Missing
source files or required projects fail Silver validation before Gold is built.

| Column | Meaning |
| --- | --- |
| `is_observed` | Whether `page_hourly` contained the topic at this hour. |
| `view_count`, `response_size`, `page_rows` | Zero for an inserted sparse hour. |
| `eligibility_history_views` | Cumulative views strictly before this origin. |
| `eligibility_observed_hours` | Active source rows strictly before this origin. |
| `eligible_at_origin` | Past-only threshold result; current/future traffic is excluded. |

### `trending_pages`

The baseline uses the previous 24 completed grid hours and excludes the current hour.

| Column | Meaning |
| --- | --- |
| `previous_hour_views` | Exact prior grid hour; zero is valid after completion. |
| `growth_rate` | `current / previous - 1`; null when previous traffic is zero. |
| `rolling_baseline_log_median` | Median of prior `log1p(view_count)` values. |
| `rolling_baseline_log_mad` | Median absolute deviation around that median. |
| `robust_z_score` | `0.6744897502 * (log1p(current)-median) / MAD`; null for zero MAD or insufficient history. |
| `trend_score` | Positive robust score times `log1p(current)` and history confidence. |
| `trend_rank` | Descending score within origin/project/access. |

`rolling_baseline_avg` and `rolling_baseline_stddev` remain diagnostics; the standard
deviation is not used in the trend score.

### `forecast_features`

| Column | Meaning |
| --- | --- |
| `view_count` | Traffic at forecast origin. |
| `lag_1h_views`, `lag_24h_views` | Exact completed-grid lags. |
| `rolling_forecast_avg` | Mean of the previous configured completed hours. |
| `forecast_history_elapsed_hours` | Completed rows available in the history window. |
| `forecast_history_active_hours` | Those rows with `is_observed=true`. |
| `mase_scale` | Past-only mean absolute lag-1 error for this series. |
| `baseline_forecast` | lag-24, then rolling mean, then lag-1 fallback. |
| `target_next_hour_views` | Next completed-grid hour; null only at dataset boundary. |
| `forecast_available` | Whether the deterministic baseline is available. |

### `forecast_evaluation`

Methods are `baseline_forecast`, `lag_1h`, `lag_24h`, and `rolling_average`. Metrics are
MASE, ND, sMAPE, and epsilon-stabilized msMAPE. The research protocol selects msMAPE as
the primary relative metric, while retaining the others for diagnosis. RMSE, MSE, and
ordinary MAPE are not part of this contract.

## LightGBM Outputs

`lightgbm_predictions/predictions` stores every scored row with origin/forecast hour,
raw model prediction, selected forecast, fallback reason, ranks, model version, feature
availability, and actual label when available. It is partitioned by forecast date/hour,
project, and access mode.

`research_top_pages` stores predicted-traffic and predicted-growth top `N` rows.
`metrics` stores paired LightGBM/lag-1 plus operational traffic metrics.
`ranking_metrics` stores coverage, NDCG@K, Recall@K, top-K Jaccard overlap, and Spearman
rank correlation.

## Reproducibility Artifacts

- `configs/pageview_download_plan.json`: intended Bronze acquisition window.
- `artifacts/manifests/bronze_83h_snapshot.json`: current immutable source hashes.
- `configs/forecast_experiment_protocol.json`: experiment design.
- `configs/forecast_fold_manifest_v2.json`: fixed generated nested folds and holdout.
- `artifacts/manifests/training_snapshot.json`: hashes of forecast Parquet and contracts;
  created only after contract-v2 Gold is finalized.
- `models/lightgbm/<version>/`: immutable model, category levels, and metadata.

## Inspection Notebooks

- `notebooks/inspect_raw.ipynb`
- `notebooks/inspect_silver.ipynb`
- `notebooks/inspect_gold.ipynb`
- `notebooks/lightgbm_regression.ipynb`
- `notebooks/inspect_lightgbm_predictions.ipynb`

The existing LightGBM notebook is exploratory. Reportable contract-v2 training is run by
`spark_jobs/train_lightgbm_nested.py` against the fixed manifest.
