# WikiTrend Data Dictionary

This document describes the current local lakehouse contracts. All timestamps are
UTC. Parquet columns are shown using their logical names; Spark and DuckDB may display
minor type differences such as `BIGINT` versus `long`.

## Current Development Snapshot

- Window: 83 hourly dumps from `2026-08-01` through `2026-08-04 10:00 UTC`.
- Projects: `en`, `en.m`, `vi`, `vi.m`, `commons.m`, and `commons.m.m`.
- `page_hourly`: 191,930,076 rows.
- Eligible `trending_pages` and `forecast_features`: 62,923,736 rows.
- `forecast_evaluation`: 24 summaries, four methods across six projects.

## Silver Pageviews

Silver is trusted, parsed, standardized, deduplicated Parquet. It is partitioned by
`date`, `hour`, and `project`.

| Column | Type | Description |
| --- | --- | --- |
| `date` | date/string | UTC dump date as `YYYY-MM-DD`. |
| `hour` | int | UTC dump hour from `0` to `23`. |
| `project` | string | Wikimedia project code from the dump row. |
| `language` | string | Inferred language code where applicable. |
| `project_family` | string | Inferred family such as `wikipedia`, `wikidata`, or `commons`. |
| `page_title` | string | Source page title before display normalization. |
| `normalized_title` | string | URL-decoded display title with underscores replaced by spaces. |
| `normalization_status` | string | Title normalization result and recovery status. |
| `view_count` | long | Hourly page views; nonnegative. |
| `response_size` | long | Response bytes for the row; nonnegative. |
| `source_file` | string | Immutable Bronze file that produced the row. |

Silver quarantine outputs are described in [Silver Quality Outputs](#silver-quality-outputs).

## Gold Table Grain

| Table | Grain and purpose |
| --- | --- |
| `page_hourly` | One normalized topic, project, and hour. Reusable topic-level aggregate. |
| `hourly_project_traffic` | One project and hour. Reconciles to sums from `page_hourly`. |
| `top_pages_hourly` | Top-ranked topics within each project and hour. |
| `trending_pages` | Eligible topic, project, and hour with trend features and rank. |
| `anomaly_alerts` | High-volume subset of `trending_pages` that crosses the robust-score threshold. |
| `forecast_features` | Eligible topic, project, and forecast-origin hour with leakage-safe baseline features. |
| `forecast_evaluation` | One summary row per project and forecast method. |

Gold topic tables use `normalized_title` as the analytical topic key. `page_title` is
retained as a representative source title for auditability.

## `page_hourly`

| Column | Description |
| --- | --- |
| `timestamp_hour` | UTC timestamp for the hour. |
| `date`, `hour` | Hive partition and decomposed UTC time fields. |
| `project`, `language`, `project_family` | Wikimedia dimensions. |
| `page_title`, `normalized_title` | Representative and analytical titles. |
| `view_count` | Sum of Silver views for the topic-hour. |
| `response_size` | Sum of Silver response bytes for the topic-hour. |
| `page_rows` | Number of Silver rows contributing to the topic-hour. |

## `trending_pages`

Trend rows are restricted to topics meeting the current eligibility rules:
`topic_total_views >= 100` and at least `6` observed historical hours.

| Column | Description |
| --- | --- |
| `previous_hour_views` | Views from the exact previous hour when that topic row exists. Nullable for sparse topics or the dataset boundary. |
| `rolling_baseline_avg` | Mean raw view count in the past-only baseline window. Diagnostic only. |
| `rolling_baseline_stddev` | Population standard deviation in raw view space. Diagnostic only; not used by the production score. |
| `rolling_baseline_log_median` | Median of `log1p(view_count)` in the past-only baseline window. |
| `rolling_baseline_log_mad` | Median absolute deviation of log-space baseline values. |
| `baseline_observed_hours` | Number of observed topic hours in the baseline window. |
| `baseline_window_hours` | Configured baseline length; currently `24`. |
| `growth_rate` | `current / previous - 1` when the previous value is positive. Nullable when the previous value is missing or zero; do not replace with zero. |
| `log1p_views` | `ln(1 + view_count)`, used to reduce heavy-tail effects. |
| `robust_z_score` | Robust anomaly score in log space: `0.67449 * (log1p_views - log_median) / log_mad`. Nullable when history is insufficient or MAD is zero. |
| `trend_score` | `max(robust_z_score, 0) * log1p_views * confidence_factor`. |
| `trend_rank` | Rank within each date, hour, and project by descending trend score. |

The confidence factor is:

```text
min(1, baseline_observed_hours / 6)
```

The current hour is excluded from all trend baselines. Missing previous-hour rows are
preserved as NULL because the source is sparse; Gold does not silently convert unknown
or undefined ratios into zero.

## `anomaly_alerts`

This table contains high-volume traffic spikes where:

- `view_count >= 1,000`.
- `robust_z_score >= 4`.

| Column | Description |
| --- | --- |
| `robust_z_score` | Robust log-space anomaly score used for the threshold. |
| `trend_score` | Corresponding positive trend score. |
| `alert_type` | Current alert type: `traffic_spike`. |
| `alert_severity` | `high` at the configured threshold; `critical` at twice the threshold. |
| `rolling_baseline_log_median`, `rolling_baseline_log_mad` | Robust baseline diagnostics. |
| `rolling_baseline_avg`, `rolling_baseline_stddev` | Raw-space comparison diagnostics only. |
| `growth_rate` | Nullable prior-hour growth feature. |

The remaining dimension and timestamp columns have the same meanings as in
`trending_pages`.

## `forecast_features`

Forecast features are generated per eligible topic and forecast-origin hour. The
current implementation evaluates a one-hour horizon using observations strictly before
the current feature timestamp.

| Column | Description |
| --- | --- |
| `view_count` | Current observed topic traffic at the feature timestamp. |
| `lag_1h_views` | Traffic from one hour before the feature timestamp. |
| `lag_24h_views` | Traffic from 24 hours before the feature timestamp. |
| `rolling_forecast_avg` | Mean of the previous six observed hours. |
| `forecast_history_observed_hours` | Observed history count used by the forecast features. |
| `baseline_forecast` | Fallback forecast: previous-day value, then rolling average, then previous-hour value. |
| `forecast_horizon_hours` | Forecast horizon; currently `1`. |
| `baseline_window_hours` | MASE scaling history window; currently `24`. |
| `forecast_average_window_hours` | Rolling forecast window; currently `6`. |
| `forecast_available` | Whether the baseline forecast is non-null. |
| `target_next_hour_views` | Observed next-hour label for offline evaluation only. Never use it as a live input feature. |

## `forecast_evaluation`

The table evaluates four deterministic methods independently:

- `baseline_forecast`
- `lag_1h`
- `lag_24h`
- `rolling_average`

| Column | Description |
| --- | --- |
| `forecast_method` | Forecast rule being evaluated. |
| `evaluated_rows` | Rows with both an observed target and prediction. |
| `mase_valid_rows` | Rows with a positive MASE scaling value. |
| `mase` | Mean absolute scaled error using past one-step naive error. Lower is better; values below `1` outperform that naive scale. |
| `nd` | Normalized deviation: total absolute error divided by total absolute actual traffic. |
| `smape` | Symmetric absolute percentage error; zero/zero contributes zero. |
| `msmape` | Epsilon-stabilized sMAPE for zero and low-volume denominators. Default epsilon is `1.0` view. |
| `evaluation_start_hour`, `evaluation_end_hour` | Observed evaluation range. |

No MAE, RMSE, or ordinary MAPE columns are part of the current evaluation contract.

## Silver Quality Outputs

`data/quarantine/pageviews` stores raw records rejected for structural or numeric
quality failures, partitioned by `reject_reason`. The rejection record preserves the
raw line, source file, parsed context, and raw numeric fields.

`data/quarantine/pageviews_rejection_summary` stores compact rejection counts by
`rejection_type`, `reject_reason`, date, hour, project, and source file. Out-of-scope
projects are summarized here rather than copied into the raw-record quarantine.

## Inspection Notebooks

- `notebooks/inspect_raw.ipynb`: immutable Bronze manifest, raw samples, and streamed raw quality checks.
- `notebooks/inspect_silver.ipynb`: Silver schema, partitioned samples, missing/invalid checks, and DuckDB inspection.
- `notebooks/inspect_gold.ipynb`: Gold schemas, trend/anomaly inspection, forecast inspection, and reconciliation checks.
