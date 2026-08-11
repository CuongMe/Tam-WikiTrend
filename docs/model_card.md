# LightGBM Model Card

## Status

Contract-v2 is specified but not trained. The current 83-hour development snapshot is
too short for the committed nested rolling experiment and its seven-day final holdout.
Any older LightGBM artifact built before canonical Wikidata/access-mode mapping and
zero-completion is invalid for this contract.

## Objective

Predict pageviews for each eligible canonical project/access/title one hour ahead, then
rank pages by predicted traffic and predicted growth for trend research. This is a
global model across English Wikipedia, Vietnamese Wikipedia, Commons, and Wikidata.

## Features

Numeric features are current traffic, lag-1, lag-24, rolling forecast average, elapsed
history hours, active source-observation hours, `is_observed`, and sine/cosine hour.
Categorical features are project, language, project family, and access mode. All feature
values exist at or before the forecast origin. The next-hour target is never an input.

## Training Protocol

- Planned acquisition: 696 UTC hours ending `2026-08-05 23:00`; experiment origins end
  before the final label buffer.
- Outer evaluation: non-overlapping one-day blocks after at least 14 training days.
- Inner objective selection: non-overlapping one-day blocks after at least seven days.
- Candidates: Poisson, Tweedie with variance power 1.3, and L1 regression.
- Sampling: deterministic project/access/volume stratification with inverse-probability
  sample weights and a maximum 500,000 fit rows per candidate.
- Early stopping: the final 24 hours inside each training split.
- Final holdout: untouched final seven days, opened once after all choices are frozen.

## Evaluation

Traffic accuracy reports MASE, ND, and msMAPE on paired model/baseline rows plus forecast
coverage. Trend discovery reports NDCG@10/50/100, Recall@10/50/100, top-K Jaccard overlap,
and Spearman rank correlation. Uncertainty is estimated with paired whole-UTC-day block
bootstrap confidence intervals, preserving within-day page dependence.

The primary traffic baseline is lag-1 pageviews. MASE below 1 means lower average scaled
absolute error than the in-series naive scale; it does not by itself prove better top-K
trend discovery. Model promotion requires both traffic and ranking evidence across days.

## Artifacts

Each version directory contains `model.txt`, `metadata.json`, and
`category_levels.json`. Its version hashes the fold manifest, training snapshot,
objective, feature order, and parameters. `current.json` points to the selected version.
The training snapshot hashes every forecast-feature Parquet file and contract source.

## Limitations

- Dump traffic is aggregated hourly and contains no user/session context.
- Sparse titles are zero-completed only within the bounded modeling universe.
- Unseen category values map to `__unknown__`.
- Historical events, redirects, bots, and publication delays may create regime changes.
- Results from this local research window must not be presented as production guarantees.
