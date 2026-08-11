# Forecast Evaluation Protocol

`configs/forecast_experiment_protocol.json` is the human-edited specification.
`configs/forecast_fold_manifest_v2.json` is its generated, fixed timestamp manifest and
must be shared unchanged by every candidate objective and baseline.

## Sequence

1. Acquire the complete planned Bronze window and validate every required project/hour.
2. Build contract-v2 Silver and zero-complete Gold.
3. Hash the Gold forecast snapshot and contract files.
4. For each outer day, choose the LightGBM objective using only that block's inner
   rolling windows.
5. Fit the chosen objective on the outer training window and score the next UTC day.
6. Aggregate paired daily traffic and ranking metrics; bootstrap whole days.
7. Freeze objective, parameters, feature order, and category handling.
8. Open the final seven-day holdout exactly once and publish the versioned artifact.

No row from an evaluation day may participate in fitting, preprocessing levels, early
stopping, objective selection, or sampling decisions for that evaluation. Blocks are
non-overlapping at the day level to avoid treating millions of correlated page rows as
independent evidence.

The final holdout lock is deliberate. If it has been opened and further tuning is
needed, move the holdout forward by collecting new data; do not repeatedly score the
same holdout.
