from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def traffic_metrics(
    actual: pd.Series,
    prediction: pd.Series,
    mase_scale: pd.Series,
    msmape_epsilon: float = 1.0,
) -> dict[str, float | int]:
    valid_prediction = prediction.notna() & actual.notna()
    valid_mase = valid_prediction & mase_scale.notna() & (mase_scale > 0)
    absolute_error = (actual - prediction).abs()
    denominator = actual.abs() + prediction.abs()
    return {
        "eligible_rows": int(actual.notna().sum()),
        "predicted_rows": int(valid_prediction.sum()),
        "forecast_coverage": float(valid_prediction.sum() / max(actual.notna().sum(), 1)),
        "mase": float((absolute_error[valid_mase] / mase_scale[valid_mase]).mean()),
        "nd": float(
            absolute_error[valid_prediction].sum()
            / max(actual[valid_prediction].abs().sum(), 1.0)
        ),
        "msmape": float(
            (
                2.0
                * absolute_error[valid_prediction]
                / denominator[valid_prediction].clip(lower=msmape_epsilon)
            ).mean()
        ),
    }


def ranking_metrics(
    frame: pd.DataFrame,
    item_column: str,
    actual_column: str,
    prediction_column: str,
    k: int,
) -> dict[str, float | int]:
    if k <= 0:
        raise ValueError("k must be positive")
    work = frame[[item_column, actual_column, prediction_column]].dropna(
        subset=[actual_column]
    )
    scored = work.dropna(subset=[prediction_column])
    if work.empty:
        raise ValueError("Ranking evaluation requires actual observations")

    actual_top = work.sort_values(
        [actual_column, item_column], ascending=[False, True]
    ).head(k)
    predicted_top = scored.sort_values(
        [prediction_column, item_column], ascending=[False, True]
    ).head(k)
    actual_items = set(actual_top[item_column])
    predicted_items = set(predicted_top[item_column])
    overlap = actual_items & predicted_items
    union = actual_items | predicted_items

    relevance = np.log1p(predicted_top[actual_column].clip(lower=0).to_numpy(dtype=float))
    ideal_relevance = np.log1p(actual_top[actual_column].clip(lower=0).to_numpy(dtype=float))
    discounts = np.log2(np.arange(2, len(relevance) + 2, dtype=float))
    ideal_discounts = np.log2(np.arange(2, len(ideal_relevance) + 2, dtype=float))
    dcg = float(np.sum(relevance / discounts)) if len(relevance) else 0.0
    idcg = float(np.sum(ideal_relevance / ideal_discounts)) if len(ideal_relevance) else 0.0

    rank_frame = scored.copy()
    rank_frame["_actual_rank"] = rank_frame[actual_column].rank(
        method="average", ascending=False
    )
    rank_frame["_prediction_rank"] = rank_frame[prediction_column].rank(
        method="average", ascending=False
    )
    spearman = rank_frame["_actual_rank"].corr(
        rank_frame["_prediction_rank"], method="pearson"
    )
    return {
        "k": k,
        "eligible_rows": len(work),
        "predicted_rows": len(scored),
        "forecast_coverage": float(len(scored) / len(work)),
        "ndcg_at_k": float(dcg / idcg) if idcg > 0 else 0.0,
        "recall_at_k": float(len(overlap) / max(len(actual_items), 1)),
        "top_k_overlap_count": len(overlap),
        "top_k_overlap": float(len(overlap) / max(len(union), 1)),
        "spearman_rank_correlation": float(spearman) if pd.notna(spearman) else 0.0,
    }


def paired_block_bootstrap_ratio_difference(
    frame: pd.DataFrame,
    block_column: str,
    method_column: str,
    numerator_column: str,
    denominator_column: str,
    challenger: str,
    baseline: str,
    resamples: int = 2000,
    confidence_level: float = 0.95,
    seed: int = 42,
    lower_is_better: bool = True,
) -> dict[str, Any]:
    if resamples <= 0 or not 0 < confidence_level < 1:
        raise ValueError("Invalid bootstrap configuration")
    grouped = (
        frame.groupby([block_column, method_column], as_index=False)[
            [numerator_column, denominator_column]
        ]
        .sum()
        .pivot(index=block_column, columns=method_column)
    )
    required = {challenger, baseline}
    available = set(grouped[numerator_column].columns)
    if not required.issubset(available):
        raise ValueError("Both paired methods must be present in every bootstrap input")
    complete = grouped.dropna(
        subset=[
            (numerator_column, challenger),
            (denominator_column, challenger),
            (numerator_column, baseline),
            (denominator_column, baseline),
        ]
    )
    if len(complete) < 2:
        raise ValueError("Block bootstrap requires at least two complete temporal blocks")

    challenger_num = complete[(numerator_column, challenger)].to_numpy(dtype=float)
    challenger_den = complete[(denominator_column, challenger)].to_numpy(dtype=float)
    baseline_num = complete[(numerator_column, baseline)].to_numpy(dtype=float)
    baseline_den = complete[(denominator_column, baseline)].to_numpy(dtype=float)
    point = challenger_num.sum() / challenger_den.sum() - baseline_num.sum() / baseline_den.sum()

    rng = np.random.default_rng(seed)
    differences = np.empty(resamples, dtype=float)
    for index in range(resamples):
        sample = rng.integers(0, len(complete), size=len(complete))
        challenger_ratio = challenger_num[sample].sum() / challenger_den[sample].sum()
        baseline_ratio = baseline_num[sample].sum() / baseline_den[sample].sum()
        differences[index] = challenger_ratio - baseline_ratio
    tail = (1.0 - confidence_level) / 2.0
    return {
        "blocks": len(complete),
        "resamples": resamples,
        "confidence_level": confidence_level,
        "point_difference": float(point),
        "lower": float(np.quantile(differences, tail)),
        "upper": float(np.quantile(differences, 1.0 - tail)),
        "probability_challenger_better": float(
            np.mean(differences < 0) if lower_is_better else np.mean(differences > 0)
        ),
        "lower_is_better": lower_is_better,
    }
