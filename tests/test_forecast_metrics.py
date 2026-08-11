from __future__ import annotations

import pandas as pd
import pytest

from wikitrend.forecast_metrics import (
    paired_block_bootstrap_ratio_difference,
    ranking_metrics,
    traffic_metrics,
)


def test_traffic_metrics_report_coverage_and_scale_aware_errors() -> None:
    metrics = traffic_metrics(
        pd.Series([10.0, 0.0, 5.0]),
        pd.Series([8.0, None, 5.0]),
        pd.Series([2.0, 1.0, 1.0]),
    )
    assert metrics["forecast_coverage"] == pytest.approx(2 / 3)
    assert metrics["mase"] == pytest.approx(0.5)
    assert metrics["nd"] == pytest.approx(2 / 15)


def test_ranking_metrics_reward_correct_top_order() -> None:
    frame = pd.DataFrame(
        {
            "item": ["a", "b", "c", "d"],
            "actual": [100, 50, 10, 1],
            "prediction": [90, 40, 8, 2],
        }
    )
    metrics = ranking_metrics(frame, "item", "actual", "prediction", k=2)
    assert metrics["ndcg_at_k"] == pytest.approx(1.0)
    assert metrics["recall_at_k"] == pytest.approx(1.0)
    assert metrics["top_k_overlap"] == pytest.approx(1.0)
    assert metrics["spearman_rank_correlation"] == pytest.approx(1.0)


def test_block_bootstrap_resamples_temporal_blocks_not_rows() -> None:
    frame = pd.DataFrame(
        {
            "day": ["d1", "d1", "d2", "d2", "d3", "d3"],
            "method": ["model", "baseline"] * 3,
            "error": [8, 10, 9, 11, 7, 12],
            "scale": [10, 10, 10, 10, 10, 10],
        }
    )
    result = paired_block_bootstrap_ratio_difference(
        frame,
        block_column="day",
        method_column="method",
        numerator_column="error",
        denominator_column="scale",
        challenger="model",
        baseline="baseline",
        resamples=200,
    )
    assert result["blocks"] == 3
    assert result["point_difference"] < 0
    assert result["probability_challenger_better"] > 0.9


def test_block_bootstrap_supports_higher_is_better_metrics() -> None:
    frame = pd.DataFrame(
        {
            "day": ["d1", "d1", "d2", "d2"],
            "method": ["model", "baseline", "model", "baseline"],
            "score": [0.9, 0.6, 0.8, 0.5],
            "rows": [1, 1, 1, 1],
        }
    )
    result = paired_block_bootstrap_ratio_difference(
        frame,
        block_column="day",
        method_column="method",
        numerator_column="score",
        denominator_column="rows",
        challenger="model",
        baseline="baseline",
        resamples=100,
        lower_is_better=False,
    )
    assert result["point_difference"] > 0
    assert result["probability_challenger_better"] == 1.0
