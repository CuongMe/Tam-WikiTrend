from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class EvaluationBlock:
    block_id: str
    training_start_hour: datetime
    training_end_hour_exclusive: datetime
    evaluation_start_hour: datetime
    evaluation_end_hour_exclusive: datetime

    def to_dict(self) -> dict[str, str]:
        payload = asdict(self)
        return {
            key: value.isoformat() if isinstance(value, datetime) else str(value)
            for key, value in payload.items()
        }


def _daily_blocks(
    block_prefix: str,
    window_start: datetime,
    evaluation_end: datetime,
    training_hours: int,
    evaluation_hours: int,
    embargo_hours: int,
) -> list[EvaluationBlock]:
    first_evaluation = window_start + timedelta(hours=training_hours + embargo_hours)
    blocks: list[EvaluationBlock] = []
    evaluation_start = first_evaluation
    while evaluation_start + timedelta(hours=evaluation_hours) <= evaluation_end:
        training_end = evaluation_start - timedelta(hours=embargo_hours)
        blocks.append(
            EvaluationBlock(
                block_id=f"{block_prefix}_{len(blocks):03d}",
                training_start_hour=training_end - timedelta(hours=training_hours),
                training_end_hour_exclusive=training_end,
                evaluation_start_hour=evaluation_start,
                evaluation_end_hour_exclusive=evaluation_start
                + timedelta(hours=evaluation_hours),
            )
        )
        evaluation_start += timedelta(hours=evaluation_hours)
    return blocks


def build_nested_manifest(protocol: dict[str, Any]) -> dict[str, Any]:
    start = datetime.fromisoformat(protocol["dataset_start_hour"])
    end_exclusive = datetime.fromisoformat(protocol["dataset_end_hour_exclusive"])
    horizon_hours = int(protocol["forecast_horizon_hours"])
    holdout_hours = int(protocol["final_holdout_days"]) * 24
    outer_training_hours = int(protocol["outer_training_days"]) * 24
    outer_evaluation_hours = int(protocol["outer_evaluation_days"]) * 24
    inner_training_hours = int(protocol["inner_training_days"]) * 24
    inner_evaluation_hours = int(protocol["inner_evaluation_days"]) * 24
    embargo_hours = int(protocol.get("embargo_hours", horizon_hours))

    if any(
        value <= 0
        for value in (
            horizon_hours,
            holdout_hours,
            outer_training_hours,
            outer_evaluation_hours,
            inner_training_hours,
            inner_evaluation_hours,
            embargo_hours,
        )
    ):
        raise ValueError("All protocol durations must be positive")
    if end_exclusive <= start:
        raise ValueError("Dataset end must be after its start")

    holdout_origin_end = end_exclusive - timedelta(hours=horizon_hours)
    holdout_start = holdout_origin_end - timedelta(hours=holdout_hours)
    outer_blocks = _daily_blocks(
        "outer",
        start,
        holdout_start,
        outer_training_hours,
        outer_evaluation_hours,
        embargo_hours,
    )
    minimum_outer_blocks = int(protocol.get("minimum_outer_blocks", 5))
    if len(outer_blocks) < minimum_outer_blocks:
        raise ValueError(
            f"Protocol creates {len(outer_blocks)} outer blocks; "
            f"at least {minimum_outer_blocks} are required"
        )

    nested_blocks = []
    minimum_inner_blocks = int(protocol.get("minimum_inner_blocks", 2))
    for outer in outer_blocks:
        inner = _daily_blocks(
            f"{outer.block_id}_inner",
            outer.training_start_hour,
            outer.training_end_hour_exclusive,
            inner_training_hours,
            inner_evaluation_hours,
            embargo_hours,
        )
        if len(inner) < minimum_inner_blocks:
            raise ValueError(
                f"{outer.block_id} creates {len(inner)} inner blocks; "
                f"at least {minimum_inner_blocks} are required"
            )
        nested_blocks.append(
            {**outer.to_dict(), "inner_blocks": [item.to_dict() for item in inner]}
        )

    final_training_end = holdout_start - timedelta(hours=embargo_hours)
    final_training_start = final_training_end - timedelta(hours=outer_training_hours)
    if final_training_start < start or holdout_origin_end <= holdout_start:
        raise ValueError("Dataset window cannot support the requested final holdout")

    return {
        "manifest_version": 2,
        "manifest_id": protocol["manifest_id"],
        "dataset": protocol["dataset"],
        "dataset_window": {
            "start_hour": start.isoformat(),
            "end_hour_exclusive": end_exclusive.isoformat(),
        },
        "forecast_horizon_hours": horizon_hours,
        "embargo_hours": embargo_hours,
        "objective_candidates": list(protocol["objective_candidates"]),
        "sampling": dict(protocol["sampling"]),
        "ranking_cutoffs": list(protocol["ranking_cutoffs"]),
        "block_bootstrap": dict(protocol["block_bootstrap"]),
        "outer_blocks": nested_blocks,
        "final_holdout": {
            "training_start_hour": final_training_start.isoformat(),
            "training_end_hour_exclusive": final_training_end.isoformat(),
            "evaluation_start_hour": holdout_start.isoformat(),
            "evaluation_end_hour_exclusive": holdout_origin_end.isoformat(),
            "untouched": True,
        },
    }
