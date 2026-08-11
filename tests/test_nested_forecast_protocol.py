from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from wikitrend.forecast_protocol import build_nested_manifest


def test_nested_manifest_has_disjoint_daily_outer_blocks() -> None:
    protocol_path = Path(__file__).parents[1] / "configs" / "forecast_experiment_protocol.json"
    manifest = build_nested_manifest(json.loads(protocol_path.read_text(encoding="utf-8")))

    blocks = manifest["outer_blocks"]
    assert len(blocks) >= 5
    for left, right in zip(blocks, blocks[1:], strict=False):
        assert left["evaluation_end_hour_exclusive"] == right["evaluation_start_hour"]
    assert all(len(block["inner_blocks"]) >= 2 for block in blocks)


def test_final_holdout_is_after_all_development_blocks() -> None:
    protocol_path = Path(__file__).parents[1] / "configs" / "forecast_experiment_protocol.json"
    manifest = build_nested_manifest(json.loads(protocol_path.read_text(encoding="utf-8")))

    final_start = datetime.fromisoformat(manifest["final_holdout"]["evaluation_start_hour"])
    last_development_end = datetime.fromisoformat(
        manifest["outer_blocks"][-1]["evaluation_end_hour_exclusive"]
    )
    assert last_development_end <= final_start
    assert manifest["final_holdout"]["untouched"] is True
