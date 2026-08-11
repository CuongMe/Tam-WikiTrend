from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wikitrend.forecast_protocol import build_nested_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the nested forecast protocol manifest.")
    parser.add_argument(
        "--protocol",
        type=Path,
        default=Path("configs/forecast_experiment_protocol.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("configs/forecast_fold_manifest_v2.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    protocol = json.loads(args.protocol.read_text(encoding="utf-8"))
    manifest = build_nested_manifest(protocol)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, args.output)
    print(f"Generated {args.output} with {len(manifest['outer_blocks'])} outer blocks")


if __name__ == "__main__":
    main()
