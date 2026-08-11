from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.dataset as ds


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hash a reproducible training-data snapshot.")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/gold/forecast_features"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/manifests/training_snapshot.json"),
    )
    parser.add_argument(
        "--contract-file",
        action="append",
        type=Path,
        default=[],
        help="Additional code/config contract to hash; repeat for multiple files.",
    )
    return parser.parse_args()


def dataset_bounds(dataset: Path) -> tuple[int, object, object]:
    parquet = ds.dataset(dataset, format="parquet", partitioning="hive")
    rows = parquet.count_rows()
    minimum = None
    maximum = None
    for batch in parquet.scanner(columns=["timestamp_hour"]).to_batches():
        values = batch.column("timestamp_hour")
        if len(values) == 0:
            continue
        batch_min = pc.min(values).as_py()
        batch_max = pc.max(values).as_py()
        minimum = batch_min if minimum is None else min(minimum, batch_min)
        maximum = batch_max if maximum is None else max(maximum, batch_max)
    if rows == 0 or minimum is None or maximum is None:
        raise ValueError(f"Training dataset is empty: {dataset}")
    return rows, minimum, maximum


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    dataset = args.dataset if args.dataset.is_absolute() else root / args.dataset
    output = args.output if args.output.is_absolute() else root / args.output
    parquet_files = sorted(dataset.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {dataset}")
    default_contracts = [
        root / "spark_jobs" / "build_gold_tables.py",
        root / "spark_jobs" / "forecast_evaluation.py",
        root / "configs" / "forecast_fold_manifest_v2.json",
    ]
    contract_files = sorted(
        {
            path if path.is_absolute() else root / path
            for path in [*default_contracts, *args.contract_file]
        }
    )
    missing_contracts = [str(path) for path in contract_files if not path.is_file()]
    if missing_contracts:
        raise FileNotFoundError(f"Snapshot contracts are missing: {missing_contracts}")

    records = [file_record(path, root) for path in [*parquet_files, *contract_files]]
    row_count, start_hour, end_hour = dataset_bounds(dataset)
    snapshot_digest = hashlib.sha256(
        "".join(record["sha256"] for record in records).encode("ascii")
    ).hexdigest()
    git_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    git_dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    )
    payload = {
        "manifest_version": 1,
        "snapshot_id": f"sha256:{snapshot_digest}",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "dataset": dataset.relative_to(root).as_posix(),
        "file_count": len(parquet_files),
        "total_dataset_bytes": sum(path.stat().st_size for path in parquet_files),
        "row_count": row_count,
        "dataset_start_hour": start_hour.isoformat(),
        "dataset_end_hour": end_hour.isoformat(),
        "git_commit": git_commit,
        "git_dirty": git_dirty,
        "files": records,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".part")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(f"Wrote {output} ({payload['snapshot_id']})")


if __name__ == "__main__":
    main()
