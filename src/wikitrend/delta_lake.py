from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from wikitrend.gold_validation import GOLD_TABLE_CONTRACTS
from wikitrend.silver import path_has_payload


@dataclass(frozen=True)
class DeltaTableSummary:
    table_name: str
    source_path: Path
    delta_path: Path
    rows: int
    version: int
    files: int
    partition_columns: tuple[str, ...]
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_path"] = str(self.source_path)
        payload["delta_path"] = str(self.delta_path)
        payload["partition_columns"] = list(self.partition_columns)
        return payload


@dataclass(frozen=True)
class DeltaLakeBuildSummary:
    delta_dir: Path
    source_gold_dir: Path
    manifest_path: Path
    tables: tuple[DeltaTableSummary, ...]
    overwrite: bool
    engine: str = "delta-rs"

    def to_dict(self) -> dict[str, Any]:
        return {
            "delta_dir": str(self.delta_dir),
            "source_gold_dir": str(self.source_gold_dir),
            "manifest_path": str(self.manifest_path),
            "tables": [table.to_dict() for table in self.tables],
            "overwrite": self.overwrite,
            "engine": self.engine,
        }


GOLD_DELTA_PARTITIONS = {
    "hourly_project_access": ("date",),
    "daily_project_access": ("date",),
    "top_pages_hourly": ("date",),
}


def assert_delta_writable(delta_dir: Path, overwrite: bool) -> None:
    if overwrite:
        return
    if path_has_payload(delta_dir):
        raise FileExistsError(
            "Refusing to write Delta outputs because data already exists. "
            f"Use --overwrite only when intentionally replacing it: {delta_dir}"
        )


def _folder_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _load_source_table(source_path: Path):
    import pyarrow.dataset as ds

    dataset = ds.dataset(source_path, format="parquet", partitioning="hive")
    return dataset.to_table()


def _delta_file_count(delta_table) -> int:
    if hasattr(delta_table, "file_uris"):
        return len(delta_table.file_uris())
    return len(delta_table.files())


def _write_gold_delta_table(
    *,
    table_name: str,
    source_path: Path,
    delta_path: Path,
) -> DeltaTableSummary:
    from deltalake import DeltaTable, write_deltalake

    partition_columns = GOLD_DELTA_PARTITIONS[table_name]
    arrow_table = _load_source_table(source_path)
    write_deltalake(
        str(delta_path),
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=list(partition_columns),
        name=f"wikitrend_gold_{table_name}",
        description=f"WikiTrend Gold Delta table for {table_name}",
        configuration={"delta.appendOnly": "true"},
    )
    delta_table = DeltaTable(str(delta_path))
    return DeltaTableSummary(
        table_name=table_name,
        source_path=source_path,
        delta_path=delta_path,
        rows=delta_table.to_pyarrow_table().num_rows,
        version=delta_table.version(),
        files=_delta_file_count(delta_table),
        partition_columns=partition_columns,
        size_bytes=_folder_size(delta_path),
    )


def build_gold_delta_lake(
    *,
    gold_dir: Path,
    delta_dir: Path,
    overwrite: bool = False,
) -> DeltaLakeBuildSummary:
    if not gold_dir.exists():
        raise FileNotFoundError(f"Gold directory does not exist: {gold_dir}")

    target_dir = delta_dir / "gold"
    assert_delta_writable(target_dir, overwrite)
    if overwrite and target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    table_summaries = []
    for table_name, contract in GOLD_TABLE_CONTRACTS.items():
        source_path = gold_dir / contract.path
        if not source_path.exists():
            raise FileNotFoundError(f"Gold table directory does not exist: {source_path}")
        if not any(source_path.rglob("*.parquet")):
            raise FileNotFoundError(f"No Gold Parquet files found under: {source_path}")
        table_summaries.append(
            _write_gold_delta_table(
                table_name=table_name,
                source_path=source_path,
                delta_path=target_dir / table_name,
            )
        )

    summary = DeltaLakeBuildSummary(
        delta_dir=delta_dir,
        source_gold_dir=gold_dir,
        manifest_path=delta_dir / "delta_manifest.json",
        tables=tuple(table_summaries),
        overwrite=overwrite,
    )
    _write_json(
        summary.manifest_path,
        {
            "manifest_version": 1,
            "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "engine": summary.engine,
            "storage_format": "delta",
            "source_layer": "gold",
            "source_gold_dir": str(gold_dir),
            "delta_dir": str(delta_dir),
            "tables": [table.to_dict() for table in summary.tables],
        },
    )
    return summary


__all__ = [
    "DeltaLakeBuildSummary",
    "DeltaTableSummary",
    "assert_delta_writable",
    "build_gold_delta_lake",
]
