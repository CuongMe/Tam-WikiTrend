from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.build_snapshot_manifest import dataset_bounds


def test_dataset_bounds_reads_forecast_snapshot(tmp_path) -> None:
    pq.write_table(
        pa.table(
            {
                "timestamp_hour": [
                    datetime(2026, 8, 1, 1),
                    datetime(2026, 8, 1, 2),
                ],
                "view_count": [1, 2],
            }
        ),
        tmp_path / "part.parquet",
    )

    rows, start, end = dataset_bounds(tmp_path)

    assert rows == 2
    assert start == datetime(2026, 8, 1, 1)
    assert end == datetime(2026, 8, 1, 2)
