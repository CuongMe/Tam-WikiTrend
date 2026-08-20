from __future__ import annotations

import sys

from wikitrend.storage import ensure_spark_path


def test_ensure_spark_path_preserves_unicode_on_windows(tmp_path) -> None:
    local_path = tmp_path / "Tâm-WikiTrend" / "pageviews.gz"
    local_path.parent.mkdir()
    local_path.write_bytes(b"test")

    spark_path = ensure_spark_path(local_path)

    if sys.platform == "win32":
        assert spark_path.startswith("file:///")
        assert "Tâm-WikiTrend" in spark_path
        assert "%C3%A2" not in spark_path
    else:
        assert spark_path == str(local_path.resolve())
