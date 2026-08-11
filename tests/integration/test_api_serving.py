from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import duckdb
import pytest

pytestmark = pytest.mark.integration


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_prediction_endpoint_reads_atomic_serving_database(tmp_path) -> None:
    database = tmp_path / "wikitrend.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            """
            CREATE TABLE predictions AS
            SELECT TIMESTAMP '2026-08-04 16:00:00' AS timestamp_hour,
                   'en' AS project, 'mobile' AS access_mode,
                   'Main Page' AS normalized_title,
                   42.0 AS predicted_views, 1 AS predicted_traffic_rank
            """
        )

    root = Path(__file__).parents[2]
    port = _free_port()
    environment = {
        **os.environ,
        "WIKITREND_SERVING_DB": str(database),
        "WIKITREND_GOLD_DIR": str(tmp_path / "missing-gold"),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "error",
        ],
        cwd=root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for _ in range(100):
            if process.poll() is not None:
                output = process.stdout.read() if process.stdout else ""
                raise RuntimeError(f"Uvicorn exited before readiness: {output}")
            try:
                with urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise TimeoutError("Uvicorn did not become ready")

        query = urlencode({"project": "en", "access_mode": "mobile"})
        with urlopen(f"http://127.0.0.1:{port}/predictions?{query}", timeout=5) as response:
            payload = json.load(response)
        assert payload[0]["normalized_title"] == "Main Page"
        assert payload[0]["predicted_views"] == 42.0
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
