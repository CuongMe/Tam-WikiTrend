from __future__ import annotations

from pathlib import Path

from wikitrend.config import Settings


def test_settings_include_local_path_defaults(monkeypatch) -> None:
    for name in (
        "WIKITREND_DELTA_DIR",
        "WIKITREND_SERVING_DB",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_env()

    assert settings.delta_dir == Path("data/processed/delta")
    assert settings.serving_db == Path("data/processed/serving/wikitrend.duckdb")


def test_settings_read_local_path_environment_overrides(monkeypatch) -> None:
    monkeypatch.setenv("WIKITREND_DELTA_DIR", "data/delta")
    monkeypatch.setenv("WIKITREND_SERVING_DB", "data/serving/custom.duckdb")

    settings = Settings.from_env()

    assert settings.delta_dir == Path("data/delta")
    assert settings.serving_db == Path("data/serving/custom.duckdb")