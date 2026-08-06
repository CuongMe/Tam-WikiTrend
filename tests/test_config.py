from __future__ import annotations

from wikitrend.config import Settings


def test_default_settings() -> None:
    settings = Settings.from_env()
    assert settings.start_date.isoformat() == "2026-01-01"
    assert settings.end_date.isoformat() == "2026-01-07"
    assert "en" in settings.project_allowlist

