from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from wikitrend.pageviews import DEFAULT_SOURCE_PROJECTS


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class Settings:
    env: str
    start_date: date
    end_date: date
    source_project_allowlist: tuple[str, ...]
    raw_dir: Path
    silver_dir: Path
    gold_dir: Path
    delta_dir: Path
    serving_db: Path
    forecast_dir: Path

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            env=os.getenv("WIKITREND_ENV", "local"),
            start_date=_parse_date(os.getenv("WIKITREND_START_DATE", "2026-01-01")),
            end_date=_parse_date(os.getenv("WIKITREND_END_DATE", "2026-01-03")),
            source_project_allowlist=_parse_csv(
                os.getenv(
                    "WIKITREND_SOURCE_PROJECT_ALLOWLIST",
                    ",".join(DEFAULT_SOURCE_PROJECTS),
                )
            ),
            raw_dir=Path(os.getenv("WIKITREND_RAW_DIR", "data/raw/pageviews")),
            silver_dir=Path(
                os.getenv("WIKITREND_SILVER_DIR", "data/processed/silver/pageviews")
            ),
            gold_dir=Path(os.getenv("WIKITREND_GOLD_DIR", "data/processed/gold")),
            delta_dir=Path(os.getenv("WIKITREND_DELTA_DIR", "data/processed/delta")),
            serving_db=Path(
                os.getenv("WIKITREND_SERVING_DB", "data/processed/serving/wikitrend.duckdb")
            ),
            forecast_dir=Path(os.getenv("WIKITREND_FORECAST_DIR", "data/processed/forecast")),
        )


def get_settings() -> Settings:
    settings = Settings.from_env()
    if settings.end_date < settings.start_date:
        msg = "WIKITREND_END_DATE must be greater than or equal to WIKITREND_START_DATE"
        raise ValueError(msg)
    return settings