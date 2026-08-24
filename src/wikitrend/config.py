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
    serving_db: Path
    s3_endpoint_url: str
    s3_region: str
    s3_bucket: str
    s3_access_key_id: str
    s3_secret_access_key: str
    kafka_bootstrap_servers: str
    kafka_pageviews_topic: str

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
            serving_db=Path(
                os.getenv("WIKITREND_SERVING_DB", "data/processed/serving/wikitrend.duckdb")
            ),
            s3_endpoint_url=os.getenv("WIKITREND_S3_ENDPOINT_URL", "http://localhost:9000"),
            s3_region=os.getenv("WIKITREND_S3_REGION", "us-east-1"),
            s3_bucket=os.getenv("WIKITREND_S3_BUCKET", "wikitrend"),
            s3_access_key_id=os.getenv("WIKITREND_S3_ACCESS_KEY_ID", "wikitrend"),
            s3_secret_access_key=os.getenv(
                "WIKITREND_S3_SECRET_ACCESS_KEY",
                "wikitrend-local-password",
            ),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
            kafka_pageviews_topic=os.getenv("KAFKA_PAGEVIEWS_TOPIC", "wikitrend.pageviews"),
        )


def get_settings() -> Settings:
    settings = Settings.from_env()
    if settings.end_date < settings.start_date:
        msg = "WIKITREND_END_DATE must be greater than or equal to WIKITREND_START_DATE"
        raise ValueError(msg)
    return settings
