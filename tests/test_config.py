from __future__ import annotations

from wikitrend.config import Settings


def test_settings_include_local_minio_defaults(monkeypatch) -> None:
    for name in (
        "WIKITREND_S3_ENDPOINT_URL",
        "WIKITREND_S3_REGION",
        "WIKITREND_S3_BUCKET",
        "WIKITREND_S3_ACCESS_KEY_ID",
        "WIKITREND_S3_SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_env()

    assert settings.s3_endpoint_url == "http://localhost:9000"
    assert settings.s3_region == "us-east-1"
    assert settings.s3_bucket == "wikitrend"
    assert settings.s3_access_key_id == "wikitrend"
    assert settings.s3_secret_access_key == "wikitrend-local-password"


def test_settings_read_minio_environment_overrides(monkeypatch) -> None:
    monkeypatch.setenv("WIKITREND_S3_ENDPOINT_URL", "http://object-store:9000")
    monkeypatch.setenv("WIKITREND_S3_REGION", "local")
    monkeypatch.setenv("WIKITREND_S3_BUCKET", "custom")
    monkeypatch.setenv("WIKITREND_S3_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("WIKITREND_S3_SECRET_ACCESS_KEY", "secret")

    settings = Settings.from_env()

    assert settings.s3_endpoint_url == "http://object-store:9000"
    assert settings.s3_region == "local"
    assert settings.s3_bucket == "custom"
    assert settings.s3_access_key_id == "access"
    assert settings.s3_secret_access_key == "secret"
