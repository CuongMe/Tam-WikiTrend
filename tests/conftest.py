from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run Spark and local database integration tests.",
    )
    parser.addoption(
        "--run-kafka-integration",
        action="store_true",
        default=False,
        help="Run tests that require Kafka on localhost:9094.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = config.getoption("--run-integration")
    run_kafka = config.getoption("--run-kafka-integration")
    skip_integration = pytest.mark.skip(reason="use --run-integration")
    skip_kafka = pytest.mark.skip(reason="use --run-kafka-integration with a running broker")
    for item in items:
        if "integration" in item.keywords and not run_integration:
            item.add_marker(skip_integration)
        if "kafka_integration" in item.keywords and not run_kafka:
            item.add_marker(skip_kafka)
