from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run Spark and local database integration tests.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = config.getoption("--run-integration")
    skip_integration = pytest.mark.skip(reason="use --run-integration")
    for item in items:
        if "integration" in item.keywords and not run_integration:
            item.add_marker(skip_integration)