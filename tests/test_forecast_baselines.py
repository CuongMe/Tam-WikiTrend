from __future__ import annotations

from models.forecast_baselines import (
    evaluate_forecast,
    previous_hour_forecast,
    rolling_average_forecast,
    same_hour_previous_day_forecast,
)


def test_previous_hour_forecast() -> None:
    assert previous_hour_forecast([10, 20, 30]) == [None, 10, 20]


def test_rolling_average_forecast() -> None:
    assert rolling_average_forecast([10, 20, 30], window=2) == [None, 10, 15]


def test_same_hour_previous_day_forecast() -> None:
    values = list(range(30))
    forecast = same_hour_previous_day_forecast(values)
    assert forecast[23] is None
    assert forecast[24] == 0
    assert forecast[29] == 5


def test_evaluate_forecast() -> None:
    metrics = evaluate_forecast([10, 20, 30], [None, 18, 33])
    assert round(metrics.mase, 2) == 0.25
    assert round(metrics.nd, 2) == 0.10
    assert round(metrics.smape, 2) == 0.10
    assert round(metrics.msmape, 2) == 0.10


def test_zero_traffic_is_robust() -> None:
    metrics = evaluate_forecast([0, 0], [None, 0])
    assert metrics.mase is None
    assert metrics.nd is None
    assert metrics.smape == 0.0
    assert metrics.msmape == 0.0
