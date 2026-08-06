from __future__ import annotations

from dataclasses import dataclass
from math import fabs
from statistics import mean


@dataclass(frozen=True)
class ForecastMetrics:
    mase: float | None
    nd: float | None
    smape: float
    msmape: float


def previous_hour_forecast(values: list[float]) -> list[float | None]:
    if not values:
        return []
    return [None, *values[:-1]]


def rolling_average_forecast(values: list[float], window: int = 6) -> list[float | None]:
    forecasts: list[float | None] = []
    for index, _ in enumerate(values):
        history = values[max(0, index - window) : index]
        forecasts.append(mean(history) if history else None)
    return forecasts


def same_hour_previous_day_forecast(
    values: list[float], hours_per_day: int = 24
) -> list[float | None]:
    return [
        values[index - hours_per_day] if index >= hours_per_day else None
        for index in range(len(values))
    ]


def evaluate_forecast(
    actuals: list[float],
    forecasts: list[float | None],
    smape_epsilon: float = 1.0,
) -> ForecastMetrics:
    if smape_epsilon <= 0:
        raise ValueError("smape_epsilon must be positive")
    pairs = [
        (actual, forecast)
        for actual, forecast in zip(actuals, forecasts, strict=False)
        if forecast is not None
    ]
    if not pairs:
        return ForecastMetrics(mase=None, nd=None, smape=0.0, msmape=0.0)

    absolute_errors = [abs(actual - forecast) for actual, forecast in pairs]
    actual_values = [actual for actual, _ in pairs]
    naive_scale_values = [
        fabs(actuals[index] - actuals[index - 1]) for index in range(1, len(actuals))
    ]
    naive_scale = mean(naive_scale_values) if naive_scale_values else 0.0
    mase_values = [error / naive_scale for error in absolute_errors] if naive_scale > 0 else []
    actual_total = sum(abs(actual) for actual in actual_values)
    smape_values = []
    msmape_values = []
    for (actual, forecast), error in zip(pairs, absolute_errors, strict=True):
        denominator = abs(actual) + abs(forecast)
        smape_values.append(2 * error / denominator if denominator > 0 else 0.0)
        msmape_values.append(2 * error / max(denominator, smape_epsilon))
    return ForecastMetrics(
        mase=mean(mase_values) if mase_values else None,
        nd=sum(absolute_errors) / actual_total if actual_total > 0 else None,
        smape=mean(smape_values),
        msmape=mean(msmape_values),
    )
