# Model Card

## Model Scope

The first forecasting component uses laptop-friendly baselines:

- Previous-hour forecast
- Same-hour previous-day forecast
- Rolling average forecast

These baselines are explainable and robust for a 7-day dataset. A machine-learning
model can be added after the data pipeline is stable.

## Intended Use

Forecast next-hour demand for:

- High-volume pages
- Selected language/project aggregates
- Pages with enough hourly history

## Limitations

Seven days of hourly data gives only 168 observations per page. This is enough for
baseline forecasting and anomaly scoring, but it is not enough to train a reliable
general model for every Wikimedia page.

Trend and anomaly scoring uses a leakage-safe `robust_z_score`. It applies `log1p` to
traffic and compares each hour with the prior-window median and median absolute
deviation (MAD). Mean and standard deviation are retained as diagnostics only because
web traffic is spiky and heavy-tailed.

## Metrics

- MASE, scaled by the past-only one-step naive error
- ND, normalized absolute deviation against total actual traffic
- sMAPE, with zero/zero observations contributing zero
- msMAPE, using an epsilon-stabilized denominator for low-volume traffic

The evaluation uses only historical observations available before each forecast
timestamp, so the scaling and error calculations do not use future traffic.
