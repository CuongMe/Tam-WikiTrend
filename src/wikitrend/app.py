from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
SERVING_DB = ROOT / "data" / "processed" / "serving" / "wikitrend.duckdb"
GOLD_VALIDATION_REPORT = (
    ROOT / "data" / "processed" / "validation" / "gold_pageviews_validation.json"
)


st.set_page_config(
    page_title="WikiTrend Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }
    [data-testid="stMetric"] {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 0.85rem 1rem;
    }
    [data-testid="stMetricLabel"] {
        color: #475569;
    }
    h1, h2, h3 {
        letter-spacing: 0;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


FORECAST_METRIC_COLUMNS = [
    "project",
    "access_mode",
    "model",
    "folds",
    "observations",
    "mdae",
    "mase",
    "rmase",
    "mdape",
    "mdsmape",
]
FORECAST_BACKTEST_COLUMNS = [
    "fold_id",
    "horizon_step",
    "timestamp_utc",
    "project",
    "access_mode",
    "model",
    "y_true",
    "y_pred",
    "mase_scale",
]
FORECAST_FUTURE_COLUMNS = [
    "generated_at_utc",
    "horizon_step",
    "timestamp_utc",
    "project",
    "access_mode",
    "model",
    "yhat",
]


def compact_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "0"
    absolute = abs(float(value))
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:,.0f}"


def format_ratio(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.3f}"


def empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def relation_exists(con, schema_name: str, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [schema_name, table_name],
        ).fetchone()[0]
    )


@st.cache_data(show_spinner=False)
def load_dashboard_data(database_path: str, validation_report_path: str) -> dict[str, pd.DataFrame]:
    db_path = Path(database_path)
    report_path = Path(validation_report_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Serving database is missing: {db_path}")
    if not report_path.exists():
        raise FileNotFoundError(f"Gold validation report is missing: {report_path}")

    validation_payload = json.loads(report_path.read_text(encoding="utf-8-sig"))
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        hourly = con.execute(
            """
            select
                cast(date as date) as date,
                cast(date as timestamp) + hour * interval '1 hour' as timestamp_utc,
                hour,
                project,
                access_mode,
                page_rows,
                total_views,
                total_response_size,
                max_page_views,
                avg_page_views,
                approx_distinct_pages
            from gold.hourly_project_access
            order by timestamp_utc, project, access_mode
            """
        ).fetchdf()
        daily = con.execute(
            """
            select *
            from gold.daily_project_access
            order by date, project, access_mode
            """
        ).fetchdf()
        top_pages = con.execute(
            """
            select
                cast(date as date) as date,
                cast(date as timestamp) + hour * interval '1 hour' as timestamp_utc,
                hour,
                project,
                access_mode,
                source_project,
                normalized_title,
                page_title,
                view_count,
                response_size,
                rank_in_hour
            from gold.top_pages_hourly
            order by timestamp_utc, project, access_mode, rank_in_hour
            """
        ).fetchdf()
        inventory = con.execute(
            """
            select table_name, view_name, grain, row_count
            from metadata.gold_table_inventory
            order by table_name
            """
        ).fetchdf()
        serving_build = con.execute("select * from metadata.serving_build").fetchdf()

        if relation_exists(con, "forecast", "forecast_metrics"):
            forecast_metrics = con.execute(
                """
                select *
                from forecast.forecast_metrics
                order by project, access_mode, rmase, model
                """
            ).fetchdf()
            forecast_backtest = con.execute(
                """
                select *
                from forecast.forecast_backtest_predictions
                order by timestamp_utc, project, access_mode, model, fold_id, horizon_step
                """
            ).fetchdf()
            forecast_future = con.execute(
                """
                select *
                from forecast.forecast_future
                order by timestamp_utc, project, access_mode, model, horizon_step
                """
            ).fetchdf()
            forecast_inventory = con.execute(
                """
                select table_name, view_name, grain, row_count
                from metadata.forecast_table_inventory
                order by table_name
                """
            ).fetchdf()
        else:
            forecast_metrics = empty_frame(FORECAST_METRIC_COLUMNS)
            forecast_backtest = empty_frame(FORECAST_BACKTEST_COLUMNS)
            forecast_future = empty_frame(FORECAST_FUTURE_COLUMNS)
            forecast_inventory = empty_frame(["table_name", "view_name", "grain", "row_count"])
    finally:
        con.close()

    hourly["date"] = pd.to_datetime(hourly["date"]).dt.date
    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    top_pages["date"] = pd.to_datetime(top_pages["date"]).dt.date
    for frame in (forecast_backtest, forecast_future):
        if not frame.empty:
            frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"])
    validation = pd.DataFrame(
        [
            {"metric": "status", "value": validation_payload.get("status")},
            {"metric": "errors", "value": len(validation_payload.get("errors", []))},
            {"metric": "warnings", "value": len(validation_payload.get("warnings", []))},
        ]
    )

    return {
        "hourly": hourly,
        "daily": daily,
        "top_pages": top_pages,
        "inventory": inventory,
        "serving_build": serving_build,
        "validation": validation,
        "forecast_metrics": forecast_metrics,
        "forecast_backtest": forecast_backtest,
        "forecast_future": forecast_future,
        "forecast_inventory": forecast_inventory,
    }


def filter_frames(
    hourly: pd.DataFrame,
    daily: pd.DataFrame,
    top_pages: pd.DataFrame,
    projects: list[str],
    access_modes: list[str],
    date_range: tuple,
    hour_range: tuple[int, int],
    rank_cap: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    start_date, end_date = date_range
    hourly_mask = (
        hourly["project"].isin(projects)
        & hourly["access_mode"].isin(access_modes)
        & hourly["date"].between(start_date, end_date)
        & hourly["hour"].between(hour_range[0], hour_range[1])
    )
    daily_mask = (
        daily["project"].isin(projects)
        & daily["access_mode"].isin(access_modes)
        & daily["date"].between(start_date, end_date)
    )
    top_mask = (
        top_pages["project"].isin(projects)
        & top_pages["access_mode"].isin(access_modes)
        & top_pages["date"].between(start_date, end_date)
        & top_pages["hour"].between(hour_range[0], hour_range[1])
        & (top_pages["rank_in_hour"] <= rank_cap)
    )
    return (
        hourly.loc[hourly_mask].copy(),
        daily.loc[daily_mask].copy(),
        top_pages.loc[top_mask].copy(),
    )


def filter_forecast_frames(
    metrics: pd.DataFrame,
    backtest: pd.DataFrame,
    future: pd.DataFrame,
    projects: list[str],
    access_modes: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if metrics.empty:
        return metrics.copy(), backtest.copy(), future.copy()
    metrics_mask = (metrics["project"] == "__all__") | (
        metrics["project"].isin(projects) & metrics["access_mode"].isin(access_modes)
    )
    backtest_mask = backtest["project"].isin(projects) & backtest["access_mode"].isin(access_modes)
    future_mask = future["project"].isin(projects) & future["access_mode"].isin(access_modes)
    return (
        metrics.loc[metrics_mask].copy(),
        backtest.loc[backtest_mask].copy(),
        future.loc[future_mask].copy(),
    )


def line_chart(
    data: pd.DataFrame,
    *,
    x: str,
    y: str,
    color: str | None = None,
    title: str,
    height: int = 320,
) -> alt.Chart:
    chart = (
        alt.Chart(data)
        .mark_line(point=False)
        .encode(
            x=alt.X(x, title="UTC hour"),
            y=alt.Y(y, title="views"),
            tooltip=list(data.columns),
        )
    )
    if color:
        chart = chart.encode(color=alt.Color(color, title="segment"))
    return chart.properties(title=title, height=height)


def bar_chart(
    data: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    height: int = 320,
) -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X(x, title=""),
            y=alt.Y(y, sort="-x", title=""),
            tooltip=list(data.columns),
        )
        .properties(title=title, height=height)
    )


def forecast_backtest_chart(backtest: pd.DataFrame, selected_model: str) -> alt.Chart:
    model_backtest = backtest[backtest["model"] == selected_model].copy()
    chart_data = (
        model_backtest.groupby(["timestamp_utc", "project", "access_mode"], as_index=False)
        .agg(actual=("y_true", "median"), predicted=("y_pred", "median"))
        .sort_values("timestamp_utc")
    )
    chart_data["segment"] = chart_data["project"] + " / " + chart_data["access_mode"]
    long_data = chart_data.melt(
        id_vars=["timestamp_utc", "segment"],
        value_vars=["actual", "predicted"],
        var_name="series_type",
        value_name="views",
    )
    return (
        alt.Chart(long_data)
        .mark_line(point=False)
        .encode(
            x=alt.X("timestamp_utc:T", title="UTC hour"),
            y=alt.Y("views:Q", title="views"),
            color=alt.Color("segment:N", title="segment"),
            strokeDash=alt.StrokeDash("series_type:N", title="series"),
            tooltip=["timestamp_utc:T", "segment:N", "series_type:N", "views:Q"],
        )
        .properties(title=f"Backtest Actual vs Predicted: {selected_model}", height=360)
    )


def forecast_future_chart(future: pd.DataFrame, selected_model: str) -> alt.Chart:
    chart_data = future[future["model"] == selected_model].copy()
    chart_data["segment"] = chart_data["project"] + " / " + chart_data["access_mode"]
    return (
        alt.Chart(chart_data)
        .mark_line(point=True)
        .encode(
            x=alt.X("timestamp_utc:T", title="UTC hour"),
            y=alt.Y("yhat:Q", title="forecast views"),
            color=alt.Color("segment:N", title="segment"),
            tooltip=[
                "timestamp_utc:T",
                "segment:N",
                "model:N",
                "horizon_step:Q",
                "yhat:Q",
            ],
        )
        .properties(title=f"Future Forecast: {selected_model}", height=320)
    )


data = load_dashboard_data(str(SERVING_DB), str(GOLD_VALIDATION_REPORT))
hourly_all = data["hourly"]
daily_all = data["daily"]
top_pages_all = data["top_pages"]
forecast_metrics_all = data["forecast_metrics"]
forecast_backtest_all = data["forecast_backtest"]
forecast_future_all = data["forecast_future"]

min_date = hourly_all["date"].min()
max_date = hourly_all["date"].max()
available_projects = sorted(hourly_all["project"].unique().tolist())
available_access_modes = sorted(hourly_all["access_mode"].unique().tolist())

with st.sidebar:
    st.title("WikiTrend")
    st.caption("Serving database")
    selected_projects = st.multiselect("Projects", available_projects, default=available_projects)
    selected_access_modes = st.multiselect(
        "Access modes",
        available_access_modes,
        default=available_access_modes,
    )
    selected_dates = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    selected_hours = st.slider("UTC hours", 0, 23, (0, 23))
    rank_cap = st.slider("Top-page rank cap", 1, 100, 25)

if not isinstance(selected_dates, tuple) or len(selected_dates) != 2:
    selected_dates = (min_date, max_date)
if not selected_projects:
    selected_projects = available_projects
if not selected_access_modes:
    selected_access_modes = available_access_modes

hourly, daily, top_pages = filter_frames(
    hourly_all,
    daily_all,
    top_pages_all,
    selected_projects,
    selected_access_modes,
    selected_dates,
    selected_hours,
    rank_cap,
)
forecast_metrics, forecast_backtest, forecast_future = filter_forecast_frames(
    forecast_metrics_all,
    forecast_backtest_all,
    forecast_future_all,
    selected_projects,
    selected_access_modes,
)

st.title("WikiTrend Serving Dashboard")
st.caption("Validated Gold aggregates and forecast outputs served from DuckDB views")

total_views = int(hourly["total_views"].sum()) if not hourly.empty else 0
page_rows = int(hourly["page_rows"].sum()) if not hourly.empty else 0
observed_hours = int(hourly["timestamp_utc"].nunique()) if not hourly.empty else 0
segments = int(hourly[["project", "access_mode"]].drop_duplicates().shape[0])
avg_hourly_views = total_views / max(observed_hours, 1)
peak_row = hourly.sort_values("total_views", ascending=False).head(1)
peak_label = (
    f"{peak_row.iloc[0]['project']} / {peak_row.iloc[0]['access_mode']}"
    if not peak_row.empty
    else "n/a"
)

metric_cols = st.columns(5)
metric_cols[0].metric("Total Views", compact_number(total_views))
metric_cols[1].metric("Page Rows", compact_number(page_rows))
metric_cols[2].metric("Observed Hours", f"{observed_hours}")
metric_cols[3].metric("Segments", f"{segments}")
metric_cols[4].metric("Avg Hourly Views", compact_number(avg_hourly_views))

tabs = st.tabs(["Overview", "Segments", "Top Pages", "Forecasting", "Quality"])

with tabs[0]:
    trend = (
        hourly.groupby("timestamp_utc", as_index=False)
        .agg(total_views=("total_views", "sum"), page_rows=("page_rows", "sum"))
        .sort_values("timestamp_utc")
    )
    segment_trend = hourly.copy()
    segment_trend["segment"] = segment_trend["project"] + " / " + segment_trend["access_mode"]
    segment_summary = (
        hourly.groupby(["project", "access_mode"], as_index=False)
        .agg(
            total_views=("total_views", "sum"),
            page_rows=("page_rows", "sum"),
            avg_hourly_views=("total_views", "mean"),
            peak_hourly_views=("total_views", "max"),
        )
        .sort_values("total_views", ascending=False)
    )
    segment_summary["segment"] = segment_summary["project"] + " / " + segment_summary["access_mode"]

    left, right = st.columns([1.55, 1])
    with left:
        st.altair_chart(
            line_chart(trend, x="timestamp_utc:T", y="total_views:Q", title="Total Views by Hour"),
            use_container_width=True,
        )
    with right:
        st.altair_chart(
            bar_chart(segment_summary, x="total_views:Q", y="segment:N", title="Views by Segment"),
            use_container_width=True,
        )

    st.altair_chart(
        line_chart(
            segment_trend,
            x="timestamp_utc:T",
            y="total_views:Q",
            color="segment:N",
            title="Hourly Views by Project and Access Mode",
            height=360,
        ),
        use_container_width=True,
    )

with tabs[1]:
    segment_metrics = (
        hourly.groupby(["project", "access_mode"], as_index=False)
        .agg(
            hourly_observations=("timestamp_utc", "nunique"),
            page_rows=("page_rows", "sum"),
            total_views=("total_views", "sum"),
            avg_hourly_views=("total_views", "mean"),
            stddev_hourly_views=("total_views", "std"),
            min_hourly_views=("total_views", "min"),
            peak_hourly_views=("total_views", "max"),
            max_single_page_views=("max_page_views", "max"),
        )
        .sort_values("total_views", ascending=False)
    )
    segment_metrics["views_per_page_row"] = (
        segment_metrics["total_views"] / segment_metrics["page_rows"]
    )
    segment_metrics["coefficient_of_variation"] = (
        segment_metrics["stddev_hourly_views"] / segment_metrics["avg_hourly_views"]
    )
    segment_metrics["segment"] = segment_metrics["project"] + " / " + segment_metrics["access_mode"]

    mobile_mix = (
        daily.groupby(["project", "access_mode"], as_index=False)
        .agg(total_views=("total_views", "sum"))
        .pivot_table(index="project", columns="access_mode", values="total_views", fill_value=0)
        .reset_index()
    )
    for column in ("desktop", "mobile"):
        if column not in mobile_mix:
            mobile_mix[column] = 0
    mobile_mix["total_views"] = mobile_mix["desktop"] + mobile_mix["mobile"]
    mobile_mix["mobile_share"] = mobile_mix["mobile"] / mobile_mix["total_views"]
    mobile_mix["desktop_share"] = mobile_mix["desktop"] / mobile_mix["total_views"]
    mobile_mix = mobile_mix.sort_values("total_views", ascending=False)

    col_a, col_b = st.columns([1.2, 1])
    with col_a:
        st.dataframe(
            segment_metrics[
                [
                    "project",
                    "access_mode",
                    "total_views",
                    "avg_hourly_views",
                    "peak_hourly_views",
                    "coefficient_of_variation",
                    "views_per_page_row",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    with col_b:
        st.altair_chart(
            bar_chart(
                segment_metrics.sort_values("coefficient_of_variation", ascending=False),
                x="coefficient_of_variation:Q",
                y="segment:N",
                title="Hourly Volatility",
            ),
            use_container_width=True,
        )

    mobile_chart = (
        alt.Chart(mobile_mix)
        .mark_bar()
        .encode(
            x=alt.X("mobile_share:Q", title="mobile share"),
            y=alt.Y("project:N", sort="-x", title=""),
            tooltip=["project", "mobile", "desktop", "mobile_share", "desktop_share"],
        )
        .properties(title="Mobile Share by Project", height=260)
    )
    st.altair_chart(mobile_chart, use_container_width=True)

with tabs[2]:
    top_summary = (
        top_pages.groupby(
            ["project", "access_mode", "normalized_title", "page_title"], as_index=False
        )
        .agg(
            appearances=("rank_in_hour", "count"),
            total_top_views=("view_count", "sum"),
            best_rank=("rank_in_hour", "min"),
            max_hourly_views=("view_count", "max"),
            avg_hourly_views_when_ranked=("view_count", "mean"),
        )
        .sort_values(["total_top_views", "max_hourly_views"], ascending=False)
    )

    top_hourly = (
        top_pages.groupby(["date", "hour", "project", "access_mode"], as_index=False)
        .agg(top_rank_views=("view_count", "sum"))
        .merge(
            hourly[["date", "hour", "project", "access_mode", "total_views"]],
            on=["date", "hour", "project", "access_mode"],
            how="left",
        )
    )
    top_hourly["top_rank_share"] = top_hourly["top_rank_views"] / top_hourly["total_views"]
    concentration = (
        top_hourly.groupby(["project", "access_mode"], as_index=False)
        .agg(
            avg_top_rank_share=("top_rank_share", "mean"),
            max_top_rank_share=("top_rank_share", "max"),
            avg_top_rank_views=("top_rank_views", "mean"),
        )
        .sort_values("avg_top_rank_share", ascending=False)
    )
    concentration["segment"] = concentration["project"] + " / " + concentration["access_mode"]

    cols = st.columns([1.05, 1])
    with cols[0]:
        st.altair_chart(
            bar_chart(
                concentration,
                x="avg_top_rank_share:Q",
                y="segment:N",
                title=f"Average Share Captured by Top {rank_cap}",
            ),
            use_container_width=True,
        )
    with cols[1]:
        st.metric("Peak Segment", peak_label)
        st.metric("Top Rank Cap", f"{rank_cap}")
        st.metric("Ranked Page Rows", compact_number(len(top_pages)))

    st.dataframe(
        top_summary[
            [
                "project",
                "access_mode",
                "page_title",
                "appearances",
                "total_top_views",
                "best_rank",
                "max_hourly_views",
                "avg_hourly_views_when_ranked",
            ]
        ].head(100),
        use_container_width=True,
        hide_index=True,
    )

with tabs[3]:
    if forecast_metrics.empty:
        st.info(
            "Forecast views are not available in the serving database. "
            "Rebuild forecasts and then rebuild the serving database."
        )
    else:
        required_columns = {"mdae", "mase", "rmase", "mdape", "mdsmape"}
        if not required_columns.issubset(forecast_metrics.columns):
            st.error(
                "Forecast metrics need to be regenerated with the current median-grounded schema."
            )
        else:
            model_options = sorted(forecast_metrics["model"].dropna().unique().tolist())
            overall_metrics = forecast_metrics[forecast_metrics["project"] == "__all__"].copy()
            if overall_metrics.empty:
                overall_metrics = forecast_metrics.copy()
            leaderboard = overall_metrics.sort_values("rmase").reset_index(drop=True)
            best_model = leaderboard.iloc[0]["model"] if not leaderboard.empty else model_options[0]
            selected_model = st.selectbox(
                "Forecast model",
                model_options,
                index=model_options.index(best_model) if best_model in model_options else 0,
            )

            beat_count = int((leaderboard["rmase"] < 1.0).sum()) if not leaderboard.empty else 0
            best_row = leaderboard.head(1)
            forecast_cols = st.columns(4)
            forecast_cols[0].metric("Best Model", str(best_model))
            forecast_cols[1].metric(
                "Best RMASE",
                format_ratio(best_row.iloc[0]["rmase"] if not best_row.empty else None),
            )
            forecast_cols[2].metric("Models Beating Naive", f"{beat_count}")
            forecast_cols[3].metric("Forecast Rows", compact_number(len(forecast_future)))

            leaderboard_display = leaderboard.copy()
            leaderboard_display["beats_naive"] = leaderboard_display["rmase"] < 1.0
            leaderboard_chart = (
                alt.Chart(leaderboard_display)
                .mark_bar()
                .encode(
                    x=alt.X("rmase:Q", title="RMASE"),
                    y=alt.Y("model:N", sort="x", title=""),
                    color=alt.condition(
                        alt.datum.rmase < 1.0,
                        alt.value("#16a34a"),
                        alt.value("#64748b"),
                    ),
                    tooltip=["model", "mdae", "mase", "rmase", "mdape", "mdsmape", "beats_naive"],
                )
                .properties(title="Model Leaderboard by RMASE", height=260)
            )
            st.altair_chart(leaderboard_chart, use_container_width=True)
            st.dataframe(
                leaderboard_display[
                    [
                        "model",
                        "folds",
                        "observations",
                        "mdae",
                        "mase",
                        "rmase",
                        "mdape",
                        "mdsmape",
                        "beats_naive",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

            chart_cols = st.columns([1, 1])
            with chart_cols[0]:
                if forecast_backtest.empty:
                    st.info("No backtest predictions match the selected filters.")
                else:
                    st.altair_chart(
                        forecast_backtest_chart(forecast_backtest, selected_model),
                        use_container_width=True,
                    )
            with chart_cols[1]:
                if forecast_future.empty:
                    st.info("No future forecasts match the selected filters.")
                else:
                    st.altair_chart(
                        forecast_future_chart(forecast_future, selected_model),
                        use_container_width=True,
                    )

            series_metrics = forecast_metrics[
                (forecast_metrics["project"] != "__all__")
                & forecast_metrics["access_mode"].isin(selected_access_modes)
                & forecast_metrics["project"].isin(selected_projects)
            ].copy()
            if not series_metrics.empty:
                best_by_series = series_metrics.loc[
                    series_metrics.groupby(["project", "access_mode"])["rmase"].idxmin()
                ].sort_values(["project", "access_mode"])
                st.dataframe(
                    best_by_series[
                        [
                            "project",
                            "access_mode",
                            "model",
                            "mdae",
                            "mase",
                            "rmase",
                            "mdape",
                            "mdsmape",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

with tabs[4]:
    quality_cols = st.columns(3)
    validation = data["validation"]
    validation_status = validation.loc[validation["metric"] == "status", "value"].iloc[0]
    validation_errors = validation.loc[validation["metric"] == "errors", "value"].iloc[0]
    validation_warnings = validation.loc[validation["metric"] == "warnings", "value"].iloc[0]
    quality_cols[0].metric("Gold Validation", str(validation_status).upper())
    quality_cols[1].metric("Validation Errors", str(validation_errors))
    quality_cols[2].metric("Validation Warnings", str(validation_warnings))

    st.dataframe(data["inventory"], use_container_width=True, hide_index=True)
    if not data["forecast_inventory"].empty:
        st.dataframe(data["forecast_inventory"], use_container_width=True, hide_index=True)
    st.dataframe(data["serving_build"], use_container_width=True, hide_index=True)
