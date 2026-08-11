from __future__ import annotations

import os
from pathlib import Path

import duckdb
import streamlit as st

GOLD_DIR = Path(os.getenv("WIKITREND_GOLD_DIR", "data/gold"))
SERVING_DB = Path(os.getenv("WIKITREND_SERVING_DB", "data/serving/wikitrend.duckdb"))


def table_glob(table_name: str) -> str:
    return str(GOLD_DIR / table_name / "**" / "*.parquet").replace("\\", "/")


def table_exists(table_name: str) -> bool:
    path = GOLD_DIR / table_name
    return path.exists() and any(path.rglob("*.parquet"))


def serving_table_exists(table_name: str) -> bool:
    if not SERVING_DB.is_file():
        return False
    with duckdb.connect(str(SERVING_DB), read_only=True) as conn:
        return bool(
            conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
        )


@st.cache_data(ttl=60)
def query_table(table_name: str, order_by: str, limit: int):
    if not table_exists(table_name):
        return None
    sql = f"""
        SELECT *
        FROM read_parquet('{table_glob(table_name)}')
        ORDER BY {order_by}
        LIMIT ?
    """
    with duckdb.connect() as conn:
        return conn.execute(sql, [limit]).fetch_df()


@st.cache_data(ttl=60)
def query_serving(
    table_name: str,
    order_by: str,
    limit: int,
    project: str | None = None,
    access_mode: str | None = None,
):
    if not serving_table_exists(table_name):
        return None
    filters = []
    parameters = []
    if project and project != "All":
        filters.append("project = ?")
        parameters.append(project)
    if access_mode and access_mode != "All":
        filters.append("access_mode = ?")
        parameters.append(access_mode)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    parameters.append(limit)
    with duckdb.connect(str(SERVING_DB), read_only=True) as conn:
        return conn.execute(
            f"SELECT * FROM {table_name}{where} ORDER BY {order_by} LIMIT ?",
            parameters,
        ).fetch_df()


@st.cache_data(ttl=60)
def serving_dimensions():
    if not serving_table_exists("predictions"):
        return ["All"], ["All"]
    with duckdb.connect(str(SERVING_DB), read_only=True) as conn:
        projects = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT project FROM predictions ORDER BY project"
            ).fetchall()
        ]
        modes = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT access_mode FROM predictions ORDER BY access_mode"
            ).fetchall()
        ]
    return ["All", *projects], ["All", *modes]


st.set_page_config(page_title="WikiTrend", layout="wide")
st.title("WikiTrend")

with st.sidebar:
    limit = st.slider("Rows", min_value=25, max_value=500, value=100, step=25)
    projects, access_modes = serving_dimensions()
    project = st.selectbox("Project", projects)
    access_mode = st.selectbox("Access mode", access_modes)

tabs = st.tabs(
    ["Top pages", "Trending", "Anomalies", "Predictions", "Metrics", "Pipeline"]
)

with tabs[0]:
    df = query_serving(
        "top_pages", "timestamp_hour DESC, rank ASC", limit, project, access_mode
    )
    if df is None:
        df = query_table("top_pages_hourly", "timestamp_hour DESC, rank ASC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[1]:
    df = query_serving(
        "trending", "timestamp_hour DESC, trend_score DESC", limit, project, access_mode
    )
    if df is None:
        df = query_table("trending_pages", "timestamp_hour DESC, trend_score DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[2]:
    df = query_serving(
        "anomalies",
        "timestamp_hour DESC, robust_z_score DESC",
        limit,
        project,
        access_mode,
    )
    if df is None:
        df = query_table("anomaly_alerts", "timestamp_hour DESC, robust_z_score DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[3]:
    df = query_serving(
        "predictions",
        "timestamp_hour DESC, predicted_traffic_rank ASC",
        limit,
        project,
        access_mode,
    )
    if df is None:
        df = query_table("forecast_features", "timestamp_hour DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[4]:
    metric_view = st.segmented_control(
        "Metric set", ["Traffic", "Ranking"], default="Traffic"
    )
    if metric_view == "Ranking":
        df = query_serving("ranking_metrics", "timestamp_hour DESC, k ASC", limit)
    else:
        df = query_serving(
            "forecast_metrics", "evaluation_start_hour DESC", limit
        )
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[5]:
    tables = [
        "page_hourly",
        "hourly_project_traffic",
        "top_pages_hourly",
        "trending_pages",
        "anomaly_alerts",
        "forecast_features",
        "forecast_evaluation",
    ]
    st.dataframe(
        [{"table": table, "available": table_exists(table)} for table in tables],
        use_container_width=True,
    )
