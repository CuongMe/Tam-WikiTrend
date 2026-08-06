from __future__ import annotations

import os
from pathlib import Path

import duckdb
import streamlit as st

GOLD_DIR = Path(os.getenv("WIKITREND_GOLD_DIR", "data/gold"))


def table_glob(table_name: str) -> str:
    return str(GOLD_DIR / table_name / "**" / "*.parquet").replace("\\", "/")


def table_exists(table_name: str) -> bool:
    path = GOLD_DIR / table_name
    return path.exists() and any(path.rglob("*.parquet"))


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


st.set_page_config(page_title="WikiTrend", layout="wide")
st.title("WikiTrend")

with st.sidebar:
    st.caption("Local Wikimedia content-demand lakehouse")
    limit = st.slider("Rows", min_value=25, max_value=500, value=100, step=25)
    st.write("Gold directory")
    st.code(str(GOLD_DIR))

tabs = st.tabs(["Top pages", "Trending", "Anomalies", "Forecasts", "Pipeline"])

with tabs[0]:
    df = query_table("top_pages_hourly", "timestamp_hour DESC, rank ASC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[1]:
    df = query_table("trending_pages", "timestamp_hour DESC, trend_score DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[2]:
    df = query_table("anomaly_alerts", "timestamp_hour DESC, robust_z_score DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[3]:
    df = query_table("forecast_features", "timestamp_hour DESC", limit)
    st.dataframe(df if df is not None else [], use_container_width=True)

with tabs[4]:
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
