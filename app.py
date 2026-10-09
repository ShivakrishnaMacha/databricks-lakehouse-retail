"""Streamlit dashboard for the Databricks Lakehouse retail demo.

Reads ONLY the gold parquet exports (gold/*.parquet) with pandas/pyarrow —
no PySpark, no JVM needed at serve time. The exports are produced either by
the real Spark pipeline (src/gold.py) or by the pandas fallback in
data/generate.py.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent
GOLD = ROOT / "gold"

st.set_page_config(page_title="Databricks Lakehouse — Retail Demo", layout="wide")
st.title("🏬 Databricks Lakehouse — Retail Demo")
st.caption("Bronze → Silver → Gold medallion marts · Delta Lake · SCD Type 2 product dimension")


def _ensure_gold() -> None:
    """Build gold parquet exports on first launch (e.g. Streamlit Community Cloud).

    The real Spark pipeline (src/run.py) writes these from the Delta gold
    tables; on a fresh cloud instance we fall back to data/generate.py's
    pandas implementation — no JVM needed.
    """
    import subprocess
    import sys

    needed = ("daily_sales.parquet", "inventory_health.parquet", "category_performance.parquet")
    if all((GOLD / f).exists() for f in needed):
        return
    with st.spinner("First launch: generating retail data and gold marts (one-time, ~1 min)…"):
        subprocess.run([sys.executable, "data/generate.py"], cwd=ROOT, check=True)


@st.cache_data
def load_marts() -> dict[str, pd.DataFrame]:
    missing = [f for f in ("daily_sales.parquet", "inventory_health.parquet", "category_performance.parquet")
               if not (GOLD / f).exists()]
    if missing:
        st.error(f"Missing gold exports: {missing}. Run `python data/generate.py` (or `python -m src.run`) first.")
        st.stop()
    return {
        "daily": pd.read_parquet(GOLD / "daily_sales.parquet"),
        "health": pd.read_parquet(GOLD / "inventory_health.parquet"),
        "cat": pd.read_parquet(GOLD / "category_performance.parquet"),
    }


_ensure_gold()
marts = load_marts()
daily, health, cat = marts["daily"], marts["health"], marts["cat"]

# ---- KPI row ------------------------------------------------------------
total_revenue = float(daily["revenue"].sum())
total_orders = int(daily["orders"].sum())
avg_daily_units = float(daily["units"].sum() / max(len(daily), 1))
risky_skus = int((health["risk"] == "high").sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total revenue (90d)", f"${total_revenue:,.0f}")
c2.metric("Total orders", f"{total_orders:,}")
c3.metric("Avg daily units", f"{avg_daily_units:,.0f}")
c4.metric("SKUs at stockout risk", risky_skus)

# ---- daily revenue trend --------------------------------------------------
st.subheader("Daily revenue trend")
fig_trend = px.line(daily, x="sale_date", y="revenue", markers=True,
                    labels={"sale_date": "Date", "revenue": "Revenue ($)"})
fig_trend.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0))
st.plotly_chart(fig_trend, use_container_width=True)

# ---- category performance -------------------------------------------------
st.subheader("Category performance")
fig_cat = px.bar(cat, x="category", y="revenue", color="category",
                 hover_data={"units": True, "orders": True,
                             "avg_order_value": ":.2f", "revenue_share": ":.1%"},
                 labels={"category": "Category", "revenue": "Revenue ($)"})
fig_cat.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
st.plotly_chart(fig_cat, use_container_width=True)

# ---- stockout-risk table ----------------------------------------------------
st.subheader("Inventory health — stockout risk")
risk_filter = st.multiselect("Risk level", options=["high", "watch", "healthy"],
                             default=["high", "watch"])
view = health[health["risk"].isin(risk_filter)].copy()
view["days_of_cover"] = view["days_of_cover"].round(1)
view["avg_daily_units"] = view["avg_daily_units"].round(1)
st.dataframe(view[["product_id", "name", "category", "units_on_hand",
                   "avg_daily_units", "days_of_cover", "risk"]],
             use_container_width=True, hide_index=True)

with st.expander("How this demo is built"):
    st.markdown(
        "- **Bronze**: raw CSV extracts → Delta tables with schema enforcement "
        "(`src/bronze.py`; on Databricks: Auto Loader)\n"
        "- **Silver**: `MERGE INTO` CDC upserts on orders, SCD Type 2 product "
        "dimension on price changes, DQ quarantine table (`src/silver.py`; on "
        "Databricks: Delta Live Tables)\n"
        "- **Gold**: daily sales, inventory health, category performance marts "
        "(`src/gold.py`; served on Databricks via Databricks SQL)\n"
        "- **Time travel**: `DESCRIBE HISTORY` + `VERSION AS OF` queries in "
        "`notebooks/04_time_travel_and_quality.py`\n"
        "- **Unity Catalog** mapping: bronze/silver/gold schemas map to "
        "catalog schemas with governed table ACLs in production"
    )
