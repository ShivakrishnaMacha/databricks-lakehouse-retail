"""Pure-python / pandas transformation helpers.

The Spark pipeline (src/bronze.py, src/silver.py, src/gold.py) implements the
same logic with PySpark + Delta Lake SQL. These helpers exist so the core
logic (SCD Type 2 merges, data-quality quarantine, gold aggregations) can be
unit-tested without a JVM, and so data/generate.py can produce the demo
gold/*.parquet exports when Spark is unavailable.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------
def scd2_merge(
    current_rows: list[dict],
    incoming_rows: list[dict],
    key: str = "product_id",
    tracked: tuple[str, ...] = ("unit_price",),
) -> list[dict]:
    """Apply Slowly Changing Dimension Type 2 changes.

    current_rows: dicts with `key`, tracked cols, effective_from/effective_to
        (ISO date strings, effective_to None when current), is_current bool.
    incoming_rows: dicts with `key`, tracked cols, effective_date (ISO str).
    Returns the new full dimension (history preserved).
    """
    rows = [dict(r) for r in current_rows]

    # Keep only the latest incoming row per key.
    latest: dict[str, dict] = {}
    for r in incoming_rows:
        k = r[key]
        if k not in latest or r["effective_date"] > latest[k]["effective_date"]:
            latest[k] = r

    for k in sorted(latest):
        inc = latest[k]
        eff = date.fromisoformat(inc["effective_date"])
        cur = next((r for r in rows if r[key] == k and r.get("is_current")), None)
        if cur is None:
            # Brand-new dimension member.
            rows.append(
                {
                    key: k,
                    **{c: inc[c] for c in tracked},
                    "effective_from": inc["effective_date"],
                    "effective_to": None,
                    "is_current": True,
                }
            )
        elif any(cur.get(c) != inc[c] for c in tracked):
            # Tracked attribute changed: close the old version, open a new one.
            cur["effective_to"] = (eff - timedelta(days=1)).isoformat()
            cur["is_current"] = False
            rows.append(
                {
                    key: k,
                    **{c: inc[c] for c in tracked},
                    "effective_from": inc["effective_date"],
                    "effective_to": None,
                    "is_current": True,
                }
            )
        # else: no change — keep the current row as-is.
    return rows


# ---------------------------------------------------------------------------
# Data-quality quarantine
# ---------------------------------------------------------------------------
def quarantine_orders(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split raw order rows into (valid, quarantined).

    Quarantine rules: order_id present, product_id present, quantity is a
    positive integer, unit_price >= 0, discount in [0, 1).
    Quarantined rows carry a `_dq_reason` field.
    """
    valid: list[dict] = []
    bad: list[dict] = []
    for r in rows:
        reasons: list[str] = []
        if not r.get("order_id"):
            reasons.append("missing order_id")
        if not r.get("product_id"):
            reasons.append("missing product_id")
        try:
            qty = int(r.get("quantity"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            qty = None
        if qty is None or qty <= 0:
            reasons.append("bad quantity")
        try:
            price = float(r.get("unit_price"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            price = None
        if price is None or price < 0:
            reasons.append("bad unit_price")
        try:
            disc = float(r.get("discount", 0))
        except (TypeError, ValueError):
            disc = None
        if disc is None or not (0 <= disc < 1):
            reasons.append("bad discount")
        if reasons:
            bad.append({**r, "_dq_reason": "; ".join(reasons)})
        else:
            valid.append(r)
    return valid, bad


# ---------------------------------------------------------------------------
# Gold aggregations (pandas)
# ---------------------------------------------------------------------------
def _with_line_revenue(orders: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["line_revenue"] = (
        df["quantity"].astype(float) * df["unit_price"].astype(float) * (1 - df["discount"].astype(float))
    )
    return df


def daily_sales_frame(orders: pd.DataFrame) -> pd.DataFrame:
    """Gold mart: one row per day with revenue, units, order count."""
    df = _with_line_revenue(orders)
    g = (
        df.groupby(df["order_date"].dt.date)
        .agg(orders=("order_id", "nunique"), units=("quantity", "sum"), revenue=("line_revenue", "sum"))
        .reset_index()
        .rename(columns={"order_date": "sale_date"})
        .sort_values("sale_date")
        .reset_index(drop=True)
    )
    g["sale_date"] = pd.to_datetime(g["sale_date"])
    return g[["sale_date", "orders", "units", "revenue"]]


def category_performance_frame(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Gold mart: revenue/units/orders by category with revenue share."""
    df = _with_line_revenue(orders).merge(products[["product_id", "category"]], on="product_id", how="left")
    g = (
        df.groupby("category")
        .agg(revenue=("line_revenue", "sum"), units=("quantity", "sum"), orders=("order_id", "nunique"))
        .reset_index()
    )
    g["avg_order_value"] = g["revenue"] / g["orders"].replace(0, float("nan"))
    g["revenue_share"] = g["revenue"] / g["revenue"].sum()
    return g.sort_values("revenue", ascending=False).reset_index(drop=True)


def inventory_health_frame(
    inventory: pd.DataFrame,
    orders: pd.DataFrame,
    products: pd.DataFrame,
    lookback_days: int = 14,
) -> pd.DataFrame:
    """Gold mart: per-SKU stock health with days-of-cover and risk flag."""
    inv = inventory.copy()
    inv["snapshot_date"] = pd.to_datetime(inv["snapshot_date"])
    latest_date = inv["snapshot_date"].max()
    latest = inv[inv["snapshot_date"] == latest_date][["product_id", "units_on_hand"]]

    ord_df = orders.copy()
    ord_df["order_date"] = pd.to_datetime(ord_df["order_date"])
    cutoff = latest_date - pd.Timedelta(days=lookback_days)
    recent = ord_df[ord_df["order_date"] >= cutoff].groupby("product_id", as_index=False)["quantity"].sum()
    recent["avg_daily_units"] = recent["quantity"] / lookback_days

    h = latest.merge(recent[["product_id", "avg_daily_units"]], on="product_id", how="left")
    h["avg_daily_units"] = h["avg_daily_units"].fillna(0.0)
    cover = h["units_on_hand"] / h["avg_daily_units"].replace(0, float("nan"))
    h["days_of_cover"] = cover.fillna(999.0).round(1)
    h["risk"] = pd.cut(
        h["days_of_cover"],
        bins=[-0.1, 7.0, 14.0, float("inf")],
        labels=["high", "watch", "healthy"],
    ).astype(str)
    h = h.merge(products[["product_id", "name", "category"]], on="product_id", how="left")
    return h.sort_values("days_of_cover").reset_index(drop=True)[
        ["product_id", "name", "category", "units_on_hand", "avg_daily_units", "days_of_cover", "risk"]
    ]
