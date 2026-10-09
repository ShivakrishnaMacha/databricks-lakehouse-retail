"""Unit tests for the lakehouse pipeline logic (no JVM required).

Tests target src/transforms.py — the pure-python/pandas implementation of
the SCD Type 2 merge, data-quality quarantine, and gold aggregations. The
Spark modules (src/silver.py, src/gold.py) implement the same logic with
PySpark + Delta Lake SQL.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.transforms import (  # noqa: E402
    category_performance_frame,
    daily_sales_frame,
    inventory_health_frame,
    quarantine_orders,
    scd2_merge,
)


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------
def _dim_row(pid, price, frm, to=None, current=True):
    return {"product_id": pid, "unit_price": price, "effective_from": frm,
            "effective_to": to, "is_current": current}


def test_scd2_price_change_opens_new_version():
    current = [_dim_row("P1", 10.0, "2026-07-01")]
    out = scd2_merge(current, [{"product_id": "P1", "unit_price": 12.0,
                                "effective_date": "2026-08-01"}])
    assert len(out) == 2
    old, new = sorted(out, key=lambda r: r["effective_from"])
    assert old["is_current"] is False
    assert old["effective_to"] == "2026-07-31"  # day before the new version
    assert new["unit_price"] == 12.0
    assert new["is_current"] is True
    assert new["effective_from"] == "2026-08-01"
    assert new["effective_to"] is None


def test_scd2_no_change_keeps_single_row():
    current = [_dim_row("P1", 10.0, "2026-07-01")]
    out = scd2_merge(current, [{"product_id": "P1", "unit_price": 10.0,
                                "effective_date": "2026-08-01"}])
    assert len(out) == 1
    assert out[0]["is_current"] is True
    assert out[0]["effective_to"] is None


def test_scd2_new_product_inserts_current_row():
    out = scd2_merge([], [{"product_id": "P9", "unit_price": 5.5,
                           "effective_date": "2026-07-15"}])
    assert len(out) == 1
    assert out[0]["product_id"] == "P9"
    assert out[0]["is_current"] is True


def test_scd2_keeps_latest_incoming_per_key():
    current = [_dim_row("P1", 10.0, "2026-07-01")]
    incoming = [
        {"product_id": "P1", "unit_price": 11.0, "effective_date": "2026-07-10"},
        {"product_id": "P1", "unit_price": 12.0, "effective_date": "2026-08-01"},
    ]
    out = scd2_merge(current, incoming)
    new_rows = [r for r in out if r["is_current"]]
    assert len(new_rows) == 1
    assert new_rows[0]["unit_price"] == 12.0  # latest wins


def test_scd2_multiple_price_changes_accumulate_history():
    rows = scd2_merge([], [{"product_id": "P1", "unit_price": 10.0, "effective_date": "2026-07-01"}])
    rows = scd2_merge(rows, [{"product_id": "P1", "unit_price": 12.0, "effective_date": "2026-08-01"}])
    rows = scd2_merge(rows, [{"product_id": "P1", "unit_price": 9.0, "effective_date": "2026-09-01"}])
    assert len(rows) == 3
    assert sum(r["is_current"] for r in rows) == 1
    assert [r["unit_price"] for r in sorted(rows, key=lambda r: r["effective_from"])] == [10.0, 12.0, 9.0]


# ---------------------------------------------------------------------------
# Data-quality quarantine
# ---------------------------------------------------------------------------
def _order(**kw):
    base = {"order_id": "O1", "product_id": "P1", "customer_id": "C1",
            "order_date": "2026-07-01", "quantity": 2, "unit_price": 10.0, "discount": 0.0}
    base.update(kw)
    return base


def test_quarantine_flags_bad_rows_with_reasons():
    rows = [
        _order(order_id="O1"),
        _order(order_id="O2", quantity=0),
        _order(order_id="O3", unit_price=-5.0),
        _order(order_id=None),
        _order(order_id="O5", discount=1.5),
    ]
    valid, bad = quarantine_orders(rows)
    assert [r["order_id"] for r in valid] == ["O1"]
    assert len(bad) == 4
    reasons = " | ".join(r["_dq_reason"] for r in bad)
    assert "bad quantity" in reasons
    assert "bad unit_price" in reasons
    assert "missing order_id" in reasons
    assert "bad discount" in reasons


def test_quarantine_passes_clean_rows():
    rows = [_order(order_id=f"O{i}") for i in range(5)]
    valid, bad = quarantine_orders(rows)
    assert len(valid) == 5 and bad == []


# ---------------------------------------------------------------------------
# Gold aggregations (pandas)
# ---------------------------------------------------------------------------
def _orders_df():
    return pd.DataFrame([
        {"order_id": "O1", "product_id": "P1", "order_date": "2026-07-01",
         "quantity": 2, "unit_price": 10.0, "discount": 0.0},
        {"order_id": "O2", "product_id": "P2", "order_date": "2026-07-01",
         "quantity": 1, "unit_price": 20.0, "discount": 0.10},
        {"order_id": "O3", "product_id": "P1", "order_date": "2026-07-02",
         "quantity": 3, "unit_price": 10.0, "discount": 0.0},
    ])


def _products_df():
    return pd.DataFrame([
        {"product_id": "P1", "name": "Alpha", "category": "Electronics"},
        {"product_id": "P2", "name": "Beta", "category": "Apparel"},
    ])


def test_daily_sales_aggregation():
    g = daily_sales_frame(_orders_df())
    assert len(g) == 2
    day1 = g[g["sale_date"] == pd.Timestamp("2026-07-01")].iloc[0]
    assert day1["orders"] == 2
    assert day1["units"] == 3
    assert day1["revenue"] == pytest.approx(2 * 10.0 + 1 * 20.0 * 0.9)


def test_category_performance_share_sums_to_one():
    g = category_performance_frame(_orders_df(), _products_df())
    assert g["revenue_share"].sum() == pytest.approx(1.0)
    assert set(g["category"]) == {"Electronics", "Apparel"}
    top = g.iloc[0]
    assert top["category"] == "Electronics"  # 50.0 revenue > 18.0


def test_inventory_health_risk_flags():
    inv = pd.DataFrame([
        {"snapshot_date": "2026-07-10", "product_id": "P1", "units_on_hand": 5},
        {"snapshot_date": "2026-07-10", "product_id": "P2", "units_on_hand": 500},
    ])
    orders = pd.DataFrame([
        {"order_id": f"O{i}", "product_id": "P1", "order_date": "2026-07-09",
         "quantity": 2, "unit_price": 10.0, "discount": 0.0} for i in range(7)
    ] + [
        {"order_id": "OX", "product_id": "P2", "order_date": "2026-07-09",
         "quantity": 1, "unit_price": 20.0, "discount": 0.0},
    ])
    h = inventory_health_frame(inv, orders, _products_df(), lookback_days=14)
    p1 = h[h["product_id"] == "P1"].iloc[0]
    p2 = h[h["product_id"] == "P2"].iloc[0]
    # P1: 5 units on hand, 1 unit/day avg -> 5 days cover -> high risk
    assert p1["risk"] == "high"
    assert p1["days_of_cover"] == pytest.approx(5.0)
    # P2: 500 units, tiny velocity -> healthy
    assert p2["risk"] == "healthy"
