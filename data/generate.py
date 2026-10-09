#!/usr/bin/env python3
"""Seeded synthetic retail data generator (seed=42).

Writes raw extracts to data/raw/:
  products.csv               — product_id, name, category
  product_price_history.csv  — product_id, effective_date, unit_price (2-3 changes per product)
  orders.csv                 — order_id, product_id, customer_id, order_date, quantity, unit_price, discount
  orders_updates.csv         — CDC-style updates to existing orders (order_id, ..., updated_at)
  inventory_snapshots.csv    — snapshot_date, product_id, units_on_hand, units_reserved

DEMO-DATA FALLBACK: the Streamlit demo (app.py) reads gold/*.parquet. When the
real Spark pipeline (src/run.py) runs, src/gold.py writes those exports from
the Delta gold tables. If PySpark/Java is unavailable, this script ALSO
computes the same gold marts in pandas (via src/transforms.py) and writes
gold/daily_sales.parquet, gold/inventory_health.parquet and
gold/category_performance.parquet directly, so the demo works either way.
"""
from __future__ import annotations

import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # so `from src.transforms import ...` works standalone
RAW = ROOT / "data" / "raw"
GOLD = ROOT / "gold"

SEED = 42
START = date(2026, 7, 1)
DAYS = 90
N_ORDERS = 15000

CATEGORIES = {
    "Electronics": (["Aurora Wireless Headphones", "Volt Power Bank 20K", "Nimbus Bluetooth Speaker",
                     "PixelPro Webcam 4K", "ChargeHub USB-C Dock", "EchoBuds Pro", "Lumen Desk Lamp"],
                    25, 220),
    "Apparel": (["Trailhead Hiking Jacket", "CloudSoft Hoodie", "Metro Slim Jeans",
                 "Drift Running Shoes", "Harbor Linen Shirt", "Summit Puffer Vest", "Atlas Cargo Pants"],
                20, 140),
    "Home & Kitchen": (["SearPro Cast Iron Skillet", "BrewMaster Pour-Over Set", "CrispAir Fryer XL",
                        "LinenWeave Duvet Set", "ChopCraft Knife Block", "GlowWick Candle Trio",
                        "StackStore Pantry Bins"],
                       15, 120),
    "Sports": (["Apex Yoga Mat Pro", "Torque Dumbbell Pair", "GlideFoam Roller",
                "PeakTrail Backpack 40L", "RapidDry Gym Towel", "CoreBand Set"],
              12, 90),
    "Beauty": (["VelvetMatte Lipstick Set", "HydraBoost Serum", "SilkFinish Foundation",
                "Botanic Clay Mask", "LashLift Mascara", "CitrusZest Body Oil"],
              8, 45),
    "Toys": (["BuildBot Robotics Kit", "StarMapper Telescope Jr", "PuzzleQuest 1000pc",
               "ZoomRacer RC Car", "CraftCastle Playset", "DinoDig Excavation Kit",
               "MarbleRun Deluxe"],
             10, 60),
}


def _price_on(product_history: dict[str, list[tuple[date, float]]], pid: str, day: date) -> float:
    price = product_history[pid][0][1]
    for eff, p in product_history[pid]:
        if eff <= day:
            price = p
        else:
            break
    return price


def main() -> None:
    rng = random.Random(SEED)
    RAW.mkdir(parents=True, exist_ok=True)
    GOLD.mkdir(parents=True, exist_ok=True)

    # ---- products -------------------------------------------------------
    products: list[dict] = []
    pid_seq = 0
    for cat, (names, lo, hi) in CATEGORIES.items():
        for name in names:
            pid_seq += 1
            pid = f"P{pid_seq:04d}"
            products.append({
                "product_id": pid,
                "name": name,
                "category": cat,
                "base_price": round(rng.uniform(lo, hi), 2),
                "velocity": rng.uniform(0.5, 2.0),  # relative demand weight
            })
    # Trim/pad to exactly 40 products
    products = products[:40]

    # A few SKUs are deliberately lean (hot sellers, thin replenishment) so the
    # demo shows genuine stockout risk. Velocity is boosted BEFORE orders are
    # generated so these SKUs actually sell faster.
    lean = set(rng.sample([p["product_id"] for p in products], 8))
    for p in products:
        if p["product_id"] in lean:
            p["velocity"] = rng.uniform(2.0, 3.0)

    # ---- price history: 2-3 changes per product over the 90 days ---------
    price_history: dict[str, list[tuple[date, float]]] = {}
    price_rows: list[dict] = []
    for p in products:
        n_changes = rng.choice([2, 2, 3])
        eff_dates = sorted(rng.sample(range(5, 86), n_changes))
        hist = [(START, p["base_price"])]
        for d in eff_dates:
            new_price = round(p["base_price"] * rng.uniform(0.85, 1.25), 2)
            hist.append((START + timedelta(days=d), new_price))
        price_history[p["product_id"]] = hist
        for eff, pr in hist:
            price_rows.append({"product_id": p["product_id"],
                               "effective_date": eff.isoformat(),
                               "unit_price": pr})

    # ---- orders: ~15k over 90 days, weekend uplift -----------------------
    weights = [p["velocity"] for p in products]
    orders: list[dict] = []
    for i in range(1, N_ORDERS + 1):
        day_offset = rng.randrange(DAYS)
        day = START + timedelta(days=day_offset)
        # weekend uplift: resample once with higher probability on Sat/Sun
        if day.weekday() < 5 and rng.random() < 0.25:
            day_offset = rng.choice([d for d in range(DAYS)
                                     if (START + timedelta(days=d)).weekday() >= 5])
            day = START + timedelta(days=day_offset)
        p = rng.choices(products, weights=weights, k=1)[0]
        qty = rng.choices([1, 2, 3, 4, 5], weights=[60, 25, 10, 3, 2])[0]
        price = _price_on(price_history, p["product_id"], day)
        discount = rng.choices([0.0, 0.05, 0.10, 0.15], weights=[55, 20, 15, 10])[0]
        orders.append({
            "order_id": f"ORD-{i:06d}",
            "product_id": p["product_id"],
            "customer_id": f"C{rng.randint(1, 3000):05d}",
            "order_date": day.isoformat(),
            "quantity": qty,
            "unit_price": price,
            "discount": discount,
        })

    # ---- CDC updates: 300 existing orders get corrected -----------------
    updates: list[dict] = []
    for upd in rng.sample(orders, 300):
        new_qty = max(1, upd["quantity"] + rng.choice([-1, 1]))
        new_disc = rng.choices([0.0, 0.05, 0.10, 0.15], weights=[55, 20, 15, 10])[0]
        updates.append({
            "order_id": upd["order_id"],
            "product_id": upd["product_id"],
            "customer_id": upd["customer_id"],
            "order_date": upd["order_date"],
            "quantity": new_qty,
            "unit_price": upd["unit_price"],
            "discount": new_disc,
            "updated_at": (date.fromisoformat(upd["order_date"]) + timedelta(days=rng.randint(1, 5))).isoformat(),
        })

    # ---- inventory: daily snapshots with replenishment ------------------
    inv_state: dict[str, dict] = {}
    for p in products:
        pid = p["product_id"]
        inv_state[pid] = {
            "on_hand": rng.randint(60, 110) if pid in lean else rng.randint(250, 700),
            "reorder_point": rng.randint(30, 60) if pid in lean else rng.randint(60, 120),
            "replenish": rng.randint(100, 160) if pid in lean else rng.randint(300, 600),
        }
    sold_by_day: dict[tuple[str, str], int] = {}
    for o in orders:
        k = (o["product_id"], o["order_date"])
        sold_by_day[k] = sold_by_day.get(k, 0) + o["quantity"]

    inv_rows: list[dict] = []
    for d in range(DAYS):
        day = START + timedelta(days=d)
        iso = day.isoformat()
        for p in products:
            pid = p["product_id"]
            st = inv_state[pid]
            st["on_hand"] -= sold_by_day.get((pid, iso), 0)
            if st["on_hand"] < st["reorder_point"]:
                st["on_hand"] += st["replenish"]  # inbound shipment arrives
            reserved = rng.randint(0, min(20, max(0, st["on_hand"])))
            inv_rows.append({"snapshot_date": iso, "product_id": pid,
                             "units_on_hand": st["on_hand"], "units_reserved": reserved})

    # ---- write raw extracts ---------------------------------------------
    def _write_csv(path: Path, rows: list[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    _write_csv(RAW / "products.csv",
               [{k: p[k] for k in ("product_id", "name", "category")} for p in products])
    _write_csv(RAW / "product_price_history.csv", price_rows)
    _write_csv(RAW / "orders.csv", orders)
    _write_csv(RAW / "orders_updates.csv", updates)
    _write_csv(RAW / "inventory_snapshots.csv", inv_rows)
    print(f"raw: {len(products)} products, {len(price_rows)} price rows, "
          f"{len(orders)} orders, {len(updates)} cdc updates, {len(inv_rows)} inventory snapshots")

    # ---- demo-data fallback: gold marts in pandas ------------------------
    # The real Spark pipeline (src/gold.py) overwrites these from Delta gold
    # tables. This keeps the Streamlit demo working without a JVM.
    from src.transforms import (category_performance_frame, daily_sales_frame,
                                inventory_health_frame)

    orders_df = pd.DataFrame(orders)
    products_df = pd.DataFrame([{k: p[k] for k in ("product_id", "name", "category")} for p in products])
    inv_df = pd.DataFrame(inv_rows)

    daily_sales_frame(orders_df).to_parquet(GOLD / "daily_sales.parquet", index=False)
    inventory_health_frame(inv_df, orders_df, products_df).to_parquet(GOLD / "inventory_health.parquet", index=False)
    category_performance_frame(orders_df, products_df).to_parquet(GOLD / "category_performance.parquet", index=False)
    print(f"gold/*.parquet written (pandas fallback): "
          f"{len(pd.read_parquet(GOLD / 'daily_sales.parquet'))} daily rows, "
          f"{len(pd.read_parquet(GOLD / 'inventory_health.parquet'))} sku rows, "
          f"{len(pd.read_parquet(GOLD / 'category_performance.parquet'))} category rows")


if __name__ == "__main__":
    main()
