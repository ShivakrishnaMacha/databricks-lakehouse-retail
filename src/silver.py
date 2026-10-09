#!/usr/bin/env python3
"""Silver layer: cleaned, conformed Delta tables.

- silver_orders: dedupe + data-quality quarantine, then MERGE INTO on
  order_id to apply CDC updates (latest update wins).
- silver_products_dim: SCD Type 2 product dimension — a new version row is
  opened whenever a product's price changes; the old row is closed with
  effective_to / is_current = false.
- silver_inventory: cleaned daily snapshots (non-negative stock), deduped.

Databricks mapping: this is the Delta Live Tables / DLT `APPLY CHANGES INTO`
pattern written as explicit MERGE statements so the logic is visible.
"""
from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number

from src.bronze import WAREHOUSE, get_spark

SILVER_ORDERS = f"{WAREHOUSE}/silver_orders"
SILVER_QUARANTINE = f"{WAREHOUSE}/silver_orders_quarantine"
SILVER_PRODUCTS = f"{WAREHOUSE}/silver_products_dim"
SILVER_INVENTORY = f"{WAREHOUSE}/silver_inventory"


def _load(spark: SparkSession, table: str):
    return spark.read.format("delta").load(f"{WAREHOUSE}/{table}")


def build_silver_orders(spark: SparkSession) -> dict[str, int]:
    base = _load(spark, "bronze_orders")
    updates = _load(spark, "bronze_order_updates")

    # Data-quality quarantine: rejects go to their own Delta table for triage.
    dq_filter = "order_id IS NOT NULL AND product_id IS NOT NULL AND quantity > 0 AND unit_price >= 0"
    quarantine = base.filter(f"NOT ({dq_filter})")
    q_count = quarantine.count()
    quarantine.write.format("delta").mode("overwrite").save(SILVER_QUARANTINE)

    # CDC staging: base rows + updates, keep the latest version per order_id.
    base_staged = base.filter(dq_filter).withColumn("updated_at", lit(None).cast("date"))
    staging = (
        base_staged.unionByName(updates.select(base_staged.columns))
        .withColumn("rn", row_number().over(Window.partitionBy("order_id").orderBy(col("updated_at").desc_nulls_last())))
        .filter("rn = 1")
        .drop("rn", "updated_at")
    )
    staged_count = staging.count()

    if DeltaTable.isDeltaTable(spark, SILVER_ORDERS):
        (
            DeltaTable.forPath(spark, SILVER_ORDERS)
            .alias("t")
            .merge(staging.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        staging.write.format("delta").mode("overwrite").save(SILVER_ORDERS)

    final_count = spark.read.format("delta").load(SILVER_ORDERS).count()
    print(f"silver.orders: staged={staged_count} quarantined={q_count} final={final_count}")
    return {"staged": staged_count, "quarantined": q_count, "final": final_count}


def build_silver_products_scd2(spark: SparkSession) -> dict[str, int]:
    products = _load(spark, "bronze_products")
    price_hist = _load(spark, "bronze_price_history")

    # Latest price per product = the incoming dimension snapshot.
    latest = (
        price_hist.join(products, "product_id")
        .withColumn("rn", row_number().over(Window.partitionBy("product_id").orderBy(col("effective_date").desc())))
        .filter("rn = 1")
        .drop("rn")
        .select("product_id", "name", "category", "unit_price", "effective_date")
    )

    if not DeltaTable.isDeltaTable(spark, SILVER_PRODUCTS):
        initial = latest.select(
            "product_id", "name", "category", "unit_price",
            col("effective_date").alias("effective_from"),
            lit(None).cast("date").alias("effective_to"),
            lit(True).alias("is_current"),
        )
        initial.write.format("delta").mode("overwrite").save(SILVER_PRODUCTS)
        n = initial.count()
        print(f"silver.products_dim: initial load {n} current rows")
        return {"current": n, "versions": n}

    dim = DeltaTable.forPath(spark, SILVER_PRODUCTS)

    # Step 1 — close current versions whose price changed.
    (
        dim.alias("t")
        .merge(latest.alias("s"), "t.product_id = s.product_id AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.unit_price <> s.unit_price",
            set={"effective_to": "date_sub(s.effective_date, 1)", "is_current": "false"},
        )
        .execute()
    )

    # Step 2 — insert new versions: brand-new products + changed prices.
    current = spark.read.format("delta").load(SILVER_PRODUCTS).filter("is_current = true")
    new_versions = (
        latest.alias("s")
        .join(
            current.select(col("product_id").alias("cp"), col("unit_price").alias("cur_price")),
            col("s.product_id") == col("cp"),
            "left",
        )
        .filter("cp IS NULL OR cur_price <> s.unit_price")
        .select(
            col("s.product_id"), col("s.name"), col("s.category"), col("s.unit_price"),
            col("s.effective_date").alias("effective_from"),
            lit(None).cast("date").alias("effective_to"),
            lit(True).alias("is_current"),
        )
    )
    added = new_versions.count()
    new_versions.write.format("delta").mode("append").save(SILVER_PRODUCTS)

    total = spark.read.format("delta").load(SILVER_PRODUCTS).count()
    cur_n = spark.read.format("delta").load(SILVER_PRODUCTS).filter("is_current = true").count()
    print(f"silver.products_dim: +{added} new versions, {cur_n} current, {total} total rows")
    return {"added": added, "current": cur_n, "versions": total}


def build_silver_inventory(spark: SparkSession) -> dict[str, int]:
    bronze = _load(spark, "bronze_inventory")
    clean = (
        bronze.filter("units_on_hand >= 0 AND units_reserved >= 0")
        .withColumn("rn", row_number().over(
            Window.partitionBy("snapshot_date", "product_id").orderBy(col("units_on_hand").desc())))
        .filter("rn = 1")
        .drop("rn")
    )
    clean.write.format("delta").mode("overwrite").save(SILVER_INVENTORY)
    n = clean.count()
    print(f"silver.inventory: {n} rows")
    return {"rows": n}


def main(spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("retail-silver")
    return {
        "orders": build_silver_orders(spark),
        "products_dim": build_silver_products_scd2(spark),
        "inventory": build_silver_inventory(spark),
    }


if __name__ == "__main__":
    main()
