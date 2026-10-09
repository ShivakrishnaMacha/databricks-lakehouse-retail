#!/usr/bin/env python3
"""Gold layer: business-ready aggregate marts.

- gold_daily_sales: revenue / units / orders per day.
- gold_inventory_health: per-SKU days-of-cover with stockout-risk flags.
- gold_category_performance: revenue / units / orders / share by category.

Each mart is written as a Delta table AND exported to gold/*.parquet so the
Streamlit demo can serve it with pandas (no JVM needed at serve time).

Databricks mapping: on Databricks these marts back Databricks SQL dashboards
and alerts; the parquet export mirrors a "publish to BI" step.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, date_sub, lit, max as smax, sum as ssum, when

from src.bronze import WAREHOUSE, get_spark
from src.silver import SILVER_INVENTORY, SILVER_ORDERS, SILVER_PRODUCTS

ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = str(ROOT / "gold")

GOLD_TABLES = {
    "daily_sales": "daily_sales.parquet",
    "inventory_health": "inventory_health.parquet",
    "category_performance": "category_performance.parquet",
}


def build_daily_sales(spark: SparkSession):
    orders = spark.read.format("delta").load(SILVER_ORDERS)
    return (
        orders.withColumn("line_revenue", col("quantity") * col("unit_price") * (1 - col("discount")))
        .groupBy("order_date")
        .agg(
            countDistinct("order_id").alias("orders"),
            ssum("quantity").alias("units"),
            ssum("line_revenue").alias("revenue"),
        )
        .withColumnRenamed("order_date", "sale_date")
        .orderBy("sale_date")
    )


def build_inventory_health(spark: SparkSession, lookback_days: int = 14):
    inv = spark.read.format("delta").load(SILVER_INVENTORY)
    orders = spark.read.format("delta").load(SILVER_ORDERS)
    products = spark.read.format("delta").load(SILVER_PRODUCTS).filter("is_current = true")

    latest_date = inv.agg(smax("snapshot_date")).collect()[0][0]
    latest = inv.filter(col("snapshot_date") == latest_date).select("product_id", "units_on_hand")

    velocity = (
        orders.filter(col("order_date") >= date_sub(lit(str(latest_date)), lookback_days))
        .groupBy("product_id")
        .agg((ssum("quantity") / lookback_days).alias("avg_daily_units"))
    )

    health = (
        latest.join(velocity, "product_id", "left")
        .fillna({"avg_daily_units": 0.0})
        .withColumn(
            "days_of_cover",
            when(col("avg_daily_units") > 0, col("units_on_hand") / col("avg_daily_units")).otherwise(999.0),
        )
        .withColumn(
            "risk",
            when(col("days_of_cover") < 7, "high")
            .when(col("days_of_cover") < 14, "watch")
            .otherwise("healthy"),
        )
        .join(products.select("product_id", "name", "category"), "product_id", "left")
        .select("product_id", "name", "category", "units_on_hand", "avg_daily_units", "days_of_cover", "risk")
        .orderBy("days_of_cover")
    )
    return health


def build_category_performance(spark: SparkSession):
    orders = spark.read.format("delta").load(SILVER_ORDERS)
    products = spark.read.format("delta").load(SILVER_PRODUCTS).filter("is_current = true")
    joined = orders.join(products.select("product_id", "category"), "product_id", "left")
    total_revenue = joined.withColumn(
        "line_revenue", col("quantity") * col("unit_price") * (1 - col("discount"))
    ).agg(ssum("line_revenue")).collect()[0][0]
    return (
        joined.withColumn("line_revenue", col("quantity") * col("unit_price") * (1 - col("discount")))
        .groupBy("category")
        .agg(
            ssum("line_revenue").alias("revenue"),
            ssum("quantity").alias("units"),
            countDistinct("order_id").alias("orders"),
        )
        .withColumn("avg_order_value", col("revenue") / col("orders"))
        .withColumn("revenue_share", col("revenue") / lit(total_revenue))
        .orderBy(col("revenue").desc())
    )


def main(spark: SparkSession | None = None) -> dict[str, int]:
    spark = spark or get_spark("retail-gold")
    Path(GOLD_DIR).mkdir(parents=True, exist_ok=True)

    marts = {
        "daily_sales": build_daily_sales(spark),
        "inventory_health": build_inventory_health(spark),
        "category_performance": build_category_performance(spark),
    }
    counts: dict[str, int] = {}
    for name, df in marts.items():
        df.write.format("delta").mode("overwrite").save(f"{WAREHOUSE}/gold_{name}")
        df.write.mode("overwrite").parquet(f"{GOLD_DIR}/{GOLD_TABLES[name]}")
        counts[name] = df.count()
        print(f"gold.{name}: {counts[name]} rows -> gold/{GOLD_TABLES[name]}")
    return counts


if __name__ == "__main__":
    main()
