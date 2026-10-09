#!/usr/bin/env python3
"""Bronze layer: raw extracts -> Delta Lake tables with schema enforcement.

Databricks mapping: on Databricks this is Auto Loader
(`cloudFiles` source) with `cloudFiles.schemaLocation` for schema evolution;
here we read the local CSV extracts with explicit schemas — same guarantee:
badly-typed rows fail fast instead of silently becoming NULLs.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from delta import configure_spark_with_delta_pip

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = str(ROOT / "spark-warehouse")

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
])

PRICE_HISTORY_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("effective_date", DateType(), False),
    StructField("unit_price", DoubleType(), False),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
])

UPDATES_SCHEMA = StructType(
    ORDERS_SCHEMA.fields + [StructField("updated_at", DateType(), True)]
)

INVENTORY_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("product_id", StringType(), False),
    StructField("units_on_hand", IntegerType(), True),
    StructField("units_reserved", IntegerType(), True),
])

SOURCES: dict[str, tuple[str, StructType]] = {
    "bronze_products": ("data/raw/products.csv", PRODUCTS_SCHEMA),
    "bronze_price_history": ("data/raw/product_price_history.csv", PRICE_HISTORY_SCHEMA),
    "bronze_orders": ("data/raw/orders.csv", ORDERS_SCHEMA),
    "bronze_order_updates": ("data/raw/orders_updates.csv", UPDATES_SCHEMA),
    "bronze_inventory": ("data/raw/inventory_snapshots.csv", INVENTORY_SCHEMA),
}


def get_spark(app_name: str = "retail-bronze") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(spark: SparkSession | None = None) -> dict[str, int]:
    spark = spark or get_spark()
    counts: dict[str, int] = {}
    for table, (rel_path, schema) in SOURCES.items():
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(schema)  # schema enforcement: no silent type coercion
            .load(str(ROOT / rel_path))
        )
        df.write.format("delta").mode("overwrite").save(f"{WAREHOUSE}/{table}")
        counts[table] = df.count()
        print(f"bronze.{table}: {counts[table]} rows")
    return counts


if __name__ == "__main__":
    main()
