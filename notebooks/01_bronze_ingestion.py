# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion (raw extracts → Delta)
# MAGIC
# MAGIC The bronze layer is the **immutable landing zone**: raw CSV extracts are
# MAGIC read with **explicit schemas** (schema enforcement — badly-typed rows
# MAGIC fail fast instead of silently becoming NULLs) and written as Delta Lake
# MAGIC tables. Nothing is cleaned or joined here.
# MAGIC
# MAGIC **Databricks production mapping:** replace the local CSV read with
# MAGIC **Auto Loader** (`cloudFiles`) pointed at an S3/ADLS landing bucket, and
# MAGIC set `cloudFiles.schemaLocation` so schema evolution is tracked:
# MAGIC ```python
# MAGIC (spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze_orders")
# MAGIC     .schema(ORDERS_SCHEMA)
# MAGIC     .load("s3://retail-lake/raw/orders/")
# MAGIC     .writeStream.format("delta")
# MAGIC     .option("checkpointLocation", "/mnt/checkpoints/bronze_orders")
# MAGIC     .start("/mnt/delta/bronze_orders"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas — the contract with the source systems

# COMMAND ----------

from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

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

INVENTORY_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("product_id", StringType(), False),
    StructField("units_on_hand", IntegerType(), True),
    StructField("units_reserved", IntegerType(), True),
])

# The CDC feed carries the same order fields plus an update timestamp.
UPDATES_SCHEMA = StructType(ORDERS_SCHEMA.fields + [StructField("updated_at", DateType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: CSV → Delta (one table per extract)

# COMMAND ----------

RAW = "/mnt/retail-lake/raw"          # local dev: ./data/raw ; Databricks: s3://retail-lake/raw/
BRONZE = "/mnt/retail-lake/bronze"    # local dev: ./spark-warehouse ; Databricks: Unity Catalog volume

sources = {
    "bronze_products":       (f"{RAW}/products.csv", PRODUCTS_SCHEMA),
    "bronze_price_history":  (f"{RAW}/product_price_history.csv", PRICE_HISTORY_SCHEMA),
    "bronze_orders":         (f"{RAW}/orders.csv", ORDERS_SCHEMA),
    "bronze_order_updates":  (f"{RAW}/orders_updates.csv", UPDATES_SCHEMA),
    "bronze_inventory":      (f"{RAW}/inventory_snapshots.csv", INVENTORY_SCHEMA),
}

for table, (path, schema) in sources.items():
    (spark.read.format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)                      # ← schema enforcement happens here
        .load(path)
        .write.format("delta").mode("overwrite")
        .save(f"{BRONZE}/{table}"))
    print(f"bronze.{table}: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — every bronze table is queryable Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lists the Delta tables we just wrote (Databricks: replace with SHOW TABLES IN bronze)
# MAGIC SELECT 'bronze_products' AS tbl UNION ALL SELECT 'bronze_price_history' UNION ALL
# MAGIC SELECT 'bronze_orders' UNION ALL SELECT 'bronze_order_updates' UNION ALL SELECT 'bronze_inventory'

# COMMAND ----------

orders = spark.read.format("delta").load(f"{BRONZE}/bronze_orders")
print(f"bronze_orders rows: {orders.count()}")
orders.printSchema()
