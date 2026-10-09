# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: CDC merge + SCD Type 2 product dimension
# MAGIC
# MAGIC The silver layer is **cleaned and conformed**:
# MAGIC - `silver_orders` — deduplicated, data-quality quarantined, then
# MAGIC   `MERGE INTO` on `order_id` applies the CDC update feed (latest wins).
# MAGIC - `silver_products_dim` — **SCD Type 2**: every price change closes the
# MAGIC   current version (`effective_to`, `is_current = false`) and opens a new
# MAGIC   one, so history is never rewritten.
# MAGIC - `silver_inventory` — cleaned daily snapshots.
# MAGIC
# MAGIC **Databricks production mapping:** this is exactly what
# MAGIC **Delta Live Tables** `APPLY CHANGES INTO` does declaratively; here the
# MAGIC MERGE statements are explicit so the logic is visible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver orders — quarantine + CDC upsert

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number

BRONZE = "/mnt/retail-lake/bronze"
SILVER = "/mnt/retail-lake/silver"

bronze_orders = spark.read.format("delta").load(f"{BRONZE}/bronze_orders")
updates = spark.read.format("delta").load(f"{BRONZE}/bronze_order_updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data-quality quarantine
# MAGIC Rejects land in their own Delta table for triage instead of silently
# MAGIC dropping — the **quarantine pattern**.

# COMMAND ----------

DQ = "order_id IS NOT NULL AND product_id IS NOT NULL AND quantity > 0 AND unit_price >= 0"

quarantine = bronze_orders.filter(f"NOT ({DQ})")
quarantine.write.format("delta").mode("overwrite").save(f"{SILVER}/silver_orders_quarantine")
print(f"quarantined rows: {quarantine.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC staging — base + updates, latest version wins per order_id

# COMMAND ----------

base = bronze_orders.filter(DQ).withColumn("updated_at", lit(None).cast("date"))
staging = (
    base.unionByName(updates.select(base.columns))
    .withColumn("rn", row_number().over(
        Window.partitionBy("order_id").orderBy(col("updated_at").desc_nulls_last())))
    .filter("rn = 1").drop("rn", "updated_at")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The same upsert as Spark SQL (this is the Databricks-idiomatic form):
# MAGIC -- MERGE INTO silver.silver_orders t
# MAGIC -- USING cdc_staging s ON t.order_id = s.order_id
# MAGIC -- WHEN MATCHED THEN UPDATE SET *
# MAGIC -- WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, f"{SILVER}/silver_orders"):
    (DeltaTable.forPath(spark, f"{SILVER}/silver_orders").alias("t")
        .merge(staging.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    staging.write.format("delta").mode("overwrite").save(f"{SILVER}/silver_orders")

print(f"silver_orders rows: {spark.read.format('delta').load(f'{SILVER}/silver_orders').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 — product dimension with price history

# COMMAND ----------

products = spark.read.format("delta").load(f"{BRONZE}/bronze_products")
price_hist = spark.read.format("delta").load(f"{BRONZE}/bronze_price_history")

# Latest price per product = today's dimension snapshot
latest = (
    price_hist.join(products, "product_id")
    .withColumn("rn", row_number().over(
        Window.partitionBy("product_id").orderBy(col("effective_date").desc())))
    .filter("rn = 1").drop("rn")
    .select("product_id", "name", "category", "unit_price", "effective_date")
)

DIM = f"{SILVER}/silver_products_dim"

if not DeltaTable.isDeltaTable(spark, DIM):
    (latest
        .select("product_id", "name", "category", "unit_price",
                col("effective_date").alias("effective_from"),
                lit(None).cast("date").alias("effective_to"),
                lit(True).alias("is_current"))
        .write.format("delta").mode("overwrite").save(DIM))
    print("SCD2 dim: initial load")
else:
    dim = DeltaTable.forPath(spark, DIM)
    # Step 1 — close current versions whose price changed
    (dim.alias("t")
        .merge(latest.alias("s"), "t.product_id = s.product_id AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.unit_price <> s.unit_price",
            set={"effective_to": "date_sub(s.effective_date, 1)", "is_current": "false"})
        .execute())
    # Step 2 — open new versions for changed + brand-new products
    current = spark.read.format("delta").load(DIM).filter("is_current = true")
    new_versions = (
        latest.alias("s")
        .join(current.select(col("product_id").alias("cp"),
                             col("unit_price").alias("cur_price")),
              col("s.product_id") == col("cp"), "left")
        .filter("cp IS NULL OR cur_price <> s.unit_price")
        .select(col("s.product_id"), col("s.name"), col("s.category"), col("s.unit_price"),
                col("s.effective_date").alias("effective_from"),
                lit(None).cast("date").alias("effective_to"),
                lit(True).alias("is_current"))
    )
    new_versions.write.format("delta").mode("append").save(DIM)
    print(f"SCD2 dim: +{new_versions.count()} new versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The dimension keeps full history — one product, many versions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT product_id, unit_price, effective_from, effective_to, is_current
# MAGIC -- FROM silver.silver_products_dim
# MAGIC -- WHERE product_id = 'P0001' ORDER BY effective_from

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver inventory — cleaned snapshots

# COMMAND ----------

(spark.read.format("delta").load(f"{BRONZE}/bronze_inventory")
    .filter("units_on_hand >= 0 AND units_reserved >= 0")
    .withColumn("rn", row_number().over(
        Window.partitionBy("snapshot_date", "product_id").orderBy(col("units_on_hand").desc())))
    .filter("rn = 1").drop("rn")
    .write.format("delta").mode("overwrite").save(f"{SILVER}/silver_inventory"))
print("silver_inventory: OK")
