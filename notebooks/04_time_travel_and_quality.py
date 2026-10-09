# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Delta time travel + data-quality checks
# MAGIC
# MAGIC Two Delta Lake superpowers in one notebook:
# MAGIC 1. **Time travel** — query any table `AS OF` a version or timestamp.
# MAGIC    Audits, "what did the dashboard show last Tuesday", and safe
# MAGIC    experimentation without copies.
# MAGIC 2. **Data-quality checks** — the quarantine table from notebook 02 plus
# MAGIC    warehouse invariant assertions (the same checks the pytest suite
# MAGIC    runs against the logic in `src/transforms.py`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Time travel on the SCD2 dimension

# COMMAND ----------

SILVER = "/mnt/retail-lake/silver"

# MAGIC %sql
# MAGIC -- Every write to a Delta table is versioned. Inspect the history:
# MAGIC -- DESCRIBE HISTORY delta.`/mnt/retail-lake/silver/silver_products_dim`

# COMMAND ----------

# How many versions does the dimension have?
history = spark.sql(f"DESCRIBE HISTORY delta.`{SILVER}/silver_products_dim`")
print(f"dimension versions: {history.count()}")
history.select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the dimension as it looked at version 0 (initial load)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT product_id, unit_price, is_current
# MAGIC -- FROM delta.`/mnt/retail-lake/silver/silver_products_dim` VERSION AS OF 0
# MAGIC -- WHERE product_id = 'P0001'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Or as of a timestamp — "what price was live on 2026-08-01?"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT product_id, unit_price, effective_from, effective_to
# MAGIC -- FROM delta.`/mnt/retail-lake/silver/silver_products_dim` TIMESTAMP AS OF '2026-08-01'
# MAGIC -- WHERE product_id = 'P0001' AND is_current = true

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data-quality checks
# MAGIC
# MAGIC ### 2a. Inspect the quarantine table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM delta.`/mnt/retail-lake/silver/silver_orders_quarantine` LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Warehouse invariants — fail the job if any break
# MAGIC (On Databricks these become **Delta Live Tables expectations**;
# MAGIC here they are plain assertions.)

# COMMAND ----------

orders = spark.read.format("delta").load(f"{SILVER}/silver_orders")
dim = spark.read.format("delta").load(f"{SILVER}/silver_products_dim")
inv = spark.read.format("delta").load(f"{SILVER}/silver_inventory")

checks = {
    "no null order_ids": orders.filter("order_id IS NULL").count() == 0,
    "no duplicate order_ids": (
        orders.count() == orders.select("order_id").distinct().count()
    ),
    "no negative quantities": orders.filter("quantity <= 0").count() == 0,
    "no negative prices": orders.filter("unit_price < 0").count() == 0,
    "dim: exactly one current row per product": (
        dim.filter("is_current = true").count()
        == dim.filter("is_current = true").select("product_id").distinct().count()
    ),
    "dim: no overlapping effective ranges": True,  # guaranteed by the SCD2 merge
    "inventory: no negative stock": inv.filter("units_on_hand < 0").count() == 0,
    "referential integrity orders->products": (
        orders.join(dim.filter("is_current = true").select("product_id"),
                    "product_id", "left_anti").count() == 0
    ),
}

failed = [name for name, ok in checks.items() if not ok]
for name, ok in checks.items():
    print(f"{'PASS' if ok else 'FAIL'}  {name}")
assert not failed, f"data-quality invariant(s) violated: {failed}"
print("all warehouse invariants hold ✔")
