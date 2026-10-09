# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold marts: business-ready aggregates
# MAGIC
# MAGIC Gold tables answer business questions directly — no joins needed at
# MAGIC query time. Each mart is written as a Delta table **and** exported to
# MAGIC Parquet for the Streamlit demo.
# MAGIC
# MAGIC **Databricks production mapping:** these marts back **Databricks SQL**
# MAGIC dashboards and alerts (e.g. a stockout-risk alert on
# MAGIC `gold_inventory_health`); the Parquet export mirrors a "publish to BI"
# MAGIC step.

# COMMAND ----------

SILVER = "/mnt/retail-lake/silver"
GOLD = "/mnt/retail-lake/gold"

orders = spark.read.format("delta").load(f"{SILVER}/silver_orders")
products = spark.read.format("delta").load(f"{SILVER}/silver_products_dim").filter("is_current = true")
inventory = spark.read.format("delta").load(f"{SILVER}/silver_inventory")

orders.createOrReplaceTempView("silver_orders")
products.createOrReplaceTempView("silver_products_dim")
inventory.createOrReplaceTempView("silver_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 1 — daily sales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_sales AS
# MAGIC SELECT
# MAGIC   order_date AS sale_date,
# MAGIC   COUNT(DISTINCT order_id) AS orders,
# MAGIC   SUM(quantity) AS units,
# MAGIC   SUM(quantity * unit_price * (1 - discount)) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY order_date
# MAGIC ORDER BY sale_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 2 — inventory health (days-of-cover + stockout risk)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_inventory_health AS
# MAGIC WITH latest AS (
# MAGIC   SELECT product_id, units_on_hand
# MAGIC   FROM silver_inventory
# MAGIC   WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM silver_inventory)
# MAGIC ),
# MAGIC velocity AS (
# MAGIC   SELECT product_id, SUM(quantity) / 14.0 AS avg_daily_units
# MAGIC   FROM silver_orders
# MAGIC   WHERE order_date >= (SELECT MAX(snapshot_date) FROM silver_inventory) - INTERVAL 14 DAYS
# MAGIC   GROUP BY product_id
# MAGIC )
# MAGIC SELECT
# MAGIC   l.product_id, p.name, p.category, l.units_on_hand,
# MAGIC   COALESCE(v.avg_daily_units, 0) AS avg_daily_units,
# MAGIC   CASE WHEN COALESCE(v.avg_daily_units, 0) > 0
# MAGIC        THEN l.units_on_hand / v.avg_daily_units ELSE 999 END AS days_of_cover,
# MAGIC   CASE WHEN COALESCE(v.avg_daily_units, 0) = 0 THEN 'healthy'
# MAGIC        WHEN l.units_on_hand / v.avg_daily_units < 7 THEN 'high'
# MAGIC        WHEN l.units_on_hand / v.avg_daily_units < 14 THEN 'watch'
# MAGIC        ELSE 'healthy' END AS risk
# MAGIC FROM latest l
# MAGIC LEFT JOIN velocity v USING (product_id)
# MAGIC LEFT JOIN silver_products_dim p USING (product_id)
# MAGIC ORDER BY days_of_cover

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 3 — category performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_category_performance AS
# MAGIC WITH lines AS (
# MAGIC   SELECT p.category,
# MAGIC          o.quantity * o.unit_price * (1 - o.discount) AS line_revenue,
# MAGIC          o.quantity, o.order_id
# MAGIC   FROM silver_orders o
# MAGIC   LEFT JOIN silver_products_dim p USING (product_id)
# MAGIC )
# MAGIC SELECT
# MAGIC   category,
# MAGIC   SUM(line_revenue) AS revenue,
# MAGIC   SUM(quantity) AS units,
# MAGIC   COUNT(DISTINCT order_id) AS orders,
# MAGIC   SUM(line_revenue) / COUNT(DISTINCT order_id) AS avg_order_value,
# MAGIC   SUM(line_revenue) / SUM(SUM(line_revenue)) OVER () AS revenue_share
# MAGIC FROM lines
# MAGIC GROUP BY category
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Parquet snapshots for the Streamlit demo

# COMMAND ----------

for tbl in ["gold_daily_sales", "gold_inventory_health", "gold_category_performance"]:
    (spark.table(tbl).write.mode("overwrite")
        .parquet(f"/mnt/retail-lake/gold/{tbl}.parquet"))
    print(f"{tbl}: {spark.table(tbl).count()} rows exported")
