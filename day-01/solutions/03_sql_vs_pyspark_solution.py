# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 · Exercise 03 — SQL vs PySpark (SOLUTION)
# MAGIC
# MAGIC This notebook contains the answer key for the SQL vs PySpark exercise.
# MAGIC Each part shows both a SQL cell and an equivalent PySpark cell so you can
# MAGIC compare the two approaches side-by-side.

# COMMAND ----------

# Setup — load all tables used in this exercise
from pyspark.sql import functions as F

orders = spark.table("training_<name>.landing.orders")
order_items = spark.table("training_<name>.landing.order_items")
customers = spark.table("training_<name>.landing.customers")
products = spark.table("training_<name>.landing.products")
order_payments = spark.table("training_<name>.landing.order_payments")
order_reviews = spark.table("training_<name>.landing.order_reviews")
sellers = spark.table("training_<name>.landing.sellers")
# WHY: spark.table() reads from the catalog without copying data — it's lazy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Filter orders with status `shipped`

# COMMAND ----------

# Part A — PySpark solution
# WHY: F.col() is preferred over string column names — enables IDE autocomplete and catches typos early
(
    orders
    .filter(F.col("order_status") == "shipped")
    .select("order_id", "order_purchase_timestamp")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Orders per customer state

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part B — SQL solution
# MAGIC SELECT
# MAGIC     c.customer_state,
# MAGIC     COUNT(DISTINCT o.order_id) AS order_count
# MAGIC FROM training_<name>.landing.orders o
# MAGIC JOIN training_<name>.landing.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# Part B — PySpark solution
# WHY: Using countDistinct because one customer_id can appear multiple times
# WHY: Chaining operations reads like a pipeline — filter → group → sort → display
(
    orders
    .join(customers, on="customer_id", how="inner")
    .groupBy("customer_state")
    .agg(F.countDistinct("order_id").alias("order_count"))
    .orderBy(F.desc("order_count"))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Join order_items with products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part C — SQL solution
# MAGIC SELECT
# MAGIC     oi.order_id,
# MAGIC     p.product_category_name,
# MAGIC     oi.price
# MAGIC FROM training_<name>.landing.order_items oi
# MAGIC -- WHY: LEFT JOIN keeps order_items rows even if product metadata is missing
# MAGIC LEFT JOIN training_<name>.landing.products p ON oi.product_id = p.product_id
# MAGIC LIMIT 20

# COMMAND ----------

# Part C — PySpark solution
# WHY: how="left" matches the LEFT JOIN — keeps all order_items rows
(
    order_items
    .join(products, on="product_id", how="left")
    .select("order_id", "product_category_name", "price")
    .limit(20)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — Top 5 products by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part D — SQL solution
# MAGIC SELECT
# MAGIC     oi.product_id,
# MAGIC     p.product_category_name,
# MAGIC     -- WHY: ROUND for readability in business reports
# MAGIC     ROUND(SUM(oi.price), 2) AS total_revenue
# MAGIC FROM training_<name>.landing.order_items oi
# MAGIC LEFT JOIN training_<name>.landing.products p ON oi.product_id = p.product_id
# MAGIC GROUP BY oi.product_id, p.product_category_name
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5

# COMMAND ----------

# Part D — PySpark solution
# WHY: F.sum() aggregates, F.round() formats — same as ROUND(SUM(...), 2) in SQL
(
    order_items
    .join(products, on="product_id", how="left")
    .groupBy("product_id", "product_category_name")
    .agg(F.round(F.sum("price"), 2).alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .limit(5)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E — Total revenue per seller state (top 10)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business question: Total revenue per seller state, top 10
# MAGIC SELECT
# MAGIC     s.seller_state,
# MAGIC     ROUND(SUM(oi.price), 2) AS total_revenue,
# MAGIC     COUNT(DISTINCT oi.order_id) AS order_count
# MAGIC FROM training_<name>.landing.order_items oi
# MAGIC JOIN training_<name>.landing.sellers s ON oi.seller_id = s.seller_id
# MAGIC GROUP BY s.seller_state
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10
# MAGIC -- NOTE: Revenue is from the seller's perspective — price paid in order_items

# COMMAND ----------

# Part E — PySpark solution
# WHY: Explicit import at top of cell keeps dependencies visible
sellers = spark.table("training_<name>.landing.sellers")

# WHY: Chaining .join().groupBy().agg().orderBy().limit() mirrors the SQL structure exactly
(
    order_items
    .join(sellers, on="seller_id", how="inner")
    .groupBy("seller_state")
    .agg(
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
    .display()
)
# NOTE: Results should match the SQL query above — always validate both approaches agree!
