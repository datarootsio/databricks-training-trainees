# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 · Exercise 04 — Dataset Exploration (SOLUTION)
# MAGIC
# MAGIC This notebook contains the answer key for the dataset exploration exercise.
# MAGIC Each question is answered with both a PySpark solution and a SQL solution.

# COMMAND ----------

# Setup
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — Order status distribution

# COMMAND ----------

# Q1 — PySpark solution
# WHY: Simple groupBy + count is the most direct approach for distributions
orders = spark.table("training_<name>.landing.orders")
(
    orders
    .groupBy("order_status")
    .agg(F.count("*").alias("order_count"))
    .orderBy(F.desc("order_count"))
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1 — SQL solution
# MAGIC SELECT order_status, COUNT(*) AS order_count
# MAGIC FROM training_<name>.landing.orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — Average delivery time

# COMMAND ----------

# Q2 — PySpark solution
orders = spark.table("training_<name>.landing.orders")
(
    orders
    # WHY: Filter NULLs — orders not yet delivered don't have a delivery date
    .filter(F.col("order_delivered_customer_date").isNotNull())
    .withColumn(
        "delivery_days",
        # WHY: F.datediff returns integer days between two date/timestamp columns
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        )
    )
    .agg(
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.min("delivery_days").alias("min_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
    )
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2 — SQL solution
# MAGIC SELECT
# MAGIC     ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)), 1) AS avg_delivery_days,
# MAGIC     MIN(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS min_days,
# MAGIC     MAX(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS max_days
# MAGIC FROM training_<name>.landing.orders
# MAGIC -- WHY: Exclude NULLs — DATEDIFF with NULL returns NULL and would skew AVG
# MAGIC WHERE order_delivered_customer_date IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 — Revenue by product category (top 10)

# COMMAND ----------

# Q3 — PySpark solution
order_items = spark.table("training_<name>.landing.order_items")
products = spark.table("training_<name>.landing.products")
(
    order_items
    .join(products, on="product_id", how="left")
    # WHY: fillna replaces NULL category names so they group as "unknown" not dropped
    .fillna({"product_category_name": "unknown"})
    .groupBy("product_category_name")
    .agg(F.round(F.sum("price"), 2).alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .limit(10)
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3 — SQL solution
# MAGIC SELECT
# MAGIC     COALESCE(p.product_category_name, 'unknown') AS product_category_name,
# MAGIC     ROUND(SUM(oi.price), 2) AS total_revenue,
# MAGIC     COUNT(DISTINCT oi.order_id) AS order_count
# MAGIC FROM training_<name>.landing.order_items oi
# MAGIC LEFT JOIN training_<name>.landing.products p ON oi.product_id = p.product_id
# MAGIC GROUP BY COALESCE(p.product_category_name, 'unknown')
# MAGIC -- WHY: COALESCE handles NULLs — products without a category get labeled 'unknown'
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 — Review score distribution

# COMMAND ----------

# Q4 — PySpark solution
order_reviews = spark.table("training_<name>.landing.order_reviews")
(
    order_reviews
    .groupBy("review_score")
    .agg(
        F.count("*").alias("review_count"),
        F.round(F.count("*") / order_reviews.count() * 100, 1).alias("pct")
    )
    .orderBy("review_score")
    .display()
)
# NOTE: Computing .count() separately triggers an extra scan — for large tables, use window functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4 — SQL solution
# MAGIC SELECT
# MAGIC     review_score,
# MAGIC     COUNT(*) AS review_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM training_<name>.landing.order_reviews
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score
# MAGIC -- WHY: SUM(COUNT(*)) OVER () is a window function that sums all group counts — elegant one-pass solution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5 — Late delivery rate by state

# COMMAND ----------

# Q5 — PySpark solution
orders = spark.table("training_<name>.landing.orders")
customers = spark.table("training_<name>.landing.customers")
(
    orders
    # WHY: Only look at delivered orders — others don't have a delivery date
    .filter(F.col("order_delivered_customer_date").isNotNull())
    .withColumn(
        "is_late",
        # WHY: Cast to integer (0/1) makes aggregation easy
        (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")).cast("int")
    )
    .join(customers, on="customer_id", how="left")
    .groupBy("customer_state")
    .agg(
        F.count("*").alias("delivered_orders"),
        F.sum("is_late").alias("late_orders"),
        F.round(F.sum("is_late") / F.count("*") * 100, 1).alias("late_rate_pct")
    )
    .orderBy(F.desc("late_rate_pct"))
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q5 — SQL solution
# MAGIC SELECT
# MAGIC     c.customer_state,
# MAGIC     COUNT(*) AS delivered_orders,
# MAGIC     SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) AS late_orders,
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END)
# MAGIC         * 100.0 / COUNT(*),
# MAGIC         1
# MAGIC     ) AS late_rate_pct
# MAGIC FROM training_<name>.landing.orders o
# MAGIC JOIN training_<name>.landing.customers c ON o.customer_id = c.customer_id
# MAGIC -- WHY: Filter to delivered orders only — undelivered have NULL delivery dates
# MAGIC WHERE o.order_delivered_customer_date IS NOT NULL
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY late_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6 (Bonus) — Most popular payment type per state

# COMMAND ----------

# Q6 — PySpark solution
from pyspark.sql.window import Window

orders = spark.table("training_<name>.landing.orders")
customers = spark.table("training_<name>.landing.customers")
order_payments = spark.table("training_<name>.landing.order_payments")

# Step 1: Join and count payment type per state
counts = (
    order_payments
    .join(orders, on="order_id", how="inner")
    .join(customers, on="customer_id", how="inner")
    .groupBy("customer_state", "payment_type")
    .agg(F.count("*").alias("payment_count"))
)

# Step 2: Rank within each state
# WHY: Window function partitions by state, ranks by count descending
w = Window.partitionBy("customer_state").orderBy(F.desc("payment_count"))
(
    counts
    .withColumn("rank", F.rank().over(w))
    .filter(F.col("rank") == 1)
    .select("customer_state", "payment_type", "payment_count")
    .orderBy("customer_state")
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q6 — SQL solution
# MAGIC WITH payment_counts AS (
# MAGIC     SELECT
# MAGIC         c.customer_state,
# MAGIC         op.payment_type,
# MAGIC         COUNT(*) AS payment_count
# MAGIC     FROM training_<name>.landing.order_payments op
# MAGIC     JOIN training_<name>.landing.orders o ON op.order_id = o.order_id
# MAGIC     JOIN training_<name>.landing.customers c ON o.customer_id = c.customer_id
# MAGIC     GROUP BY c.customer_state, op.payment_type
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         customer_state,
# MAGIC         payment_type,
# MAGIC         payment_count,
# MAGIC         RANK() OVER (PARTITION BY customer_state ORDER BY payment_count DESC) AS rnk
# MAGIC     FROM payment_counts
# MAGIC )
# MAGIC SELECT customer_state, payment_type, payment_count
# MAGIC FROM ranked
# MAGIC WHERE rnk = 1
# MAGIC -- WHY: RANK() in CTE, filter in outer — window functions can't be filtered in same SELECT
# MAGIC ORDER BY customer_state
