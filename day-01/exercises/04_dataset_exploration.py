# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 04: Dataset Exploration
# MAGIC
# MAGIC In this exercise you will explore the **Olist e-commerce dataset** to answer real business questions.
# MAGIC
# MAGIC You are free to use **SQL** (`%sql` cells) or **PySpark** — or both. There is no single correct approach.
# MAGIC
# MAGIC **Catalog:** `training_<name>` | **Schema:** `landing`
# MAGIC
# MAGIC **Available tables:**
# MAGIC - `orders` — order lifecycle and timestamps
# MAGIC - `order_items` — products and prices per order
# MAGIC - `customers` — customer location data
# MAGIC - `products` — product metadata and category
# MAGIC - `sellers` — seller location data
# MAGIC - `order_reviews` — customer review scores and comments
# MAGIC - `order_payments` — payment type and value
# MAGIC - `geolocation` — zip code to lat/lng mapping
# MAGIC - `product_category_name_translation` — Portuguese to English category names
# MAGIC
# MAGIC Work through each question below. Try to interpret the results — what do they tell you about the business?

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1: Order Status Distribution
# MAGIC
# MAGIC **How many orders are in each status?** (delivered, shipped, canceled, etc.)
# MAGIC
# MAGIC **Hints:**
# MAGIC - Table: `orders`
# MAGIC - Relevant column: `order_status`
# MAGIC - Use `COUNT(*)` grouped by `order_status`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_status, count(*) as nb_order_status from training_nacer_bellil.landing.orders GROUP BY order_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2: Average Delivery Time
# MAGIC
# MAGIC **What is the average delivery time in days** — from `order_purchase_timestamp` to `order_delivered_customer_date`?
# MAGIC
# MAGIC **Hints:**
# MAGIC - Table: `orders`
# MAGIC - Filter out rows where `order_delivered_customer_date` is NULL (undelivered orders)
# MAGIC - SQL: `DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)`
# MAGIC - PySpark: `F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))`
# MAGIC - Take the `AVG()` of that difference

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) as avg_date_diff from training_nacer_bellil.landing.orders where order_delivered_customer_date IS NOT NULL 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3: Revenue by Product Category
# MAGIC
# MAGIC **Which product categories generate the most total revenue? Show the top 10.**
# MAGIC
# MAGIC **Hints:**
# MAGIC - Join `order_items` with `products` on `product_id`
# MAGIC - Revenue = `SUM(price)`
# MAGIC - Group by `product_category_name`
# MAGIC - Sort descending, limit to 10
# MAGIC - Note: some category names may be NULL — you can include or exclude them

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Top 10 product categories by total revenue
# MAGIC -- your code here
# MAGIC select * from training_nacer_bellil.landing.order_items join training_nacer_bellil.landing.orders on training_nacer_bellil.landing.order_items.order_id = training_nacer_bellil.landing.orders.order_id where order_status = 'delivered' order by price desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4: Review Score Distribution
# MAGIC
# MAGIC **What is the distribution of review scores (1–5)?** How many orders received each score?
# MAGIC
# MAGIC **Hints:**
# MAGIC - Table: `order_reviews`
# MAGIC - Relevant column: `review_score`
# MAGIC - Group by `review_score`, count rows, sort by score ascending

# COMMAND ----------

# DBTITLE 1,Cell 10
# TODO: Distribution of review scores — count per score value
# your code here



spark.table("training_nacer_bellil.landing.order_reviews").groupBy('review_creation_date').agg(F.avg('review_answer_timestamp').alias('avg_date_diff')).orderBy(F.col('avg_date_diff').desc())
# TODO: Top 10 most reviewed products
# your code here

display(
    spark.table("training_nacer_bellil.landing.order_reviews")
    .join(spark.table("training_nacer_bellil.landing.order_items"), "order_id")
    .groupBy("product_id")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 5: Late Delivery Rate by State
# MAGIC
# MAGIC **Which customer states have the highest late delivery rate?**
# MAGIC
# MAGIC A delivery is **late** when `order_delivered_customer_date > order_estimated_delivery_date`.
# MAGIC
# MAGIC **Hints:**
# MAGIC - Join `orders` with `customers` on `customer_id`
# MAGIC - Filter to orders with `order_status = 'delivered'` and non-NULL delivered/estimated dates
# MAGIC - Create an `is_late` flag: 1 if late, 0 otherwise
# MAGIC   - SQL: `CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END`
# MAGIC   - PySpark: `F.when(F.col(...) > F.col(...), 1).otherwise(0)`
# MAGIC - Aggregate by `customer_state`:
# MAGIC   - `late_rate = SUM(is_late) / COUNT(*) * 100`
# MAGIC - Sort descending by late_rate

# COMMAND ----------

# DBTITLE 1,Cell 12
# TODO: Late delivery rate per customer state, sorted by highest rate first
# your code here

display(
    spark.table("training_nacer_bellil.landing.order_reviews")
    .join(spark.table("training_nacer_bellil.landing.orders"), "order_id")
    .join(spark.table("training_nacer_bellil.landing.order_items"), "order_id")
    .join(spark.table("training_nacer_bellil.landing.customers"), "customer_id")
    .withColumn("late_delivery", F.when(F.datediff(F.expr("try_to_timestamp(review_answer_timestamp)"), F.expr("try_to_timestamp(order_purchase_timestamp)")) > 3, 1).otherwise(0))
    .groupBy("customer_state")
    .agg(F.avg("late_delivery").alias("late_delivery_rate"))
    .orderBy(F.col("late_delivery_rate").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 6 (Bonus): Most Popular Payment Type by State
# MAGIC
# MAGIC **What is the most popular payment type in each customer state?**
# MAGIC
# MAGIC **Hints:**
# MAGIC - Join `orders`, `customers`, and `order_payments` (join key: `order_id` for payments, `customer_id` for customers)
# MAGIC - Count occurrences of each `payment_type` per `customer_state`
# MAGIC - Keep only the payment type with the **highest count** per state
# MAGIC - SQL approach: use a subquery or CTE to rank payment types per state, then filter to rank = 1
# MAGIC - PySpark approach: use a Window function with `F.rank()` or `F.row_number()` partitioned by `customer_state`, ordered by count descending

# COMMAND ----------

# TODO: Most popular payment type per customer state
# your code here


display(
    spark.table("training_nacer_bellil.landing.order_reviews")
    .join(spark.table("training_nacer_bellil.landing.order_items"), "order_id")
    .groupBy("product_id")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)
# TODO: Top 10 most reviewed products

# COMMAND ----------


