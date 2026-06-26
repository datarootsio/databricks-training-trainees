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

from pyspark.sql import functions as F

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
# MAGIC SELECT order_status, count(*) AS order_status_count
# MAGIC FROM swatch_training.landing.orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY order_status_count DESC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Q1 - Order Status Distribution (PySpark)
orders = spark.table("swatch_training.landing.orders")

result = (
    orders
    .groupBy("order_status")
    .agg(F.count("*").alias("order_status_count"))
    .orderBy(F.col("order_status_count").desc())
)

display(result)

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

# DBTITLE 1,Cell 7
# TODO: Calculate the average delivery time in days across all delivered orders
# your code here
orders = spark.table("swatch_training.landing.orders")

result = (
    orders
    .filter(F.col("order_delivered_customer_date").isNotNull())
    .withColumn("delivery_delay", F.date_diff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")))
    .agg(F.avg("delivery_delay").alias("delivery_delay_avg"))
    )

display(result)


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

# DBTITLE 1,Cell 9
# TODO: Top 10 product categories by total revenue
# your code here
orders_items = spark.table("swatch_training.landing.order_items")
products = spark.table("swatch_training.landing.products")

result = (
    orders_items
    .join(products, "product_id", "left")
    .groupBy("product_category_name")
    .agg(F.sum("price"))
    
)

display(result)

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

# DBTITLE 1,Cell 11
# TODO: Distribution of review scores — count per score value
# your code here
orders_reviews = spark.table("swatch_training.landing.order_reviews")

result = (
    orders_reviews
    .withColumn("review_score", F.expr("try_cast(review_score AS INT)"))
    .filter(F.col("review_score").isNotNull())
    .groupBy("review_score")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("review_score").asc())
)

display(result)

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

# TODO: Late delivery rate per customer state, sorted by highest rate first
# your code here
orders = spark.table("swatch_training.landing.orders")
customers = spark.table("swatch_training.landing.customers")

result = (
    orders
    .filter((F.col("order_status") == "delivered") 
            & (F.col("order_estimated_delivery_date").isNotNull()) 
            & (F.col("order_delivered_customer_date").isNotNull()))
    .withColumn("is_late", F.when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),1).otherwise(0))
    .join(customers,"customer_id", "left")
    .groupBy("customer_state")
    .agg((F.sum("is_late")/F.count("*")*100).alias("late_rate"))
    .orderBy(F.col("late_rate").desc())
)

display(result)

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

# DBTITLE 1,Q6 - Most Popular Payment Type by State (PySpark)
# TODO: Most popular payment type per customer state
# your code here
orders = spark.table("swatch_training.landing.orders")
customers = spark.table("swatch_training.landing.customers")
order_payments = spark.table("swatch_training.landing.order_payments")

from pyspark.sql import Window

window = Window.partitionBy("customer_state").orderBy(F.col("count").desc())

result = (
    orders
    .join(customers, "customer_id", "left")
    .join(order_payments, "order_id", "left")
    .groupBy("customer_state", "payment_type")
    .agg(F.count("*").alias("count"))
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .drop("rank")
    .orderBy(F.col("count").desc())
)

display(result)

# COMMAND ----------

# DBTITLE 1,Q6 - Most Popular Payment Type by State (SQL)
# MAGIC %sql
# MAGIC WITH payment_counts AS (
# MAGIC     SELECT
# MAGIC         c.customer_state,
# MAGIC         p.payment_type,
# MAGIC         COUNT(*) AS count,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY c.customer_state ORDER BY COUNT(*) DESC) AS rank
# MAGIC     FROM swatch_training.landing.orders o
# MAGIC     JOIN swatch_training.landing.customers c ON o.customer_id = c.customer_id
# MAGIC     JOIN swatch_training.landing.order_payments p ON o.order_id = p.order_id
# MAGIC     GROUP BY c.customer_state, p.payment_type
# MAGIC )
# MAGIC SELECT customer_state, payment_type, count
# MAGIC FROM payment_counts
# MAGIC WHERE rank = 1
# MAGIC ORDER BY customer_state
