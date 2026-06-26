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
from pyspark.sql.window import Window

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

# TODO: Count orders per status and display the results
# your code here

orders = spark.table("training_marcelino_collajunior.bronze.orders")
orders.limit(10).display()

count_orders = orders.groupBy("order_status").count().orderBy("count")

count_orders.display()




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

# TODO: Calculate the average delivery time in days across all delivered orders
# your code here

filtered_orders = orders.filter(orders.order_delivered_customer_date.isNotNull())

avg_delivery_time = filtered_orders.select(
    F.avg(F.datediff("order_delivered_customer_date", "order_purchase_timestamp")).alias("avg_delivery_time_days")
)

avg_delivery_time.display()

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

# TODO: Top 10 product categories by total revenue
# your code here

orders_items = spark.table("training_marcelino_collajunior.bronze.order_items")
products = spark.table("training_marcelino_collajunior.bronze.products")

category_revenue = orders_items.join(products, "product_id").groupBy("product_category_name").sum("price")


category_revenue.orderBy(F.desc("sum(price)")).limit(10).display()


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

# TODO: Distribution of review scores — count per score value
# your code here

order_reviews = spark.table("training_marcelino_collajunior.bronze.order_reviews")

grouped_orders = order_reviews.groupBy("review_score").count()

grouped_orders.orderBy(F.asc("count")).display()

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
orders = spark.table("training_marcelino_collajunior.bronze.orders")

orders2 = orders.withColumn(
    "is_late",
    F.when(
        (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")),
        1
    ).otherwise(0)
)

orders2.limit(10).display()



order_items = spark.table("training_marcelino_collajunior.bronze.order_items")


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

orders = spark.table("training_marcelino_collajunior.bronze.orders")
orders_payments = spark.table("training_marcelino_collajunior.bronze.order_payments")
customers = spark.table("training_marcelino_collajunior.bronze.customers")

intermediate_table = orders.join(orders_payments, "order_id").join(customers, "customer_id")

occurances_pay_state = intermediate_table.groupBy("customer_state", "payment_type").count()

window_spec = Window.partitionBy("customer_state").orderBy(F.desc("count"))

only_the_highest = (
    occurances_pay_state
    .withColumn("rank", F.rank().over(window_spec))
    .filter(F.col("rank") == 1)
    .drop("rank")
)


only_the_highest.limit(10).display()
