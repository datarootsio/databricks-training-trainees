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
# MAGIC USE CATALOG `training_julien_schneider`;
# MAGIC USE SCHEMA landing;
# MAGIC --# TODO: Count orders per status and display the results
# MAGIC --# your code here
# MAGIC
# MAGIC SELECT order_status, count(*) AS count_order_status
# MAGIC FROM orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY count_order_status DESC
# MAGIC

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
# MAGIC SELECT round(AVG(datediff(order_delivered_customer_date, order_purchase_timestamp)),4)
# MAGIC FROM orders
# MAGIC WHERE order_delivered_customer_date IS NOT NULL

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
# MAGIC SELECT product_category_name, sum(price) sum_price
# MAGIC FROM order_items
# MAGIC JOIN products on products.product_id = order_items.product_id
# MAGIC GROUP BY product_category_name
# MAGIC ORDER BY sum_price DESC
# MAGIC --LIMIT 10

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

# MAGIC %sql
# MAGIC SELECT review_score, count(*) AS count_review
# MAGIC FROM order_reviews
# MAGIC WHERE review_score BETWEEN 1 AND 5
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score ASC

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

# MAGIC %sql
# MAGIC WITH islateorder AS(
# MAGIC SELECT customer_state, 
# MAGIC CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END AS isLate
# MAGIC FROM orders
# MAGIC JOIN customers ON orders.customer_id = customers.customer_id
# MAGIC WHERE order_status = 'delivered')
# MAGIC
# MAGIC SELECT customer_state, SUM(isLate) / COUNT(*) * 100 as latePercent
# MAGIC FROM islateorder
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY latepercent DESC

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

# MAGIC %sql
# MAGIC WITH customer_payment AS(
# MAGIC
# MAGIC SELECT customer_state, payment_type, count(*) as payment_count
# MAGIC FROM orders
# MAGIC JOIN customers ON orders.customer_id = customers.customer_id
# MAGIC JOIN order_payments ON orders.order_id = order_payments.order_id
# MAGIC GROUP BY customer_state, payment_type)
# MAGIC , ranked AS(
# MAGIC     SELECT customer_state, payment_type, payment_count, rank() OVER(PARTITION BY customer_state ORDER BY payment_count DESC) as rank
# MAGIC     FROM customer_payment)
# MAGIC SELECT customer_state, payment_type, payment_count, rank from ranked
# MAGIC WHERE rank = 1
# MAGIC ORDER BY customer_state
