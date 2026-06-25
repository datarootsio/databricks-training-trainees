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
# MAGIC -- TODO: Count orders per status and display the results
# MAGIC -- your code here
# MAGIC SELECT order_status, COUNT(*) as order_status_count FROM training_marlene_mezzi.bronze.orders
# MAGIC GROUP BY order_status

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
# MAGIC -- TODO: Calculate the average delivery time in days across all delivered orders
# MAGIC -- your code here
# MAGIC
# MAGIC SELECT AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) as avg_delivery_time 
# MAGIC FROM training_marlene_mezzi.bronze.orders 
# MAGIC WHERE order_delivered_customer_date is not null 
# MAGIC

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
# MAGIC SELECT product_category_name, SUM(price) as total_revenue 
# MAGIC FROM training_marlene_mezzi.bronze.order_items 
# MAGIC INNER JOIN training_marlene_mezzi.bronze.products 
# MAGIC ON order_items.product_id = products.product_id
# MAGIC GROUP BY product_category_name 
# MAGIC ORDER BY total_revenue DESC 
# MAGIC LIMIT 10

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
# MAGIC -- TODO: Distribution of review scores — count per score value
# MAGIC -- your code here
# MAGIC select review_score, count(*) as review_score_count
# MAGIC from training_marlene_mezzi.bronze.order_reviews
# MAGIC group by review_score
# MAGIC order by review_score

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
# MAGIC -- TODO: Late delivery rate per customer state, sorted by highest rate first
# MAGIC -- your code here
# MAGIC with cte as (
# MAGIC     SELECT customer_state, CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END as is_late
# MAGIC FROM training_marlene_mezzi.bronze.orders
# MAGIC INNER JOIN training_marlene_mezzi.bronze.customers
# MAGIC ON orders.customer_id = customers.customer_id
# MAGIC WHERE order_status ='delivered' AND order_estimated_delivery_date is not null
# MAGIC )
# MAGIC select customer_state, round(sum(is_late)/count(*),2) *100 as late_rate
# MAGIC from cte
# MAGIC group by customer_state
# MAGIC order by late_rate desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.customer_state,
# MAGIC    ROUND(
# MAGIC     SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END)
# MAGIC     * 100.0 / COUNT(*),
# MAGIC     1
# MAGIC   ) AS late_rate
# MAGIC FROM training_marlene_mezzi.bronze.orders o
# MAGIC JOIN training_marlene_mezzi.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_delivered_customer_date IS NOT NULL
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY late_rate DESC;

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
