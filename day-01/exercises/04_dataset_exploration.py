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
# MAGIC SELECT order_status, count(*) as count
# MAGIC FROM training_sandro_couto.bronze.orders
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
# MAGIC select AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) as avg_delivery_days
# MAGIC from training_sandro_couto.bronze.orders
# MAGIC where order_delivered_customer_date is not null

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
# MAGIC select p.product_category_name, sum(oi.price) as total_revenue
# MAGIC from training_sandro_couto.bronze.order_items oi
# MAGIC join training_sandro_couto.bronze.products p
# MAGIC on oi.product_id = p.product_id
# MAGIC group by p.product_category_name
# MAGIC order by total_revenue desc
# MAGIC limit 10

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
# MAGIC select count(*) as count, review_score
# MAGIC from training_sandro_couto.bronze.order_reviews
# MAGIC where review_score between 1 and 5
# MAGIC group by review_score
# MAGIC order by review_score desc

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
# MAGIC
# MAGIC select 
# MAGIC     c.customer_state,
# MAGIC     case when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1 else 0 end as is_late
# MAGIC from training_sandro_couto.bronze.orders o
# MAGIC join training_sandro_couto.bronze.customers c
# MAGIC on o.customer_id = c.customer_id
# MAGIC where o.order_status = "delivered" and o.order_status is not null
# MAGIC group by c.customer_state, case when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1 else 0 end

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
# MAGIC
# MAGIC with state_rank as (
# MAGIC     
# MAGIC     select ROW_NUMBER() OVER (ORDER BY  DESC) AS rank
# MAGIC     FROM training_sandro_couto.bronze.customers c
# MAGIC )
# MAGIC
# MAGIC select * from state_rank
# MAGIC
# MAGIC /*
# MAGIC select 
# MAGIC     count(op.payment_type) as payment_type_count, 
# MAGIC     c.customer_state,
# MAGIC     ROW_NUMBER() OVER (ORDER BY customer_state DESC) AS state_rank
# MAGIC from training_sandro_couto.bronze.orders o 
# MAGIC join training_sandro_couto.bronze.order_payments op
# MAGIC on o.order_id = op.order_id
# MAGIC join training_sandro_couto.bronze.customers c
# MAGIC on o.customer_id = c.customer_id
# MAGIC --where state_rank = 1
# MAGIC group by c.customer_state, state_rank
# MAGIC order by payment_type_count desc
# MAGIC limit 1*/
