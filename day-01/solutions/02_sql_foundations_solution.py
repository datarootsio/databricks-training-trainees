# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 — SQL Foundations: SOLUTION
# MAGIC
# MAGIC This notebook contains the complete solutions for the SQL exercises.
# MAGIC All queries use Databricks SQL (Spark SQL / Delta Lake dialect).
# MAGIC
# MAGIC **Dataset context:** Olist Brazilian e-commerce — tables in `landing` schema:
# MAGIC `orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `customers`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Catalog and Schema
# MAGIC
# MAGIC Set the active catalog and schema so every subsequent SQL cell can reference
# MAGIC tables by their short name (e.g. `orders` instead of `training_<name>.landing.orders`).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `training_<name>`;
# MAGIC USE SCHEMA landing;
# MAGIC -- NOTE: Setting the catalog and schema avoids repeating them in every query.
# MAGIC -- Replace <name> with your own name (e.g., training_jan).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: Basic SELECT + WHERE + ORDER BY
# MAGIC
# MAGIC **Task:** Return all canceled orders, most recent first.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: canceled orders, most recent first
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     order_purchase_timestamp
# MAGIC FROM orders
# MAGIC WHERE order_status = 'canceled'
# MAGIC -- WHY: ORDER BY DESC puts the most recent records first — common in operational dashboards
# MAGIC ORDER BY order_purchase_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part A — Explanation
# MAGIC
# MAGIC - `WHERE` filters rows **before** any grouping or sorting — it operates on raw row data.
# MAGIC - `ORDER BY ... DESC` sorts the result set in descending order; without it the row order is non-deterministic in distributed systems.
# MAGIC - Always be explicit about sort direction (`ASC`/`DESC`) for clarity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: GROUP BY + Aggregation
# MAGIC
# MAGIC **Task:** Compute the average payment value per payment type, rounded to 2 decimal places,
# MAGIC sorted from highest to lowest average.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: average payment value per payment type, rounded to 2 decimal places
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     COUNT(*) AS payment_count,
# MAGIC     -- WHY: ROUND(..., 2) ensures monetary values display cleanly
# MAGIC     ROUND(AVG(payment_value), 2) AS avg_payment_value
# MAGIC FROM order_payments
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY avg_payment_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part B — Explanation
# MAGIC
# MAGIC - `GROUP BY payment_type` collapses all rows sharing the same payment type into a single output row.
# MAGIC - `AVG()`, `COUNT()`, `SUM()`, `MIN()`, `MAX()` are aggregate functions — they reduce many rows to one value per group.
# MAGIC - Including `payment_count` alongside the average gives useful context (a high average on 3 transactions means less than on 3,000).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: HAVING
# MAGIC
# MAGIC **Task:** Find all products (`product_id`) that appear in more than 100 order line items.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: product categories with more than 100 order line items
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     COUNT(*) AS item_count
# MAGIC FROM order_items
# MAGIC GROUP BY product_id
# MAGIC -- WHY: HAVING filters AFTER aggregation; WHERE filters BEFORE (cannot use aggregate functions in WHERE)
# MAGIC HAVING COUNT(*) > 100
# MAGIC ORDER BY item_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part C — Explanation
# MAGIC
# MAGIC - `WHERE` cannot reference aggregate functions (e.g. `WHERE COUNT(*) > 100` is invalid).
# MAGIC - `HAVING` is evaluated **after** `GROUP BY` and **after** aggregation, so it can filter on aggregated values.
# MAGIC - A common mistake: using `WHERE` instead of `HAVING` for post-aggregation filters. The database engine will raise an error.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: JOINs
# MAGIC
# MAGIC **Task:** Join `orders`, `order_items`, and `products` to see the category name and price
# MAGIC alongside each order.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: join orders, order_items, products
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     p.product_category_name,
# MAGIC     oi.price
# MAGIC FROM orders o
# MAGIC -- WHY: JOIN (INNER) here keeps only orders that have matching line items and products.
# MAGIC -- Switch to LEFT JOIN if you want to keep orders even when product metadata is missing.
# MAGIC JOIN order_items oi ON o.order_id = oi.order_id
# MAGIC JOIN products p ON oi.product_id = p.product_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part D — Explanation
# MAGIC
# MAGIC - An `INNER JOIN` returns only rows where the join condition matches on **both** sides.
# MAGIC - A `LEFT JOIN` keeps all rows from the left table even when there is no match on the right (missing right-side columns become `NULL`).
# MAGIC - Table aliases (`o`, `oi`, `p`) are essential when joining multiple tables — they make column references unambiguous.
# MAGIC - `LIMIT 20` is good practice during development to avoid accidentally returning millions of rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E: Common Table Expressions (CTEs)
# MAGIC
# MAGIC **Task:** Using a CTE, compute the average review score per product, then filter to
# MAGIC products with an average of 4.0 or higher.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: CTE computing avg review score per product, filtered to >= 4.0
# MAGIC WITH product_avg_reviews AS (
# MAGIC     SELECT
# MAGIC         oi.product_id,
# MAGIC         -- WHY: ROUND for readability; AVG needs GROUP BY
# MAGIC         ROUND(AVG(r.review_score), 2) AS avg_score,
# MAGIC         COUNT(r.review_id) AS review_count
# MAGIC     FROM order_items oi
# MAGIC     JOIN order_reviews r ON oi.order_id = r.order_id
# MAGIC     GROUP BY oi.product_id
# MAGIC )
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     avg_score,
# MAGIC     review_count
# MAGIC FROM product_avg_reviews
# MAGIC -- WHY: Filter in outer query because HAVING would also work inside the CTE,
# MAGIC --      but filtering after the CTE makes the intent clearer and keeps CTEs reusable.
# MAGIC WHERE avg_score >= 4.0
# MAGIC ORDER BY avg_score DESC, review_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part E — Explanation
# MAGIC
# MAGIC - A CTE (`WITH ... AS (...)`) is a named sub-query scoped to the current statement.
# MAGIC - CTEs improve readability by letting you name and describe intermediate result sets.
# MAGIC - You can chain multiple CTEs: `WITH cte1 AS (...), cte2 AS (...) SELECT ...`.
# MAGIC - The outer `WHERE avg_score >= 4.0` filters the already-aggregated CTE result — equivalent to `HAVING` inside the CTE, but more transparent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part F: Window Functions
# MAGIC
# MAGIC **Task:** Rank customers by their number of orders within each state.
# MAGIC Return only the top-ranked customer per state.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO solution: rank customers by order count within each state
# MAGIC -- WHY customer_unique_id: in Olist, customer_id is generated per order, so counting
# MAGIC --     orders by customer_id returns ~1 for everyone. customer_unique_id is the
# MAGIC --     persistent identity (the busiest customer has 17 orders).
# MAGIC WITH customer_order_counts AS (
# MAGIC     SELECT
# MAGIC         c.customer_unique_id,
# MAGIC         c.customer_state,
# MAGIC         COUNT(*) AS order_count
# MAGIC     FROM orders o
# MAGIC     JOIN customers c ON o.customer_id = c.customer_id
# MAGIC     GROUP BY c.customer_unique_id, c.customer_state
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         customer_unique_id,
# MAGIC         customer_state,
# MAGIC         order_count,
# MAGIC         -- WHY: RANK() gives the same rank to ties (vs ROW_NUMBER which always gives unique ranks)
# MAGIC         RANK() OVER (PARTITION BY customer_state ORDER BY order_count DESC) AS state_rank
# MAGIC     FROM customer_order_counts
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC -- NOTE: Filtering on a window function result (state_rank = 1) is only possible
# MAGIC --       in an outer query — you cannot use RANK() in a WHERE clause directly.
# MAGIC WHERE state_rank = 1
# MAGIC ORDER BY customer_state;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part F — Explanation
# MAGIC
# MAGIC Window functions (`RANK()`, `ROW_NUMBER()`, `LAG()`, `LEAD()`, etc.) compute a value
# MAGIC **across a set of rows** related to the current row, without collapsing them like `GROUP BY` does.
# MAGIC
# MAGIC Key clauses:
# MAGIC - `PARTITION BY customer_state` — resets the ranking counter for each state.
# MAGIC - `ORDER BY order_count DESC` — defines the ranking criterion within each partition.
# MAGIC - `RANK()` vs `ROW_NUMBER()`:
# MAGIC   - `RANK()` assigns the same rank to tied rows and skips numbers (1, 1, 3 ...).
# MAGIC   - `ROW_NUMBER()` always assigns unique numbers regardless of ties (1, 2, 3 ...).
# MAGIC - You **cannot** filter on a window function in the same query level — wrap it in a CTE or subquery first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC | Part | Feature | Key rule |
# MAGIC |------|---------|----------|
# MAGIC | A | SELECT / WHERE / ORDER BY | `WHERE` filters rows; `ORDER BY` sorts results |
# MAGIC | B | GROUP BY / Aggregates | Every non-aggregate column must appear in `GROUP BY` |
# MAGIC | C | HAVING | Use `HAVING` (not `WHERE`) to filter after aggregation |
# MAGIC | D | JOINs | `INNER JOIN` = intersection; `LEFT JOIN` = keep all left rows |
# MAGIC | E | CTEs | Named sub-queries that improve readability and reusability |
# MAGIC | F | Window functions | `OVER (PARTITION BY ... ORDER BY ...)` — ranks without collapsing rows |
