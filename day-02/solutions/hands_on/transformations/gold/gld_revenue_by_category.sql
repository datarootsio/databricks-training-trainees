-- =============================================================================
-- GOLD LAYER — gld_revenue_by_category
-- =============================================================================
-- SQL equivalent of gld_revenue_by_category.py
--
-- KEY RULE: Gold uses MATERIALIZED VIEW (not STREAMING TABLE).
-- Aggregations (GROUP BY, SUM, COUNT) need to see the full dataset to stay
-- correct when historical rows change. Materialized Views handle this by
-- recomputing (or incrementally refreshing on serverless pipelines).
--
-- Python vs SQL:
--   Python: @dp.materialized_view() + spark.read.table("slv_*")   ← batch read
--   SQL:    CREATE OR REFRESH MATERIALIZED VIEW + SELECT FROM slv_* ← no STREAM()
--
-- IMPORTANT: do NOT use STREAM() in Gold. Using STREAM() here would make it a
-- Streaming Table, which cannot correctly maintain aggregations over changing data.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.gld_revenue_by_category
COMMENT "Gold (SQL): total revenue, order count, and average item value by product category (English). Delivered orders only."
CLUSTER BY (category)
TBLPROPERTIES (
  "layer"                    = "gold",
  -- Row tracking enables incremental refresh on serverless pipelines.
  -- Without it, Databricks performs a full recompute on every pipeline run.
  "delta.enableRowTracking"  = "true"
)
AS
-- CTEs (WITH clauses) are supported and recommended in Gold for readability.
WITH delivered AS (
  -- Filter once and reuse — only delivered orders count as revenue.
  SELECT order_id FROM silver.slv_orders WHERE order_status = 'delivered'
),
enriched_items AS (
  SELECT
    i.order_id,
    i.price,
    i.freight_value,
    i.total_item_value,
    -- Replace NULL category with a placeholder for clean GROUP BY results
    COALESCE(p.product_category_name_english, 'uncategorized') AS category
  FROM silver.slv_order_items i
  -- Only items from delivered orders count toward revenue
  INNER JOIN delivered       d ON i.order_id    = d.order_id
  -- Left join to get category — some products may not have a category
  LEFT  JOIN silver.slv_products p ON i.product_id = p.product_id
)
SELECT
  category,
  ROUND(SUM(total_item_value), 2)  AS total_revenue,
  ROUND(SUM(price), 2)             AS total_product_revenue,
  ROUND(SUM(freight_value), 2)     AS total_freight_revenue,
  COUNT(DISTINCT order_id)         AS order_count,
  COUNT(*)                         AS item_count,
  ROUND(AVG(total_item_value), 2)  AS avg_item_value
FROM enriched_items
GROUP BY category
ORDER BY total_revenue DESC
