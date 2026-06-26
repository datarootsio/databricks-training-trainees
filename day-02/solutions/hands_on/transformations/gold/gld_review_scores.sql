-- GOLD LAYER — gld_review_scores
-- Average review scores and sentiment distribution by product category.
-- Joins: slv_reviews → slv_orders → slv_order_items → slv_products

CREATE OR REFRESH MATERIALIZED VIEW gold.gld_review_scores
COMMENT "Gold (SQL): avg review score and sentiment by product category. Joins reviews → orders → items → products."
CLUSTER BY (category)
TBLPROPERTIES (
  "layer"                   = "gold",
  "delta.enableRowTracking" = "true"
)
AS
WITH order_products AS (
  -- Deduplicate: one order can contain multiple items (multiple products).
  -- DISTINCT ensures each (order_id, product_id) pair is counted once,
  -- so a review doesn't get inflated by the number of items in the order.
  SELECT DISTINCT order_id, product_id
  FROM silver.slv_order_items
)
SELECT
  COALESCE(p.product_category_name_english, 'uncategorized') AS category,
  COUNT(r.review_id)                                          AS review_count,
  ROUND(AVG(r.review_score), 3)                              AS avg_score,

  -- 5-star rate: identifies top-performing categories
  ROUND(SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_5_stars,

  -- 1-star rate: proxy for customer dissatisfaction
  ROUND(SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_1_stars,

  SUM(CASE WHEN r.has_comment = true THEN 1 ELSE 0 END)      AS reviews_with_comment

FROM silver.slv_reviews       r
JOIN silver.slv_orders        o  ON r.order_id   = o.order_id
JOIN order_products        op ON o.order_id   = op.order_id
LEFT JOIN silver.slv_products p  ON op.product_id = p.product_id

GROUP BY COALESCE(p.product_category_name_english, 'uncategorized')
ORDER BY review_count DESC
