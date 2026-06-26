CREATE OR REFRESH MATERIALIZED VIEW gld_revenue_by_category
  COMMENT "Monthly revenue by product category (English name)"
AS SELECT
  COALESCE(t.product_category_name_english,
           p.product_category_name)           AS category,
  date_trunc('month', o.order_purchase_ts)    AS month,
  ROUND(SUM(oi.price), 2)                     AS revenue,
  COUNT(DISTINCT oi.order_id)                 AS order_count
FROM slv_order_items oi
JOIN slv_orders  o  ON oi.order_id  = o.order_id
JOIN slv_products p ON oi.product_id = p.product_id
LEFT JOIN brz_translation t
  ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC;


CREATE OR REFRESH MATERIALIZED VIEW gld_delivery_performance
  COMMENT "Delivery KPIs by seller state"
AS SELECT
  s.seller_state,
  COUNT(*)                                                              AS total_orders,
  ROUND(AVG(DATEDIFF(day, o.order_purchase_ts, o.delivered_at)), 1)    AS avg_delivery_days,
  ROUND(
    100.0 * SUM(CASE WHEN o.delivered_at > o.estimated_delivery_ts THEN 1 ELSE 0 END)
    / COUNT(*), 2
  )                                                                     AS late_pct,
  ROUND(
    100.0 * SUM(CASE WHEN o.delivered_at <= o.estimated_delivery_ts THEN 1 ELSE 0 END)
    / COUNT(*), 2
  )                                                                     AS on_time_pct
FROM slv_orders o
JOIN slv_order_items oi ON o.order_id  = oi.order_id
JOIN slv_sellers     s  ON oi.seller_id = s.seller_id
WHERE o.delivered_at IS NOT NULL
  AND o.order_status = 'delivered'
GROUP BY s.seller_state
ORDER BY avg_delivery_days;


CREATE OR REFRESH MATERIALIZED VIEW gld_review_scores
  COMMENT "Average review score per seller (min 10 reviews)"
AS SELECT
  s.seller_id,
  s.seller_city,
  s.seller_state,
  ROUND(AVG(r.review_score), 2)                                    AS avg_review_score,
  COUNT(*)                                                          AS total_reviews,
  ROUND(
    100.0 * SUM(CASE WHEN r.review_score = 5 THEN 1 ELSE 0 END)
    / COUNT(*), 1
  )                                                                 AS pct_5_star
FROM slv_order_items oi
JOIN slv_order_reviews r ON oi.order_id  = r.order_id
JOIN slv_sellers       s ON oi.seller_id = s.seller_id
WHERE r.review_score IS NOT NULL
GROUP BY s.seller_id, s.seller_city, s.seller_state
HAVING COUNT(*) >= 10
ORDER BY avg_review_score DESC;
