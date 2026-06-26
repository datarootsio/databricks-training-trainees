-- GOLD LAYER — gld_delivery_performance
-- Monthly delivery performance metrics: on_time_rate, avg_delivery_days,
-- cancellation rate. Source: slv_orders.

CREATE OR REFRESH MATERIALIZED VIEW gold.gld_delivery_performance
COMMENT "Gold (SQL): monthly delivery performance metrics. on_time_rate, avg_delivery_days, cancellation rates by month."
CLUSTER BY (order_month)
TBLPROPERTIES (
  "layer"                   = "gold",
  "delta.enableRowTracking" = "true"
)
AS SELECT
  DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM') AS order_month,
  COUNT(*)                                          AS order_count,

  -- Conditional aggregation: SUM(CASE WHEN condition THEN 1 ELSE 0 END)
  -- is the standard SQL pattern for counting rows matching a boolean expression.
  SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
  SUM(CASE WHEN order_status = 'canceled'  THEN 1 ELSE 0 END) AS canceled_count,

  -- avg_delivery_days: computed only for delivered orders (NULLIF for non-delivered)
  ROUND(AVG(CASE WHEN order_status = 'delivered' THEN delivery_days END), 1) AS avg_delivery_days,

  -- on_time_rate: count(on time) / count(has delivery result)
  -- NULLIF(..., 0) prevents division by zero when no deliveries exist yet
  ROUND(
    SUM(CASE WHEN is_on_time = true  THEN 1 ELSE 0 END)
    / NULLIF(SUM(CASE WHEN is_on_time IS NOT NULL THEN 1 ELSE 0 END), 0),
    4
  ) AS on_time_rate,

  SUM(CASE WHEN is_on_time = false THEN 1 ELSE 0 END) AS late_delivery_count

FROM silver.slv_orders
GROUP BY DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM')
ORDER BY order_month
