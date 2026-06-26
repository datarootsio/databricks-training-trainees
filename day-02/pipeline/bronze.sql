CREATE OR REFRESH STREAMING TABLE brz_orders
  COMMENT "Raw orders"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/orders/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_customers
  COMMENT "Raw customers"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/customers/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_order_items
  COMMENT "Raw order items"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/order_items/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_products
  COMMENT "Raw products"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/products/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_sellers
  COMMENT "Raw sellers"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/sellers/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_order_payments
  COMMENT "Raw order payments"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/order_payments/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_order_reviews
  COMMENT "Raw order reviews"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/order_reviews/", format => "parquet");

CREATE OR REFRESH STREAMING TABLE brz_translation
  COMMENT "Category name translation PT → EN"
AS SELECT *, current_timestamp() AS _ingested_at
FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/translation/", format => "parquet");
