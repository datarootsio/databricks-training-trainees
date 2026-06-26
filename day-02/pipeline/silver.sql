CREATE OR REFRESH STREAMING TABLE slv_orders
  COMMENT "Cleaned orders — types cast, invalid rows dropped"
  CONSTRAINT valid_order_id
    EXPECT (order_id IS NOT NULL)
    ON VIOLATION DROP ROW
  CONSTRAINT valid_order_status
    EXPECT (order_status IS NOT NULL)
AS SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_purchase_timestamp      AS TIMESTAMP) AS order_purchase_ts,
  CAST(order_approved_at             AS TIMESTAMP) AS approved_at,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_at
FROM STREAM brz_orders;


CREATE OR REFRESH STREAMING TABLE slv_order_items
  COMMENT "Cleaned order items — numeric types cast"
  CONSTRAINT valid_order_id
    EXPECT (order_id IS NOT NULL)
    ON VIOLATION DROP ROW
AS SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  CAST(price         AS DOUBLE) AS price,
  CAST(freight_value AS DOUBLE) AS freight_value,
  CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS price_with_freight
FROM STREAM brz_order_items;


CREATE OR REFRESH STREAMING TABLE slv_products
  COMMENT "Products — passed through from bronze"
AS SELECT * FROM STREAM brz_products;


CREATE OR REFRESH STREAMING TABLE slv_sellers
  COMMENT "Sellers — validated"
  CONSTRAINT valid_seller_id
    EXPECT (seller_id IS NOT NULL)
    ON VIOLATION DROP ROW
AS SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM STREAM brz_sellers;


CREATE OR REFRESH STREAMING TABLE slv_order_reviews
  COMMENT "Order reviews — score validated"
  CONSTRAINT valid_score
    EXPECT (review_score BETWEEN 1 AND 5)
AS SELECT
  order_id,
  CAST(review_score AS INT) AS review_score,
  CAST(review_creation_date AS TIMESTAMP) AS review_ts
FROM STREAM brz_order_reviews;
