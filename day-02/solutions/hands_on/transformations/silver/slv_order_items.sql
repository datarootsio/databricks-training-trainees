-- SILVER LAYER — slv_order_items  (Auto CDC / SCD Type 1)
-- Clé primaire composite : (order_id, order_item_id).
-- Si le prix ou les frais d'une ligne sont corrigés dans un nouveau CSV,
-- le flux CDC met à jour uniquement cette ligne sans toucher aux autres.

-- ── STEP 1 : Vue de staging ───────────────────────────────────────────────────

CREATE TEMPORARY VIEW slv_order_items_staged AS
SELECT
  order_id,
  CAST(order_item_id   AS INT)       AS order_item_id,
  product_id,
  seller_id,
  CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
  CAST(price           AS DOUBLE)    AS price,
  CAST(freight_value   AS DOUBLE)    AS freight_value,
  CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS total_item_value,
  _ingestion_timestamp
FROM STREAM(bronze.brz_order_items);

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_order_items
(
  CONSTRAINT order_id_not_null   EXPECT (order_id IS NOT NULL)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT seller_id_not_null  EXPECT (seller_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_price         EXPECT (price > 0)              ON VIOLATION DROP ROW,
  CONSTRAINT valid_freight       EXPECT (freight_value >= 0)
)
COMMENT "Silver (SQL): SCD Type 1 order items via Auto CDC. Clé composite (order_id, order_item_id)."
CLUSTER BY (order_id, product_id, seller_id)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────

CREATE FLOW slv_order_items_cdc_flow AS AUTO CDC INTO silver.slv_order_items
FROM STREAM(slv_order_items_staged)
KEYS (order_id, order_item_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
