-- SILVER LAYER — slv_sellers  (Auto CDC / SCD Type 1)
-- Les coordonnées géographiques des vendeurs peuvent être corrigées dans un CSV ultérieur.
-- SCD Type 1 : seul le dernier enregistrement par seller_id est conservé.

-- ── STEP 1 : Vue de staging ───────────────────────────────────────────────────

CREATE TEMPORARY VIEW slv_sellers_staged AS
SELECT
  seller_id,
  CAST(seller_zip_code_prefix AS STRING) AS seller_zip_code_prefix,
  UPPER(TRIM(seller_city))               AS seller_city,
  UPPER(TRIM(seller_state))              AS seller_state,
  _ingestion_timestamp
FROM STREAM(bronze.brz_sellers);

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_sellers
(
  CONSTRAINT seller_id_not_null EXPECT (seller_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_state EXPECT (UPPER(seller_state) IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS',
    'MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC',
    'SP','SE','TO'
  ))
)
COMMENT "Silver (SQL): SCD Type 1 sellers via Auto CDC. Dernier enregistrement par seller_id."
CLUSTER BY (seller_state, seller_city)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────

CREATE FLOW slv_sellers_cdc_flow AS AUTO CDC INTO silver.slv_sellers
FROM STREAM(slv_sellers_staged)
KEYS (seller_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
