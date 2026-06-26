-- =============================================================================
-- SILVER LAYER — slv_products  (Auto CDC / SCD Type 1)
-- =============================================================================
-- La vue de staging réalise la jointure stream-statique avec brz_translation
-- AVANT que le flux CDC ne propage les lignes dans la table cible.
--
-- Jointure stream-statique en SQL :
--   FROM STREAM(brz_products) p          ← lecture incrémentale (streaming)
--   LEFT JOIN brz_translation t ON ...   ← snapshot statique (pas de STREAM)
-- Databricks gère automatiquement ce pattern sans watermark ni state store.
-- =============================================================================

-- ── STEP 1 : Vue de staging (jointure stream-statique) ────────────────────────

CREATE TEMPORARY VIEW slv_products_staged AS
SELECT
  p.product_id,
  p.product_category_name,
  t.product_category_name_english,
  CAST(p.product_name_lenght        AS INT)    AS product_name_length,
  CAST(p.product_description_lenght AS INT)    AS product_description_length,
  CAST(p.product_photos_qty         AS INT)    AS product_photos_qty,
  CAST(p.product_weight_g           AS DOUBLE) AS product_weight_g,
  CAST(p.product_length_cm          AS DOUBLE) AS product_length_cm,
  CAST(p.product_height_cm          AS DOUBLE) AS product_height_cm,
  CAST(p.product_width_cm           AS DOUBLE) AS product_width_cm,
  p._ingestion_timestamp
FROM STREAM(bronze.brz_products) p
-- Pas de STREAM() sur brz_translation → lecture batch (snapshot au démarrage)
LEFT JOIN bronze.brz_translation t ON p.product_category_name = t.product_category_name;

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_products
(
  CONSTRAINT product_id_not_null EXPECT (product_id IS NOT NULL)                ON VIOLATION FAIL UPDATE,
  CONSTRAINT category_not_null   EXPECT (product_category_name IS NOT NULL),
  CONSTRAINT valid_weight        EXPECT (product_weight_g IS NULL OR product_weight_g > 0)
)
COMMENT "Silver (SQL): SCD Type 1 products via Auto CDC. Nom de catégorie anglais joint depuis brz_translation."
CLUSTER BY (product_category_name_english)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────

CREATE FLOW slv_products_cdc_flow AS AUTO CDC INTO silver.slv_products
FROM STREAM(slv_products_staged)
KEYS (product_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
