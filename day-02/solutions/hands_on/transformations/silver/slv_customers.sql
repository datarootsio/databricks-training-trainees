-- =============================================================================
-- SILVER LAYER — slv_customers  (Auto CDC / SCD Type 1)
-- =============================================================================
-- Pattern: 3 instructions dans un seul fichier
--   1. CREATE TEMPORARY VIEW  → transformations de la source
--   2. CREATE STREAMING TABLE → table cible vide avec contraintes
--   3. CREATE FLOW            → flux CDC qui alimente la cible
--
-- Pourquoi SCD Type 1 ?
--   Quand un nouveau CSV est déposé dans le Volume (ex: corrections d'adresses),
--   Auto Loader l'ajoute à brz_customers avec un _ingestion_timestamp plus récent.
--   Le flux CDC upserte chaque ligne par customer_id : la valeur la plus récente
--   gagne, sans conserver l'historique (SCD Type 1).
-- =============================================================================

-- ── STEP 1 : Vue de staging ───────────────────────────────────────────────────
-- Transformations uniquement ; les contraintes sont sur la table cible.
-- STREAM(brz_customers) = lecture incrémentale du Streaming Table bronze.

CREATE TEMPORARY VIEW slv_customers_staged AS
SELECT
  customer_id,
  customer_unique_id,
  CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
  UPPER(TRIM(customer_city))               AS customer_city,
  UPPER(TRIM(customer_state))              AS customer_state,
  _ingestion_timestamp
FROM STREAM(bronze.brz_customers);

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_customers
(
  CONSTRAINT customer_id_not_null        EXPECT (customer_id IS NOT NULL)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT customer_unique_id_not_null EXPECT (customer_unique_id IS NOT NULL) ON VIOLATION DROP ROW,
  -- Avertissement seulement : un nouveau territoire ne doit pas bloquer le pipeline
  CONSTRAINT valid_state EXPECT (UPPER(customer_state) IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS',
    'MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC',
    'SP','SE','TO'
  ))
)
COMMENT "Silver (SQL): SCD Type 1 customers via Auto CDC. Le dernier enregistrement par customer_id gagne."
CLUSTER BY (customer_state, customer_city)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────
-- SEQUENCE BY _ingestion_timestamp : les lignes chargées plus tard (CSV plus récent)
-- écrasent les valeurs précédentes pour le même customer_id.
-- IGNORE NULL UPDATES : un NULL dans le nouveau CSV ne remplace pas une valeur existante.

CREATE FLOW slv_customers_cdc_flow AS AUTO CDC INTO silver.slv_customers
FROM STREAM(slv_customers_staged)
KEYS (customer_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
