-- =============================================================================
-- SILVER LAYER — slv_orders  (Auto CDC / SCD Type 1)
-- =============================================================================
-- Les commandes changent de statut au fil du temps (created → shipped →
-- delivered → canceled). Chaque nouveau CSV peut contenir des mises à jour
-- de statut : SCD Type 1 écrase l'ancienne ligne pour le même order_id.
--
-- Les colonnes dérivées (delivery_days, is_on_time) sont calculées dans la
-- vue de staging pour que la table cible stocke directement les valeurs métier.
-- =============================================================================

-- ── STEP 1 : Vue de staging ───────────────────────────────────────────────────

CREATE TEMPORARY VIEW slv_orders_staged AS
SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_purchase_timestamp      AS TIMESTAMP) AS order_purchase_timestamp,
  CAST(order_approved_at             AS TIMESTAMP) AS order_approved_at,
  CAST(order_delivered_carrier_date  AS TIMESTAMP) AS order_delivered_carrier_date,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
  _source_file,
  _ingestion_timestamp,
  -- Nombre de jours entre l'achat et la livraison réelle (NULL si non livré)
  CASE
    WHEN order_delivered_customer_date IS NOT NULL
    THEN DATEDIFF(
      CAST(order_delivered_customer_date AS DATE),
      CAST(order_purchase_timestamp      AS DATE)
    )
  END AS delivery_days,
  -- TRUE = livré à temps, FALSE = en retard, NULL = pas encore livré
  CASE
    WHEN order_delivered_customer_date IS NOT NULL
     AND order_estimated_delivery_date IS NOT NULL
    THEN CAST(order_delivered_customer_date AS TIMESTAMP)
          <= CAST(order_estimated_delivery_date AS TIMESTAMP)
  END AS is_on_time
FROM STREAM(bronze.brz_orders);

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_orders
(
  CONSTRAINT order_id_not_null    EXPECT (order_id IS NOT NULL)    ON VIOLATION FAIL UPDATE,
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_status   EXPECT (order_status IN (
    'delivered','shipped','processing','canceled',
    'invoiced','unavailable','approved','created'
  ))
)
COMMENT "Silver (SQL): SCD Type 1 orders via Auto CDC. Mises à jour de statut appliquées par upsert sur order_id."
CLUSTER BY (order_status, order_purchase_timestamp)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────

CREATE FLOW slv_orders_cdc_flow AS AUTO CDC INTO silver.slv_orders
FROM STREAM(slv_orders_staged)
KEYS (order_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
