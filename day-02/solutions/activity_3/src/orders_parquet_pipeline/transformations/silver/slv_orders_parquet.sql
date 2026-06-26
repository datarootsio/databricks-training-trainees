-- =============================================================================
-- SILVER LAYER — slv_orders_parquet
-- =============================================================================
-- Auto CDC pattern (SCD Type 1) in three steps:
--
-- STEP 1 — TEMPORARY VIEW (stg_orders_for_cdc)
--   Transformations before CDC: TIMESTAMP casts, derived columns.
--   The CDC engine (AUTO CDC INTO) cannot transform data
--   on its own — it can only select/exclude columns.
--   All transformation logic is therefore delegated to the temporary view,
--   and the CDC flow reads from that view.
--
-- STEP 2 — CREATE OR REFRESH STREAMING TABLE (empty declaration)
--   The CDC target table is created separately, without AS SELECT.
--   Constraints (expectations) are declared here.
--
-- STEP 3 — CREATE FLOW ... AS AUTO CDC INTO
--   Applies CDC events from the view to the Silver table.
--   KEYS (order_id)        → one row per order (deduplication)
--   SEQUENCE BY ...        → the version with the most recent timestamp wins
--   STORED AS SCD TYPE 1   → in-place update (no history)
--   IGNORE NULL UPDATES    → a NULL cannot overwrite an existing value
--
-- WHY AUTO CDC HERE?
-- ┌──────────────────────────────────────────────────────────────────────────┐
-- │ • Idempotence: re-running the pipeline on the same files does not create │
-- │   duplicates — the order_id key guarantees uniqueness in Silver.        │
-- │ • Adding 2018: when 2018 files arrive in the Volume,                    │
-- │   Auto Loader only reads those new files (Bronze checkpoint),            │
-- │   and Auto CDC upserts only the relevant order_ids in Silver.           │
-- │ • If an order appears in two distinct files (corrections),               │
-- │   the version with the most recent order_purchase_timestamp wins.        │
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- NOTE: a Streaming Table targeted by AUTO CDC contains updates/deletes.
-- If a downstream table reads from slv_orders_parquet with STREAM(),
-- add the option skipChangeCommits => true to avoid errors.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Temporary view — transformations before CDC
-- ─────────────────────────────────────────────────────────────────────────────
-- TEMPORARY VIEW = pipeline-private (not published in Unity Catalog)
-- FROM STREAM(brz_orders_parquet) = streaming read from the Bronze table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW stg_orders_for_cdc AS
SELECT
  order_id,
  customer_id,
  order_status,

  -- Explicit TIMESTAMP cast: documents intent even if parquet already
  -- encodes types — guards against edge cases at ingestion.
  CAST(order_purchase_timestamp      AS TIMESTAMP) AS order_purchase_timestamp,
  CAST(order_approved_at             AS TIMESTAMP) AS order_approved_at,
  CAST(order_delivered_carrier_date  AS TIMESTAMP) AS order_delivered_carrier_date,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,

  -- Hive-style partition columns inherited from Bronze
  year,
  month,
  day,

  _source_file,
  _ingestion_timestamp,

  -- Actual delivery duration in calendar days (NULL if not yet delivered)
  CASE
    WHEN order_delivered_customer_date IS NOT NULL
    THEN DATEDIFF(
      CAST(order_delivered_customer_date AS DATE),
      CAST(order_purchase_timestamp      AS DATE)
    )
  END AS delivery_days,

  -- TRUE = delivered on time, FALSE = late, NULL = not yet delivered
  CASE
    WHEN order_delivered_customer_date IS NOT NULL
     AND order_estimated_delivery_date IS NOT NULL
    THEN CAST(order_delivered_customer_date AS TIMESTAMP)
          <= CAST(order_estimated_delivery_date AS TIMESTAMP)
  END AS is_on_time

FROM STREAM(brz_orders_parquet);


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Silver table declaration (CDC target)
-- ─────────────────────────────────────────────────────────────────────────────
-- No AS SELECT here — the table is empty at creation.
-- The AUTO CDC flow (step 3) is responsible for populating it.
-- Constraints (expectations) are declared in this definition.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING TABLE slv_orders_parquet
(
  -- FAIL UPDATE: the pipeline stops if order_id is NULL (CDC primary key)
  CONSTRAINT order_id_not_null    EXPECT (order_id IS NOT NULL)    ON VIOLATION FAIL UPDATE,
  -- DROP ROW: the row is silently discarded if customer_id is NULL
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  -- Warn only: a new business status does not break the pipeline
  CONSTRAINT valid_order_status   EXPECT (order_status IN (
    'delivered','shipped','processing','canceled',
    'invoiced','unavailable','approved','created'
  ))
)
COMMENT "Silver: deduplicated orders via Auto CDC — one canonical row per order_id (SCD Type 1).
  Explicit TIMESTAMP casts, derived columns delivery_days and is_on_time.
  Adding 2018 data: Auto Loader processes only new files,
  Auto CDC upserts only the relevant order_ids."
CLUSTER BY (order_status, order_purchase_timestamp)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Auto CDC flow — applies upserts to the Silver table
-- ─────────────────────────────────────────────────────────────────────────────
-- CREATE FLOW  : names the flow (visible in the pipeline UI)
-- AUTO CDC INTO: current syntax (APPLY CHANGES INTO = deprecated)
-- FROM STREAM(): reads from the temporary view with streaming semantics
-- KEYS         : column(s) uniquely identifying a row
-- IGNORE NULL UPDATES: a NULL field cannot overwrite an existing value
-- SEQUENCE BY  : determines which version "wins" in case of conflict
-- SCD TYPE 1   : in-place update (no version history)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE FLOW slv_orders_cdc_flow AS 
AUTO CDC INTO slv_orders_parquet
FROM STREAM(stg_orders_for_cdc)
KEYS (order_id)
IGNORE NULL UPDATES
SEQUENCE BY order_purchase_timestamp
STORED AS SCD TYPE 1;
