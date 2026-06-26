-- =============================================================================
-- SILVER LAYER — slv_orders_parquet_scd2 (SCD Type 2)
-- =============================================================================
-- Same three-step structure as slv_orders_parquet.sql (SCD Type 1),
-- but with STORED AS SCD TYPE 2 and TRACK HISTORY ON.
--
-- KEY DIFFERENCE vs SCD TYPE 1
-- ┌────────────────────────────────────────────────────────────────────────────┐
-- │ SCD Type 1: one row per order_id — in-place update                        │
-- │   order_id │ order_status │ __START_AT │ __END_AT                         │
-- │   abc123   │ delivered    │ 2017-01-05 │ NULL      ← single row           │
-- │                                                                            │
-- │ SCD Type 2: one row per version of order_id                               │
-- │   order_id │ order_status │ __START_AT │ __END_AT                         │
-- │   abc123   │ processing   │ 2017-01-05 │ 2017-01-07  ← old version        │
-- │   abc123   │ delivered    │ 2017-01-07 │ NULL        ← current version    │
-- └────────────────────────────────────────────────────────────────────────────┘
--
-- __START_AT / __END_AT: columns automatically added by the runtime,
--   same type as SEQUENCE BY (here TIMESTAMP).
--   __END_AT = NULL → this is the current version of the row.
--   Standard filter for "current state": WHERE __END_AT IS NULL
--
-- TRACK HISTORY ON * EXCEPT (...)
--   Only changes on the listed columns create a new version.
--   Excluded columns are updated in-place (Type 1) on the current version
--   without creating a new history row.
--   Derived and audit columns are excluded because their change is not
--   an independent business event:
--     - _source_file / _ingestion_timestamp : technical metadata
--     - delivery_days / is_on_time          : computed from other columns
--     - year / month / day                  : immutable partitions per order
--
-- DATASET LIMITATION
--   order_purchase_timestamp never changes for a given order_id.
--   With Olist data, each order_id appears only once —
--   SCD2 history only materializes if a correction arrives
--   in a later file (e.g. a rectified order_status).
--   This script is mainly useful for learning purposes and to prepare for
--   a real CDC flow where statuses evolve.
--
-- RECOMMENDED CLUSTERING FOR SCD2
--   (order_id, __START_AT): optimizes entity lookups AND
--   point-in-time queries (WHERE __START_AT <= D AND (__END_AT > D OR NULL)).
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Temporary view — transformations before CDC
-- Different name from stg_orders_for_cdc to avoid any conflict if both
-- silver files coexist in the same pipeline.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW stg_orders_for_scd2 AS
SELECT
  order_id,
  customer_id,
  order_status,

  CAST(order_purchase_timestamp      AS TIMESTAMP) AS order_purchase_timestamp,
  CAST(order_approved_at             AS TIMESTAMP) AS order_approved_at,
  CAST(order_delivered_carrier_date  AS TIMESTAMP) AS order_delivered_carrier_date,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,

  year,
  month,
  day,

  _source_file,
  _ingestion_timestamp,

  CASE
    WHEN order_delivered_customer_date IS NOT NULL
    THEN DATEDIFF(
      CAST(order_delivered_customer_date AS DATE),
      CAST(order_purchase_timestamp      AS DATE)
    )
  END AS delivery_days,

  CASE
    WHEN order_delivered_customer_date IS NOT NULL
     AND order_estimated_delivery_date IS NOT NULL
    THEN CAST(order_delivered_customer_date AS TIMESTAMP)
          <= CAST(order_estimated_delivery_date AS TIMESTAMP)
  END AS is_on_time

FROM STREAM(brz_orders_parquet);


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Silver SCD2 table declaration (CDC target)
-- No AS SELECT — populated by the AUTO CDC flow below.
-- __START_AT and __END_AT are automatically added by the runtime
-- (same type as SEQUENCE BY = TIMESTAMP). Do not declare them here.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REFRESH STREAMING TABLE slv_orders_parquet_scd2
(
  CONSTRAINT order_id_not_null    EXPECT (order_id IS NOT NULL)    ON VIOLATION FAIL UPDATE,
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_status   EXPECT (order_status IN (
    'delivered','shipped','processing','canceled',
    'invoiced','unavailable','approved','created'
  ))
)
COMMENT "Silver SCD2: full order history — one row per version of order_id.
  __END_AT IS NULL selects the current state. Use __START_AT / __END_AT
  for point-in-time queries or status change analysis."
CLUSTER BY (order_id, __START_AT)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Auto CDC SCD Type 2 flow
-- ─────────────────────────────────────────────────────────────────────────────
-- STORED AS SCD TYPE 2    : preserves version history
-- TRACK HISTORY ON * EXCEPT (...): only business columns trigger
--   a new history row — technical columns are updated Type 1
--   on the current version without creating a new entry
-- IGNORE NULL UPDATES     : a NULL cannot overwrite an existing value
-- ─────────────────────────────────────────────────────────────────────────────
CREATE FLOW slv_orders_scd2_flow AS AUTO CDC INTO slv_orders_parquet_scd2
FROM STREAM(stg_orders_for_scd2)
KEYS (order_id)
IGNORE NULL UPDATES
SEQUENCE BY order_purchase_timestamp
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (
  _source_file,
  _ingestion_timestamp,
  delivery_days,
  is_on_time,
  year,
  month,
  day
);
