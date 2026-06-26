-- =============================================================================
-- BRONZE LAYER — brz_orders_parquet
-- =============================================================================
-- Auto Loader on partitioned Parquet files (year/month/day) stored in a Volume.
--
-- INCREMENTAL PROCESSING (Auto Loader checkpoint)
-- ┌──────────────────────────────────────────────────────────────────────────┐
-- │ 1st run : Volume contains 2016 + 2017 → all these files are processed   │
-- │ 2nd run : 2018 files are dropped into the Volume                        │
-- │           Auto Loader resumes from the checkpoint — only the new        │
-- │           files (year=2018/) are read. Already-processed files are      │
-- │           not re-ingested.                                              │
-- └──────────────────────────────────────────────────────────────────────────┘
--
-- HIVE-STYLE PARTITIONS
-- The Volume path follows the pattern  year=YYYY/month=M/day=D/
-- Auto Loader automatically detects these partitions and exposes them
-- as year, month, day columns in the result.
--
-- PARQUET vs CSV
-- With the parquet format, inferColumnTypes is not needed:
-- types are already encoded in the parquet file metadata.
-- schemaEvolutionMode is still useful to accommodate new fields
-- if the source schema evolves.
--
-- OPERATIONAL NOTE — Volume path to configure
-- Copy local files to the Volume before running the pipeline:
--   databricks fs cp --recursive ./data/parquet/orders/ \
--     dbfs:/Volumes/dbwdemo/default/sources/orders_parquet/
-- (or via the Databricks UI → Catalog → Volumes → Upload)
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE brz_orders_parquet
COMMENT "Bronze: raw orders incrementally ingested from partitioned Parquet files
  (year/month/day) stored in the Volume.
  Auto Loader maintains a checkpoint: only new files are processed on each run.
  The year/month/day columns come from the Hive-style partitions."
CLUSTER BY AUTO
TBLPROPERTIES (
  "layer"                       = "bronze",
  "delta.enableDeletionVectors" = "true"
)
AS SELECT
  *,
  -- Unity Catalog: _metadata.file_path provides the full path of the source file
  -- (replaces input_file_name() which is blocked under UC)
  _metadata.file_path   AS _source_file,
  current_timestamp()   AS _ingestion_timestamp
FROM STREAM read_files(
  "<VOLUME _ PATH>",

  -- ── FORMAT ──────────────────────────────────────────────────────────────
  format              => "parquet",

  -- ── SCHEMA EVOLUTION ────────────────────────────────────────────────────
  -- addNewColumns: if a future parquet file adds a column,
  -- the table schema is automatically extended.
  -- The first run that detects the new column fails once
  -- (to apply the schema change), then succeeds on the next run.
  schemaEvolutionMode => "addNewColumns",

  -- ── DATA RESCUE ─────────────────────────────────────────────────────────
  -- Data that does not match the schema is saved here
  -- as JSON rather than being silently lost.
  rescuedDataColumn   => "_rescued_data"
)
