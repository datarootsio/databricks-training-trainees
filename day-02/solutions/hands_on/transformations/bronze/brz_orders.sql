-- =============================================================================
-- BRONZE LAYER — brz_orders
-- =============================================================================
-- SQL equivalent of brz_orders.py
--
-- PYTHON vs SQL — side-by-side comparison
-- ┌────────────────────────────────────────────┬──────────────────────────────────────────┐
-- │ Python                                     │ SQL                                      │
-- ├────────────────────────────────────────────┼──────────────────────────────────────────┤
-- │ @dp.table(name="brz_orders", ...)          │ CREATE OR REFRESH STREAMING TABLE        │
-- │ def brz_orders():                          │   brz_orders                         │
-- │   return spark.readStream                  │ AS SELECT * FROM STREAM read_files(...)  │
-- │     .format("cloudFiles")...               │                                          │
-- ├────────────────────────────────────────────┼──────────────────────────────────────────┤
-- │ .option("cloudFiles.inferColumnTypes",...) │ inferColumnTypes => "true"               │
-- │ .option("cloudFiles.schemaEvolutionMode")  │ schemaEvolutionMode => "addNewColumns"   │
-- │  ↑ "cloudFiles." prefix in Python          │  ↑ no prefix in SQL read_files()         │
-- ├────────────────────────────────────────────┼──────────────────────────────────────────┤
-- │ cluster_by_auto=True                       │ CLUSTER BY AUTO                          │
-- │ table_properties={"layer": "bronze"}       │ TBLPROPERTIES ("layer" = "bronze")       │
-- │ F.col("_metadata.file_path")               │ _metadata.file_path  (direct reference) │
-- └────────────────────────────────────────────┴──────────────────────────────────────────┘
--
-- AUTO LOADER IN SQL
-- read_files() is the SQL equivalent of spark.readStream.format("cloudFiles").
-- Wrap it in STREAM(...) to tell the pipeline engine to use streaming semantics.
-- Databricks manages the checkpoint automatically — do NOT pass schemaLocation.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze.brz_orders
COMMENT "Bronze (SQL): raw orders ingested via Auto Loader. New olist_orders_dataset*.csv files are processed incrementally."
CLUSTER BY AUTO
TBLPROPERTIES (
  "layer"                          = "bronze",
  "delta.enableDeletionVectors"    = "true"
)
AS SELECT
  *,
  -- Unity Catalog: _metadata.file_path replaces the blocked input_file_name()
  -- _metadata is a hidden struct exposed by all file-based sources:
  --   _metadata.file_path              → full volume path of the source file
  --   _metadata.file_name              → filename only
  --   _metadata.file_modification_time → last-modified timestamp
  _metadata.file_path  AS _source_file,
  current_timestamp()  AS _ingestion_timestamp
FROM STREAM read_files(
  "/Volumes/swatch_training/main/main_volume/olist/csv/",

  -- ── FORMAT ────────────────────────────────────────────────────────────
  format              => "csv",

  -- ── CSV PARSING ───────────────────────────────────────────────────────
  -- Note: in read_files(), CSV options are passed directly (no "cloudFiles." prefix)
  header              => "true",

  -- ── TYPE INFERENCE ────────────────────────────────────────────────────
  -- Scans a sample of files to infer column types instead of defaulting to STRING
  inferColumnTypes    => "true",

  -- ── SCHEMA EVOLUTION ──────────────────────────────────────────────────
  -- addNewColumns: future files with extra columns extend the schema automatically.
  -- The first update to detect new columns fails once (to apply the schema change),
  -- then succeeds on the next run — same behaviour as in Python.
  schemaEvolutionMode => "addNewColumns",

  -- ── DATA RESCUE ───────────────────────────────────────────────────────
  -- Data that doesn't match the schema is saved as JSON here instead of dropped.
  rescuedDataColumn   => "_rescued_data",

  -- ── FILE SELECTION ─────────────────────────────────────────────────────
  -- Wildcard glob: picks up olist_orders_dataset.csv today AND any future
  -- olist_orders_dataset_*.csv files dropped into the volume.
  pathGlobFilter      => "olist_orders_dataset*.csv"
)
