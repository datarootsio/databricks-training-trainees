-- BRONZE LAYER — brz_customers
-- Same Auto Loader pattern as brz_orders.sql (see that file for full documentation).
-- Schema: customer_id, customer_unique_id, customer_zip_code_prefix,
--         customer_city, customer_state

CREATE OR REFRESH STREAMING TABLE bronze.brz_customers
COMMENT "Bronze (SQL): raw customer data ingested via Auto Loader. New olist_customers_dataset*.csv files processed incrementally."
CLUSTER BY AUTO
TBLPROPERTIES (
  "layer"                       = "bronze",
  "delta.enableDeletionVectors" = "true"
)
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingestion_timestamp
FROM STREAM read_files(
  "/Volumes/swatch_training/main/main_volume/olist/csv/",
  format              => "csv",
  header              => "true",
  inferColumnTypes    => "true",
  schemaEvolutionMode => "addNewColumns",
  rescuedDataColumn   => "_rescued_data",
  pathGlobFilter      => "olist_customers_dataset*.csv"
)
