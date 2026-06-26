-- BRONZE LAYER — brz_sellers
-- Schema: seller_id, seller_zip_code_prefix, seller_city, seller_state

CREATE OR REFRESH STREAMING TABLE bronze.brz_sellers
COMMENT "Bronze (SQL): raw seller data ingested via Auto Loader."
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
  pathGlobFilter      => "olist_sellers_dataset*.csv"
)
