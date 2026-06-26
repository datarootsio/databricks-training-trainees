-- BRONZE LAYER — brz_translation
-- Schema: product_category_name (Portuguese), product_category_name_english
-- Small lookup table used in slv_products via a stream-static join.

CREATE OR REFRESH STREAMING TABLE bronze.brz_translation
COMMENT "Bronze (SQL): Portuguese→English category name lookup. Used in slv_products as a static dimension."
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
  pathGlobFilter      => "product_category_name_translation*.csv"
)
