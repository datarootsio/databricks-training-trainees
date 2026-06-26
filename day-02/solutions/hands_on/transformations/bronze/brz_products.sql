-- BRONZE LAYER — brz_products
-- Schema: product_id, product_category_name (Portuguese), product_name_lenght,
--         product_description_lenght, product_photos_qty, product_weight_g,
--         product_length_cm, product_height_cm, product_width_cm

CREATE OR REFRESH STREAMING TABLE bronze.brz_products
COMMENT "Bronze (SQL): raw product catalog. Category names in Portuguese — see slv_products for English translation."
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
  pathGlobFilter      => "olist_products_dataset*.csv"
)
