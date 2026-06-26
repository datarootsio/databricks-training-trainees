-- BRONZE LAYER — brz_order_items
-- Schema: order_id, order_item_id, product_id, seller_id,
--         shipping_limit_date, price, freight_value

CREATE OR REFRESH STREAMING TABLE bronze.brz_order_items
COMMENT "Bronze (SQL): raw order line items. Each row is one item within an order."
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
  pathGlobFilter      => "olist_order_items_dataset*.csv"
)
