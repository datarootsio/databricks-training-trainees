-- BRONZE LAYER — brz_payments
-- Schema: order_id, payment_sequential, payment_type,
--         payment_installments, payment_value
-- One order can have multiple payment rows (installments / split payments).

CREATE OR REFRESH STREAMING TABLE bronze.brz_payments
COMMENT "Bronze (SQL): raw payment records. One order may have multiple rows (split/installment payments)."
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
  pathGlobFilter      => "olist_order_payments_dataset*.csv"
)
