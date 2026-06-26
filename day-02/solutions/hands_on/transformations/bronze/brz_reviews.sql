-- BRONZE LAYER — brz_reviews
-- Schema: review_id, order_id, review_score, review_comment_title,
--         review_comment_message, review_creation_date, review_answer_timestamp
-- Comment fields are often NULL — validated downstream in slv_reviews.

CREATE OR REFRESH STREAMING TABLE bronze.brz_reviews
COMMENT "Bronze (SQL): raw customer reviews. Comment fields are often NULL — validated in slv_reviews."
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
  pathGlobFilter      => "olist_order_reviews_dataset*.csv"
)
