-- SILVER LAYER — slv_reviews  (Auto CDC / SCD Type 1)
-- Les avis peuvent être mis à jour (ex: review_answer_timestamp renseigné plus tard).
-- SCD Type 1 : le flux CDC applique le dernier enregistrement par review_id.

-- ── STEP 1 : Vue de staging ───────────────────────────────────────────────────

CREATE TEMPORARY VIEW slv_reviews_staged AS
SELECT
  review_id,
  order_id,
  CAST(review_score AS INT)                  AS review_score,
  TRIM(review_comment_title)                 AS review_comment_title,
  TRIM(review_comment_message)               AS review_comment_message,
  CAST(review_creation_date AS TIMESTAMP)    AS review_creation_date,
  CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
  (review_comment_message IS NOT NULL AND LENGTH(TRIM(review_comment_message)) > 0) AS has_comment,
  _ingestion_timestamp
FROM STREAM(bronze.brz_reviews);

-- ── STEP 2 : Table cible CDC ──────────────────────────────────────────────────

CREATE OR REFRESH STREAMING TABLE silver.slv_reviews
(
  CONSTRAINT review_id_not_null EXPECT (review_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT order_id_not_null  EXPECT (order_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_score        EXPECT (review_score BETWEEN 1 AND 5) ON VIOLATION DROP ROW
)
COMMENT "Silver (SQL): SCD Type 1 reviews via Auto CDC. Mises à jour (ex: réponse ajoutée) écrasent l'enregistrement existant."
CLUSTER BY (order_id, review_score)
TBLPROPERTIES (
  "layer"                       = "silver",
  "delta.enableDeletionVectors" = "true"
);

-- ── STEP 3 : Flux CDC ─────────────────────────────────────────────────────────

CREATE FLOW slv_reviews_cdc_flow AS AUTO CDC INTO silver.slv_reviews
FROM STREAM(slv_reviews_staged)
KEYS (review_id)
IGNORE NULL UPDATES
SEQUENCE BY _ingestion_timestamp
STORED AS SCD TYPE 1;
