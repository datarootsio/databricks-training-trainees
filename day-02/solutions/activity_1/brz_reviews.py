# =============================================================================
# BRONZE LAYER — brz_reviews
# =============================================================================
# Schema: review_id, order_id, review_score, review_comment_title,
#         review_comment_message, review_creation_date, review_answer_timestamp
# Note: review_comment fields can be NULL (many reviews have no text).
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_PATH = "<TO COMPLETE>"


@dp.table(
    name="brz_reviews",
    comment=(
        "Bronze: raw customer reviews ingested via Auto Loader. "
        "Comment fields are often NULL — validated in slv_reviews."
    ),
    cluster_by_auto=True,
    table_properties={
        "layer": "bronze",
        "delta.enableDeletionVectors": "true",
    },
)
def brz_reviews():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "olist_order_reviews_dataset*.csv")
        .load(VOLUME_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )
