# Databricks notebook source

# MAGIC %md
# MAGIC # Day 1 — Bonus 06: Spark Performance & Delta Optimization (SOLUTION)
# MAGIC
# MAGIC Complete answers for the bonus performance / Delta-optimization notebook.
# MAGIC **Catalog:** `training_<name>` | **Schema:** `landing`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

CATALOG = "training_<name>"   # <-- replace <name> with your own
SCHEMA  = "landing"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")

orders      = spark.table("orders")
order_items = spark.table("order_items")
products    = spark.table("products")
order_reviews = spark.table("order_reviews")
customers   = spark.table("customers")
translation = spark.table("product_category_name_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A — Narrow vs wide + .explain()

# COMMAND ----------

# A solution: a join is a wide op -> at least one Exchange (shuffle).
# order_id is high-cardinality, so Spark typically picks SortMergeJoin (2 Exchanges,
# one per side). If one side were small enough it would auto-broadcast instead.
joined = order_items.join(orders, on="order_id", how="inner")
joined.explain()
# Read the plan bottom-up: look for "Exchange hashpartitioning(order_id, ...)" on both
# sides and a "SortMergeJoin" node above them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B — Broadcast vs no broadcast

# COMMAND ----------

items_with_cat = order_items.join(products, on="product_id", how="left")

# B solution: without broadcast(), Spark still auto-broadcasts the 71-row table
# (well under the 10 MB threshold) -> BroadcastHashJoin. If the table were ~5 GB it
# would exceed the threshold and fall back to a shuffle-heavy SortMergeJoin.
no_hint = items_with_cat.join(translation, on="product_category_name", how="left")
no_hint.select("order_id", "product_category_name_english", "price").explain()

# Force-disable auto broadcast to SEE the SortMergeJoin it would use for a big table:
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C — Deduplicate the duplicated review_id rows

# COMMAND ----------

# C solution: keep one row per review_id (the most recently answered)
w = Window.partitionBy("review_id").orderBy(F.col("review_answer_timestamp").desc())
reviews_dedup = (
    order_reviews
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
print("before:", order_reviews.count(), " after dedup:", reviews_dedup.count())
# The dedup'd table is now safe to use as a one-row-per-review dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D — Cardinality of the partition column

# COMMAND ----------

# D solution
n_months = spark.table("orders_by_month").select("purchase_month").distinct().count()
print("distinct purchase_month values:", n_months)   # ~25
# Partitioning by the full order_purchase_timestamp would create ~99k partitions
# (one per distinct second) -> millions of tiny files, huge metadata overhead, slow
# reads. Partition only on LOW-cardinality columns; for finer access use liquid
# clustering (Part F) instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E — OPTIMIZE + ZORDER on the real filter column

# COMMAND ----------

# MAGIC %sql
# MAGIC -- E solution: we filter order_items by seller_id most often, so Z-order on it
# MAGIC OPTIMIZE order_items_opt ZORDER BY (seller_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the metrics: numFilesAdded / numFilesRemoved, etc.
# MAGIC DESCRIBE HISTORY order_items_opt;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F — Change liquid-clustering keys

# COMMAND ----------

# MAGIC %sql
# MAGIC -- F solution: liquid clustering lets you change keys without rewriting the table layout
# MAGIC ALTER TABLE order_items_lc CLUSTER BY (order_id);
# MAGIC OPTIMIZE order_items_lc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## G — VACUUM with default retention

# COMMAND ----------

# MAGIC %sql
# MAGIC -- G solution
# MAGIC VACUUM order_items_opt RETAIN 168 HOURS;
# MAGIC -- After this, files needed ONLY by versions older than 7 days are deleted.
# MAGIC -- order_items_opt was created moments ago, so version 0's files are < 7 days old
# MAGIC -- and are kept -> VERSION AS OF 0 still works here. In a table older than the
# MAGIC -- retention window, vacuuming WOULD make old versions un-queryable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H — Sentiment without a UDF

# COMMAND ----------

# H solution: built-in when/otherwise -> Catalyst-optimised, no Python serialization
fast = order_reviews.withColumn(
    "sentiment",
    F.when(F.col("review_score") >= 4, "positive")
     .when(F.col("review_score") == 3, "neutral")
     .otherwise("negative")
)
fast.groupBy("sentiment").count().orderBy(F.desc("count")).show()
# Same result as the UDF, but it stays inside the JVM/Photon and can be optimised.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC | Symptom | Tool |
# MAGIC |---------|------|
# MAGIC | `Exchange` in the plan you didn't expect | reduce shuffles; broadcast small sides |
# MAGIC | One task far slower than the rest | data skew -> salt the hot key, or dedupe |
# MAGIC | Queries scan the whole table | ZORDER / liquid clustering on the filter column |
# MAGIC | Thousands of tiny files | `OPTIMIZE` (compaction) |
# MAGIC | Storage keeps growing | `VACUUM` (mind the 7-day retention) |
# MAGIC | A slow row-by-row transform | replace the Python UDF with built-ins |
