# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 · Exercise 05 — Spark and Delta Tips (SOLUTION)
# MAGIC
# MAGIC This notebook contains the answer key for the Spark and Delta tips exercise.
# MAGIC Topics covered: explicit schemas, caching, broadcast joins, MERGE INTO, time travel,
# MAGIC and repartition vs coalesce.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — StructType schema for order_items

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# WHY: Explicit schema avoids a full data scan at read time and prevents silent type coercion errors
order_items_schema = StructType([
    StructField("order_id",            StringType(),    nullable=True),
    StructField("order_item_id",       IntegerType(),   nullable=True),
    StructField("product_id",          StringType(),    nullable=True),
    StructField("seller_id",           StringType(),    nullable=True),
    StructField("shipping_limit_date", TimestampType(), nullable=True),
    StructField("price",               DoubleType(),    nullable=True),
    StructField("freight_value",       DoubleType(),    nullable=True),
])

# NOTE: In Databricks, spark.table() reads from the metastore and already has the schema;
# StructType is most useful when reading raw files (CSV, JSON) where schema is not yet known
print("Schema defined successfully:")
for field in order_items_schema.fields:
    print(f"  {field.name}: {field.dataType} (nullable={field.nullable})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Cache order_items and run two aggregations

# COMMAND ----------

from pyspark.sql import functions as F

order_items = spark.table("training_<name>.landing.order_items")

# WHY: Cache is useful when the same DataFrame is used in multiple actions
#      Without cache, each action re-reads and recomputes the entire lineage
order_items.cache()

# First action — triggers the cache population
total_items = order_items.count()
print(f"Total order items: {total_items:,}")

# Second action — served from cache (much faster)
total_revenue = order_items.agg(F.round(F.sum("price"), 2).alias("total_revenue")).collect()[0][0]
print(f"Total revenue: R$ {total_revenue:,.2f}")

# WHY: Always unpersist when done — releases memory for other operations
order_items.unpersist()
print("Cache released.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Broadcast join through product_category_name_translation

# COMMAND ----------

from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

order_items = spark.table("training_<name>.landing.order_items")
products = spark.table("training_<name>.landing.products")
translation = spark.table("training_<name>.landing.product_category_name_translation")

# WHY: product_category_name_translation is tiny (~71 rows) — perfect broadcast candidate
# Broadcast sends the small table to every executor, eliminating the shuffle join
result = (
    order_items
    .join(products, on="product_id", how="left")
    # WHY: Join on Portuguese category name to get the English translation
    .join(
        broadcast(translation),
        on="product_category_name",
        how="left"
    )
    .select(
        "order_id",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "price"
    )
)
result.limit(10).display()

# NOTE: You can verify the broadcast in the Spark UI — look for "BroadcastHashJoin" in the plan
result.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — MERGE INTO (upsert with Delta Lake)

# COMMAND ----------

# Setup — create target table and source view
spark.sql("""
    CREATE OR REPLACE TABLE `training_<name>`.landing.orders_sample
    AS SELECT * FROM `training_<name>`.landing.orders LIMIT 100
""")
print("Target table created.")

# Create "incoming updates" — a new fake row to be inserted
updates_data = [
    ("FAKE-ORDER-001", "FAKE-CUSTOMER", "processing", None, None, None, None, None),
]
updates_df = spark.createDataFrame(
    updates_data,
    schema=(
        "order_id STRING, customer_id STRING, order_status STRING, "
        "order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, "
        "order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP, "
        "order_estimated_delivery_date TIMESTAMP"
    )
)
updates_df.createOrReplaceTempView("orders_updates")
print("Source view created.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WHY: MERGE INTO is Delta Lake's upsert — handles both updates and inserts in one atomic operation
# MAGIC MERGE INTO `training_<name>`.landing.orders_sample AS target
# MAGIC USING orders_updates AS source
# MAGIC     ON target.order_id = source.order_id
# MAGIC     -- NOTE: The ON clause is the join key — must uniquely identify a record
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC     -- WHY: Update only changed columns when the key matches
# MAGIC     UPDATE SET
# MAGIC         target.order_status = source.order_status
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     -- WHY: Insert new records that don't exist in the target
# MAGIC     INSERT (
# MAGIC         order_id, customer_id, order_status,
# MAGIC         order_purchase_timestamp, order_approved_at,
# MAGIC         order_delivered_carrier_date, order_delivered_customer_date,
# MAGIC         order_estimated_delivery_date
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.order_id, source.customer_id, source.order_status,
# MAGIC         source.order_purchase_timestamp, source.order_approved_at,
# MAGIC         source.order_delivered_carrier_date, source.order_delivered_customer_date,
# MAGIC         source.order_estimated_delivery_date
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E — Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: View table history
# MAGIC DESCRIBE HISTORY `training_<name>`.landing.orders_sample

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Query version 0 (original CTAS — before the MERGE)
# MAGIC SELECT * FROM `training_<name>`.landing.orders_sample VERSION AS OF 0 LIMIT 5

# COMMAND ----------

# WHY: Using spark.sql() for time travel queries inside Python — more flexible than %sql for variable interpolation
current_count = spark.table("training_<name>.landing.orders_sample").count()
v0_count = spark.sql("SELECT * FROM `training_<name>`.landing.orders_sample VERSION AS OF 0").count()

print(f"Current count:   {current_count}")
print(f"Version 0 count: {v0_count}")
print(f"Difference:      {current_count - v0_count} rows (added by MERGE)")
# NOTE: If MERGE inserted new rows, current_count > v0_count

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to version 0 (WARNING: this is a destructive operation — it creates a new version)
# MAGIC -- WHY: RESTORE doesn't delete history — it adds a new version that matches the old state
# MAGIC -- RESTORE TABLE `training_<name>`.landing.orders_sample TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus — repartition vs coalesce

# COMMAND ----------

from pyspark.sql import functions as F

order_items = spark.table("training_<name>.landing.order_items")

# WHY: repartition() triggers a full shuffle — use when you need balanced partitions
#      or when partitioning by a specific column (e.g., for downstream joins on seller_id)
repartitioned = order_items.repartition(4, "seller_id")
print(f"After repartition(4, 'seller_id'): {repartitioned.rdd.getNumPartitions()} partitions")

# WHY: coalesce() reduces partitions WITHOUT a full shuffle — much cheaper for reducing partition count
#      Use coalesce before writing to avoid many small output files
coalesced = order_items.coalesce(2)
print(f"After coalesce(2): {coalesced.rdd.getNumPartitions()} partitions")

# NOTE: coalesce() cannot increase partition count — use repartition() for that
# NOTE: For writing to Delta, Databricks auto-optimizes file sizes (OPTIMIZE command or Auto Optimize)
#       so manual repartitioning before write is less often needed than in vanilla Spark
