# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver Transformation (Delta Lake)
# MAGIC
# MAGIC Reads raw NDJSON from S3 bronze, enforces schema, deduplicates, and writes Delta Lake to silver.
# MAGIC
# MAGIC **Run on:** Databricks Community Edition (single-node cluster)
# MAGIC
# MAGIC **INTERVIEW NOTE:** This notebook demonstrates the core medallion pattern —
# MAGIC bronze (raw) to silver (curated). Every design choice here maps directly
# MAGIC to a common interview question about data lake architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your S3 bucket name and target date before running.

# COMMAND ----------

# -- Imports ------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# -- Configuration -----------------------------------------------------------
S3_BUCKET = "pyspark-analytics-987684850401" 
PROCESS_DATE = "2026-04-10"

# PIVOT: Using Workspace File path for Community Edition compatibility
BRONZE_PATH = "/Workspace/Users/shareitalike@gmail.com/FileStore/tables/all_events_merged.json"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/events"


# -- UNIFIED EXPLICIT SCHEMA -------------------------------------------------
# INTERVIEW NOTE: We use the EXACT same Schema here as in our Streaming job.
# Consistency between Batch and Streaming (Unified) prevents "Data Skew" 
# bugs where the two jobs interpret fields differently.

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("device", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("ingestion_timestamp", StringType(), nullable=True),
    StructField("processing_date", StringType(), nullable=True),
])

# -- Read merged file ---------------------------------------------------------
print(f"Reading bronze from: {BRONZE_PATH}")

df_bronze = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .json(BRONZE_PATH)
)

bronze_count = df_bronze.count()
print(f"Bronze records read: {bronze_count}")


# Quick data quality check — see what we're working with
df_bronze.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Type Casting
# MAGIC
# MAGIC Convert string timestamps to proper TimestampType for downstream queries.

# COMMAND ----------

# -- Type Casting --------------------------------------------------------------
# INTERVIEW NOTE: Why cast types in silver, not at ingestion?
# Bronze stores data as-is from producers (raw JSON). If we cast at ingestion
# and the casting logic has a bug (e.g., wrong timezone handling), we've
# permanently corrupted the data. With immutable bronze, we can fix the
# casting logic and reprocess.
#
# Example: A producer sends timestamps in IST (UTC+5:30) but we assumed UTC.
# With mutable bronze: data is permanently wrong, re-ingestion needed.
# With immutable bronze: fix the cast, rerun this notebook, silver is corrected.

df_typed = (
    df_bronze
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    .withColumn("processing_timestamp", F.current_timestamp())
)

print("Schema after type casting:")
df_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deduplication
# MAGIC
# MAGIC Remove duplicate event_ids, keeping earliest ingestion.

# COMMAND ----------

# -- Deduplication using Window + ROW_NUMBER ----------------------------------
# INTERVIEW NOTE: Why ROW_NUMBER instead of dropDuplicates?
#
# dropDuplicates("event_id") is simpler but NON-DETERMINISTIC — Spark picks
# an arbitrary row when duplicates exist. If you run the job twice, you might
# keep different rows each time. This breaks reproducibility.
#
# ROW_NUMBER with ORDER BY ingestion_timestamp is DETERMINISTIC — always keeps
# the first-arriving record. Re-running produces identical results.
#
# Why this matters in interviews:
#   Interviewer: "Your pipeline ran twice due to a retry. How do you ensure
#                 the silver table is identical both times?"
#   Answer:      "Deterministic deduplication. ROW_NUMBER ordered by ingestion
#                 timestamp always keeps the same record, regardless of how
#                 many times the Spark job runs."
#
# INTERVIEW NOTE: Why deduplicate in silver, not in Lambda?
# Real-time dedup in Lambda requires a state store (DynamoDB at ~$1.25 per
# million writes). At 10K events/day, that's pennies — but the engineering
# complexity is significant (TTL management, consistency, cold start latency).
# Batch dedup in Spark is:
#   - Cheaper: no additional AWS service
#   - Simpler: one SQL window function
#   - Definitive: processes ALL records at once, no TTL-based gaps

# Track duplicates BEFORE removing (for observability)
duplicate_counts = (
    df_typed
    .groupBy("event_id")
    .agg(F.count("*").alias("occurrence_count"))
    .filter(F.col("occurrence_count") > 1)
)

dupe_count = duplicate_counts.count()
print(f"Duplicate event_ids found: {dupe_count}")

if dupe_count > 0:
    print("Sample duplicates:")
    duplicate_counts.orderBy("occurrence_count", ascending=False).show(5)

# Deduplicate — keep earliest ingestion
window_spec = (
    Window
    .partitionBy("event_id")
    .orderBy(F.col("ingestion_timestamp").asc())
)

df_deduped = (
    df_typed
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

deduped_count = df_deduped.count()
removed = bronze_count - deduped_count
print(f"After dedup: {deduped_count} records ({removed} duplicates removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Partition Columns and Write Delta Lake

# COMMAND ----------

# -- Partition Columns ---------------------------------------------------------
# INTERVIEW NOTE: Why derive partition columns from event_time (not ingestion time)?
# Using event_time (business time) means queries like "show me March 15th revenue"
# scan one partition. Using ingestion time, a late-arriving March 15th event
# that arrived on March 16th would be in the wrong partition.
#
# TRADEOFF: Late-arriving events update old partitions, requiring dynamic
# partition overwrite. But queries are always aligned with business dates.

df_silver = (
    df_deduped
    .withColumn("year", F.date_format("event_time", "yyyy"))
    .withColumn("month", F.date_format("event_time", "MM"))
    .withColumn("day", F.date_format("event_time", "dd"))
)

# COMMAND ----------

# -- Write Delta Lake -----------------------------------------------------------
# INTERVIEW NOTE: Why Delta Lake instead of plain Parquet?
#
# Plain Parquet has three critical limitations:
#   1. NO ACID transactions: If a write fails halfway, you get partial/corrupt data.
#      Delta's transaction log ensures all-or-nothing writes.
#   2. NO schema enforcement: You can accidentally write a DataFrame with a
#      different schema to the same path. Delta rejects schema-incompatible writes.
#   3. NO time travel: Once you overwrite Parquet, the old data is gone.
#      Delta keeps history — you can query yesterday's version of silver.
#
# Example production scenario:
#   A Glue job writes 50 of 100 Parquet files, then OOMs and crashes.
#   Plain Parquet: 50 files written, 50 missing. Partial data in silver.
#   Delta Lake: Transaction rolled back. Silver stays at previous state.
#   You fix the OOM, rerun, and get all 100 files atomically.
#
# Cost: Delta adds ~5% overhead for the transaction log (_delta_log/).
# Worth it for the ACID guarantees.

# Set dynamic partition overwrite
# INTERVIEW NOTE: Without this, writing year=2024/month=03/day=15 would
# DELETE all other partitions and replace with only today's data.
# Dynamic mode overwrites ONLY the partitions present in the DataFrame.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .save(SILVER_PATH)
)

print(f"Silver layer written to: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validation — Read back and verify

# COMMAND ----------

# -- Read back for validation ---------------------------------------------------
df_verify = spark.read.format("delta").load(SILVER_PATH)

print(f"Silver verification — total records: {df_verify.count()}")
print("\nSchema:")
df_verify.printSchema()
print("\nEvent distribution:")
df_verify.groupBy("event_type").count().orderBy("count", ascending=False).show()
print("\nPartitions:")
df_verify.select("year", "month", "day").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Delta Lake Features Demo

# COMMAND ----------

# -- Delta Lake History (Time Travel) ------------------------------------------
# INTERVIEW NOTE: Delta's VERSION history lets you audit every change.
# In regulated industries (finance, healthcare), this is a compliance requirement.
# SELECT * FROM silver_events VERSION AS OF 0 → query the original data.

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    dt = DeltaTable.forPath(spark, SILVER_PATH)
    print("Delta table history:")
    dt.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

print("\n✅ Bronze → Silver transformation complete.")
print(f"   Bronze records: {bronze_count}")
print(f"   Silver records: {deduped_count}")
print(f"   Duplicates removed: {removed}")
