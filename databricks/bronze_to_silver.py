# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver Transformation (Delta Lake)
# MAGIC
# MAGIC Reads raw NDJSON from S3 bronze, enforces schema, deduplicates, and writes Delta Lake to silver.
# MAGIC
# MAGIC **Run on:** Databricks Community Edition (single-node cluster)

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
df_silver = (
    df_deduped
    .withColumn("year", F.date_format("event_time", "yyyy"))
    .withColumn("month", F.date_format("event_time", "MM"))
    .withColumn("day", F.date_format("event_time", "dd"))
)

# COMMAND ----------

# -- Write Delta Lake -----------------------------------------------------------
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
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    dt = DeltaTable.forPath(spark, SILVER_PATH)
    print("Delta table history:")
    dt.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

print("\n✅ Bronze → Silver transformation complete.")
print(f"   Bronze records: {bronze_count}")
print(f"   Silver records: {deduped_count}")
print(f"   Duplicates removed: {removed}")
