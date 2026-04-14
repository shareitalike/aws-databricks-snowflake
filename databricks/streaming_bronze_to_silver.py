# Databricks Notebook: Bronze → Silver (Structured Streaming)
# MAGIC %md
# MAGIC # 🌊 Structured Streaming: Bronze → Silver
# MAGIC
# MAGIC This notebook implements **Structured Streaming** using Databricks **Auto Loader**. 
# MAGIC It watches the S3 Bronze folder and processes new files as soon as they land.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC 1. **Auto Loader (`cloudFiles`):** Efficiently discovers new files in S3.
# MAGIC 2. **Explicit Schema:** Uses our central `BRONZE_SCHEMA`.
# MAGIC 3. **Checkpointing:** Ensures "Exactly-Once" processing if the job restarts.
# MAGIC 4. **Micro-Batching:** Processes data in small chunks for low latency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Schema Setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- CONFIGURATION (Match your S3 Bucket) ---
S3_BUCKET = "pyspark-analytics-987684850401" # Auto-filled from your config
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/events"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/events"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/_checkpoints/bronze_to_silver"

# --- EXPLICIT SCHEMA (Industry Best Practice) ---
BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("device", StringType(), False),
    StructField("country", StringType(), False),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("processing_date", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Stream (Auto Loader)

# COMMAND ----------
streaming_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema") # Tracks schema evolution
    .schema(BRONZE_SCHEMA)         # <--- EXPLICIT SCHEMA ENFORCEMENT
    .load(BRONZE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformations (Silver Layer)

# COMMAND ----------

# 1. Cast types
# 2. Add partition columns
transformed_df = (streaming_df
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    .withColumn("year", F.date_format("event_time", "yyyy"))
    .withColumn("month", F.date_format("event_time", "MM"))
    .withColumn("day", F.date_format("event_time", "dd"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Stream to Delta

# COMMAND ----------
query = (transformed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("year", "month", "day")
    .trigger(availableNow=True) # Run like a batch, but with stream benefits
    .start(SILVER_PATH)
)

query.awaitTermination()

print(f"✅ Streaming Batch Complete. Silver table updated at {SILVER_PATH}")
