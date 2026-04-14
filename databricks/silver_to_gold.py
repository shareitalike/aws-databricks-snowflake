# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold Transformation (Incremental Processing)
# MAGIC
# MAGIC Compute business metrics from silver layer and write to gold using Delta MERGE.
# MAGIC Uses watermark-based incremental processing — only new data is processed each run.
# MAGIC
# MAGIC **Run on:** Databricks Community Edition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

S3_BUCKET = "YOUR-BUCKET-NAME"  # <-- Replace
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/events"
GOLD_BASE = f"s3a://{S3_BUCKET}/gold"
WATERMARK_PATH = f"s3a://{S3_BUCKET}/metadata/last_processed_date.json"

# Gold table paths
GOLD_DAILY_ACTIVE_USERS = f"{GOLD_BASE}/daily_active_users"
GOLD_CONVERSION_FUNNEL = f"{GOLD_BASE}/conversion_funnel"
GOLD_DAILY_REVENUE = f"{GOLD_BASE}/daily_revenue"
GOLD_TOP_PRODUCTS = f"{GOLD_BASE}/top_products"
GOLD_EVENTS_BY_DEVICE = f"{GOLD_BASE}/events_by_device"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Processing — Watermark Strategy

# COMMAND ----------

def read_watermark(spark, path: str) -> str:
    """Read last processed date from watermark file."""
    try:
        watermark_df = spark.read.text(path)
        content = watermark_df.first()[0]
        data = json.loads(content)
        last_date = data["last_processed_date"]
        print(f"Watermark found: last_processed_date = {last_date}")
        return last_date
    except Exception:
        # First run — no watermark exists. Process from beginning of time.
        default_date = "2020-01-01"
        print(f"No watermark found. Starting from: {default_date}")
        return default_date


def write_watermark(spark, path: str, process_date: str) -> None:
    """Update watermark after successful processing."""
    content = json.dumps({
        "last_processed_date": process_date,
        "updated_at": datetime.utcnow().isoformat(),
    })
    # Write as single-line text file
    spark.sparkContext.parallelize([content]).coalesce(1).saveAsTextFile(
        path + "_tmp"
    )
    print(f"Watermark updated to: {process_date}")

# COMMAND ----------

# -- Read watermark and determine processing range ----------------------------
last_processed = read_watermark(spark, WATERMARK_PATH)
process_up_to = datetime.utcnow().strftime("%Y-%m-%d")

print(f"Processing silver data from {last_processed} to {process_up_to}")

# COMMAND ----------

# -- Read silver data (only new partitions) ------------------------------------
df_silver = (
    spark.read.format("delta").load(SILVER_PATH)
    .filter(
        F.to_date(F.col("event_time")) > F.lit(last_processed)
    )
)

new_records = df_silver.count()
print(f"New silver records to process: {new_records}")

if new_records == 0:
    print("No new data. Skipping gold processing.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: Daily Active Users

# COMMAND ----------

# -- Daily Active Users --------------------------------------------------------
df_dau = (
    df_silver
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date")
    .agg(
        F.countDistinct("user_id").alias("daily_active_users"),
        F.count("*").alias("total_events"),
        F.countDistinct(
            F.when(F.col("event_type") == "purchase", F.col("user_id"))
        ).alias("purchasing_users"),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

# -- Delta MERGE (Upsert) ------------------------------------------------------
if DeltaTable.isDeltaTable(spark, GOLD_DAILY_ACTIVE_USERS):
    dt_dau = DeltaTable.forPath(spark, GOLD_DAILY_ACTIVE_USERS)
    (
        dt_dau.alias("target")
        .merge(
            df_dau.alias("source"),
            "target.event_date = source.event_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("DAU: MERGE completed (upsert)")
else:
    df_dau.write.format("delta").mode("overwrite").save(GOLD_DAILY_ACTIVE_USERS)
    print("DAU: Initial write completed")

spark.read.format("delta").load(GOLD_DAILY_ACTIVE_USERS).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Conversion Funnel

# COMMAND ----------

# -- Conversion Funnel ---------------------------------------------------------
df_funnel = (
    df_silver
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date")
    .agg(
        F.countDistinct(
            F.when(F.col("event_type") == "product_view", F.col("user_id"))
        ).alias("viewers"),
        F.countDistinct(
            F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))
        ).alias("cart_adders"),
        F.countDistinct(
            F.when(F.col("event_type") == "purchase", F.col("user_id"))
        ).alias("purchasers"),
    )
    .withColumn(
        "view_to_cart_pct",
        F.round(F.col("cart_adders") / F.col("viewers") * 100, 2)
    )
    .withColumn(
        "cart_to_purchase_pct",
        F.round(F.col("purchasers") / F.col("cart_adders") * 100, 2)
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

if DeltaTable.isDeltaTable(spark, GOLD_CONVERSION_FUNNEL):
    dt_funnel = DeltaTable.forPath(spark, GOLD_CONVERSION_FUNNEL)
    (
        dt_funnel.alias("t")
        .merge(df_funnel.alias("s"), "t.event_date = s.event_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Conversion funnel: MERGE completed")
else:
    df_funnel.write.format("delta").mode("overwrite").save(GOLD_CONVERSION_FUNNEL)
    print("Conversion funnel: Initial write completed")

spark.read.format("delta").load(GOLD_CONVERSION_FUNNEL).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: Daily Revenue

# COMMAND ----------

# -- Daily Revenue --------------------------------------------------------------
df_revenue = (
    df_silver
    .filter(
        (F.col("event_type") == "purchase") & F.col("price").isNotNull()
    )
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date")
    .agg(
        F.count("*").alias("purchase_count"),
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.round(F.avg("price"), 2).alias("avg_order_value"),
        F.round(F.min("price"), 2).alias("min_order"),
        F.round(F.max("price"), 2).alias("max_order"),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

if DeltaTable.isDeltaTable(spark, GOLD_DAILY_REVENUE):
    dt_rev = DeltaTable.forPath(spark, GOLD_DAILY_REVENUE)
    (
        dt_rev.alias("t")
        .merge(df_revenue.alias("s"), "t.event_date = s.event_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Daily revenue: MERGE completed")
else:
    df_revenue.write.format("delta").mode("overwrite").save(GOLD_DAILY_REVENUE)
    print("Daily revenue: Initial write completed")

spark.read.format("delta").load(GOLD_DAILY_REVENUE).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4: Top Products

# COMMAND ----------

# -- Top Products ----------------------------------------------------------------
df_products = (
    df_silver
    .filter(F.col("product_id").isNotNull())
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "product_id")
    .agg(
        F.count("*").alias("total_interactions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.count(F.when(F.col("event_type") == "product_view", True)).alias("views"),
        F.count(F.when(F.col("event_type") == "add_to_cart", True)).alias("cart_adds"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases"),
        F.round(F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price"))
        ), 2).alias("revenue"),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

if DeltaTable.isDeltaTable(spark, GOLD_TOP_PRODUCTS):
    dt_prod = DeltaTable.forPath(spark, GOLD_TOP_PRODUCTS)
    (
        dt_prod.alias("t")
        .merge(
            df_products.alias("s"),
            "t.event_date = s.event_date AND t.product_id = s.product_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Top products: MERGE completed")
else:
    df_products.write.format("delta").mode("overwrite").save(GOLD_TOP_PRODUCTS)
    print("Top products: Initial write completed")

spark.read.format("delta").load(GOLD_TOP_PRODUCTS).orderBy("total_interactions", ascending=False).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 5: Events by Device

# COMMAND ----------

# -- Events by Device -----------------------------------------------------------
df_device = (
    df_silver
    .withColumn("event_date", F.to_date("event_time"))
    .groupBy("event_date", "device")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases"),
        F.round(F.sum(
            F.when(F.col("event_type") == "purchase", F.col("price"))
        ), 2).alias("revenue"),
    )
    .withColumn("processing_timestamp", F.current_timestamp())
)

if DeltaTable.isDeltaTable(spark, GOLD_EVENTS_BY_DEVICE):
    dt_dev = DeltaTable.forPath(spark, GOLD_EVENTS_BY_DEVICE)
    (
        dt_dev.alias("t")
        .merge(
            df_device.alias("s"),
            "t.event_date = s.event_date AND t.device = s.device"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Events by device: MERGE completed")
else:
    df_device.write.format("delta").mode("overwrite").save(GOLD_EVENTS_BY_DEVICE)
    print("Events by device: Initial write completed")

spark.read.format("delta").load(GOLD_EVENTS_BY_DEVICE).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Watermark (Post-Processing)

# COMMAND ----------

# -- Update watermark only after ALL gold tables succeed -------------------------
try:
    write_watermark(spark, WATERMARK_PATH, process_up_to)
    print(f"\n✅ Silver → Gold complete. Watermark updated to {process_up_to}")
except Exception as e:
    print(f"\n⚠️ Gold tables written but watermark update failed: {e}")
    print("Next run will safely reprocess (MERGE is idempotent)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# -- Final Summary ---------------------------------------------------------------
print("=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)

gold_tables = {
    "daily_active_users": GOLD_DAILY_ACTIVE_USERS,
    "conversion_funnel": GOLD_CONVERSION_FUNNEL,
    "daily_revenue": GOLD_DAILY_REVENUE,
    "top_products": GOLD_TOP_PRODUCTS,
    "events_by_device": GOLD_EVENTS_BY_DEVICE,
}

for name, path in gold_tables.items():
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  {name:25s} → {count:>6d} rows")
    except Exception:
        print(f"  {name:25s} → NOT FOUND")

print("=" * 60)
