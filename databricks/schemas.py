# Databricks Python Module: Central Data Schemas

from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)

"""
"""

# ============================================================================
# Bronze Table Schema (Raw Ingestion)
BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_time", StringType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("device", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    
    # Enrichment fields added by AWS Lambda
    StructField("ingestion_timestamp", StringType(), nullable=True),
    StructField("processing_date", StringType(), nullable=True),
])

# ============================================================================
# Silver Table Schema (Cleaned & Curated)
# ============================================================================
# In Silver, fields like event_time and ingestion_timestamp are cast to 
# proper TimestampType for faster filtering and joins.
# ============================================================================

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("device", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=True),
    
    # Partitioning columns
    StructField("year", StringType(), nullable=False),
    StructField("month", StringType(), nullable=False),
    StructField("day", StringType(), nullable=False),
])
