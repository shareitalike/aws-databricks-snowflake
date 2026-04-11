# Databricks Python Module: Central Data Schemas

from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)

"""
INTERVIEW NOTE: Centralizing schemas in a single module is a "Production-First" 
practice. It ensures that your Producer, Lambda, Batch ETL, and Streaming ETL 
all agree on the "Data Contract." If the business adds a new field (e.g., product_category), 
you update it here in ONE place, and your entire pipeline stays consistent.
"""

# ============================================================================
# Bronze Table Schema (Raw Ingestion)
# ============================================================================
# DECISION: We use StringType for timestamps in Bronze to avoid data loss 
# during malformed ingestion. We cast to proper TimestampType in the Silver layer.
# 
# TRADEOFF: DoubleType vs DecimalType for price.
# - Double (8 bytes) is faster for large-scale scientific calculations.
# - Decimal (up to 38 precision) is standard for financial data to avoid 
#   floating-point rounding errors (e.g., 0.1 + 0.2 != 0.3).
# In this learning project, we use Double for simplicity, but in a real Bank 
# project, we would use Decimal(18, 2).
# ============================================================================

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
