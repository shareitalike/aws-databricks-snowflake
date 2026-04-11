# Real-Time Event Analytics Platform
### AWS + Databricks + Snowflake (Lakehouse + Warehouse Architecture)

A production-style streaming data platform that ingests simulated e-commerce events, validates at the ingestion boundary, transforms through a medallion architecture using Delta Lake, and serves analytics through both Snowflake (warehouse) and Athena (audit).

## Architecture

```
Producer → Kinesis → Lambda (validate) → S3 Bronze (JSON)
                                       → S3 Quarantine (bad records)
                                       ↓
                         Databricks Bronze → Silver (Delta Lake)
                         Databricks Silver → Gold  (Delta MERGE)
                                       ↓
                              Snowflake (star schema)
                              Athena (audit queries)
```

See [`architecture/architecture_explanation.md`](architecture/architecture_explanation.md) for detailed design decisions.

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Streaming | Kinesis Data Streams | Event buffering + ordering |
| Ingestion | Lambda (Python 3.11) | Validation + routing |
| Storage | S3 | Bronze / Silver / Gold / Quarantine |
| Processing | Databricks (PySpark) | Delta Lake transforms + aggregations |
| Serving | Snowflake | Star schema interactive analytics |
| Audit | Athena | Ad-hoc queries on S3 |
| Format | Delta Lake + Parquet | ACID writes + columnar efficiency |

## Project Structure

```
aws-databricks-snowflake-pipeline/
├── producer/event_generator.py          # Kinesis event producer
├── lambda/
│   ├── transform_handler.py            # Lambda validation + S3 routing
│   └── data_quality.py                 # Shared validation module
├── databricks/
│   ├── bronze_to_silver.py             # Delta Lake transform (notebook)
│   └── silver_to_gold.py              # Incremental aggregations (notebook)
├── snowflake/
│   ├── schema.sql                      # Star schema DDL
│   └── load_gold.sql                   # S3 → Snowflake loading
├── sql/analytics_queries.sql           # Production analytics queries
├── tests/test_data_quality.py          # Unit tests (pytest)
├── config/config.yaml                  # Centralized configuration
├── architecture/
│   ├── architecture_explanation.md     # Architecture + tradeoffs
│   └── monitoring_queries.md           # Observability queries
├── requirements.txt
└── README.md
```

## Setup Guide

### Prerequisites
- AWS Account (free tier)
- Databricks Community Edition (free)
- Snowflake trial account (free for 30 days)
- Python 3.11+, AWS CLI configured

### Step 1: AWS Resources

```bash
# S3 bucket
aws s3 mb s3://your-bucket-name --region ap-south-1

# Kinesis stream
aws kinesis create-stream --stream-name user-activity-stream --shard-count 1

# Lambda (see README in previous project or LEARNING_GUIDE.md for full steps)
```

### Step 2: Run Producer

```bash
export AWS_REGION=ap-south-1
export KINESIS_STREAM_NAME=user-activity-stream
python producer/event_generator.py --total-events 5000 --eps 10
```

### Step 3: Databricks Notebooks

1. Sign up for [Databricks Community Edition](https://community.cloud.databricks.com/)
2. Create a cluster (single-node, auto-assigned)
3. Import `databricks/bronze_to_silver.py` as a notebook
4. Import `databricks/silver_to_gold.py` as a notebook
5. Update S3_BUCKET variable in both notebooks
6. Run `bronze_to_silver` first, then `silver_to_gold`

### Step 4: Snowflake

1. Sign up for [Snowflake Trial](https://signup.snowflake.com/)
2. Run `snowflake/schema.sql` to create tables
3. Run `snowflake/load_gold.sql` to load data from S3
4. Run queries from `sql/analytics_queries.sql`

### Step 5: Athena (Optional Audit)

1. Open Athena Query Editor
2. Create tables using Glue catalog DDL (see architecture doc)
3. Run `MSCK REPAIR TABLE` to register partitions
4. Query bronze/quarantine for data quality audits

## Data Quality Rules

| Rule | Check Type | Failure Action |
|------|-----------|----------------|
| Required fields present | Schema | Quarantine |
| Valid data types | Type | Quarantine |
| Event type in valid set | Enum | Quarantine |
| Price in range ($0.01–$50K) | Anomaly | Quarantine |
| No future timestamps | Anomaly | Quarantine |
| Purchase has product + price | Business | Quarantine |
| Login/logout has no price | Business | Quarantine |

Shared validation module: `lambda/data_quality.py` (used by Lambda AND Databricks).

## Incremental Processing

The silver→gold pipeline uses watermark-based incremental processing:

1. Read watermark file from `s3://bucket/metadata/last_processed_date.json`
2. Process only silver partitions newer than the watermark
3. MERGE INTO gold tables (upsert — idempotent)
4. Update watermark after all gold tables succeed

**Failure recovery:** If the job crashes after processing but before updating the watermark, the next run reprocesses the same data. MERGE is idempotent — same result either way.

## Design Tradeoff Table

| Decision | Alternative | Rationale |
|----------|-------------|-----------|
| Lambda over Firehose | Firehose direct | Validation before storage |
| Delta over Parquet | Plain Parquet | ACID transactions, MERGE, time travel |
| Databricks + Snowflake | Databricks only | Sub-second serving vs ETL processing |
| Incremental over full recompute | Full recompute | Cost-efficient at scale |
| Star schema in Snowflake | Normalized 3NF | Faster analytics queries, fewer JOINs |
| Watermark in S3 | Database state | Simpler, no extra service |
| MERGE over overwrite | Full table overwrite | Idempotent, handles retries |

## Cost Estimate (Monthly)

| Service | Cost | Notes |
|---------|------|-------|
| Kinesis (1 shard) | ~$11 | Delete when not studying |
| Lambda | < $1 | Free tier |
| S3 | < $1 | Free tier |
| Databricks Community | $0 | Free forever |
| Snowflake trial | ~$2 | Trial credits |
| Athena | < $1 | $5/TB |
| **Total** | **~$15/month** | |

## Future Improvements

- **Gold layer expansion** — Pre-computed cohort analysis, retention curves
- **Real-time dedup** — DynamoDB at Lambda layer for exact-once semantics
- **Schema Registry** — Glue Schema Registry for version management
- **Orchestration** — Airflow/Dagster for dependency management and scheduling
- **Alerting** — CloudWatch Alarms on quarantine rate spikes
- **Data contracts** — Producer-consumer SLAs with versioned schemas
- **CI/CD** — Automated testing of Databricks notebooks with `nutter`
