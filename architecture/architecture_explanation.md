# Architecture Explanation

## Overview

A lakehouse + warehouse analytics platform that ingests simulated e-commerce events through a streaming pipeline and serves analytics through both Delta Lake (Databricks) and Snowflake.

## Architecture Diagram

```
┌──────────────┐    ┌───────────────┐    ┌────────────────────┐
│ Event        │───▶│ Kinesis Data  │───▶│ Lambda             │
│ Producer     │    │ Streams       │    │ (Validation)       │
│              │    │ (1 shard)     │    │                    │
│ 5K events    │    │ 24h retain    │    │ validate → route   │
│ 5% bad data  │    │               │    │                    │
└──────────────┘    └───────────────┘    └────┬──────────┬────┘
                                              │          │
                                    ┌─────────┘          └────────┐
                                    ▼                             ▼
                             ┌─────────────┐             ┌──────────────┐
                             │ S3 Bronze   │             │ S3 Quarantine│
                             │ (NDJSON)    │             │ (failed)     │
                             │ Immutable   │             └──────────────┘
                             └──────┬──────┘
                                    │
                                    ▼
                      ┌─────────────────────────┐
                      │ Databricks (PySpark)     │
                      │                          │
                      │ Bronze → Silver          │
                      │ • Schema enforcement     │
                      │ • Type casting           │
                      │ • Deduplication           │
                      │ • Delta Lake format      │
                      │                          │
                      │ Silver → Gold            │
                      │ • Business aggregations  │
                      │ • Incremental processing │
                      │ • Delta MERGE (upsert)   │
                      └────────┬────────────────┘
                               │
                    ┌──────────┼───────────┐
                    ▼          ▼           ▼
          ┌───────────┐  ┌────────┐  ┌──────────┐
          │ S3 Silver │  │S3 Gold │  │ Athena   │
          │ (Delta)   │  │(Delta) │  │ (audit)  │
          └───────────┘  └───┬────┘  └──────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Snowflake        │
                    │                  │
                    │ Star schema      │
                    │ • fact_events    │
                    │ • dim_users      │
                    │ • dim_products   │
                    │ • dim_date       │
                    │                  │
                    │ Interactive SQL  │
                    └──────────────────┘
```

## Data Flow Walkthrough

| Step | Component | Input | Output | Format |
|------|-----------|-------|--------|--------|
| 1 | Producer | Config | Events | JSON → Kinesis |
| 2 | Kinesis | Events | Buffered stream | Internal |
| 3 | Lambda | Kinesis batch | Valid + invalid records | NDJSON → S3 |
| 4 | Databricks B→S | S3 bronze JSON | Deduplicated, typed data | Delta Lake |
| 5 | Databricks S→G | S3 silver Delta | Business aggregations | Delta Lake |
| 6 | Snowflake | S3 gold Parquet | Star schema tables | Snowflake tables |
| 7 | Athena | S3 bronze/silver | Ad-hoc audit queries | N/A |

## Key Design Decisions

### Lakehouse vs Warehouse — Why Both?

**Delta Lake (Databricks)** handles:
- Heavy transformations (PySpark)
- ACID transactions during writes
- Schema enforcement and evolution
- Incremental processing with watermarks
- Cost-efficient storage (S3 prices, not warehouse prices)

**Snowflake** handles:
- Sub-second interactive queries (dashboards)
- Concurrent analyst access (BI tools connect natively)
- Star schema for self-service analytics
- Query result caching (repeat queries are free)

**Why not just one?**
- Databricks alone: Great for processing, but interactive query latency is 5-15s (cluster spin-up). Not suitable for dashboards.
- Snowflake alone: Great for queries, but complex PySpark transformations (window functions, UDFs) are awkward in SQL. Also expensive for large-scale processing.
- Together: "Best of both worlds" — Databricks for ETL, Snowflake for serving.

### Why Delta Lake over Plain Parquet?

| Feature | Plain Parquet | Delta Lake |
|---------|--------------|------------|
| ACID transactions | ❌ Partial writes are possible | ✅ All-or-nothing |
| Schema enforcement | ❌ Wrong schema writes silently | ✅ Rejects incompatible schemas |
| Time travel | ❌ Overwritten data is lost | ✅ Query historical versions |
| MERGE (upsert) | ❌ Read-modify-write needed | ✅ Native MERGE INTO |
| Dedup guarantee | ❌ Concurrent writes may create dupes | ✅ Transaction isolation |

### Why Incremental Processing?

| Approach | Data Volume | Compute Cost | Latency |
|----------|------------|-------------|---------|
| Full recompute | ALL data | Grows daily | Hours at scale |
| Incremental | NEW data only | Constant | Minutes |

At 10K events, both take seconds. At 10M events/day after 1 year (3.6B rows), full recompute is infeasible.

## Failure Modes and Mitigations

| Failure | Impact | Mitigation |
|---------|--------|-----------|
| Producer crashes | Events stop flowing | Structured logs + CloudWatch alarm on Kinesis IncomingRecords = 0 |
| Lambda fails mid-batch | Partial S3 write | UUID filenames (no overwrite) + silver dedup |
| Kinesis throttle | Lost events | Exponential backoff retry in producer |
| Databricks job OOM | Silver not updated | Delta ACID rollback — silver stays consistent |
| Watermark not updated | Reprocessing | MERGE is idempotent — reprocessing is safe |
| Snowflake load fails | Gold tables stale | COPY INTO is idempotent — retry is safe |

## Cost Estimate (Monthly, Free-Tier)

| Service | Estimated Cost | Notes |
|---------|---------------|-------|
| Kinesis (1 shard) | ~$11 | Delete when not studying |
| Lambda | < $1 | Free tier: 1M invocations |
| S3 | < $1 | Free tier: 5 GB |
| Databricks | $0 | Community Edition is free |
| Snowflake | ~$2 | Trial credits, X-Small, auto-suspend |
| Athena | < $1 | $5/TB, tiny data |
| **Total** | **~$15/month** | |

## Scaling Beyond Free Tier

| Component | Free-Tier Config | Production Config |
|-----------|-----------------|-------------------|
| Kinesis | 1 shard (1 MB/s) | 10+ shards (10 MB/s) |
| Lambda | 256 MB | 1024 MB + provisioned concurrency |
| Databricks | Community Ed. | Multi-node cluster + autoscaling |
| Snowflake | X-Small | MEDIUM + multi-cluster warehouse |
| S3 | Standard | Intelligent-Tiering + lifecycle policies |
