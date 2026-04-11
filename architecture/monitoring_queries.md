# Monitoring & Observability Queries

## Why Structured Logging?

All components (Producer, Lambda, Databricks) emit structured JSON logs:

```json
{
    "timestamp": "2024-03-15T14:30:22",
    "component": "lambda_handler",
    "status": "quarantined",
    "record_id": "a1b2c3d4-e5f6-7890",
    "rejection_reason": "anomaly_price_too_low:-50.0",
    "processing_time_ms": 2.34
}
```

**Why not print()?** `print("Error processing record")` is human-readable but machine-unparseable. You can't filter, count, or aggregate print statements. At 1M events/day, you need `CloudWatch Logs Insights` queries to find issues — and those require structured fields.

---

## CloudWatch Logs Insights Queries

### 1. Lambda Error Rate (Last 1 Hour)

```
fields @timestamp, @message
| filter component = "lambda_handler"
| stats count(*) as total,
        sum(case when status = "quarantined" then 1 else 0 end) as quarantined,
        sum(case when status = "valid" then 1 else 0 end) as valid
    by bin(5m)
| sort @timestamp desc
```

**What to watch:** Quarantine rate should hover around 5% (our intentional bad record rate). If it spikes to 20%+, a producer is sending bad data.

### 2. Lambda Processing Latency

```
fields @timestamp, processing_time_ms
| filter component = "lambda_handler" and status = "valid"
| stats avg(processing_time_ms) as avg_ms,
        max(processing_time_ms) as max_ms,
        p95(processing_time_ms) as p95_ms,
        count(*) as records
    by bin(5m)
```

**What to watch:** p95 latency > 100ms suggests the Lambda is struggling (cold starts or memory pressure). Consider increasing memory from 256MB to 512MB.

### 3. Top Quarantine Reasons

```
fields @timestamp, rejection_reason
| filter component = "lambda_handler" and status = "quarantined"
| stats count(*) as failures by rejection_reason
| sort failures desc
| limit 10
```

**What to watch:** The #1 reason should change rarely. If a new reason appears (e.g., `invalid_event_type:wishlist`), a producer is sending a new event type not yet in our enum — either a bug or a legitimate new feature that needs schema update.

### 4. Producer Throughput

```
fields @timestamp, @message
| filter component = "event_producer" and @message like "batch_sent"
| parse @message "records=*" as batch_size
| stats sum(batch_size) as total_sent by bin(1m)
| sort @timestamp desc
```

### 5. Kinesis Throttle Detection

```
fields @timestamp, @message
| filter component = "event_producer" and @message like "retry_backoff"
| stats count(*) as throttle_events by bin(5m)
| sort @timestamp desc
```

**What to watch:** Any throttle events mean the shard is at capacity. If sustained, add shards.

---

## CloudWatch Alarms (Recommended)

| Alarm | Metric | Threshold | Action |
|-------|--------|-----------|--------|
| High quarantine rate | Quarantine % > 15% for 15 min | SNS notification | Investigate producer |
| Lambda errors | Error count > 10 in 5 min | SNS notification | Check Lambda logs |
| Lambda duration | Avg duration > 30s | SNS notification | Increase memory/timeout |
| Kinesis age | Iterator age > 1 hour | SNS notification | Lambda can't keep up |
| Zero events | IncomingRecords = 0 for 30 min | SNS notification | Producer is down |

---

## Databricks Observability

The Databricks notebooks print structured summaries at the end of each run:

```
==========================================================
GOLD LAYER SUMMARY
==========================================================
  daily_active_users          →    30 rows
  conversion_funnel           →    30 rows
  daily_revenue               →    30 rows
  top_products                →   450 rows
  events_by_device            →    90 rows
==========================================================
```

**What to check after each run:**
1. Row counts should increase (incremental processing adds new dates)
2. Zero rows indicates no new data or a read failure
3. Compare bronze count vs silver count — difference = duplicates removed
4. Delta table history shows write operations and metrics

---

## Snowflake Query History (Observability)

```sql
-- Recent query performance (find slow queries)
SELECT
    query_text,
    execution_time / 1000 AS exec_seconds,
    bytes_scanned / 1024 / 1024 AS mb_scanned,
    rows_produced,
    warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time > DATEADD(hour, -24, CURRENT_TIMESTAMP())
  AND database_name = 'EVENT_ANALYTICS'
ORDER BY execution_time DESC
LIMIT 20;

-- Credit consumption by warehouse (cost monitoring)
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 2 AS estimated_cost_usd  -- $2/credit for standard
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;
```
