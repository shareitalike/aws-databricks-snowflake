# 🗺️ Project Architecture Diagram — Unified Batch & Streaming

This diagram shows how your data travels from a "click" on a website all the way to a "business chart" in Snowflake — using **both Batch and Streaming** pipelines.

```mermaid
graph TD
    subgraph "Phase 3: Data Source"
        P["🖥️ Event Producer<br/>Python Script<br/>5K events, 5% bad data"]
    end

    subgraph "Phase 2 & 4: Ingestion & Quality Gate"
        K["📡 AWS Kinesis<br/>Data Stream<br/>1 shard, 24h retention"]
        L["🛡️ AWS Lambda<br/>Validation + Routing"]
        B[("📦 S3 Bronze<br/>Raw NDJSON<br/>Immutable")]
        Q[("🚫 S3 Quarantine<br/>Failed Records")]
    end

    subgraph "Phase 5: Unified Lakehouse Processing"
        direction TB
        SCHEMA["📋 schemas.py<br/>Explicit Schema Contract<br/>Single Source of Truth"]

        subgraph "Option A: Streaming Path"
            STREAM["🌊 Auto Loader<br/>streaming_bronze_to_silver.py<br/>readStream + cloudFiles<br/>+ Checkpointing"]
        end

        subgraph "Option B: Batch Path"
            BATCH["📅 Batch ETL<br/>bronze_to_silver.py<br/>spark.read + ROW_NUMBER dedup"]
        end

        S[("✨ S3 Silver<br/>Delta Lake<br/>Deduplicated, Typed")]
        GOLD_ETL["⚙️ silver_to_gold.py<br/>Aggregations + Delta MERGE"]
        G[("🏆 S3 Gold<br/>Delta Lake<br/>Business Metrics")]
    end

    subgraph "Phase 6 & 7: Serving & Analytics"
        SN["❄️ Snowflake<br/>Star Schema<br/>fact_events + dim tables"]
        DASH["📊 Business Dashboard"]
        ATH["🔍 AWS Athena<br/>Ad-hoc Auditing"]
    end

    P --> K
    K -- "Trigger" --> L
    L -- "✅ Valid" --> B
    L -- "❌ Invalid" --> Q

    B -- "New files land" --> STREAM
    B -- "Historical date" --> BATCH
    SCHEMA -.-> STREAM
    SCHEMA -.-> BATCH

    STREAM --> S
    BATCH --> S
    S --> GOLD_ETL
    GOLD_ETL --> G

    G -- "COPY INTO" --> SN
    SN --> DASH
    B -. "Audit queries" .-> ATH
    S -. "Audit queries" .-> ATH

    style P fill:#e1bee7,stroke:#6a1b9a,stroke-width:2px,color:#000
    style K fill:#fff9c4,stroke:#f9a825,stroke-width:2px,color:#000
    style L fill:#b3e5fc,stroke:#0277bd,stroke-width:2px,color:#000
    style B fill:#ffe0b2,stroke:#e65100,stroke-width:2px,color:#000
    style Q fill:#ffcdd2,stroke:#b71c1c,stroke-width:2px,color:#000
    style SCHEMA fill:#c8e6c9,stroke:#2e7d32,stroke-width:3px,color:#000
    style STREAM fill:#bbdefb,stroke:#1565c0,stroke-width:2px,color:#000
    style BATCH fill:#d1c4e9,stroke:#4527a0,stroke-width:2px,color:#000
    style S fill:#dcedc8,stroke:#33691e,stroke-width:2px,color:#000
    style G fill:#fff59d,stroke:#f57f17,stroke-width:2px,color:#000
    style SN fill:#b2ebf2,stroke:#00838f,stroke-width:2px,color:#000
    style DASH fill:#f8bbd0,stroke:#880e4f,stroke-width:2px,color:#000
```

---

## 🔍 Component Breakdown

### 1. **Producer (Python)** — `producer/event_generator.py`
*   **Role:** Simulates a real e-commerce website. Creates random JSON events (login, product_view, add_to_cart, purchase, logout).
*   **Learning Point:** In production, this would be your website's backend or a mobile app SDK.

### 2. **Kinesis (The Highway)** — AWS Managed
*   **Role:** Buffers high-speed events. Decouples the website from storage.
*   **Learning Point:** Even if S3 or Lambda is slow, Kinesis catches events so nothing is lost. Retention = 24 hours by default.

### 3. **Lambda (The Security Guard)** — `lambda/transform_handler.py`
*   **Role:** Validates every event using rules in `data_quality.py`. Routes valid → Bronze, invalid → Quarantine.
*   **Learning Point:** "Garbage In, Garbage Out." We stop bad data at the door, not after it corrupts your dashboards.

### 4. **schemas.py (The Data Contract)** — `databricks/schemas.py` 🆕
*   **Role:** Defines the **Explicit Schema** used by BOTH Batch and Streaming jobs.
*   **Learning Point:** This is YOUR suggestion! Interviewers love seeing schema control instead of lazy `inferSchema=True`. It proves you care about data integrity.

### 5a. **Streaming Path (Auto Loader)** — `databricks/streaming_bronze_to_silver.py` 🆕
*   **Role:** Watches S3 Bronze and processes new files as soon as they arrive.
*   **Learning Point:** Uses `readStream` + `cloudFiles` + checkpointing for exactly-once processing. Best for near real-time dashboards.

### 5b. **Batch Path (Traditional ETL)** — `databricks/bronze_to_silver.py`
*   **Role:** Processes a specific date's data in one go. Uses `ROW_NUMBER()` for deterministic deduplication.
*   **Learning Point:** Best for historical reprocessing and backfills. Uses `spark.read` (not `readStream`).

### 6. **Gold Layer (Business Metrics)** — `databricks/silver_to_gold.py`
*   **Role:** Calculates DAU, conversion funnels, revenue metrics using Delta MERGE (upsert).
*   **Learning Point:** Gold is always Batch — business metrics need full historical context.

### 7. **Snowflake (The Analytics Store)** — `snowflake/schema.sql`
*   **Role:** Serves a Star Schema (fact_events + dim tables) for fast interactive SQL.
*   **Learning Point:** Analysts don't use Spark. They use SQL. Snowflake gives them sub-second query performance.

---

## 🎯 When to use Streaming vs Batch?

| Scenario | Use | Why |
|----------|-----|-----|
| New data arriving continuously | **Streaming** | Auto Loader picks up files instantly |
| Reprocessing last month's data | **Batch** | Targeted partition read is more efficient |
| Daily Gold metrics calculation | **Batch** | Aggregations need full day's data |
| Real-time fraud detection | **Streaming** | Latency matters (seconds, not hours) |
| Initial historical backfill | **Batch** | Process millions of old files at once |

*Last updated: April 2026 — Unified Architecture*
