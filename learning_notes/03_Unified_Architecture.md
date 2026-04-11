# 🏗️ Unified Batch & Streaming Architecture (Medallion)

In modern Data Engineering, we don't just pick "Batch" or "Streaming." We use **Unified Architectures** where both work together on the same data.

## 1. Why Unified? (The Interview Answer)
Interviewer: *"Why did you implement both a Batch and a Streaming version of your pipeline?"*

**Your Answer:** 
> "I implemented a Unified Architecture (Lambda/Kappa inspired) to handle different business needs. 
> 
> *   **Streaming** is used for the **Bronze → Silver** layer using Databricks **Auto Loader**. This ensures that we have near real-time visibility into user behavior (e.g., detecting a sudden drop in purchases).
> *   **Batch** is used for the **Silver → Gold** layer because business metrics (like Monthly Active Users or Conversion Funnels) require complex aggregations across long time windows, which is more cost-effective to run as a scheduled daily job."

---

## 2. Shared Explicit Schema (Your Suggestion!)
You specifically asked for an **Explicit Schema**. This is a major engineering upgrade.

| Feature | Schema Inference (`inferSchema=True`) | Explicit Schema (`StructType`) |
|---------|--------------------------------------|--------------------------------|
| **Speed** | Slow (Spark must scan files first) | **Fast** (Spark starts reading immediately) |
| **Safety** | Risky (Data types can change randomly) | **Safe** (Data types are guaranteed) |
| **Control** | Spark decides for you | **You decide** (e.g., Decimal for money) |

### What happens when data is "dirty"?
With an explicit schema, if a numeric field arrives as a string, Spark will:
1.  Try to cast it (if possible).
2.  If it can't, it sets it to `null`.
This prevents a single "bad" file from crashing your entire 24/7 streaming pipeline.

---

## 3. How to Execute in Databricks

### 🚀 For Streaming (Real-Time):
1.  Upload `databricks/streaming_bronze_to_silver.py` to your Databricks workspace.
2.  Run it. It will use **Auto Loader** to watch your S3 bucket.
3.  Note that it uses **Checkpoints**. This means if the stream stops, it remembers exactly which files it already processed.

### 📅 For Batch (Historical):
1.  Upload `databricks/bronze_to_silver.py`.
2.  Run it for a specific `PROCESS_DATE`.
3.  It uses the **Exact Same Schema**, ensuring both versions of the truth match.

---

## 🧠 Key Concept: Auto Loader
This project uses `format("cloudFiles")`. This is Databricks' best-in-class way to handle streaming from a Data Lake. It is thousands of times more efficient than standard Spark streaming because it doesn't have to "list" all files in S3 repeatedly.
