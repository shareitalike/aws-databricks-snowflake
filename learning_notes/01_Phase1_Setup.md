# Phase 1: Local & AWS Setup - Learning Notes

## 🌟 The Big Picture: What are we building?
Imagine an e-commerce website where users log in, view products, and buy items. We will:
1. **Fake user events** on your laptop.
2. Send them to **AWS Kinesis** (a fast highway for data).
3. Validate them using **AWS Lambda** (our security guard checking for bad data).
4. Store raw events in **AWS S3** (the Bronze layer).
5. Use **Databricks** to clean the data and calculate metrics (Silver and Gold layers).
6. Load the final cleaned data into **Snowflake** to run fast SQL analytics.

## 🛠️ Your Execution Steps

### 1. Install Dependencies
Open your PowerShell terminal and run:
```powershell
cd f:\pyspark_study\project2_aws\aws-databricks-snowflake-pipeline
pip install boto3 pyarrow pyyaml pytest
```

### 2. Update Configuration [DONE ✅]
I have already updated `config/config.yaml` for you with:
* **Account ID:** `987684850401`
* **Suggested Bucket:** `pyspark-analytics-987684850401`

### 3. Set up AWS Infrastructure (S3 & Kinesis) [FOR TOMORROW]
In your PowerShell terminal, create your filing cabinet (S3) and streaming pipeline (Kinesis):
```powershell
# Create S3 Bucket
aws s3 mb s3://pyspark-analytics-987684850401 --region ap-south-1

# Create Kinesis Stream
aws kinesis create-stream --stream-name user-activity-stream --shard-count 1 --region ap-south-1
```

Check if the stream is ready after ~30 seconds:
```powershell
aws kinesis describe-stream --stream-name user-activity-stream --region ap-south-1
```
*(Look for `"StreamStatus": "ACTIVE"` in the output)*

---

## 🧠 What to Learn Here:

### Why Kinesis instead of direct to S3?
Writing thousands of tiny events directly to S3 causes API rate limiting and creates a mess of micro-files that are terrible for analytics performance. Kinesis acts as a "buffer" that catches high-speed events, holds them temporarily, and allows us to batch them before saving.

### What is a "Shard"?
We used `--shard-count 1` when creating the Kinesis stream. A shard is a single pipe of throughput (guaranteeing 1MB/sec or 1000 records/sec ingest). Large enterprise apps scale by simply adding more shards (e.g., hundreds of shards for Black Friday traffic).

### Why S3 for the Bronze Layer?
S3 is our "Data Lake". It is highly durable and extremely cheap. Best practice is to dump everything here first in its raw format ("Bronze layer") before doing expensive transformations. If downstream data gets corrupted, you can always reload it from the Bronze layer.
