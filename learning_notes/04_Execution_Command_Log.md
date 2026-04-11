# 📜 execution_command_log.md - Tracked Commands

This document tracks exactly which commands have been executed on your AWS account during our session. 

> [!NOTE]
> I am executing these for you to save time, but you can copy-paste them yourself if you ever need to recreate the environment!

---

## ✅ PHASE 1: Basic Infrastructure (DONE)

### 1. Create S3 Bucket
```powershell
aws s3 mb s3://pyspark-analytics-987684850401 --region ap-south-1
```
*   **Result:** Created `s3://pyspark-analytics-987684850401`

### 2. Create Kinesis Stream
```powershell
aws kinesis create-stream --stream-name user-activity-stream --shard-count 1 --region ap-south-1
```
*   **Result:** Stream `user-activity-stream` is now **ACTIVE**.

---

## ⏳ PHASE 2: Lambda & Security (IN PROGRESS)

### 3. Create IAM Role for Lambda
```powershell
# Create the role
aws iam create-role --role-name lambda-kinesis-s3-role --assume-role-policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}"

# Attach Kinesis permissions
aws iam attach-role-policy --role-name lambda-kinesis-s3-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

# Attach S3 permissions
aws iam attach-role-policy --role-name lambda-kinesis-s3-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```
*   **Result:** IAM Role `lambda-kinesis-s3-role` is active with correct policies. ✅

### 4. Package & Deploy Lambda
```powershell
# Zip the code
Compress-Archive -Path lambda\transform_handler.py, lambda\data_quality.py -DestinationPath lambda_package.zip -Force

# Create function
aws lambda create-function `
    --function-name kinesis-event-processor `
    --runtime python3.11 `
    --handler transform_handler.lambda_handler `
    --role arn:aws:iam::987684850401:role/lambda-kinesis-s3-role `
    --zip-file fileb://lambda_package.zip `
    --timeout 60 --memory-size 256 `
    --environment "Variables={S3_BUCKET=pyspark-analytics-987684850401,BRONZE_PREFIX=bronze/events,QUARANTINE_PREFIX=quarantine/events}" `
    --region ap-south-1
```
*   **Result:** Lambda created. (Fixed handler from GUI to `transform_handler.lambda_handler`). ✅

### 5. Connect Kinesis to Lambda
```powershell
aws lambda create-event-source-mapping `
    --function-name kinesis-event-processor `
    --event-source-arn arn:aws:kinesis:ap-south-1:987684850401:stream/user-activity-stream `
    --starting-position LATEST --batch-size 100 --region ap-south-1
```
*   **Result:** Connection active. ✅

---

## ✅ PHASE 3 & 4: Production & Verification (DONE)

### 6. Run Python Producer
```powershell
$env:AWS_REGION = "ap-south-1"
python producer/event_generator.py --total-events 5000 --eps 20 --batch-size 25
```
*   **Result:** 5,000 events sent to Kinesis. ✅

### 7. Verify S3 Data Lake
```powershell
aws s3 ls s3://pyspark-analytics-987684850401 --recursive --human-readable --summarize
```
*   **Result:** **199 objects (3.0 MiB)** successfully landed in S3. ✅

---

## ✅ PHASE 5: Databricks & Spark (DONE - DBFS Bridge)

### 8. Download & Merge (Laptop)
```powershell
aws s3 sync s3://pyspark-analytics-987684850401/bronze/events/ staging_data/
Get-ChildItem -Path staging_data -Filter *.json -Recurse | Get-Content | Out-File -FilePath all_events_merged.json -Encoding utf8
```
*   **Result:** 199 files merged into 1 for easy upload. ✅

### 9. Upload & Create Table (Databricks)
1. Go to **Catalog** ➡️ **Create** ➡️ **Add Data**.
2. Upload `all_events_merged.json`.
3. Created table: `dev.default.all_events_merged`. ✅

### 10. Run Batch ETL (Bronze ➡️ Silver)
1. Open `bronze_to_silver.py`.
2. Update read line to: `df = spark.table("dev.default.all_events_merged")`.
3. **Result:** Data successfully loaded into Spark! 🚀

