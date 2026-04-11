"""
Lambda Ingestion Handler — Validates Kinesis events and routes to S3.

INTERVIEW NOTE: Why Lambda instead of Kinesis Data Firehose?
DECISION: Firehose delivers to S3 directly but has ZERO validation capability.
    Every record — valid or garbage — lands in the data lake. Lambda lets us:
    1. Validate schema/types/enums BEFORE storage
    2. Route invalid records to quarantine (with rejection reason)
    3. Enrich records with ingestion_timestamp (audit trail)
    4. Apply business rules at the ingestion boundary
TRADEOFF: Lambda adds one more component vs. Firehose's zero-code setup.
    But the cost difference is negligible (~$1/month at our scale), while
    the data quality benefit is enormous.

Example cost comparison at 10K events/day:
    Firehose: ~$0.035/GB ingested = ~$0.001/day → BUT bad data cleanup costs hours
    Lambda:   ~1000 invocations × $0.0000002 = ~$0.0002/day → clean data from day 1

INTERVIEW NOTE: How Lambda handles Kinesis at-least-once delivery
    Kinesis guarantees at-least-once delivery. Lambda may process the same
    batch twice if it fails mid-execution. We handle this by:
    1. UUID filenames → no overwrite risk (worst case: duplicate file)
    2. Deduplication in silver layer (Databricks dropDuplicates on event_id)
    This is the standard medallion approach — accept potential dupes in bronze,
    remove in silver.

Environment Variables:
    S3_BUCKET         — Target S3 bucket
    BRONZE_PREFIX     — S3 prefix for valid records
    QUARANTINE_PREFIX — S3 prefix for invalid records
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3

from data_quality import validate

# ============================================================================
# Structured JSON Logging
# ============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "your-bucket-name")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/events")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine/events")

s3_client = boto3.client("s3")


# ============================================================================
# S3 Writer
# ============================================================================

def write_records_to_s3(
    records: list[dict[str, Any]],
    prefix: str,
    partition_date: datetime,
) -> str:
    """
    Write records as newline-delimited JSON to S3 with Hive partitioning.

    INTERVIEW NOTE: Why NDJSON (newline-delimited JSON)?
    Each line is one complete JSON object. Benefits:
    1. Appendable — add lines without rewriting the file
    2. Splittable — Spark/Athena parallelize reads across lines
    3. Debuggable — read individual lines with head/tail/jq
    CSV looks simpler but struggles with nested fields and escaping.

    INTERVIEW NOTE: Why UUID filenames?
    If Lambda processes the same Kinesis batch twice (at-least-once delivery),
    each invocation writes a DIFFERENT file (different UUID). No data is
    overwritten. The silver layer deduplicates on event_id, so duplicate
    files are harmless. This is idempotency through append-only design.
    """
    partition_path = (
        f"{prefix}/"
        f"year={partition_date.strftime('%Y')}/"
        f"month={partition_date.strftime('%m')}/"
        f"day={partition_date.strftime('%d')}"
    )

    filename = f"{uuid.uuid4().hex}.json"
    s3_key = f"{partition_path}/{filename}"

    body = "\n".join(json.dumps(r) for r in records)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(json.dumps({
        "component": "lambda_handler",
        "action": "s3_write",
        "bucket": S3_BUCKET,
        "key": s3_key,
        "record_count": len(records),
    }))

    return s3_key


# ============================================================================
# Lambda Handler
# ============================================================================

def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Process Kinesis records: decode → validate → enrich → route.

    Processing flow per record:
    1. Base64 decode Kinesis payload → JSON dict
    2. Run validate() from data_quality module
    3. Valid:   append ingestion_timestamp → collect for bronze write
    4. Invalid: append rejection metadata → collect for quarantine write
    5. Batch write valid records to bronze, invalid to quarantine
    """
    start_time = time.time()
    kinesis_records = event.get("Records", [])
    request_id = getattr(context, "aws_request_id", "local-test")

    logger.info(json.dumps({
        "component": "lambda_handler",
        "action": "invocation_start",
        "incoming_records": len(kinesis_records),
        "request_id": request_id,
    }))

    valid_records: list[dict[str, Any]] = []
    quarantine_records: list[dict[str, Any]] = []
    parse_errors = 0
    now = datetime.now(timezone.utc)

    for kinesis_record in kinesis_records:
        record_start = time.time()

        # --- Decode ---
        try:
            raw = base64.b64decode(kinesis_record["kinesis"]["data"])
            record = json.loads(raw)
        except (KeyError, json.JSONDecodeError, Exception) as e:
            parse_errors += 1
            logger.error(json.dumps({
                "component": "lambda_handler",
                "action": "parse_error",
                "error": str(e),
                "sequence": kinesis_record.get("kinesis", {}).get("sequenceNumber", "unknown"),
            }))
            continue

        record_id = record.get("event_id", "unknown")

        # --- Validate ---
        is_valid, rejection_reason = validate(record)
        processing_ms = round((time.time() - record_start) * 1000, 2)

        if is_valid:
            record["ingestion_timestamp"] = now.isoformat()
            record["processing_date"] = now.strftime("%Y-%m-%d")
            valid_records.append(record)

            logger.info(json.dumps({
                "component": "lambda_handler",
                "status": "valid",
                "record_id": record_id,
                "processing_time_ms": processing_ms,
            }))
        else:
            record["rejection_reason"] = rejection_reason
            record["_quarantine_timestamp"] = now.isoformat()
            quarantine_records.append(record)

            logger.warning(json.dumps({
                "component": "lambda_handler",
                "status": "quarantined",
                "record_id": record_id,
                "rejection_reason": rejection_reason,
                "processing_time_ms": processing_ms,
            }))

    # --- Batch write to S3 ---
    # INTERVIEW NOTE: Why batch writes instead of per-record?
    # S3 PUT requests cost $0.005 per 1000 requests. Writing 100 records as
    # 100 individual files = 100 PUTs. Writing as 1 batch = 1 PUT.
    # Also avoids the "small file problem" — thousands of tiny files are
    # expensive to list and slow for Spark to read (each file = 1 task).
    if valid_records:
        write_records_to_s3(valid_records, BRONZE_PREFIX, now)

    if quarantine_records:
        write_records_to_s3(quarantine_records, QUARANTINE_PREFIX, now)

    total_ms = round((time.time() - start_time) * 1000, 2)

    summary = {
        "statusCode": 200,
        "total_received": len(kinesis_records),
        "valid_count": len(valid_records),
        "quarantine_count": len(quarantine_records),
        "parse_errors": parse_errors,
        "processing_time_ms": total_ms,
    }

    logger.info(json.dumps({
        "component": "lambda_handler",
        "action": "invocation_complete",
        **summary,
    }))

    return summary
