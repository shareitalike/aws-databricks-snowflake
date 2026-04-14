"""
Lambda Ingestion Handler — Validates Kinesis events and routes to S3.

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
