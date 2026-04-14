"""
Event Producer — Simulates e-commerce user activity and sends to Kinesis.

Usage:
    python event_generator.py --total-events 5000 --eps 10 --batch-size 25

Environment Variables:
    AWS_REGION            — AWS region (default: ap-south-1)
    KINESIS_STREAM_NAME   — Kinesis stream name (default: user-activity-stream)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# ============================================================================
# Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","component":"event_producer","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("event_producer")

# ============================================================================
# Event Distribution — Realistic E-commerce Funnel
EVENT_TYPES_WEIGHTED: list[tuple[str, int]] = [
    ("product_view", 60),
    ("add_to_cart", 20),
    ("purchase", 10),
    ("login", 5),
    ("logout", 5),
]

DEVICES_WEIGHTED: list[tuple[str, int]] = [
    ("mobile", 55),
    ("desktop", 35),
    ("tablet", 10),
]

COUNTRIES: list[str] = [
    "US", "IN", "GB", "DE", "FR", "JP", "BR", "CA", "AU", "SG", "AE", "KR",
]

PRODUCT_IDS: list[int] = list(range(1001, 1051))  # 50 products
PRICE_RANGE: tuple[float, float] = (4.99, 999.99)
BAD_RECORD_PCT: float = 0.05


# ============================================================================
# Event Generation
# ============================================================================

def weighted_choice(choices: list[tuple[str, int]]) -> str:
    """Select a value based on weighted distribution."""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


def generate_valid_event() -> dict[str, Any]:
    """Generate a single valid event conforming to the data model."""
    event_type = weighted_choice(EVENT_TYPES_WEIGHTED)

    event: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 100_000),
        "event_type": event_type,
        "device": weighted_choice(DEVICES_WEIGHTED),
        "country": random.choice(COUNTRIES),
    }

    if event_type in ("product_view", "add_to_cart", "purchase"):
        event["product_id"] = random.choice(PRODUCT_IDS)
        event["price"] = round(random.uniform(*PRICE_RANGE), 2)
    else:
        event["product_id"] = None
        event["price"] = None

    return event


def generate_bad_event() -> dict[str, Any]:
    """
    Generate an intentionally malformed event for testing validation.
    """
    corruption = random.choice([
        "missing_field",
        "invalid_event_type",
        "negative_price",
        "string_user_id",
        "future_timestamp",
        "price_on_login",
    ])

    event = generate_valid_event()

    if corruption == "missing_field":
        field = random.choice(["event_id", "user_id", "event_type"])
        event.pop(field, None)

    elif corruption == "invalid_event_type":
        event["event_type"] = "invalid_click_event"

    elif corruption == "negative_price":
        event["price"] = round(random.uniform(-100, -0.01), 2)
        event["event_type"] = "purchase"
        event["product_id"] = random.choice(PRODUCT_IDS)

    elif corruption == "string_user_id":
        event["user_id"] = "not_a_number"

    elif corruption == "future_timestamp":
        event["event_time"] = "2099-12-31T23:59:59+00:00"

    elif corruption == "price_on_login":
        event["event_type"] = "login"
        event["price"] = 29.99
        event["product_id"] = None

    logger.info("bad_record_generated | corruption=%s", corruption)
    return event


def generate_event() -> dict[str, Any]:
    """Generate a valid or bad event based on configured bad record %."""
    if random.random() < BAD_RECORD_PCT:
        return generate_bad_event()
    return generate_valid_event()


# ============================================================================
# Kinesis Batch Producer with Exponential Backoff
# ============================================================================

def send_batch_to_kinesis(
    client: Any,
    stream_name: str,
    records: list[dict[str, Any]],
    max_retries: int = 3,
) -> int:
    """
    Send a batch of records to Kinesis with exponential backoff retry.
    """
    kinesis_records = [
        {
            "Data": json.dumps(record).encode("utf-8"),
            "PartitionKey": record.get("event_id", str(uuid.uuid4())),
        }
        for record in records
    ]

    sent_count = 0

    for attempt in range(max_retries + 1):
        try:
            response = client.put_records(
                StreamName=stream_name,
                Records=kinesis_records,
            )

            failed = response.get("FailedRecordCount", 0)
            sent_count = len(kinesis_records) - failed

            if failed == 0:
                logger.info(
                    "batch_sent | records=%d | attempt=%d",
                    sent_count, attempt + 1,
                )
                return sent_count

            # Retry only failed records
            kinesis_records = [
                rec for rec, res in zip(kinesis_records, response["Records"])
                if "ErrorCode" in res
            ]

        except ClientError as e:
            logger.error(
                "kinesis_error | code=%s | attempt=%d",
                e.response["Error"]["Code"], attempt + 1,
            )

        if attempt < max_retries:
            backoff = (2 ** attempt) + random.uniform(0, 1)
            logger.info("retry_backoff | seconds=%.2f", backoff)
            time.sleep(backoff)

    logger.error("batch_exhausted_retries | unsent=%d", len(kinesis_records))
    return sent_count


# ============================================================================
# Main Producer Loop
# ============================================================================

def run_producer(
    total_events: int = 5000,
    events_per_second: int = 10,
    batch_size: int = 25,
) -> None:
    """Main loop — generate events and send to Kinesis in batches."""
    region = os.environ.get("AWS_REGION", "ap-south-1")
    stream_name = os.environ.get("KINESIS_STREAM_NAME", "user-activity-stream")

    logger.info(
        "producer_start | total=%d | eps=%d | batch=%d | stream=%s",
        total_events, events_per_second, batch_size, stream_name,
    )

    client = boto3.client("kinesis", region_name=region)

    total_sent = 0
    total_failed = 0
    batch: list[dict[str, Any]] = []
    start = time.time()

    for i in range(1, total_events + 1):
        batch.append(generate_event())

        if len(batch) >= batch_size:
            sent = send_batch_to_kinesis(client, stream_name, batch)
            total_sent += sent
            total_failed += len(batch) - sent
            batch = []

            # Rate limiting
            elapsed = time.time() - start
            expected = i / events_per_second
            if elapsed < expected:
                time.sleep(expected - elapsed)

        if i % 500 == 0:
            logger.info(
                "progress | generated=%d/%d | sent=%d | failed=%d",
                i, total_events, total_sent, total_failed,
            )

    # Flush remaining
    if batch:
        sent = send_batch_to_kinesis(client, stream_name, batch)
        total_sent += sent
        total_failed += len(batch) - sent

    elapsed_total = time.time() - start
    logger.info(
        "producer_complete | total=%d | sent=%d | failed=%d | seconds=%.1f",
        total_events, total_sent, total_failed, elapsed_total,
    )


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate e-commerce events → Kinesis")
    parser.add_argument("--total-events", type=int, default=5000)
    parser.add_argument("--eps", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=25)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.batch_size > 500:
        logger.error("batch_size cannot exceed 500 (Kinesis API limit)")
        sys.exit(1)
    run_producer(args.total_events, args.eps, args.batch_size)
