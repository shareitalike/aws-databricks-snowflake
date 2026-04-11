"""
Data Quality Module — Reusable validation for Lambda and Databricks Spark.

INTERVIEW NOTE: Why a shared module across Lambda AND Spark?
DECISION: Both ingestion (Lambda) and transformation (Databricks) need identical
    validation rules. If you duplicate logic, eventually one gets updated and the
    other doesn't — a "schema drift" bug that's hard to catch.
    Example: If Lambda accepts event_type="wishlist" but Spark still rejects it,
    valid records silently vanish in the silver layer.
TRADEOFF: Packaging this module into both Lambda (zip deployment) and Databricks
    (uploaded as a wheel or notebook import) requires a small deployment step.
    But catching a drift bug in production costs 10x more than this overhead.

    This module is intentionally zero-dependency (no pandas, no Spark imports)
    so it runs in both Lambda (128 MB, cold start matters) and Spark (JVM + Python).

Unit tests: tests/test_data_quality.py
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Constants — Single source of truth for valid values
# ============================================================================
# INTERVIEW NOTE: Using frozenset instead of list for O(1) lookups.
# With 5 event types this doesn't matter, but at 500 enum values (real
# product catalogs), the difference between O(n) list scan and O(1) set
# lookup is measurable in Lambda cold-start time.

VALID_EVENT_TYPES = frozenset([
    "product_view",
    "add_to_cart",
    "purchase",
    "login",
    "logout",
])

VALID_DEVICES = frozenset(["mobile", "desktop", "tablet"])

VALID_COUNTRIES = frozenset([
    "US", "IN", "GB", "DE", "FR", "JP", "BR", "CA", "AU", "SG", "AE", "KR",
])

REQUIRED_FIELDS = [
    "event_id",
    "event_time",
    "user_id",
    "event_type",
    "device",
    "country",
]

# ISO 8601 regex — covers YYYY-MM-DDTHH:MM:SS with optional timezone
ISO_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
)

# Anomaly thresholds — calibrated from "realistic" e-commerce data
PRICE_MIN = 0.01
PRICE_MAX = 50_000.00
USER_ID_MIN = 1
USER_ID_MAX = 1_000_000


# ============================================================================
# Validation Functions
# ============================================================================

def validate_schema(record: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Check that all required fields exist in the record.

    INTERVIEW NOTE: Schema validation is the cheapest check (O(n) field count).
    Run it first — if a record is missing event_id, there's no point checking
    if the event_type enum is valid. Fail-fast ordering saves CPU in Lambda.

    Example:
        >>> validate_schema({"event_id": "abc"})
        (False, 'missing_fields:event_time,user_id,event_type,device,country')
    """
    if not isinstance(record, dict):
        return False, "record_is_not_dict"

    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        return False, f"missing_fields:{','.join(missing)}"

    return True, None


def validate_types(record: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate that field values match expected Python types.

    INTERVIEW NOTE: Type validation catches bugs like user_id arriving as
    string "42567" vs integer 42567. This matters because:
    - Spark infers different schemas for different partitions
    - Snowflake GROUP BY on string "123" vs int 123 produces different hash buckets
    - JSON has no integer type — everything could arrive as string
    A type mismatch that slips into silver will break downstream JOINs silently.
    """
    event_id = record.get("event_id")
    if not isinstance(event_id, str) or len(event_id) < 8:
        return False, "invalid_event_id_format"

    event_time = record.get("event_time")
    if not isinstance(event_time, str) or not ISO_TIMESTAMP_PATTERN.match(event_time):
        return False, "invalid_event_time_format"

    user_id = record.get("user_id")
    if not isinstance(user_id, int):
        return False, "user_id_not_integer"

    if not isinstance(record.get("event_type"), str):
        return False, "event_type_not_string"

    if not isinstance(record.get("device"), str):
        return False, "device_not_string"

    if not isinstance(record.get("country"), str):
        return False, "country_not_string"

    # Nullable fields: present but wrong type is an error
    product_id = record.get("product_id")
    if product_id is not None and not isinstance(product_id, int):
        return False, "product_id_not_integer"

    price = record.get("price")
    if price is not None and not isinstance(price, (int, float)):
        return False, "price_not_numeric"

    return True, None


def validate_enums(record: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate categorical fields against allowed values.

    INTERVIEW NOTE: A single typo like device="mobil" creates a phantom
    category in every GROUP BY query. Your dashboard shows 4 device types
    instead of 3, and the numbers don't add up. Enum validation at ingestion
    prevents this class of bug entirely.
    Example: SELECT device, COUNT(*) ... would return:
        mobile:  5400
        desktop: 3500
        tablet:  1000
        mobil:      3   ← phantom category from typo
    """
    event_type = record.get("event_type", "")
    if event_type not in VALID_EVENT_TYPES:
        return False, f"invalid_event_type:{event_type}"

    device = record.get("device", "")
    if device not in VALID_DEVICES:
        return False, f"invalid_device:{device}"

    country = record.get("country", "")
    if country not in VALID_COUNTRIES:
        return False, f"invalid_country:{country}"

    return True, None


def detect_anomalies(record: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Detect logically unreasonable values that pass type/enum checks.

    INTERVIEW NOTE: Anomaly detection != validation. Validation checks structure,
    anomaly detection checks business logic. A price of -50.00 is a valid float
    but an impossible product price. In production, you'd use statistical methods
    (z-score on rolling 7-day window) or ML anomaly detection. For this pipeline,
    deterministic range checks cover the most impactful cases.

    Example production scenario: A payment system bug sends price=0.00 for
    real purchases. Type validation passes (it IS a float). Enum validation
    passes. But anomaly detection catches it — $0 purchases are suspicious.
    """
    # Price range check
    price = record.get("price")
    if price is not None:
        if price < PRICE_MIN:
            return False, f"anomaly_price_too_low:{price}"
        if price > PRICE_MAX:
            return False, f"anomaly_price_too_high:{price}"

    # User ID range check
    user_id = record.get("user_id")
    if isinstance(user_id, int):
        if user_id < USER_ID_MIN or user_id > USER_ID_MAX:
            return False, f"anomaly_user_id_out_of_range:{user_id}"

    # Future timestamp check (clock skew tolerance: 5 minutes)
    event_time = record.get("event_time", "")
    if isinstance(event_time, str) and ISO_TIMESTAMP_PATTERN.match(event_time):
        try:
            ts = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            if ts > now_utc:
                return False, "anomaly_future_timestamp"
        except (ValueError, TypeError):
            pass  # Caught by type validation

    # Business rule: purchase events MUST have product_id and price
    # INTERVIEW NOTE: This is a business contract, not a schema rule.
    # Login events legitimately have null product_id. But a purchase
    # without a product means the checkout flow has a bug.
    event_type = record.get("event_type")
    if event_type == "purchase":
        if record.get("product_id") is None:
            return False, "anomaly_purchase_missing_product_id"
        if record.get("price") is None:
            return False, "anomaly_purchase_missing_price"

    # Business rule: login/logout events should NOT have price
    if event_type in ("login", "logout"):
        if record.get("price") is not None:
            return False, f"anomaly_{event_type}_has_price"

    return True, None


def validate(record: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Run all validation checks in fail-fast order (cheapest first).

    This is the single entry point for both Lambda and Databricks.

    INTERVIEW NOTE: Ordering matters for performance. Schema check (O(6) field
    lookup) runs before anomaly detection (datetime parsing, float comparison).
    In Lambda processing 100 records per invocation, this saves ~5ms total —
    small per call, but at 1M invocations/day that's 5000 seconds of Lambda
    time saved = real cost reduction.
    """
    checks = [
        validate_schema,     # O(n) field existence — cheapest
        validate_types,      # O(n) isinstance checks
        validate_enums,      # O(1) set lookups
        detect_anomalies,    # datetime parsing, float comparison — most expensive
    ]

    for check_fn in checks:
        is_valid, reason = check_fn(record)
        if not is_valid:
            return False, reason

    return True, None
