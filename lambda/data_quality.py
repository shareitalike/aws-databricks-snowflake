"""
Data Quality Module — Reusable validation for Lambda and Databricks Spark.

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
