"""
Unit Tests for Data Quality Module.

Run with: python -m pytest tests/test_data_quality.py -v
"""

from __future__ import annotations

import sys
import os
import pytest

# Add lambda directory to path so we can import data_quality
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))

from data_quality import (
    validate_schema,
    validate_types,
    validate_enums,
    detect_anomalies,
    validate,
)


# ============================================================================
# Fixtures — Reusable test data
# ============================================================================

@pytest.fixture
def valid_event() -> dict:
    """A fully valid event that should pass all checks."""
    return {
        "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "event_time": "2024-03-15T14:30:22+00:00",
        "user_id": 42567,
        "event_type": "purchase",
        "product_id": 1023,
        "price": 79.99,
        "device": "mobile",
        "country": "IN",
    }


@pytest.fixture
def valid_login_event() -> dict:
    """Valid login event with nullable fields."""
    return {
        "event_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
        "event_time": "2024-03-15T14:31:05+00:00",
        "user_id": 18234,
        "event_type": "login",
        "product_id": None,
        "price": None,
        "device": "desktop",
        "country": "US",
    }


# ============================================================================
# Schema Validation Tests
# ============================================================================

class TestValidateSchema:
    """Tests for validate_schema()."""

    def test_valid_event_passes(self, valid_event):
        is_valid, reason = validate_schema(valid_event)
        assert is_valid is True
        assert reason is None

    def test_missing_event_id(self, valid_event):
        del valid_event["event_id"]
        is_valid, reason = validate_schema(valid_event)
        assert is_valid is False
        assert "event_id" in reason

    def test_missing_multiple_fields(self):
        record = {"event_time": "2024-03-15T14:30:22+00:00"}
        is_valid, reason = validate_schema(record)
        assert is_valid is False
        assert "missing_fields:" in reason

    def test_non_dict_input(self):
        is_valid, reason = validate_schema("not a dict")
        assert is_valid is False
        assert reason == "record_is_not_dict"

    def test_empty_dict(self):
        is_valid, reason = validate_schema({})
        assert is_valid is False
        assert "missing_fields:" in reason


# ============================================================================
# Type Validation Tests
# ============================================================================

class TestValidateTypes:
    """Tests for validate_types()."""

    def test_valid_types_pass(self, valid_event):
        is_valid, reason = validate_types(valid_event)
        assert is_valid is True

    def test_string_user_id_fails(self, valid_event):
        valid_event["user_id"] = "not_a_number"
        is_valid, reason = validate_types(valid_event)
        assert is_valid is False
        assert reason == "user_id_not_integer"

    def test_short_event_id_fails(self, valid_event):
        valid_event["event_id"] = "abc"
        is_valid, reason = validate_types(valid_event)
        assert is_valid is False
        assert reason == "invalid_event_id_format"

    def test_invalid_timestamp_format(self, valid_event):
        valid_event["event_time"] = "March 15, 2024"
        is_valid, reason = validate_types(valid_event)
        assert is_valid is False
        assert reason == "invalid_event_time_format"

    def test_string_product_id_fails(self, valid_event):
        valid_event["product_id"] = "abc"
        is_valid, reason = validate_types(valid_event)
        assert is_valid is False
        assert reason == "product_id_not_integer"

    def test_null_product_id_passes(self, valid_login_event):
        is_valid, reason = validate_types(valid_login_event)
        assert is_valid is True

    def test_string_price_fails(self, valid_event):
        valid_event["price"] = "seventy-nine"
        is_valid, reason = validate_types(valid_event)
        assert is_valid is False
        assert reason == "price_not_numeric"


# ============================================================================
# Enum Validation Tests
# ============================================================================

class TestValidateEnums:
    """Tests for validate_enums()."""

    def test_valid_enums_pass(self, valid_event):
        is_valid, reason = validate_enums(valid_event)
        assert is_valid is True

    def test_invalid_event_type(self, valid_event):
        valid_event["event_type"] = "click"
        is_valid, reason = validate_enums(valid_event)
        assert is_valid is False
        assert "invalid_event_type:click" in reason

    def test_invalid_device(self, valid_event):
        valid_event["device"] = "smartwatch"
        is_valid, reason = validate_enums(valid_event)
        assert is_valid is False
        assert "invalid_device:smartwatch" in reason

    def test_invalid_country(self, valid_event):
        valid_event["country"] = "XX"
        is_valid, reason = validate_enums(valid_event)
        assert is_valid is False
        assert "invalid_country:XX" in reason

    def test_all_valid_event_types(self):
        """Every valid event type should pass."""
        for et in ["product_view", "add_to_cart", "purchase", "login", "logout"]:
            record = {"event_type": et, "device": "mobile", "country": "US"}
            is_valid, _ = validate_enums(record)
            assert is_valid is True, f"{et} should be valid"


# ============================================================================
# Anomaly Detection Tests
# ============================================================================

class TestDetectAnomalies:
    """Tests for detect_anomalies()."""

    def test_valid_event_passes(self, valid_event):
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is True

    def test_negative_price(self, valid_event):
        valid_event["price"] = -50.00
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_price_too_low" in reason

    def test_price_too_high(self, valid_event):
        valid_event["price"] = 100_000.00
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_price_too_high" in reason

    def test_user_id_out_of_range(self, valid_event):
        valid_event["user_id"] = 0
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_user_id_out_of_range" in reason

    def test_future_timestamp(self, valid_event):
        valid_event["event_time"] = "2099-12-31T23:59:59+00:00"
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert reason == "anomaly_future_timestamp"

    def test_purchase_without_product_id(self, valid_event):
        valid_event["event_type"] = "purchase"
        valid_event["product_id"] = None
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_purchase_missing_product_id" in reason

    def test_purchase_without_price(self, valid_event):
        valid_event["event_type"] = "purchase"
        valid_event["price"] = None
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_purchase_missing_price" in reason

    def test_login_with_price(self, valid_event):
        valid_event["event_type"] = "login"
        valid_event["price"] = 29.99
        is_valid, reason = detect_anomalies(valid_event)
        assert is_valid is False
        assert "anomaly_login_has_price" in reason


# ============================================================================
# Full Validation Pipeline Tests
# ============================================================================

class TestValidate:
    """Tests for validate() — the orchestrating function."""

    def test_valid_event_passes_all(self, valid_event):
        is_valid, reason = validate(valid_event)
        assert is_valid is True
        assert reason is None

    def test_valid_login_passes_all(self, valid_login_event):
        is_valid, reason = validate(valid_login_event)
        assert is_valid is True

    def test_schema_failure_short_circuits(self):
        """Schema check runs first — type/enum/anomaly checks never run."""
        record = {"only_one_field": True}
        is_valid, reason = validate(record)
        assert is_valid is False
        assert "missing_fields:" in reason

    def test_type_failure_before_enum(self, valid_event):
        """Type check runs before enum — catches type issues first."""
        valid_event["user_id"] = "not_a_number"
        is_valid, reason = validate(valid_event)
        assert is_valid is False
        assert reason == "user_id_not_integer"

    def test_multiple_issues_returns_first(self, valid_event):
        """Fail-fast: only first failure reason is returned."""
        valid_event["user_id"] = "bad"
        valid_event["event_type"] = "invalid"
        valid_event["price"] = -100
        is_valid, reason = validate(valid_event)
        assert is_valid is False
        # Type check runs before enum, so user_id error comes first
        assert reason == "user_id_not_integer"
