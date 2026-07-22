from __future__ import annotations

import base64
import copy
import json
from typing import Any

import pytest

from quality.validation_rules import (
    ReasonCode,
    ValidationResult,
    validate_bronze_record,
)


def valid_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": (
            "11111111-1111-4111-8111-111111111111"
        ),
        "event_type": "payment_authorized",
        "order_id": "order-001",
        "payment_id": "payment-001",
        "customer_id": "customer-001",
        "store_id": "store-001",
        "amount": "150000.00",
        "currency": "VND",
        "event_time": "2026-07-20T08:00:00Z",
        "producer_time": "2026-07-20T08:01:00Z",
        "schema_version": "1.0",
        "idempotency_key": (
            "11111111-1111-4111-8111-111111111111"
        ),
        "source": "synthetic-retail-producer",
    }

    event.update(overrides)
    return event


def bronze_from_bytes(
    payload: bytes,
) -> dict[str, Any]:
    return {
        "value_base64": base64.b64encode(
            payload
        ).decode("ascii")
    }


def bronze_from_json(
    value: Any,
) -> dict[str, Any]:
    payload = json.dumps(
        value,
        separators=(",", ":"),
    ).encode("utf-8")

    return bronze_from_bytes(payload)


def assert_invalid(
    result: ValidationResult,
    expected: ReasonCode,
) -> None:
    assert result.is_valid is False
    assert result.reason_code is expected
    assert result.reason_detail


def test_valid_payment_event() -> None:
    result = validate_bronze_record(
        bronze_from_json(valid_event())
    )

    assert result.is_valid is True
    assert result.reason_code is None
    assert result.reason_detail is None
    assert result.parsed_event == valid_event()


def test_valid_non_payment_event_with_null_amount() -> None:
    event = valid_event(
        event_type="order_created",
        payment_id=None,
        amount=None,
        currency=None,
    )

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert result.is_valid is True


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (
            {},
            ReasonCode.MISSING_RAW_PAYLOAD,
        ),
        (
            {"value_base64": None},
            ReasonCode.MISSING_RAW_PAYLOAD,
        ),
        (
            {"value_base64": 123},
            ReasonCode.INVALID_RAW_PAYLOAD_TYPE,
        ),
        (
            {"value_base64": "%%%"},
            ReasonCode.RAW_PAYLOAD_DECODE_ERROR,
        ),
        (
            bronze_from_bytes(b"\xff\xfe"),
            ReasonCode.INVALID_UTF8,
        ),
        (
            bronze_from_bytes(b'{"event_id":'),
            ReasonCode.MALFORMED_JSON,
        ),
        (
            bronze_from_json([]),
            ReasonCode.JSON_NOT_OBJECT,
        ),
        (
            bronze_from_json("hello"),
            ReasonCode.JSON_NOT_OBJECT,
        ),
        (
            bronze_from_json(123),
            ReasonCode.JSON_NOT_OBJECT,
        ),
        (
            bronze_from_json(None),
            ReasonCode.JSON_NOT_OBJECT,
        ),
    ],
)
def test_transport_and_json_failures(
    record: dict[str, Any],
    expected: ReasonCode,
) -> None:
    result = validate_bronze_record(record)

    assert_invalid(result, expected)


@pytest.mark.parametrize(
    ("updates", "removed", "expected"),
    [
        (
            {},
            "schema_version",
            ReasonCode.MISSING_SCHEMA_VERSION,
        ),
        (
            {"schema_version": None},
            None,
            ReasonCode.MISSING_SCHEMA_VERSION,
        ),
        (
            {"schema_version": "99.0"},
            None,
            ReasonCode.UNSUPPORTED_SCHEMA_VERSION,
        ),
        (
            {},
            "event_id",
            ReasonCode.MISSING_EVENT_ID,
        ),
        (
            {"event_id": None},
            None,
            ReasonCode.MISSING_EVENT_ID,
        ),
        (
            {"event_id": "not-a-uuid"},
            None,
            ReasonCode.INVALID_EVENT_ID,
        ),
        (
            {},
            "event_type",
            ReasonCode.MISSING_EVENT_TYPE,
        ),
        (
            {"event_type": "payment_done"},
            None,
            ReasonCode.INVALID_EVENT_TYPE,
        ),
        (
            {},
            "event_time",
            ReasonCode.MISSING_EVENT_TIME,
        ),
        (
            {"event_time": "not-a-time"},
            None,
            ReasonCode.INVALID_EVENT_TIME,
        ),
        (
            {
                "event_time": (
                    "2026-07-20T08:00:00"
                )
            },
            None,
            ReasonCode.INVALID_EVENT_TIME,
        ),
        (
            {},
            "producer_time",
            ReasonCode.MISSING_PRODUCER_TIME,
        ),
        (
            {"producer_time": "not-a-time"},
            None,
            ReasonCode.INVALID_PRODUCER_TIME,
        ),
        (
            {
                "producer_time": (
                    "2026-07-20T07:59:00Z"
                )
            },
            None,
            (
                ReasonCode
                .PRODUCER_TIME_BEFORE_EVENT_TIME
            ),
        ),
    ],
)
def test_contract_failures(
    updates: dict[str, Any],
    removed: str | None,
    expected: ReasonCode,
) -> None:
    event = valid_event(**updates)

    if removed is not None:
        event.pop(removed)

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert_invalid(result, expected)


@pytest.mark.parametrize(
    "field_name",
    [
        "order_id",
        "customer_id",
        "store_id",
        "idempotency_key",
        "source",
    ],
)
def test_missing_common_required_fields(
    field_name: str,
) -> None:
    event = valid_event()
    event.pop(field_name)

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert_invalid(
        result,
        ReasonCode.MISSING_REQUIRED_FIELD,
    )


def test_invalid_common_required_field_type() -> None:
    event = valid_event(order_id=123)

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert_invalid(
        result,
        ReasonCode.INVALID_REQUIRED_FIELD,
    )


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        (
            {"amount": "abc"},
            ReasonCode.INVALID_AMOUNT,
        ),
        (
            {"amount": 100},
            ReasonCode.INVALID_AMOUNT,
        ),
        (
            {"amount": "-100.00"},
            ReasonCode.NEGATIVE_AMOUNT,
        ),
        (
            {"currency": "EUR"},
            ReasonCode.INVALID_CURRENCY,
        ),
        (
            {
                "amount": "100.00",
                "currency": None,
            },
            ReasonCode.AMOUNT_CURRENCY_MISMATCH,
        ),
        (
            {
                "amount": None,
                "currency": "VND",
            },
            ReasonCode.AMOUNT_CURRENCY_MISMATCH,
        ),
        (
            {"payment_id": None},
            ReasonCode.MISSING_PAYMENT_ID,
        ),
        (
            {"payment_id": 123},
            ReasonCode.INVALID_PAYMENT_ID,
        ),
        (
            {
                "amount": None,
                "currency": None,
            },
            ReasonCode.MISSING_PAYMENT_DETAILS,
        ),
    ],
)
def test_business_rule_failures(
    updates: dict[str, Any],
    expected: ReasonCode,
) -> None:
    event = valid_event(**updates)

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert_invalid(result, expected)


def test_primary_reason_uses_priority_order() -> None:
    event = valid_event(
        schema_version="99.0",
        event_id="not-a-uuid",
        amount="-100",
        currency="ABC",
    )

    result = validate_bronze_record(
        bronze_from_json(event)
    )

    assert_invalid(
        result,
        ReasonCode.UNSUPPORTED_SCHEMA_VERSION,
    )


def test_validation_is_deterministic() -> None:
    record = bronze_from_json(
        valid_event(amount="-100.00")
    )

    first_result = validate_bronze_record(record)
    second_result = validate_bronze_record(record)

    assert first_result == second_result


def test_validator_does_not_mutate_input() -> None:
    record = bronze_from_json(valid_event())
    before = copy.deepcopy(record)

    validate_bronze_record(record)

    assert record == before


def test_invalid_result_has_enum_and_detail() -> None:
    result = validate_bronze_record(
        bronze_from_json(
            valid_event(amount="-1")
        )
    )

    assert isinstance(
        result.reason_code,
        ReasonCode,
    )
    assert result.reason_detail