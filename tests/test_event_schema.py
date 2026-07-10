import json
from copy import deepcopy

import pytest
from pydantic import ValidationError

from producer.schemas import (
    Currency,
    EventType,
    RetailPaymentEvent,
)


def valid_payment_event() -> dict:
    return {
        "event_id": "5e84235a-82a4-4f5a-862a-10c903c2173f",
        "event_type": "payment_authorized",
        "order_id": "order-1001",
        "payment_id": "payment-1001",
        "customer_id": "customer-1001",
        "store_id": "store-001",
        "amount": "150000.00",
        "currency": "VND",
        "event_time": "2026-07-10T03:00:00Z",
        "producer_time": "2026-07-10T03:00:01Z",
        "schema_version": "1.0",
        "idempotency_key": (
            "5e84235a-82a4-4f5a-862a-10c903c2173f"
        ),
        "source": "synthetic-retail-producer",
    }


def test_valid_payment_event_passes() -> None:
    event = RetailPaymentEvent.model_validate(
        valid_payment_event()
    )

    assert event.event_type is EventType.PAYMENT_AUTHORIZED
    assert event.currency is Currency.VND
    assert event.order_id == "order-1001"


def test_event_serializes_to_expected_json() -> None:
    event = RetailPaymentEvent.model_validate(
        valid_payment_event()
    )

    payload = json.loads(event.model_dump_json())

    assert payload["event_type"] == "payment_authorized"
    assert payload["amount"] == "150000.00"
    assert payload["schema_version"] == "1.0"


def test_missing_event_id_fails() -> None:
    payload = valid_payment_event()
    payload.pop("event_id")

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_invalid_event_type_fails() -> None:
    payload = valid_payment_event()
    payload["event_type"] = "unknown_event"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_naive_timestamp_fails() -> None:
    payload = valid_payment_event()
    payload["event_time"] = "2026-07-10T03:00:00"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_payment_event_without_payment_id_fails() -> None:
    payload = valid_payment_event()
    payload["payment_id"] = None

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_negative_amount_fails() -> None:
    payload = valid_payment_event()
    payload["amount"] = "-1000.00"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_unsupported_schema_version_fails() -> None:
    payload = valid_payment_event()
    payload["schema_version"] = "99.0"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_producer_time_before_event_time_fails() -> None:
    payload = valid_payment_event()
    payload["producer_time"] = "2026-07-10T02:59:59Z"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_unknown_field_fails() -> None:
    payload = valid_payment_event()
    payload["unexpected_field"] = "unexpected-value"

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate(payload)


def test_malformed_json_fails() -> None:
    malformed_json = '{"event_id":'

    with pytest.raises(ValidationError):
        RetailPaymentEvent.model_validate_json(malformed_json)


def test_original_fixture_is_not_mutated() -> None:
    payload = valid_payment_event()
    original = deepcopy(payload)

    RetailPaymentEvent.model_validate(payload)

    assert payload == original