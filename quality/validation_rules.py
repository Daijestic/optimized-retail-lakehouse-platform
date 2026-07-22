"""Quy tắc validation stateless cho Bronze record trước khi vào Silver."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Mapping
from uuid import UUID


SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})

SUPPORTED_EVENT_TYPES = frozenset(
    {
        "order_created",
        "order_confirmed",
        "payment_authorized",
        "payment_failed",
        "order_shipped",
        "order_delivered",
        "refund_requested",
    }
)

SUPPORTED_CURRENCIES = frozenset({"VND", "USD"})

PAYMENT_EVENT_TYPES = frozenset(
    {
        "payment_authorized",
        "payment_failed",
        "refund_requested",
    }
)

COMMON_REQUIRED_FIELDS = (
    "order_id",
    "customer_id",
    "store_id",
    "idempotency_key",
    "source",
)


class ReasonCode(StrEnum):
    """Mã lỗi ổn định dùng cho routing, thống kê và DLQ."""

    MISSING_RAW_PAYLOAD = "MISSING_RAW_PAYLOAD"
    INVALID_RAW_PAYLOAD_TYPE = "INVALID_RAW_PAYLOAD_TYPE"
    RAW_PAYLOAD_DECODE_ERROR = "RAW_PAYLOAD_DECODE_ERROR"
    INVALID_UTF8 = "INVALID_UTF8"
    MALFORMED_JSON = "MALFORMED_JSON"
    JSON_NOT_OBJECT = "JSON_NOT_OBJECT"

    MISSING_SCHEMA_VERSION = "MISSING_SCHEMA_VERSION"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"

    MISSING_EVENT_ID = "MISSING_EVENT_ID"
    INVALID_EVENT_ID = "INVALID_EVENT_ID"

    MISSING_EVENT_TYPE = "MISSING_EVENT_TYPE"
    INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"

    MISSING_EVENT_TIME = "MISSING_EVENT_TIME"
    INVALID_EVENT_TIME = "INVALID_EVENT_TIME"

    MISSING_PRODUCER_TIME = "MISSING_PRODUCER_TIME"
    INVALID_PRODUCER_TIME = "INVALID_PRODUCER_TIME"

    PRODUCER_TIME_BEFORE_EVENT_TIME = (
        "PRODUCER_TIME_BEFORE_EVENT_TIME"
    )

    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_REQUIRED_FIELD = "INVALID_REQUIRED_FIELD"

    INVALID_AMOUNT = "INVALID_AMOUNT"
    NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"

    INVALID_CURRENCY = "INVALID_CURRENCY"
    AMOUNT_CURRENCY_MISMATCH = "AMOUNT_CURRENCY_MISMATCH"

    MISSING_PAYMENT_ID = "MISSING_PAYMENT_ID"
    INVALID_PAYMENT_ID = "INVALID_PAYMENT_ID"
    MISSING_PAYMENT_DETAILS = "MISSING_PAYMENT_DETAILS"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Kết quả validation của đúng một Bronze source record."""

    is_valid: bool
    reason_code: ReasonCode | None
    reason_detail: str | None
    parsed_event: dict[str, Any] | None

    def __post_init__(self) -> None:
        if self.is_valid:
            if (
                self.reason_code is not None
                or self.reason_detail is not None
            ):
                raise ValueError(
                    "valid result must not contain a failure reason"
                )
        elif self.reason_code is None or not self.reason_detail:
            raise ValueError(
                "invalid result must contain "
                "reason_code and reason_detail"
            )


def _valid(event: Mapping[str, Any]) -> ValidationResult:
    return ValidationResult(
        is_valid=True,
        reason_code=None,
        reason_detail=None,
        parsed_event=dict(event),
    )


def _invalid(
    reason_code: ReasonCode,
    reason_detail: str,
    parsed_event: Mapping[str, Any] | None = None,
) -> ValidationResult:
    return ValidationResult(
        is_valid=False,
        reason_code=reason_code,
        reason_detail=reason_detail,
        parsed_event=(
            None
            if parsed_event is None
            else dict(parsed_event)
        ),
    )


def _is_missing(value: Any) -> bool:
    return value is None or (
        isinstance(value, str)
        and not value.strip()
    )


def _parse_aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    normalized = value.strip()

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
    ):
        return None

    return parsed


def _parse_decimal(value: Any) -> Decimal | None:
    # Data contract yêu cầu amount là decimal string,
    # không nhận float/int JSON.
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None

    if not parsed.is_finite():
        return None

    return parsed


def decode_raw_payload(
    value_base64: Any,
) -> ValidationResult | str:
    """Decode Base64 nghiêm ngặt và UTF-8.

    Returns:
        Payload text nếu thành công, nếu không trả ValidationResult
        không hợp lệ.
    """

    if value_base64 is None or value_base64 == "":
        return _invalid(
            ReasonCode.MISSING_RAW_PAYLOAD,
            "value_base64 is required",
        )

    if not isinstance(value_base64, str):
        return _invalid(
            ReasonCode.INVALID_RAW_PAYLOAD_TYPE,
            (
                "value_base64 must be str, got "
                f"{type(value_base64).__name__}"
            ),
        )

    try:
        payload_bytes = base64.b64decode(
            value_base64,
            validate=True,
        )
    except (
        binascii.Error,
        ValueError,
        UnicodeEncodeError,
    ) as exc:
        return _invalid(
            ReasonCode.RAW_PAYLOAD_DECODE_ERROR,
            (
                "value_base64 is not valid Base64: "
                f"{exc}"
            ),
        )

    try:
        return payload_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        return _invalid(
            ReasonCode.INVALID_UTF8,
            (
                "decoded payload is not valid UTF-8: "
                f"{exc}"
            ),
        )


def parse_json_object(
    payload_text: str,
) -> ValidationResult | dict[str, Any]:
    """Parse JSON và yêu cầu top-level phải là object."""

    try:
        parsed = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        return _invalid(
            ReasonCode.MALFORMED_JSON,
            (
                "payload is not valid JSON: "
                f"{exc.msg} at position {exc.pos}"
            ),
        )

    if not isinstance(parsed, dict):
        return _invalid(
            ReasonCode.JSON_NOT_OBJECT,
            (
                "top-level JSON must be object, got "
                f"{type(parsed).__name__}"
            ),
        )

    return parsed


def validate_event(
    event: Mapping[str, Any],
) -> ValidationResult:
    """Áp dụng validation theo priority cố định."""

    event_copy = dict(event)

    # ---------------------------------------------------------
    # Priority 200–210: schema version
    # ---------------------------------------------------------

    schema_version = event_copy.get("schema_version")

    if _is_missing(schema_version):
        return _invalid(
            ReasonCode.MISSING_SCHEMA_VERSION,
            "schema_version is required",
            event_copy,
        )

    if (
        not isinstance(schema_version, str)
        or schema_version not in SUPPORTED_SCHEMA_VERSIONS
    ):
        return _invalid(
            ReasonCode.UNSUPPORTED_SCHEMA_VERSION,
            (
                f"schema_version={schema_version!r} "
                "is not supported"
            ),
            event_copy,
        )

    # ---------------------------------------------------------
    # Priority 300–330: event identity và event type
    # ---------------------------------------------------------

    event_id = event_copy.get("event_id")

    if _is_missing(event_id):
        return _invalid(
            ReasonCode.MISSING_EVENT_ID,
            "event_id is required",
            event_copy,
        )

    if not isinstance(event_id, str):
        return _invalid(
            ReasonCode.INVALID_EVENT_ID,
            (
                "event_id must be UUID string, got "
                f"{type(event_id).__name__}"
            ),
            event_copy,
        )

    try:
        UUID(event_id)
    except (ValueError, AttributeError):
        return _invalid(
            ReasonCode.INVALID_EVENT_ID,
            (
                f"event_id={event_id!r} "
                "is not a valid UUID"
            ),
            event_copy,
        )

    event_type = event_copy.get("event_type")

    if _is_missing(event_type):
        return _invalid(
            ReasonCode.MISSING_EVENT_TYPE,
            "event_type is required",
            event_copy,
        )

    if (
        not isinstance(event_type, str)
        or event_type not in SUPPORTED_EVENT_TYPES
    ):
        return _invalid(
            ReasonCode.INVALID_EVENT_TYPE,
            (
                f"event_type={event_type!r} "
                "is not supported"
            ),
            event_copy,
        )

    # ---------------------------------------------------------
    # Priority 340–380: timestamps
    # ---------------------------------------------------------

    event_time_raw = event_copy.get("event_time")

    if _is_missing(event_time_raw):
        return _invalid(
            ReasonCode.MISSING_EVENT_TIME,
            "event_time is required",
            event_copy,
        )

    event_time = _parse_aware_datetime(event_time_raw)

    if event_time is None:
        return _invalid(
            ReasonCode.INVALID_EVENT_TIME,
            (
                f"event_time={event_time_raw!r} "
                "must be ISO 8601 with timezone"
            ),
            event_copy,
        )

    producer_time_raw = event_copy.get("producer_time")

    if _is_missing(producer_time_raw):
        return _invalid(
            ReasonCode.MISSING_PRODUCER_TIME,
            "producer_time is required",
            event_copy,
        )

    producer_time = _parse_aware_datetime(
        producer_time_raw
    )

    if producer_time is None:
        return _invalid(
            ReasonCode.INVALID_PRODUCER_TIME,
            (
                f"producer_time={producer_time_raw!r} "
                "must be ISO 8601 with timezone"
            ),
            event_copy,
        )

    if producer_time < event_time:
        return _invalid(
            ReasonCode.PRODUCER_TIME_BEFORE_EVENT_TIME,
            (
                "producer_time must not be earlier "
                "than event_time"
            ),
            event_copy,
        )

    # ---------------------------------------------------------
    # Priority 400–410: common required fields
    # ---------------------------------------------------------

    for field_name in COMMON_REQUIRED_FIELDS:
        value = event_copy.get(field_name)

        if _is_missing(value):
            return _invalid(
                ReasonCode.MISSING_REQUIRED_FIELD,
                f"{field_name} is required",
                event_copy,
            )

        if not isinstance(value, str):
            return _invalid(
                ReasonCode.INVALID_REQUIRED_FIELD,
                (
                    f"{field_name} must be "
                    "non-empty string"
                ),
                event_copy,
            )

    # ---------------------------------------------------------
    # Priority 500–550: amount, currency, payment rules
    # ---------------------------------------------------------

    amount = event_copy.get("amount")
    currency = event_copy.get("currency")

    has_amount = not _is_missing(amount)
    has_currency = not _is_missing(currency)

    if has_amount != has_currency:
        return _invalid(
            ReasonCode.AMOUNT_CURRENCY_MISMATCH,
            (
                "amount and currency must both be "
                "present or both be null"
            ),
            event_copy,
        )

    if has_amount:
        parsed_amount = _parse_decimal(amount)

        if parsed_amount is None:
            return _invalid(
                ReasonCode.INVALID_AMOUNT,
                (
                    f"amount={amount!r} must be "
                    "a finite decimal string"
                ),
                event_copy,
            )

        if parsed_amount < 0:
            return _invalid(
                ReasonCode.NEGATIVE_AMOUNT,
                (
                    f"amount={amount!r} must be "
                    "greater than or equal to zero"
                ),
                event_copy,
            )

        if (
            not isinstance(currency, str)
            or currency not in SUPPORTED_CURRENCIES
        ):
            return _invalid(
                ReasonCode.INVALID_CURRENCY,
                (
                    f"currency={currency!r} "
                    "is not supported"
                ),
                event_copy,
            )

    if event_type in PAYMENT_EVENT_TYPES:
        payment_id = event_copy.get("payment_id")

        if _is_missing(payment_id):
            return _invalid(
                ReasonCode.MISSING_PAYMENT_ID,
                (
                    "payment_id is required for "
                    f"event_type={event_type}"
                ),
                event_copy,
            )

        if not isinstance(payment_id, str):
            return _invalid(
                ReasonCode.INVALID_PAYMENT_ID,
                "payment_id must be non-empty string",
                event_copy,
            )

        if not has_amount or not has_currency:
            return _invalid(
                ReasonCode.MISSING_PAYMENT_DETAILS,
                (
                    "amount and currency are required "
                    f"for event_type={event_type}"
                ),
                event_copy,
            )

    return _valid(event_copy)


def validate_bronze_record(
    bronze_record: Mapping[str, Any],
) -> ValidationResult:
    """Validate một Bronze envelope từ `value_base64`.

    `value_base64` là raw source of truth. Các field event
    đã extract trong Bronze không được dùng để thay thế
    việc parse và validate lại raw payload.
    """

    decoded = decode_raw_payload(
        bronze_record.get("value_base64")
    )

    if isinstance(decoded, ValidationResult):
        return decoded

    parsed = parse_json_object(decoded)

    if isinstance(parsed, ValidationResult):
        return parsed

    return validate_event(parsed)