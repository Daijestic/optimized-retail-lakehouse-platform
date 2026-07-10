from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    model_validator,
)


class EventType(StrEnum):
    """Các loại event được hỗ trợ trong schema version 1.0."""

    ORDER_CREATED = "order_created"
    ORDER_CONFIRMED = "order_confirmed"
    PAYMENT_AUTHORIZED = "payment_authorized"
    PAYMENT_FAILED = "payment_failed"
    ORDER_SHIPPED = "order_shipped"
    ORDER_DELIVERED = "order_delivered"
    REFUND_REQUESTED = "refund_requested"


class Currency(StrEnum):
    """Các đơn vị tiền tệ được MVP hỗ trợ."""

    VND = "VND"
    USD = "USD"


PAYMENT_EVENT_TYPES = {
    EventType.PAYMENT_AUTHORIZED,
    EventType.PAYMENT_FAILED,
    EventType.REFUND_REQUESTED,
}


class RetailPaymentEvent(BaseModel):
    """Schema version 1.0 cho retail/payment event hợp lệ."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    event_id: UUID
    event_type: EventType

    order_id: str = Field(min_length=1, max_length=64)
    payment_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
    )
    customer_id: str = Field(min_length=1, max_length=64)
    store_id: str = Field(min_length=1, max_length=64)

    amount: Decimal | None = Field(
        default=None,
        ge=Decimal("0"),
        max_digits=18,
        decimal_places=2,
    )
    currency: Currency | None = None

    event_time: AwareDatetime
    producer_time: AwareDatetime

    schema_version: Literal["1.0"] = "1.0"

    idempotency_key: str = Field(min_length=1, max_length=128)

    source: Literal["synthetic-retail-producer"] = (
        "synthetic-retail-producer"
    )

    @model_validator(mode="after")
    def validate_cross_field_rules(self) -> Self:
        """Kiểm tra các quy tắc phụ thuộc nhiều field."""

        if self.producer_time < self.event_time:
            raise ValueError(
                "producer_time must be greater than or equal to event_time"
            )

        amount_is_missing = self.amount is None
        currency_is_missing = self.currency is None

        if amount_is_missing != currency_is_missing:
            raise ValueError(
                "amount and currency must either both be provided "
                "or both be null"
            )

        if self.event_type in PAYMENT_EVENT_TYPES:
            missing_fields: list[str] = []

            if self.payment_id is None:
                missing_fields.append("payment_id")

            if self.amount is None:
                missing_fields.append("amount")

            if self.currency is None:
                missing_fields.append("currency")

            if missing_fields:
                missing = ", ".join(missing_fields)
                raise ValueError(
                    f"{self.event_type.value} requires: {missing}"
                )

        return self

    @field_serializer("amount")
    def serialize_amount(
        self,
        value: Decimal | None,
    ) -> str | None:
        """Giữ precision của amount khi serialize sang JSON."""

        if value is None:
            return None

        return format(value, "f")

    def kafka_key(self) -> bytes:
        """Kafka key dùng để giữ thứ tự theo order."""

        return self.order_id.encode("utf-8")

    def kafka_value(self) -> bytes:
        """Kafka value dạng UTF-8 JSON."""

        return self.model_dump_json().encode("utf-8")