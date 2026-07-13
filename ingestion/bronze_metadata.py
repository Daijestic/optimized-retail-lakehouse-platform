"""Build audit metadata for immutable Bronze Kafka records.

Bronze metadata is best-effort only:
- raw Kafka key/value bytes remain authoritative;
- malformed or invalid business payloads are never rejected here;
- business validation belongs to the Silver layer.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ingestion.kafka_consumer import RawKafkaRecord


BRONZE_RECORD_VERSION = "bronze-raw-v1"

PARSE_STATUS_PARSED_OBJECT = "parsed_object"
PARSE_STATUS_INVALID_JSON = "invalid_json"
PARSE_STATUS_INVALID_UTF8 = "invalid_utf8"
PARSE_STATUS_JSON_NOT_OBJECT = "json_not_object"
PARSE_STATUS_NULL_PAYLOAD = "null_payload"


def utc_now() -> datetime:
    """Return a timezone-aware UTC datetime."""

    return datetime.now(timezone.utc)


def to_utc_iso_z(value: datetime) -> str:
    """Serialize an aware datetime as ISO-8601 UTC ending in Z."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "datetime must be timezone-aware"
        )

    return (
        value
        .astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class BronzeIngestionContext:
    """Metadata shared by records in one ingestion batch."""

    ingestion_run_id: str
    ingestion_batch_number: int
    ingestion_time: datetime

    def __post_init__(self) -> None:
        if not self.ingestion_run_id.strip():
            raise ValueError(
                "ingestion_run_id must not be empty"
            )

        if self.ingestion_batch_number <= 0:
            raise ValueError(
                "ingestion_batch_number must be greater than zero"
            )

        if (
            self.ingestion_time.tzinfo is None
            or self.ingestion_time.utcoffset() is None
        ):
            raise ValueError(
                "ingestion_time must be timezone-aware"
            )

    @classmethod
    def create(
        cls,
        *,
        ingestion_run_id: str,
        ingestion_batch_number: int,
        now: datetime | None = None,
    ) -> "BronzeIngestionContext":
        """Create a context; injectable time keeps tests deterministic."""

        return cls(
            ingestion_run_id=ingestion_run_id,
            ingestion_batch_number=ingestion_batch_number,
            ingestion_time=now or utc_now(),
        )

    @property
    def ingestion_batch_id(self) -> str:
        """Return a stable batch label inside an ingestion run."""

        return (
            f"{self.ingestion_run_id}"
            f"-batch-{self.ingestion_batch_number:06d}"
        )

    @property
    def ingestion_time_iso(self) -> str:
        """Return ingestion time as canonical UTC text."""

        return to_utc_iso_z(self.ingestion_time)


@dataclass(frozen=True, slots=True)
class ExtractedEventMetadata:
    """Best-effort fields extracted from a raw Kafka payload."""

    payload_parse_status: str
    event_id: str | None = None
    event_type: str | None = None
    event_time: str | None = None
    producer_time: str | None = None
    schema_version: str | None = None


def encode_optional_bytes(
    value: bytes | None,
) -> str | None:
    """Encode bytes as Base64 without changing their content."""

    if value is None:
        return None

    return base64.b64encode(value).decode("ascii")


def _string_field(
    payload: dict[str, Any],
    field_name: str,
) -> str | None:
    """Return a string field without coercing invalid types."""

    value = payload.get(field_name)

    return value if isinstance(value, str) else None


def extract_event_metadata(
    raw_value: bytes | None,
) -> ExtractedEventMetadata:
    """Extract top-level fields without validating business rules."""

    if raw_value is None:
        return ExtractedEventMetadata(
            payload_parse_status=(
                PARSE_STATUS_NULL_PAYLOAD
            )
        )

    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError:
        return ExtractedEventMetadata(
            payload_parse_status=(
                PARSE_STATUS_INVALID_UTF8
            )
        )

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ExtractedEventMetadata(
            payload_parse_status=(
                PARSE_STATUS_INVALID_JSON
            )
        )

    if not isinstance(payload, dict):
        return ExtractedEventMetadata(
            payload_parse_status=(
                PARSE_STATUS_JSON_NOT_OBJECT
            )
        )

    return ExtractedEventMetadata(
        payload_parse_status=(
            PARSE_STATUS_PARSED_OBJECT
        ),
        event_id=_string_field(
            payload,
            "event_id",
        ),
        event_type=_string_field(
            payload,
            "event_type",
        ),
        event_time=_string_field(
            payload,
            "event_time",
        ),
        producer_time=_string_field(
            payload,
            "producer_time",
        ),
        schema_version=_string_field(
            payload,
            "schema_version",
        ),
    )


def build_source_record_id(
    record: RawKafkaRecord,
) -> str:
    """Build a unique technical identity for one Kafka record."""

    return (
        f"{record.topic}:"
        f"{record.partition}:"
        f"{record.offset}"
    )


def build_bronze_envelope(
    record: RawKafkaRecord,
    *,
    context: BronzeIngestionContext,
) -> dict[str, Any]:
    """Build one JSON-safe Bronze row."""

    extracted = extract_event_metadata(
        record.value
    )

    return {
        "record_version": BRONZE_RECORD_VERSION,

        "ingestion_run_id": (
            context.ingestion_run_id
        ),
        "ingestion_batch_number": (
            context.ingestion_batch_number
        ),
        "ingestion_batch_id": (
            context.ingestion_batch_id
        ),
        "ingestion_time": (
            context.ingestion_time_iso
        ),

        "source_record_id": (
            build_source_record_id(record)
        ),
        "source_topic": record.topic,
        "source_partition": record.partition,
        "source_offset": record.offset,

        "kafka_timestamp_type": (
            record.kafka_timestamp_type
        ),
        "kafka_timestamp_ms": (
            record.kafka_timestamp_ms
        ),

        "key_base64": encode_optional_bytes(
            record.key
        ),
        "value_base64": encode_optional_bytes(
            record.value
        ),

        "headers": [
            {
                "name": name,
                "value_base64": (
                    encode_optional_bytes(value)
                ),
            }
            for name, value in record.headers
        ],

        "payload_parse_status": (
            extracted.payload_parse_status
        ),
        "event_id": extracted.event_id,
        "event_type": extracted.event_type,
        "event_time": extracted.event_time,
        "producer_time": extracted.producer_time,
        "schema_version": (
            extracted.schema_version
        ),
    }