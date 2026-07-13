"""Tests for Bronze record and ingestion metadata."""

from __future__ import annotations

import base64
from datetime import datetime, timezone

import pytest

from ingestion.bronze_metadata import (
    BronzeIngestionContext,
    build_bronze_envelope,
    extract_event_metadata,
    to_utc_iso_z,
)
from ingestion.kafka_consumer import RawKafkaRecord


def make_record(
    *,
    value: bytes | None,
    partition: int = 2,
    offset: int = 10,
) -> RawKafkaRecord:
    return RawKafkaRecord(
        key=b"order-1001",
        value=value,
        topic="retail-payment-events",
        partition=partition,
        offset=offset,
        kafka_timestamp_type=1,
        kafka_timestamp_ms=1_783_296_000_000,
        headers=(),
    )


def make_context() -> BronzeIngestionContext:
    return BronzeIngestionContext.create(
        ingestion_run_id="bronze-day04-test",
        ingestion_batch_number=3,
        now=datetime(
            2026,
            7,
            13,
            1,
            30,
            0,
            123456,
            tzinfo=timezone.utc,
        ),
    )


def test_context_builds_stable_batch_id() -> None:
    context = make_context()

    assert (
        context.ingestion_batch_id
        == "bronze-day04-test-batch-000003"
    )

    assert (
        context.ingestion_time_iso
        == "2026-07-13T01:30:00.123456Z"
    )


def test_naive_ingestion_time_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="timezone-aware",
    ):
        BronzeIngestionContext(
            ingestion_run_id="run-1",
            ingestion_batch_number=1,
            ingestion_time=datetime(
                2026,
                7,
                13,
                1,
                30,
            ),
        )


def test_valid_json_extracts_event_fields() -> None:
    raw = (
        b'{'
        b'"event_id":"event-1",'
        b'"event_type":"payment_authorized",'
        b'"event_time":"2026-07-06T00:00:00Z",'
        b'"producer_time":"2026-07-06T00:01:00Z",'
        b'"schema_version":"1.0"'
        b'}'
    )

    metadata = extract_event_metadata(raw)

    assert (
        metadata.payload_parse_status
        == "parsed_object"
    )
    assert metadata.event_id == "event-1"
    assert (
        metadata.event_type
        == "payment_authorized"
    )
    assert metadata.schema_version == "1.0"


def test_malformed_json_is_not_rejected() -> None:
    raw = b'{"event_id":"broken"'

    metadata = extract_event_metadata(raw)

    assert (
        metadata.payload_parse_status
        == "invalid_json"
    )
    assert metadata.event_id is None


def test_negative_amount_is_still_parsed_object() -> None:
    raw = (
        b'{'
        b'"event_id":"event-negative",'
        b'"event_type":"payment_authorized",'
        b'"amount":"-1000.00",'
        b'"schema_version":"1.0"'
        b'}'
    )

    metadata = extract_event_metadata(raw)

    assert (
        metadata.payload_parse_status
        == "parsed_object"
    )
    assert (
        metadata.event_id
        == "event-negative"
    )


def test_unsupported_schema_is_extracted_not_rejected() -> None:
    raw = (
        b'{'
        b'"event_id":"event-v99",'
        b'"event_type":"payment_failed",'
        b'"schema_version":"99.0"'
        b'}'
    )

    metadata = extract_event_metadata(raw)

    assert (
        metadata.payload_parse_status
        == "parsed_object"
    )
    assert metadata.schema_version == "99.0"


def test_json_array_is_not_object() -> None:
    metadata = extract_event_metadata(
        b'[1, 2, 3]'
    )

    assert (
        metadata.payload_parse_status
        == "json_not_object"
    )


def test_null_payload_is_preserved() -> None:
    metadata = extract_event_metadata(None)

    assert (
        metadata.payload_parse_status
        == "null_payload"
    )


def test_envelope_preserves_raw_bytes() -> None:
    raw = b'{"event_id":"broken"'

    record = make_record(
        value=raw,
        partition=2,
        offset=17,
    )

    envelope = build_bronze_envelope(
        record,
        context=make_context(),
    )

    restored = base64.b64decode(
        envelope["value_base64"]
    )

    assert restored == raw

    assert (
        envelope["source_record_id"]
        == "retail-payment-events:2:17"
    )

    assert (
        envelope["ingestion_run_id"]
        == "bronze-day04-test"
    )

    assert (
        envelope["payload_parse_status"]
        == "invalid_json"
    )


def test_to_utc_iso_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError):
        to_utc_iso_z(
            datetime(2026, 7, 13)
        )