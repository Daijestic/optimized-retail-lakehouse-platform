"""Tests for processing-date Bronze partitioning."""

from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

from ingestion.bronze_metadata import (
    BronzeIngestionContext,
    build_bronze_envelope,
)
from ingestion.bronze_writer import (
    build_object_key,
    validate_processing_date,
)
from ingestion.kafka_consumer import (
    RawKafkaRecord,
)


def make_record() -> RawKafkaRecord:
    return RawKafkaRecord(
        key=b"order-1001",
        value=(
            b'{'
            b'"event_id":"event-1",'
            b'"event_type":"payment_authorized",'
            b'"schema_version":"1.0"'
            b'}'
        ),
        topic="retail-payment-events",
        partition=2,
        offset=320,
        kafka_timestamp_type=1,
        kafka_timestamp_ms=1_783_296_000_000,
        headers=(),
    )


def test_processing_date_is_derived_from_utc() -> None:
    vietnam_time = datetime(
        2026,
        7,
        13,
        0,
        30,
        tzinfo=timezone(
            timedelta(hours=7)
        ),
    )

    context = BronzeIngestionContext.create(
        ingestion_run_id="run-1",
        ingestion_batch_number=1,
        now=vietnam_time,
    )

    # 00:30 GMT+7 is still 17:30 of the previous UTC day.
    assert (
        context.processing_date
        == "2026-07-12"
    )


def test_envelope_contains_processing_date() -> None:
    context = BronzeIngestionContext.create(
        ingestion_run_id="run-1",
        ingestion_batch_number=1,
        now=datetime(
            2026,
            7,
            13,
            10,
            0,
            tzinfo=timezone.utc,
        ),
    )

    envelope = build_bronze_envelope(
        make_record(),
        context=context,
    )

    assert (
        envelope["processing_date"]
        == "2026-07-13"
    )


def test_object_key_contains_processing_date() -> None:
    key = build_object_key(
        prefix="bronze/events",
        processing_date="2026-07-13",
        topic="retail-payment-events",
        partition=2,
        start_offset=320,
        end_offset=351,
    )

    assert (
        "processing_date=2026-07-13/"
        in key
    )


def test_same_input_produces_same_object_key() -> None:
    arguments = {
        "prefix": "bronze/events",
        "processing_date": "2026-07-13",
        "topic": "retail-payment-events",
        "partition": 2,
        "start_offset": 320,
        "end_offset": 351,
    }

    assert (
        build_object_key(**arguments)
        == build_object_key(**arguments)
    )


def test_different_processing_dates_use_different_keys() -> None:
    first = build_object_key(
        prefix="bronze/events",
        processing_date="2026-07-13",
        topic="retail-payment-events",
        partition=2,
        start_offset=320,
        end_offset=351,
    )

    second = build_object_key(
        prefix="bronze/events",
        processing_date="2026-07-14",
        topic="retail-payment-events",
        partition=2,
        start_offset=320,
        end_offset=351,
    )

    assert first != second


@pytest.mark.parametrize(
    "value",
    [
        "",
        "2026/07/13",
        "13-07-2026",
        "2026-7-13",
        "2026-02-30",
    ],
)
def test_invalid_processing_date_is_rejected(
    value: str,
) -> None:
    with pytest.raises(ValueError):
        validate_processing_date(value)