"""Tests for Bronze-to-Silver parse and split transformation."""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession

from processing.silver_transform import (
    BRONZE_SCHEMA,
    read_bronze_jsonl,
    transform_bronze_dataframe,
)


os.environ.setdefault(
    "SPARK_LOCAL_IP",
    "127.0.0.1",
)
os.environ.setdefault(
    "SPARK_LOCAL_HOSTNAME",
    "localhost",
)
os.environ.setdefault(
    "PYSPARK_PYTHON",
    sys.executable,
)
os.environ.setdefault(
    "PYSPARK_DRIVER_PYTHON",
    sys.executable,
)


VALID_EVENT_ID = (
    "11111111-1111-4111-8111-111111111111"
)

LATE_EVENT_ID = (
    "44444444-4444-4444-8444-444444444444"
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Create one lightweight local SparkSession."""

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-transform-tests")
        .config(
            "spark.ui.enabled",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "1",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def make_event(
    **overrides: Any,
) -> dict[str, Any]:
    """Build one valid event before applying overrides."""

    event: dict[str, Any] = {
        "event_id": VALID_EVENT_ID,
        "event_type": "payment_authorized",
        "order_id": "order-000001",
        "payment_id": "payment-000001",
        "customer_id": "customer-000001",
        "store_id": "store-001",
        "amount": "150000.00",
        "currency": "VND",
        "event_time": "2026-07-20T08:00:00Z",
        "producer_time": "2026-07-20T08:01:00Z",
        "schema_version": "1.0",
        "idempotency_key": f"idem-{VALID_EVENT_ID}",
        "source": "synthetic-retail-producer",
    }

    event.update(overrides)

    return event


def encode_event(
    event: dict[str, Any],
) -> bytes:
    """Serialize one event into deterministic UTF-8 JSON."""

    return json.dumps(
        event,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def make_bronze_row(
    *,
    offset: int,
    raw_value: bytes | None = None,
    value_base64: str | None = None,
    payload_parse_status: str = "parsed_object",
    best_effort_event_id: str | None = None,
    best_effort_event_type: str | None = None,
) -> dict[str, Any]:
    """Build one Bronze envelope test row."""

    if (
        raw_value is not None
        and value_base64 is not None
    ):
        raise ValueError(
            "Provide raw_value or value_base64, not both"
        )

    if raw_value is not None:
        encoded_value = base64.b64encode(
            raw_value
        ).decode("ascii")
    else:
        encoded_value = value_base64

    topic = "retail-payment-events"
    partition = offset % 3

    return {
        "record_version": "bronze-raw-v1",
        "ingestion_run_id": "silver-day02-test",
        "ingestion_batch_number": 1,
        "ingestion_batch_id": (
            "silver-day02-test-batch-000001"
        ),
        "ingestion_time": (
            "2026-07-20T10:00:00.000000Z"
        ),
        "processing_date": "2026-07-20",
        "source_record_id": (
            f"{topic}:{partition}:{offset}"
        ),
        "source_topic": topic,
        "source_partition": partition,
        "source_offset": offset,
        "kafka_timestamp_type": 1,
        "kafka_timestamp_ms": (
            1_784_544_400_000 + offset
        ),
        "key_base64": base64.b64encode(
            f"order-{offset}".encode("utf-8")
        ).decode("ascii"),
        "value_base64": encoded_value,
        "headers": [],
        "payload_parse_status": (
            payload_parse_status
        ),
        # Các field này cố ý chỉ là best-effort.
        "event_id": best_effort_event_id,
        "event_type": best_effort_event_type,
        "event_time": None,
        "producer_time": None,
        "schema_version": None,
    }


def build_bronze_rows() -> list[dict[str, Any]]:
    """Build eight records covering Day 2 behavior."""

    valid_event = make_event()
    valid_payload = encode_event(valid_event)

    unsupported_event = make_event(
        event_id=(
            "22222222-2222-4222-8222-222222222222"
        ),
        idempotency_key=(
            "idem-22222222-2222-4222-"
            "8222-222222222222"
        ),
        schema_version="99.0",
    )

    negative_event = make_event(
        event_id=(
            "33333333-3333-4333-8333-333333333333"
        ),
        idempotency_key=(
            "idem-33333333-3333-4333-"
            "8333-333333333333"
        ),
        amount="-1000.00",
    )

    missing_event_id = make_event()
    missing_event_id.pop("event_id")

    late_event = make_event(
        event_id=LATE_EVENT_ID,
        order_id="order-late-000001",
        payment_id="payment-late-000001",
        idempotency_key=f"idem-{LATE_EVENT_ID}",
        event_time="2026-07-20T08:00:00Z",
        producer_time="2026-07-20T09:00:00Z",
    )

    return [
        # 0: valid
        make_bronze_row(
            offset=0,
            raw_value=valid_payload,
            best_effort_event_id=VALID_EVENT_ID,
            best_effort_event_type=(
                "payment_authorized"
            ),
        ),
        # 1: malformed JSON
        make_bronze_row(
            offset=1,
            raw_value=b'{"event_id":',
            payload_parse_status="invalid_json",
        ),
        # 2: unsupported schema
        make_bronze_row(
            offset=2,
            raw_value=encode_event(
                unsupported_event
            ),
        ),
        # 3: negative amount
        make_bronze_row(
            offset=3,
            raw_value=encode_event(
                negative_event
            ),
        ),
        # 4: missing event_id
        make_bronze_row(
            offset=4,
            raw_value=encode_event(
                missing_event_id
            ),
        ),
        # 5: duplicate payload of offset 0.
        # Day 2 must not remove it.
        make_bronze_row(
            offset=5,
            raw_value=valid_payload,
            best_effort_event_id=VALID_EVENT_ID,
            best_effort_event_type=(
                "payment_authorized"
            ),
        ),
        # 6: late but contract-valid.
        # Day 2 must keep it as a valid candidate.
        make_bronze_row(
            offset=6,
            raw_value=encode_event(late_event),
            best_effort_event_id=LATE_EVENT_ID,
            best_effort_event_type=(
                "payment_authorized"
            ),
        ),
        # 7: invalid Base64
        make_bronze_row(
            offset=7,
            value_base64="%%%",
            payload_parse_status="invalid_json",
        ),
    ]


@pytest.fixture
def bronze_df(
    spark: SparkSession,
) -> DataFrame:
    return spark.createDataFrame(
        build_bronze_rows(),
        schema=BRONZE_SCHEMA,
    )


def normalized_rows(
    dataframe: DataFrame,
) -> list[dict[str, Any]]:
    """Collect rows in deterministic source order."""

    return [
        row.asDict(recursive=True)
        for row in (
            dataframe
            .orderBy("source_record_id")
            .collect()
        )
    ]


def test_split_valid_and_invalid_records(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    input_count = bronze_df.count()
    valid_count = result.valid_df.count()
    invalid_count = result.invalid_df.count()

    assert input_count == 8
    assert valid_count == 3
    assert invalid_count == 5

    assert (
        input_count
        == valid_count + invalid_count
    )


def test_valid_events_are_parsed_and_flattened(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    rows = (
        result.valid_df
        .select(
            "source_offset",
            "event_id",
            "event_type",
            "amount",
            "currency",
        )
        .orderBy("source_offset")
        .collect()
    )

    assert [
        row["source_offset"]
        for row in rows
    ] == [0, 5, 6]

    assert rows[0]["event_id"] == VALID_EVENT_ID
    assert rows[0]["event_type"] == (
        "payment_authorized"
    )
    assert rows[0]["amount"] == "150000.00"
    assert rows[0]["currency"] == "VND"


def test_duplicate_is_not_removed_on_day02(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    duplicate_count = (
        result.valid_df
        .filter(
            result.valid_df.event_id
            == VALID_EVENT_ID
        )
        .count()
    )

    assert duplicate_count == 2


def test_late_event_remains_a_valid_candidate(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    late_count = (
        result.valid_df
        .filter(
            result.valid_df.event_id
            == LATE_EVENT_ID
        )
        .count()
    )

    assert late_count == 1


def test_invalid_reason_codes_are_correct(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    actual = {
        row["source_offset"]: row["reason_code"]
        for row in (
            result.invalid_df
            .select(
                "source_offset",
                "reason_code",
            )
            .collect()
        )
    }

    assert actual == {
        1: "MALFORMED_JSON",
        2: "UNSUPPORTED_SCHEMA_VERSION",
        3: "NEGATIVE_AMOUNT",
        4: "MISSING_EVENT_ID",
        7: "RAW_PAYLOAD_DECODE_ERROR",
    }


def test_invalid_records_keep_source_metadata(
    bronze_df: DataFrame,
) -> None:
    result = transform_bronze_dataframe(
        bronze_df
    )

    row = (
        result.invalid_df
        .filter(
            result.invalid_df.source_offset == 3
        )
        .select(
            "source_record_id",
            "source_topic",
            "source_partition",
            "source_offset",
            "value_base64",
            "reason_code",
            "reason_detail",
        )
        .first()
    )

    assert row is not None
    assert row["source_record_id"] == (
        "retail-payment-events:0:3"
    )
    assert row["source_topic"] == (
        "retail-payment-events"
    )
    assert row["source_partition"] == 0
    assert row["source_offset"] == 3
    assert row["value_base64"]
    assert row["reason_code"] == "NEGATIVE_AMOUNT"
    assert row["reason_detail"]


def test_value_base64_is_the_source_of_truth(
    spark: SparkSession,
) -> None:
    raw_event_id = (
        "55555555-5555-4555-8555-555555555555"
    )

    wrong_bronze_event_id = (
        "99999999-9999-4999-8999-999999999999"
    )

    event = make_event(
        event_id=raw_event_id,
        idempotency_key=f"idem-{raw_event_id}",
    )

    row = make_bronze_row(
        offset=100,
        raw_value=encode_event(event),
        best_effort_event_id=(
            wrong_bronze_event_id
        ),
        best_effort_event_type="wrong_event_type",
    )

    dataframe = spark.createDataFrame(
        [row],
        schema=BRONZE_SCHEMA,
    )

    result = transform_bronze_dataframe(
        dataframe
    )

    output = (
        result.valid_df
        .select(
            "event_id",
            "event_type",
        )
        .first()
    )

    assert output is not None
    assert output["event_id"] == raw_event_id
    assert output["event_type"] == (
        "payment_authorized"
    )


def test_transformation_is_deterministic(
    bronze_df: DataFrame,
) -> None:
    first = transform_bronze_dataframe(
        bronze_df
    )

    second = transform_bronze_dataframe(
        bronze_df
    )

    assert normalized_rows(
        first.valid_df
    ) == normalized_rows(
        second.valid_df
    )

    assert normalized_rows(
        first.invalid_df
    ) == normalized_rows(
        second.invalid_df
    )


def test_transformation_does_not_mutate_input(
    bronze_df: DataFrame,
) -> None:
    columns_before = list(bronze_df.columns)
    schema_before = bronze_df.schema.json()

    transform_bronze_dataframe(bronze_df)

    assert bronze_df.columns == columns_before
    assert bronze_df.schema.json() == schema_before


def test_missing_bronze_column_fails_fast(
    bronze_df: DataFrame,
) -> None:
    invalid_dataframe = bronze_df.drop(
        "value_base64"
    )

    with pytest.raises(
        ValueError,
        match="value_base64",
    ):
        transform_bronze_dataframe(
            invalid_dataframe
        )


def test_read_bronze_jsonl_with_explicit_schema(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "bronze.jsonl"

    row = build_bronze_rows()[0]

    input_file.write_text(
        json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    dataframe = read_bronze_jsonl(
        spark,
        input_file.as_uri(),
    )

    assert dataframe.count() == 1
    assert dataframe.schema == BRONZE_SCHEMA

    output = dataframe.select(
        "source_record_id",
        "source_offset",
    ).first()

    assert output is not None
    assert output["source_record_id"] == (
        "retail-payment-events:0:0"
    )
    assert output["source_offset"] == 0