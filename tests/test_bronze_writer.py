"""Unit tests for deterministic Bronze object writing."""

from __future__ import annotations

import base64

from ingestion.bronze_writer import (
    BronzeWriter,
    BronzeWriterConfig,
    build_commit_offsets,
    build_object_key,
    raw_record_to_dict,
    serialize_jsonl,
)
from ingestion.kafka_consumer import RawKafkaRecord


def make_record(
    *,
    partition: int = 0,
    offset: int = 0,
    value: bytes = b'{"event_id":"broken"',
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


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[
            tuple[str, str],
            dict[str, object],
        ] = {}

    def head_bucket(self, *, Bucket: str) -> dict:
        return {}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentLength: int,
        ContentType: str,
        Metadata: dict[str, str],
    ) -> dict:
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentLength": ContentLength,
            "ContentType": ContentType,
            "Metadata": Metadata,
        }
        return {"ETag": '"fake-etag"'}

    def head_object(
        self,
        *,
        Bucket: str,
        Key: str,
    ) -> dict:
        obj = self.objects[(Bucket, Key)]

        return {
            "ContentLength": obj["ContentLength"],
            "ContentType": obj["ContentType"],
            "Metadata": obj["Metadata"],
        }


def test_raw_record_keeps_malformed_payload_exactly() -> None:
    original = b'{"event_id":"broken"'
    record = make_record(value=original)

    output = raw_record_to_dict(record)

    restored = base64.b64decode(
        output["value_base64"]
    )

    assert restored == original


def test_object_key_is_deterministic() -> None:
    first = build_object_key(
        prefix="bronze/events/_unpartitioned",
        topic="retail-payment-events",
        partition=1,
        start_offset=10,
        end_offset=29,
    )

    second = build_object_key(
        prefix="bronze/events/_unpartitioned",
        topic="retail-payment-events",
        partition=1,
        start_offset=10,
        end_offset=29,
    )

    assert first == second

    assert first.endswith(
        "partition=00001/"
        "offsets=00000000000000000010-"
        "00000000000000000029.jsonl"
    )


def test_jsonl_has_one_line_per_record() -> None:
    body = serialize_jsonl(
        [
            make_record(offset=0),
            make_record(offset=1),
            make_record(offset=2),
        ]
    )

    lines = body.decode("utf-8").splitlines()

    assert len(lines) == 3


def test_commit_offsets_use_max_offset_plus_one() -> None:
    offsets = build_commit_offsets(
        [
            make_record(partition=0, offset=3),
            make_record(partition=0, offset=4),
            make_record(partition=1, offset=8),
        ]
    )

    values = {
        (item.topic, item.partition): item.offset
        for item in offsets
    }

    assert values[
        ("retail-payment-events", 0)
    ] == 5

    assert values[
        ("retail-payment-events", 1)
    ] == 9


def test_writer_creates_one_object_per_partition() -> None:
    fake_s3 = FakeS3Client()

    config = BronzeWriterConfig(
        endpoint_url="http://localhost:9000",
        access_key="test",
        secret_key="test-secret",
        bucket="lakehouse",
    )

    writer = BronzeWriter(
        config,
        s3_client=fake_s3,  # type: ignore[arg-type]
    )

    results = writer.write_batch(
        [
            make_record(partition=0, offset=0),
            make_record(partition=0, offset=1),
            make_record(partition=1, offset=0),
        ]
    )

    assert len(results) == 2
    assert len(fake_s3.objects) == 2

    record_counts = sorted(
        result.record_count
        for result in results
    )

    assert record_counts == [1, 2]


def test_rewriting_same_range_uses_same_object_key() -> None:
    fake_s3 = FakeS3Client()

    config = BronzeWriterConfig(
        endpoint_url="http://localhost:9000",
        access_key="test",
        secret_key="test-secret",
        bucket="lakehouse",
    )

    writer = BronzeWriter(
        config,
        s3_client=fake_s3,  # type: ignore[arg-type]
    )

    records = [
        make_record(partition=0, offset=10),
        make_record(partition=0, offset=11),
    ]

    first = writer.write_batch(records)
    second = writer.write_batch(records)

    assert first[0].object_key == second[0].object_key
    assert len(fake_s3.objects) == 1