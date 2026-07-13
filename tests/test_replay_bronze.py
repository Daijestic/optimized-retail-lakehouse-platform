"""Tests for processing-date Bronze replay."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from scripts.replay_bronze import (
    BronzeObjectReference,
    list_replay_objects,
    parse_bronze_object_key,
    replay_processing_date,
    validate_object_ranges,
)


def make_row(
    *,
    processing_date: str,
    partition: int,
    offset: int,
) -> bytes:
    raw_value = (
        b'{"event_id":"broken"'
    )

    row = {
        "record_version": "bronze-raw-v1",
        "processing_date": processing_date,
        "source_topic": (
            "retail-payment-events"
        ),
        "source_partition": partition,
        "source_offset": offset,
        "source_record_id": (
            "retail-payment-events:"
            f"{partition}:{offset}"
        ),
        "key_base64": base64.b64encode(
            b"order-1001"
        ).decode("ascii"),
        "value_base64": base64.b64encode(
            raw_value
        ).decode("ascii"),
        "payload_parse_status": (
            "invalid_json"
        ),
    }

    return json.dumps(
        row,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class FakeBody:
    def __init__(self, value: bytes) -> None:
        self.value = value
        self.closed = False

    def read(self) -> bytes:
        return self.value

    def close(self) -> None:
        self.closed = True


class FakePaginator:
    def __init__(
        self,
        objects: dict[str, dict],
    ) -> None:
        self.objects = objects

    def paginate(
        self,
        *,
        Bucket: str,
        Prefix: str,
    ):
        contents = [
            {
                "Key": key,
                "Size": len(
                    value["Body"]
                ),
            }
            for key, value
            in self.objects.items()
            if key.startswith(Prefix)
        ]

        yield {
            "Contents": contents,
        }


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[
            str,
            dict,
        ] = {}

    def add_object(
        self,
        *,
        key: str,
        body: bytes,
        processing_date: str,
        partition: int,
        start_offset: int,
        end_offset: int,
    ) -> None:
        self.objects[key] = {
            "Body": body,
            "Metadata": {
                "processing-date": (
                    processing_date
                ),
                "source-topic": (
                    "retail-payment-events"
                ),
                "source-partition": str(
                    partition
                ),
                "start-offset": str(
                    start_offset
                ),
                "end-offset": str(
                    end_offset
                ),
                "record-version": (
                    "bronze-raw-v1"
                ),
            },
        }

    def get_paginator(
        self,
        name: str,
    ) -> FakePaginator:
        assert name == "list_objects_v2"

        return FakePaginator(
            self.objects
        )

    def head_object(
        self,
        *,
        Bucket: str,
        Key: str,
    ) -> dict:
        obj = self.objects[Key]

        return {
            "ContentLength": len(
                obj["Body"]
            ),
            "Metadata": obj[
                "Metadata"
            ],
        }

    def get_object(
        self,
        *,
        Bucket: str,
        Key: str,
    ) -> dict:
        return {
            "Body": FakeBody(
                self.objects[Key]["Body"]
            )
        }


def build_key(
    *,
    partition: int,
    start: int,
    end: int,
) -> str:
    return (
        "bronze/events/"
        "processing_date=2026-07-13/"
        "topic=retail-payment-events/"
        f"partition={partition:05d}/"
        f"offsets={start:020d}-"
        f"{end:020d}.jsonl"
    )


def test_object_key_is_parsed() -> None:
    reference = parse_bronze_object_key(
        bucket="lakehouse",
        key=build_key(
            partition=2,
            start=10,
            end=29,
        ),
        size_bytes=100,
        root_prefix="bronze/events",
    )

    assert reference.processing_date == (
        "2026-07-13"
    )
    assert reference.partition == 2
    assert reference.start_offset == 10
    assert reference.end_offset == 29


def test_overlap_is_rejected() -> None:
    references = [
        BronzeObjectReference(
            bucket="lakehouse",
            key="first",
            processing_date="2026-07-13",
            topic="retail-payment-events",
            partition=0,
            start_offset=0,
            end_offset=99,
            size_bytes=1,
        ),
        BronzeObjectReference(
            bucket="lakehouse",
            key="second",
            processing_date="2026-07-13",
            topic="retail-payment-events",
            partition=0,
            start_offset=80,
            end_offset=179,
            size_bytes=1,
        ),
    ]

    with pytest.raises(
        RuntimeError,
        match="Overlapping",
    ):
        validate_object_ranges(
            references,
            allow_gaps=False,
        )


def test_gap_is_rejected_by_default() -> None:
    references = [
        BronzeObjectReference(
            bucket="lakehouse",
            key="first",
            processing_date="2026-07-13",
            topic="retail-payment-events",
            partition=0,
            start_offset=0,
            end_offset=79,
            size_bytes=1,
        ),
        BronzeObjectReference(
            bucket="lakehouse",
            key="second",
            processing_date="2026-07-13",
            topic="retail-payment-events",
            partition=0,
            start_offset=90,
            end_offset=99,
            size_bytes=1,
        ),
    ]

    with pytest.raises(
        RuntimeError,
        match="Gap detected",
    ):
        validate_object_ranges(
            references,
            allow_gaps=False,
        )


def test_replay_preserves_rows(
    tmp_path: Path,
) -> None:
    s3 = FakeS3Client()

    first_body = b"\n".join(
        [
            make_row(
                processing_date=(
                    "2026-07-13"
                ),
                partition=0,
                offset=0,
            ),
            make_row(
                processing_date=(
                    "2026-07-13"
                ),
                partition=0,
                offset=1,
            ),
        ]
    ) + b"\n"

    second_body = b"\n".join(
        [
            make_row(
                processing_date=(
                    "2026-07-13"
                ),
                partition=0,
                offset=2,
            ),
            make_row(
                processing_date=(
                    "2026-07-13"
                ),
                partition=0,
                offset=3,
            ),
        ]
    ) + b"\n"

    s3.add_object(
        key=build_key(
            partition=0,
            start=0,
            end=1,
        ),
        body=first_body,
        processing_date="2026-07-13",
        partition=0,
        start_offset=0,
        end_offset=1,
    )

    s3.add_object(
        key=build_key(
            partition=0,
            start=2,
            end=3,
        ),
        body=second_body,
        processing_date="2026-07-13",
        partition=0,
        start_offset=2,
        end_offset=3,
    )

    output_path = (
        tmp_path / "replay.jsonl"
    )

    result = replay_processing_date(
        s3=s3,  # type: ignore[arg-type]
        bucket="lakehouse",
        root_prefix="bronze/events",
        processing_date="2026-07-13",
        output_path=output_path,
        replay_run_id="replay-test",
        overwrite=False,
        allow_gaps=False,
    )

    output = output_path.read_bytes()

    assert output == first_body + second_body
    assert result.object_count == 2
    assert result.record_count == 4
    assert result.status == "success"
    assert Path(
        result.manifest_path
    ).exists()


def test_no_objects_is_an_error() -> None:
    s3 = FakeS3Client()

    with pytest.raises(
        FileNotFoundError,
    ):
        list_replay_objects(
            s3=s3,  # type: ignore[arg-type]
            bucket="lakehouse",
            root_prefix="bronze/events",
            processing_date="2026-07-13",
        )