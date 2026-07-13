"""Write raw Kafka records to MinIO Bronze storage.

Day 3 responsibilities:
- preserve Kafka key/value bytes;
- encode raw bytes as Base64 inside JSON Lines;
- write one object per Kafka partition in each micro-batch;
- verify object size and SHA-256 metadata;
- return the Kafka offsets that are safe to commit.

Full Bronze ingestion metadata and processing-date partitioning
are added in later days.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Sequence
from uuid import uuid4
import re
from datetime import date

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import KafkaException, TopicPartition

from ingestion.bronze_metadata import (
    BronzeIngestionContext,
    build_bronze_envelope,
)

from ingestion.kafka_consumer import (
    KafkaConsumerConfig,
    RawKafkaConsumer,
    RawKafkaRecord,
)
from logging_config import configure_logging, log_event


LOGGER = logging.getLogger("ingestion.bronze_writer")


@dataclass(frozen=True, slots=True)
class BronzeWriterConfig:
    """Configuration for MinIO/S3 Bronze writes."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str = "lakehouse"
    prefix: str = "bronze/events"
    region_name: str = "us-east-1"

    def __post_init__(self) -> None:
        if not self.endpoint_url.strip():
            raise ValueError("endpoint_url must not be empty")

        if not self.access_key.strip():
            raise ValueError("access_key must not be empty")

        if not self.secret_key.strip():
            raise ValueError("secret_key must not be empty")

        if not self.bucket.strip():
            raise ValueError("bucket must not be empty")

        if not self.prefix.strip():
            raise ValueError("prefix must not be empty")

    @classmethod
    def from_environment(cls) -> "BronzeWriterConfig":
        """Read MinIO configuration from environment variables."""

        endpoint_url = os.getenv(
            "MINIO_ENDPOINT_URL",
            "http://localhost:9000",
        )
        access_key = os.getenv("MINIO_ROOT_USER", "")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "")
        bucket = os.getenv("MINIO_BUCKET", "lakehouse")
        prefix = os.getenv(
            "BRONZE_PREFIX",
            "bronze/events",
        )
        return cls(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
            prefix=prefix,
        )


@dataclass(frozen=True, slots=True)
class BronzeObjectWriteResult:
    """Evidence returned after one Bronze object is verified."""

    bucket: str
    object_key: str
    topic: str
    partition: int
    start_offset: int
    end_offset: int
    record_count: int
    size_bytes: int
    sha256: str

    ingestion_run_id: str
    ingestion_batch_id: str
    ingestion_time: str
    processing_date: str

def serialize_jsonl(
    records: Sequence[RawKafkaRecord],
    *,
    context: BronzeIngestionContext,
) -> bytes:
    """Serialize Bronze envelopes as UTF-8 JSON Lines."""

    if not records:
        raise ValueError(
            "records must not be empty"
        )

    lines = [
        json.dumps(
            build_bronze_envelope(
                record,
                context=context,
            ),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        for record in records
    ]

    return b"\n".join(lines) + b"\n"

PROCESSING_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}$"
)


def validate_processing_date(
    value: str,
) -> str:
    """Validate canonical YYYY-MM-DD processing date."""

    if not PROCESSING_DATE_PATTERN.fullmatch(
        value
    ):
        raise ValueError(
            "processing_date must use YYYY-MM-DD"
        )

    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "processing_date must be a valid calendar date"
        ) from exc

    return value

def build_object_key(
    *,
    prefix: str,
    processing_date: str,
    topic: str,
    partition: int,
    start_offset: int,
    end_offset: int,
) -> str:
    """Build a deterministic processing-date Bronze object key."""

    clean_prefix = prefix.strip("/")
    valid_date = validate_processing_date(
        processing_date
    )

    if not topic.strip():
        raise ValueError(
            "topic must not be empty"
        )

    if partition < 0:
        raise ValueError(
            "partition must be zero or greater"
        )

    if start_offset < 0:
        raise ValueError(
            "start_offset must be zero or greater"
        )

    if end_offset < start_offset:
        raise ValueError(
            "end_offset must be >= start_offset"
        )

    return (
        f"{clean_prefix}/"
        f"processing_date={valid_date}/"
        f"topic={topic}/"
        f"partition={partition:05d}/"
        f"offsets={start_offset:020d}-"
        f"{end_offset:020d}.jsonl"
    )


def group_records_by_partition(
    records: Iterable[RawKafkaRecord],
) -> dict[tuple[str, int], list[RawKafkaRecord]]:
    """Group records while preserving offset order per partition."""

    grouped: dict[
        tuple[str, int],
        list[RawKafkaRecord],
    ] = defaultdict(list)

    for record in records:
        grouped[(record.topic, record.partition)].append(record)

    for partition_records in grouped.values():
        partition_records.sort(key=lambda item: item.offset)

    return dict(grouped)


def build_commit_offsets(
    records: Iterable[RawKafkaRecord],
) -> list[TopicPartition]:
    """Return next offsets that are safe to commit per partition."""

    highest_offsets: dict[tuple[str, int], int] = {}

    for record in records:
        key = (record.topic, record.partition)
        current = highest_offsets.get(key)

        if current is None or record.offset > current:
            highest_offsets[key] = record.offset

    return [
        TopicPartition(
            topic,
            partition,
            highest_offset + 1,
        )
        for (topic, partition), highest_offset
        in sorted(highest_offsets.items())
    ]


def create_s3_client(
    config: BronzeWriterConfig,
) -> BaseClient:
    """Create a Boto3 S3 client for local MinIO."""

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region_name,
        config=Config(
            signature_version="s3v4",
            s3={
                "addressing_style": "path",
            },
            retries={
                "max_attempts": 3,
                "mode": "standard",
            },
        ),
    )


class BronzeWriter:
    """Write and verify partition-scoped Bronze objects."""

    def __init__(
        self,
        config: BronzeWriterConfig,
        *,
        s3_client: BaseClient | None = None,
    ) -> None:
        self.config = config
        self.s3 = s3_client or create_s3_client(config)

    def verify_bucket(self) -> None:
        """Fail early when the configured bucket is unavailable."""

        self.s3.head_bucket(
            Bucket=self.config.bucket,
        )

    def write_batch(
        self,
        records: Sequence[RawKafkaRecord],
        *,
        context: BronzeIngestionContext,
    ) -> list[BronzeObjectWriteResult]:
        """Write one object per topic-partition."""

        if not records:
            raise ValueError(
                "records must not be empty"
            )

        grouped = group_records_by_partition(
            records
        )

        results: list[
            BronzeObjectWriteResult
        ] = []

        for (
            topic,
            partition,
        ), partition_records in grouped.items():
            result = self._write_partition_batch(
                topic=topic,
                partition=partition,
                records=partition_records,
                context=context,
            )
            results.append(result)

        return results

    def _write_partition_batch(
        self,
        *,
        topic: str,
        partition: int,
        records: Sequence[RawKafkaRecord],
        context: BronzeIngestionContext,
    ) -> BronzeObjectWriteResult:
        """Write and verify one partition batch."""

        if not records:
            raise ValueError("records must not be empty")

        start_offset = records[0].offset
        end_offset = records[-1].offset

        object_key = build_object_key(
            prefix=self.config.prefix,
            processing_date=(
                context.processing_date
            ),
            topic=topic,
            partition=partition,
            start_offset=start_offset,
            end_offset=end_offset,
        )

        body = serialize_jsonl(
            records,
            context=context,
        )
        digest = hashlib.sha256(body).hexdigest()

        self.s3.put_object(
            Bucket=self.config.bucket,
            Key=object_key,
            Body=body,
            ContentLength=len(body),
            ContentType="application/x-ndjson",
            Metadata={
                "sha256": digest,
                "record-count": str(
                    len(records)
                ),
                "source-topic": topic,
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
                "processing-date": (
                    context.processing_date
                ),
                "ingestion-run-id": (
                    context.ingestion_run_id
                ),
                "ingestion-batch-id": (
                    context.ingestion_batch_id
                ),
                "ingestion-time": (
                    context.ingestion_time_iso
                ),
            },
        )

        head = self.s3.head_object(
            Bucket=self.config.bucket,
            Key=object_key,
        )

        actual_size = int(
            head["ContentLength"]
        )

        stored_metadata = head.get(
            "Metadata",
            {},
        )

        actual_digest = stored_metadata.get(
            "sha256"
        )

        actual_processing_date = stored_metadata.get(
            "processing-date"
        )

        actual_ingestion_run_id = stored_metadata.get(
            "ingestion-run-id"
        )

        actual_ingestion_batch_id = stored_metadata.get(
            "ingestion-batch-id"
        )

        actual_ingestion_time = stored_metadata.get(
            "ingestion-time"
        )

        if actual_size != len(body):
            raise RuntimeError(
                "Bronze object size verification failed: "
                f"expected={len(body)}, "
                f"actual={actual_size}, "
                f"key={object_key}"
            )

        if actual_digest != digest:
            raise RuntimeError(
                "Bronze object SHA-256 verification failed: "
                f"expected={digest}, "
                f"actual={actual_digest}, "
                f"key={object_key}"
            )

        if (
            actual_processing_date
            != context.processing_date
        ):
            raise RuntimeError(
                "Bronze processing-date metadata "
                "verification failed: "
                f"expected={context.processing_date}, "
                f"actual={actual_processing_date}, "
                f"key={object_key}"
            )

        if (
            actual_ingestion_run_id
            != context.ingestion_run_id
        ):
            raise RuntimeError(
                "Bronze ingestion-run-id metadata "
                "verification failed: "
                f"expected={context.ingestion_run_id}, "
                f"actual={actual_ingestion_run_id}, "
                f"key={object_key}"
            )

        if (
            actual_ingestion_batch_id
            != context.ingestion_batch_id
        ):
            raise RuntimeError(
                "Bronze ingestion-batch-id metadata "
                "verification failed: "
                f"expected={context.ingestion_batch_id}, "
                f"actual={actual_ingestion_batch_id}, "
                f"key={object_key}"
            )

        if (
            actual_ingestion_time
            != context.ingestion_time_iso
        ):
            raise RuntimeError(
                "Bronze ingestion-time metadata "
                "verification failed: "
                f"expected={context.ingestion_time_iso}, "
                f"actual={actual_ingestion_time}, "
                f"key={object_key}"
            )

        return BronzeObjectWriteResult(
            bucket=self.config.bucket,
            object_key=object_key,
            processing_date=(
                context.processing_date
            ),
            topic=topic,
            partition=partition,
            start_offset=start_offset,
            end_offset=end_offset,
            record_count=len(records),
            size_bytes=len(body),
            sha256=digest,

            ingestion_run_id=(
                context.ingestion_run_id
            ),
            ingestion_batch_id=(
                context.ingestion_batch_id
            ),
            ingestion_time=(
                context.ingestion_time_iso
            ),
        )
def consume_and_write_bronze(
    *,
    consumer_config: KafkaConsumerConfig,
    writer_config: BronzeWriterConfig,
    batch_size: int,
    batch_wait_seconds: float,
    max_messages: int | None,
    idle_timeout_seconds: float,
    run_id: str,
) -> int:
    """Consume Kafka records, write Bronze, then commit offsets."""

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    if batch_wait_seconds <= 0:
        raise ValueError(
            "batch_wait_seconds must be greater than zero"
        )

    consumer = RawKafkaConsumer(
        consumer_config,
        run_id=run_id,
        logger=LOGGER,
    )

    writer = BronzeWriter(writer_config)

    buffer: list[RawKafkaRecord] = []
    consumed_count = 0
    written_count = 0
    object_count = 0
    batch_number = 0

    started_at = time.perf_counter()
    batch_started_at = time.monotonic()
    last_message_at = time.monotonic()

    def flush_buffer() -> None:
        nonlocal buffer
        nonlocal written_count
        nonlocal object_count
        nonlocal batch_number
        nonlocal batch_started_at

        if not buffer:
            return

        batch_number += 1

        context = BronzeIngestionContext.create(
            ingestion_run_id=run_id,
            ingestion_batch_number=batch_number,
        )

        # 1. Write every partition object.
        results = writer.write_batch(
            buffer,
            context=context,
        )

        # 2. Only after all objects are verified, calculate
        #    the next offsets and commit them to Kafka.
        commit_offsets = build_commit_offsets(buffer)
        consumer.commit_offsets(commit_offsets)

        written_count += len(buffer)
        object_count += len(results)

        log_event(
            LOGGER,
            logging.INFO,
            "bronze_batch_committed",
            run_id=run_id,
            batch_number=batch_number,
            record_count=len(buffer),
            object_count=len(results),
            ingestion_run_id=context.ingestion_run_id,
            ingestion_batch_id=context.ingestion_batch_id,
            ingestion_time=context.ingestion_time_iso,
            processing_date=context.processing_date,
            objects=[
                {
                    "bucket": result.bucket,
                    "object_key": result.object_key,
                    "topic": result.topic,
                    "partition": result.partition,
                    "start_offset": result.start_offset,
                    "end_offset": result.end_offset,
                    "record_count": result.record_count,
                    "size_bytes": result.size_bytes,
                    "sha256": result.sha256,
                    "processing_date": (
                        result.processing_date
                    ),
                    "ingestion_run_id": (
                        result.ingestion_run_id
                    ),
                    "ingestion_batch_id": (
                        result.ingestion_batch_id
                    ),
                    "ingestion_time": (
                        result.ingestion_time
                    ),
                }
                for result in results
            ],
            committed_offsets=[
                {
                    "topic": item.topic,
                    "partition": item.partition,
                    "offset": item.offset,
                }
                for item in commit_offsets
            ],
        )

        buffer = []
        batch_started_at = time.monotonic()

    log_event(
        LOGGER,
        logging.INFO,
        "bronze_ingestion_started",
        run_id=run_id,
        kafka_topic=consumer_config.topic,
        kafka_group_id=consumer_config.group_id,
        bucket=writer_config.bucket,
        prefix=writer_config.prefix,
        batch_size=batch_size,
        batch_wait_seconds=batch_wait_seconds,
        max_messages=max_messages,
    )

    try:
        writer.verify_bucket()
        consumer.subscribe()

        while (
            max_messages is None
            or consumed_count < max_messages
        ):
            result = consumer.poll_record()

            now = time.monotonic()

            if result is None:
                if (
                    buffer
                    and now - batch_started_at
                    >= batch_wait_seconds
                ):
                    flush_buffer()

                if (
                    idle_timeout_seconds > 0
                    and now - last_message_at
                    >= idle_timeout_seconds
                ):
                    break

                continue

            _, raw_record = result

            buffer.append(raw_record)
            consumed_count += 1
            last_message_at = now

            if len(buffer) >= batch_size:
                flush_buffer()

        # Commit the remaining partial batch.
        flush_buffer()

    finally:
        consumer.close()

    duration = time.perf_counter() - started_at

    log_event(
        LOGGER,
        logging.INFO,
        "bronze_ingestion_summary",
        run_id=run_id,
        consumed_count=consumed_count,
        written_count=written_count,
        object_count=object_count,
        batch_count=batch_number,
        duration_seconds=round(duration, 6),
        status="success",
    )

    return written_count

def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consume raw Kafka records and write verified "
            "Bronze objects to MinIO."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )
    parser.add_argument(
        "--topic",
        default="retail-payment-events",
    )
    parser.add_argument(
        "--group-id",
        default="bronze-ingestion-v1",
    )
    parser.add_argument(
        "--client-id",
        default="bronze-writer-local",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--batch-wait-seconds",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=100,
        help="Use 0 for continuous ingestion.",
    )
    parser.add_argument(
        "--idle-timeout-seconds",
        type=float,
        default=15.0,
        help="Use 0 to disable idle shutdown.",
    )
    parser.add_argument(
        "--log-level",
        choices=(
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
        ),
        default="INFO",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Optional ingestion run ID. "
            "A UUID-based ID is generated when omitted."
        ),
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")

    if args.max_messages < 0:
        parser.error("--max-messages must be zero or greater")

    configure_logging(args.log_level)

    run_id = (
        args.run_id.strip()
        if args.run_id
        else f"bronze-{uuid4()}"
    )

    if not run_id:
        parser.error(
            "--run-id must not be empty"
        )

    try:
        consumer_config = KafkaConsumerConfig(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            client_id=args.client_id,
            poll_timeout_seconds=1.0,
            idle_timeout_seconds=args.idle_timeout_seconds,
        )

        writer_config = (
            BronzeWriterConfig.from_environment()
        )

        max_messages = (
            None
            if args.max_messages == 0
            else args.max_messages
        )

        consume_and_write_bronze(
            consumer_config=consumer_config,
            writer_config=writer_config,
            batch_size=args.batch_size,
            batch_wait_seconds=args.batch_wait_seconds,
            max_messages=max_messages,
            idle_timeout_seconds=args.idle_timeout_seconds,
            run_id=run_id,
        )

        return 0

    except KeyboardInterrupt:
        log_event(
            LOGGER,
            logging.WARNING,
            "bronze_ingestion_interrupted",
            run_id=run_id,
            status="interrupted",
        )
        return 130

    except (
        BotoCoreError,
        ClientError,
        KafkaException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        log_event(
            LOGGER,
            logging.ERROR,
            "bronze_ingestion_failed",
            run_id=run_id,
            error_type=type(exc).__name__,
            error=str(exc),
            status="failed",
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())