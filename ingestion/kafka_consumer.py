"""Consume raw retail/payment events from Apache Kafka.

Day 2 scope:
- subscribe to the Kafka topic;
- poll raw messages;
- preserve key/value bytes and Kafka source metadata;
- print records for local verification;
- do not store or commit offsets yet.

Offset storage and commits will be connected to the durable Bronze
write in Day 3.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Sequence
from uuid import uuid4

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from logging_config import configure_logging, log_event


LOGGER = logging.getLogger("ingestion.kafka_consumer")


@dataclass(frozen=True, slots=True)
class KafkaConsumerConfig:
    """Application-level configuration for the raw Kafka consumer."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "retail-payment-events"
    group_id: str = "bronze-ingestion-v1"
    client_id: str = "bronze-consumer-local"

    poll_timeout_seconds: float = 1.0
    idle_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        """Fail early when command-line configuration is invalid."""

        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")

        if not self.topic.strip():
            raise ValueError("topic must not be empty")

        if not self.group_id.strip():
            raise ValueError("group_id must not be empty")

        if not self.client_id.strip():
            raise ValueError("client_id must not be empty")

        if self.poll_timeout_seconds <= 0:
            raise ValueError(
                "poll_timeout_seconds must be greater than zero"
            )

        if self.idle_timeout_seconds < 0:
            raise ValueError(
                "idle_timeout_seconds must be zero or greater"
            )


@dataclass(frozen=True, slots=True)
class RawKafkaRecord:
    """Raw Kafka record plus source coordinates.

    Key and value intentionally remain bytes. Bronze must not depend
    on valid JSON parsing.
    """

    key: bytes | None
    value: bytes | None

    topic: str
    partition: int
    offset: int

    kafka_timestamp_type: int
    kafka_timestamp_ms: int | None

    headers: tuple[tuple[str, bytes | None], ...]

    @classmethod
    def from_message(
        cls,
        message: Message,
    ) -> "RawKafkaRecord":
        """Build a raw record without parsing the message value."""

        timestamp_type, timestamp_ms = message.timestamp()

        normalized_timestamp_ms = (
            timestamp_ms
            if timestamp_ms is not None and timestamp_ms >= 0
            else None
        )

        return cls(
            key=message.key(),
            value=message.value(),
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            kafka_timestamp_type=timestamp_type,
            kafka_timestamp_ms=normalized_timestamp_ms,
            headers=tuple(message.headers() or ()),
        )

    def to_console_payload(self) -> dict[str, Any]:
        """Return a JSON-safe representation for local verification.

        This decoding is only for display. The original bytes remain
        available in self.key and self.value.
        """

        return {
            "key": decode_for_display(self.key),
            "value": decode_for_display(self.value),
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "kafka_timestamp_type": self.kafka_timestamp_type,
            "kafka_timestamp_ms": self.kafka_timestamp_ms,
            "headers": [
                {
                    "name": name,
                    "value": decode_for_display(value),
                }
                for name, value in self.headers
            ],
            "key_size_bytes": (
                len(self.key)
                if self.key is not None
                else 0
            ),
            "value_size_bytes": (
                len(self.value)
                if self.value is not None
                else 0
            ),
        }


def decode_for_display(
    value: bytes | None,
) -> str | None:
    """Decode bytes for terminal output without affecting raw storage."""

    if value is None:
        return None

    return value.decode(
        "utf-8",
        errors="replace",
    )


def build_client_config(
    config: KafkaConsumerConfig,
    *,
    error_callback: Any | None = None,
) -> dict[str, Any]:
    """Translate application config to confluent-kafka config."""

    client_config: dict[str, Any] = {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": config.group_id,
        "client.id": config.client_id,

        # A new group should read retained history.
        "auto.offset.reset": "earliest",

        # Day 2 must not acknowledge messages.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }

    if error_callback is not None:
        client_config["error_cb"] = error_callback

    return client_config


def partition_details(
    partitions: list[TopicPartition],
) -> list[dict[str, int | str]]:
    """Convert TopicPartition objects into structured log fields."""

    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": partition.offset,
        }
        for partition in partitions
    ]


class RawKafkaConsumer:
    """Small wrapper around confluent-kafka Consumer."""

    def __init__(
        self,
        config: KafkaConsumerConfig,
        *,
        run_id: str,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.run_id = run_id
        self.logger = logger
        self._closed = False

        def on_client_error(
            error: KafkaError,
        ) -> None:
            log_event(
                self.logger,
                logging.WARNING,
                "kafka_consumer_client_error",
                run_id=self.run_id,
                error=str(error),
                retriable=error.retriable(),
                fatal=error.fatal(),
            )

        self._consumer = Consumer(
            build_client_config(
                config,
                error_callback=on_client_error,
            )
        )

    def subscribe(self) -> None:
        """Join the consumer group and subscribe to the topic."""

        def on_assign(
            consumer: Consumer,
            partitions: list[TopicPartition],
        ) -> None:
            # No call to assign() is needed here.
            # The client performs the normal assignment automatically.
            log_event(
                self.logger,
                logging.INFO,
                "kafka_partitions_assigned",
                run_id=self.run_id,
                group_id=self.config.group_id,
                partitions=partition_details(partitions),
            )

        def on_revoke(
            consumer: Consumer,
            partitions: list[TopicPartition],
        ) -> None:
            log_event(
                self.logger,
                logging.INFO,
                "kafka_partitions_revoked",
                run_id=self.run_id,
                group_id=self.config.group_id,
                partitions=partition_details(partitions),
            )

        def on_lost(
            consumer: Consumer,
            partitions: list[TopicPartition],
        ) -> None:
            log_event(
                self.logger,
                logging.WARNING,
                "kafka_partitions_lost",
                run_id=self.run_id,
                group_id=self.config.group_id,
                partitions=partition_details(partitions),
            )

        self._consumer.subscribe(
            [self.config.topic],
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
        )

    def poll_record(
        self,
    ) -> tuple[Message, RawKafkaRecord] | None:
        """Poll one raw Kafka message.

        The original Message is returned together with RawKafkaRecord.
        Day 3 will need the original Message when offsets are stored
        and committed after a successful Bronze write.
        """

        message = self._consumer.poll(
            self.config.poll_timeout_seconds
        )

        if message is None:
            return None

        error = message.error()

        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                log_event(
                    self.logger,
                    logging.DEBUG,
                    "kafka_partition_eof",
                    run_id=self.run_id,
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                return None

            raise KafkaException(error)

        return (
            message,
            RawKafkaRecord.from_message(message),
        )

    def close(self) -> None:
        """Leave the consumer group and release client resources."""

        if self._closed:
            return

        self._consumer.close()
        self._closed = True

        log_event(
            self.logger,
            logging.INFO,
            "kafka_consumer_closed",
            run_id=self.run_id,
            group_id=self.config.group_id,
        )


def consume_to_console(
    config: KafkaConsumerConfig,
    *,
    max_messages: int | None,
    run_id: str,
) -> int:
    """Consume raw records and print an outer JSON object per record."""

    consumer = RawKafkaConsumer(
        config,
        run_id=run_id,
        logger=LOGGER,
    )

    consumed_count = 0
    started = time.perf_counter()
    last_message_at = time.monotonic()

    log_event(
        LOGGER,
        logging.INFO,
        "kafka_consumer_run_started",
        run_id=run_id,
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
        group_id=config.group_id,
        client_id=config.client_id,
        max_messages=max_messages,
        auto_commit=False,
        auto_offset_store=False,
    )

    try:
        consumer.subscribe()

        while (
            max_messages is None
            or consumed_count < max_messages
        ):
            result = consumer.poll_record()

            if result is None:
                idle_timeout = config.idle_timeout_seconds

                if (
                    idle_timeout > 0
                    and time.monotonic() - last_message_at
                    >= idle_timeout
                ):
                    log_event(
                        LOGGER,
                        logging.INFO,
                        "kafka_consumer_idle_timeout",
                        run_id=run_id,
                        idle_timeout_seconds=idle_timeout,
                    )
                    break

                continue

            _, record = result

            # stdout is used as verification output.
            # Structured logs continue to go to stderr.
            print(
                json.dumps(
                    record.to_console_payload(),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )

            consumed_count += 1
            last_message_at = time.monotonic()

            log_event(
                LOGGER,
                logging.INFO,
                "kafka_message_received",
                run_id=run_id,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key_size_bytes=(
                    len(record.key)
                    if record.key is not None
                    else 0
                ),
                value_size_bytes=(
                    len(record.value)
                    if record.value is not None
                    else 0
                ),
            )

    finally:
        consumer.close()

    duration = time.perf_counter() - started

    log_event(
        LOGGER,
        logging.INFO,
        "kafka_consumer_run_summary",
        run_id=run_id,
        consumed_count=consumed_count,
        committed_count=0,
        duration_seconds=round(duration, 6),
        status="success",
    )

    return consumed_count


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the command-line interface."""

    defaults = KafkaConsumerConfig()

    parser = argparse.ArgumentParser(
        description=(
            "Read raw retail/payment events from Kafka "
            "without committing offsets."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=defaults.bootstrap_servers,
    )
    parser.add_argument(
        "--topic",
        default=defaults.topic,
    )
    parser.add_argument(
        "--group-id",
        default=defaults.group_id,
    )
    parser.add_argument(
        "--client-id",
        default=defaults.client_id,
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=float,
        default=defaults.poll_timeout_seconds,
    )
    parser.add_argument(
        "--idle-timeout-seconds",
        type=float,
        default=defaults.idle_timeout_seconds,
        help=(
            "Stop after this many seconds without a message. "
            "Use 0 to disable the idle timeout."
        ),
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=20,
        help=(
            "Maximum records to print. "
            "Use 0 to keep consuming until Ctrl+C."
        ),
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

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    """CLI entry point."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if args.max_messages < 0:
        parser.error("--max-messages must be zero or greater")

    configure_logging(args.log_level)

    run_id = f"consumer-{uuid4()}"

    try:
        config = KafkaConsumerConfig(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            client_id=args.client_id,
            poll_timeout_seconds=args.poll_timeout_seconds,
            idle_timeout_seconds=args.idle_timeout_seconds,
        )

        max_messages = (
            None
            if args.max_messages == 0
            else args.max_messages
        )

        consume_to_console(
            config,
            max_messages=max_messages,
            run_id=run_id,
        )

        return 0

    except KeyboardInterrupt:
        log_event(
            LOGGER,
            logging.WARNING,
            "kafka_consumer_interrupted",
            run_id=run_id,
            status="interrupted",
        )
        return 130

    except (
        KafkaException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        log_event(
            LOGGER,
            logging.ERROR,
            "kafka_consumer_run_failed",
            run_id=run_id,
            error_type=type(exc).__name__,
            error=str(exc),
            status="failed",
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())