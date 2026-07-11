"""Generate deterministic events and publish them to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence
from uuid import uuid4

from confluent_kafka import (
    KafkaError,
    KafkaException,
    Message,
    Producer,
)
from pydantic import ValidationError

from logging_config import (
    configure_logging,
    log_event,
)
from producer.bad_event_generator import (
    BadEventGenerator,
    EventScenario,
    GeneratedRecord,
)
from producer.config import ProducerConfig


LOGGER = logging.getLogger(
    "producer.event_producer"
)


DeliveryCallback = Callable[
    [
        KafkaError | None,
        Message,
    ],
    None,
]


@dataclass(slots=True)
class DeliveryTracker:
    """Theo dõi kết quả delivery cuối cùng từ Kafka."""

    run_id: str
    logger: logging.Logger
    success_count: int = 0
    failure_count: int = 0

    def callback_for(
        self,
        scenario: EventScenario,
    ) -> DeliveryCallback:
        """Tạo callback nhớ scenario của record."""

        def on_delivery(
            error: KafkaError | None,
            message: Message,
        ) -> None:
            if error is not None:
                self.failure_count += 1

                log_event(
                    self.logger,
                    logging.ERROR,
                    "kafka_delivery_failed",
                    run_id=self.run_id,
                    scenario=scenario.value,
                    error=str(error),
                )
                return

            self.success_count += 1

            log_event(
                self.logger,
                logging.DEBUG,
                "kafka_delivery_succeeded",
                run_id=self.run_id,
                scenario=scenario.value,
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

        return on_delivery


@dataclass(
    frozen=True,
    slots=True,
)
class PublishResult:
    """Tổng hợp kết quả publish vào Kafka."""

    queued_count: int
    delivery_success_count: int
    delivery_failure_count: int
    undelivered_after_flush: int

    @property
    def succeeded(self) -> bool:
        return (
            self.delivery_failure_count == 0
            and self.undelivered_after_flush == 0
            and self.delivery_success_count
            == self.queued_count
        )


def build_argument_parser() -> argparse.ArgumentParser:
    """Khai báo command-line arguments."""

    defaults = ProducerConfig()

    parser = argparse.ArgumentParser(
        description=(
            "Generate deterministic retail/payment "
            "events and publish them to Kafka."
        )
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=defaults.random_seed,
        help=(
            "Random seed used for deterministic "
            "generation."
        ),
    )

    parser.add_argument(
        "--data-volume",
        type=int,
        default=defaults.data_volume,
        help=(
            "Total number of records to generate."
        ),
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=(
            defaults.kafka_bootstrap_servers
        ),
        help="Kafka bootstrap servers.",
    )

    parser.add_argument(
        "--topic",
        default=defaults.kafka_topic,
        help="Kafka topic.",
    )

    parser.add_argument(
        "--client-id",
        default=defaults.client_id,
        help="Kafka producer client ID.",
    )

    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=defaults.duplicate_rate,
    )

    parser.add_argument(
        "--late-event-rate",
        type=float,
        default=defaults.late_event_rate,
    )

    parser.add_argument(
        "--malformed-rate",
        type=float,
        default=defaults.malformed_rate,
    )

    parser.add_argument(
        "--negative-amount-rate",
        type=float,
        default=defaults.negative_amount_rate,
    )

    parser.add_argument(
        "--unsupported-schema-version-rate",
        type=float,
        default=(
            defaults
            .unsupported_schema_version_rate
        ),
    )

    parser.add_argument(
        "--skew-mode",
        choices=(
            "none",
            "hot_order",
            "hot_store",
        ),
        default=defaults.skew_mode,
    )

    parser.add_argument(
        "--base-event-time",
        default=(
            defaults
            .base_event_time
            .isoformat()
        ),
        help=(
            "Timezone-aware fixed timestamp origin."
        ),
    )

    parser.add_argument(
        "--late-threshold-minutes",
        type=int,
        default=(
            defaults
            .late_threshold_minutes
        ),
    )

    parser.add_argument(
        "--flush-timeout-seconds",
        type=float,
        default=(
            defaults
            .flush_timeout_seconds
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not connect to Kafka; write "
            "deterministic JSONL instead."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output JSONL path for --dry-run. "
            "If omitted, JSONL is written "
            "to stdout."
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


def config_from_arguments(
    args: argparse.Namespace,
) -> ProducerConfig:
    """Chuyển argparse Namespace thành ProducerConfig."""

    return ProducerConfig(
        random_seed=args.seed,
        data_volume=args.data_volume,
        kafka_bootstrap_servers=(
            args.bootstrap_servers
        ),
        kafka_topic=args.topic,
        client_id=args.client_id,
        duplicate_rate=args.duplicate_rate,
        late_event_rate=args.late_event_rate,
        malformed_rate=args.malformed_rate,
        negative_amount_rate=(
            args.negative_amount_rate
        ),
        unsupported_schema_version_rate=(
            args
            .unsupported_schema_version_rate
        ),
        skew_mode=args.skew_mode,
        base_event_time=args.base_event_time,
        late_threshold_minutes=(
            args.late_threshold_minutes
        ),
        flush_timeout_seconds=(
            args.flush_timeout_seconds
        ),
    )


def scenario_counts(
    records: Sequence[GeneratedRecord],
) -> dict[str, int]:
    """Đếm record theo từng scenario."""

    counter = Counter(
        record.scenario.value
        for record in records
    )

    return {
        scenario.value: counter.get(
            scenario.value,
            0,
        )
        for scenario in EventScenario
    }


def record_to_json_line(
    sequence: int,
    record: GeneratedRecord,
) -> str:
    """Chuyển một GeneratedRecord thành JSONL ổn định.

    Giá trị Kafka được đặt bên trong field "value".
    Kể cả khi value là malformed JSON, dòng JSONL
    bên ngoài vẫn hợp lệ vì value là một JSON string.
    """

    payload = {
        "sequence": sequence,
        "scenario": (
            record.scenario.value
        ),
        "key": record.key.decode(
            "utf-8"
        ),
        "value": record.value.decode(
            "utf-8"
        ),
        "event_id": (
            str(record.event_id)
            if record.event_id is not None
            else None
        ),
        "source_event_id": (
            str(record.source_event_id)
            if (
                record.source_event_id
                is not None
            )
            else None
        ),
    }

    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def write_jsonl(
    records: Sequence[GeneratedRecord],
    output_path: Path | None,
) -> str:
    """Ghi dry-run dataset mà không thêm metadata ngẫu nhiên."""

    lines = [
        record_to_json_line(
            sequence,
            record,
        )
        for sequence, record
        in enumerate(records)
    ]

    if output_path is None:
        for line in lines:
            print(line)

        return "stdout"

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    return str(output_path)


def create_kafka_producer(
    config: ProducerConfig,
    *,
    logger: logging.Logger,
    run_id: str,
) -> Producer:
    """Tạo Kafka producer có transport idempotence."""

    def on_kafka_error(
        error: KafkaError,
    ) -> None:
        # Global error callback thường dùng để quan sát
        # tình trạng client. Delivery failure vẫn được
        # đếm riêng trong delivery callback.
        log_event(
            logger,
            logging.WARNING,
            "kafka_client_error",
            run_id=run_id,
            error=str(error),
        )

    producer_config = {
        "bootstrap.servers": (
            config.kafka_bootstrap_servers
        ),
        "client.id": config.client_id,

        # Cấu hình transport idempotence.
        "acks": "all",
        "enable.idempotence": True,
        (
            "max.in.flight.requests."
            "per.connection"
        ): 5,
        "retries": 10,

        # Khi broker không khả dụng, message không
        # nằm trong queue vô thời hạn.
        "message.timeout.ms": max(
            1000,
            int(
                config.flush_timeout_seconds
                * 1000
            ),
        ),

        "error_cb": on_kafka_error,
    }

    return Producer(
        producer_config,
        logger=logger,
    )


def publish_records(
    config: ProducerConfig,
    records: Sequence[GeneratedRecord],
    *,
    logger: logging.Logger,
    run_id: str,
) -> PublishResult:
    """Enqueue records, poll callbacks và flush queue."""

    producer = create_kafka_producer(
        config,
        logger=logger,
        run_id=run_id,
    )

    tracker = DeliveryTracker(
        run_id=run_id,
        logger=logger,
    )

    queued_count = 0

    for sequence, record in enumerate(
        records
    ):
        queue_retry_count = 0

        while True:
            try:
                producer.produce(
                    topic=config.kafka_topic,
                    key=record.key,
                    value=record.value,
                    on_delivery=(
                        tracker.callback_for(
                            record.scenario
                        )
                    ),
                )

                queued_count += 1
                break

            except BufferError:
                # Local producer queue đầy.
                # Poll để phục vụ callback và giải phóng queue.
                queue_retry_count += 1

                if queue_retry_count > 10:
                    raise RuntimeError(
                        "Kafka producer local queue "
                        "remained full after 10 "
                        "poll-and-retry attempts"
                    )

                log_event(
                    logger,
                    logging.WARNING,
                    "kafka_local_queue_full",
                    run_id=run_id,
                    sequence=sequence,
                    retry_count=(
                        queue_retry_count
                    ),
                )

                producer.poll(0.5)

            except KafkaException:
                log_event(
                    logger,
                    logging.ERROR,
                    "kafka_produce_call_failed",
                    run_id=run_id,
                    sequence=sequence,
                    scenario=(
                        record.scenario.value
                    ),
                )
                raise

        # Phục vụ callback của các record trước.
        # poll(0) không block.
        producer.poll(0)

    # flush() vừa chờ delivery, vừa phục vụ callback.
    # Return value là số message còn trong queue.
    undelivered = producer.flush(
        config.flush_timeout_seconds
    )

    return PublishResult(
        queued_count=queued_count,
        delivery_success_count=(
            tracker.success_count
        ),
        delivery_failure_count=(
            tracker.failure_count
        ),
        undelivered_after_flush=(
            undelivered
        ),
    )


def main(
    argv: Sequence[str] | None = None,
) -> int:
    """Entry point của producer CLI."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    configure_logging(
        args.log_level
    )

    # run_id phải khác ở mỗi execution.
    # Nó không nằm trong generated dataset nên
    # không ảnh hưởng fixed-seed reproducibility.
    run_id = f"producer-{uuid4()}"

    started = time.perf_counter()

    try:
        config = config_from_arguments(
            args
        )

        records = BadEventGenerator(
            config
        ).generate()

        counts = scenario_counts(
            records
        )

        log_event(
            LOGGER,
            logging.INFO,
            "producer_run_started",
            run_id=run_id,
            random_seed=(
                config.random_seed
            ),
            event_count=len(records),
            kafka_topic=(
                config.kafka_topic
            ),
            dry_run=args.dry_run,
            **counts,
        )

        if args.dry_run:
            destination = write_jsonl(
                records,
                args.output,
            )

            duration = (
                time.perf_counter()
                - started
            )

            log_event(
                LOGGER,
                logging.INFO,
                "producer_run_summary",
                run_id=run_id,
                random_seed=(
                    config.random_seed
                ),
                event_count=len(records),
                error_rate=(
                    config.error_rate
                ),
                output=destination,
                delivery_success_count=0,
                delivery_failure_count=0,
                duration_seconds=round(
                    duration,
                    6,
                ),
                status="dry_run_success",
                **counts,
            )

            return 0

        result = publish_records(
            config,
            records,
            logger=LOGGER,
            run_id=run_id,
        )

        duration = (
            time.perf_counter()
            - started
        )

        status = (
            "success"
            if result.succeeded
            else "failed"
        )

        log_level = (
            logging.INFO
            if result.succeeded
            else logging.ERROR
        )

        log_event(
            LOGGER,
            log_level,
            "producer_run_summary",
            run_id=run_id,
            random_seed=(
                config.random_seed
            ),
            event_count=len(records),
            error_rate=(
                config.error_rate
            ),
            kafka_topic=(
                config.kafka_topic
            ),
            queued_count=(
                result.queued_count
            ),
            delivery_success_count=(
                result
                .delivery_success_count
            ),
            delivery_failure_count=(
                result
                .delivery_failure_count
            ),
            undelivered_after_flush=(
                result
                .undelivered_after_flush
            ),
            duration_seconds=round(
                duration,
                6,
            ),
            status=status,
            **counts,
        )

        return (
            0
            if result.succeeded
            else 1
        )

    except (
        ValidationError,
        KafkaException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        duration = (
            time.perf_counter()
            - started
        )

        log_event(
            LOGGER,
            logging.ERROR,
            "producer_run_failed",
            run_id=run_id,
            error_type=(
                type(exc).__name__
            ),
            error=str(exc),
            duration_seconds=round(
                duration,
                6,
            ),
            status="failed",
        )

        return 1

    except KeyboardInterrupt:
        duration = (
            time.perf_counter()
            - started
        )

        log_event(
            LOGGER,
            logging.WARNING,
            "producer_run_interrupted",
            run_id=run_id,
            duration_seconds=round(
                duration,
                6,
            ),
            status="interrupted",
        )

        return 130


if __name__ == "__main__":
    raise SystemExit(
        main()
    )