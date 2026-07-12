"""Unit tests for the raw Kafka consumer helpers."""

from __future__ import annotations

from ingestion.kafka_consumer import (
    KafkaConsumerConfig,
    RawKafkaRecord,
    build_client_config,
    decode_for_display,
)


class FakeMessage:
    """Minimal test double for confluent_kafka.Message."""

    def key(self) -> bytes:
        return b"order-1001"

    def value(self) -> bytes:
        # Intentionally malformed JSON, but valid UTF-8 bytes.
        return b'{"event_id":"broken"'

    def topic(self) -> str:
        return "retail-payment-events"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 17

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_783_296_000_000)

    def headers(
        self,
    ) -> list[tuple[str, bytes]]:
        return [
            ("source", b"synthetic-producer"),
        ]


def test_client_config_disables_offset_acknowledgement() -> None:
    config = KafkaConsumerConfig()

    client_config = build_client_config(config)

    assert client_config["enable.auto.commit"] is False
    assert client_config["enable.auto.offset.store"] is False
    assert client_config["auto.offset.reset"] == "earliest"


def test_client_config_uses_expected_group_and_topic_connection() -> None:
    config = KafkaConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="bronze-ingestion-v1",
    )

    client_config = build_client_config(config)

    assert (
        client_config["bootstrap.servers"]
        == "localhost:9092"
    )
    assert (
        client_config["group.id"]
        == "bronze-ingestion-v1"
    )


def test_raw_record_preserves_malformed_payload_bytes() -> None:
    record = RawKafkaRecord.from_message(
        FakeMessage()  # type: ignore[arg-type]
    )

    assert record.key == b"order-1001"
    assert record.value == b'{"event_id":"broken"'
    assert record.topic == "retail-payment-events"
    assert record.partition == 2
    assert record.offset == 17


def test_console_output_does_not_parse_json() -> None:
    record = RawKafkaRecord.from_message(
        FakeMessage()  # type: ignore[arg-type]
    )

    output = record.to_console_payload()

    assert output["value"] == '{"event_id":"broken"'
    assert output["partition"] == 2
    assert output["offset"] == 17


def test_decode_none_preserves_null() -> None:
    assert decode_for_display(None) is None


def test_decode_invalid_utf8_is_safe_for_display() -> None:
    result = decode_for_display(b"\xff")

    assert result == "\ufffd"