"""Tests for deterministic controlled event generation."""

from __future__ import annotations

import json
from collections import Counter
from datetime import timedelta

import pytest
from pydantic import ValidationError

from producer.bad_event_generator import (
    BadEventGenerator,
    EventScenario,
    calculate_scenario_counts,
)
from producer.config import ProducerConfig
from producer.schemas import (
    RetailPaymentEvent,
)


def test_default_scenario_counts_are_exact() -> None:
    config = ProducerConfig()

    assert calculate_scenario_counts(
        config
    ) == {
        EventScenario.VALID: 65,
        EventScenario.DUPLICATE: 10,
        EventScenario.LATE: 10,
        EventScenario.MALFORMED: 5,
        EventScenario.NEGATIVE_AMOUNT: 5,
        (
            EventScenario
            .UNSUPPORTED_SCHEMA_VERSION
        ): 5,
    }


def test_total_generated_records_equals_data_volume() -> None:
    config = ProducerConfig(
        data_volume=137
    )

    records = BadEventGenerator(
        config
    ).generate()

    assert len(records) == 137


def test_same_seed_and_config_generate_identical_output() -> None:
    config = ProducerConfig(
        random_seed=42
    )

    generator = BadEventGenerator(
        config
    )

    first = generator.generate()
    second = generator.generate()

    assert first == second


def test_different_seeds_generate_different_output() -> None:
    first = BadEventGenerator(
        ProducerConfig(
            random_seed=42
        )
    ).generate()

    second = BadEventGenerator(
        ProducerConfig(
            random_seed=43
        )
    ).generate()

    assert first != second


def test_actual_scenario_counts_match_plan() -> None:
    config = ProducerConfig()

    records = BadEventGenerator(
        config
    ).generate()

    actual = Counter(
        record.scenario
        for record in records
    )

    expected = Counter(
        calculate_scenario_counts(
            config
        )
    )

    assert actual == expected


def test_valid_records_pass_schema_validation() -> None:
    records = BadEventGenerator(
        ProducerConfig()
    ).generate()

    valid_records = [
        record
        for record in records
        if (
            record.scenario
            is EventScenario.VALID
        )
    ]

    assert valid_records

    for record in valid_records:
        event = (
            RetailPaymentEvent
            .model_validate_json(
                record.value
            )
        )

        assert (
            record.key
            == event.kafka_key()
        )

        assert (
            record.event_id
            == event.event_id
        )


def test_duplicate_appears_after_identical_valid_original() -> None:
    records = BadEventGenerator(
        ProducerConfig()
    ).generate()

    duplicate_records = [
        (
            index,
            record,
        )
        for index, record
        in enumerate(records)
        if (
            record.scenario
            is EventScenario.DUPLICATE
        )
    ]

    assert duplicate_records

    for (
        duplicate_index,
        duplicate,
    ) in duplicate_records:
        matching_originals = [
            record
            for record
            in records[:duplicate_index]
            if (
                record.scenario
                is EventScenario.VALID
                and (
                    record.event_id
                    == duplicate.event_id
                )
                and (
                    record.key
                    == duplicate.key
                )
                and (
                    record.value
                    == duplicate.value
                )
            )
        ]

        assert len(
            matching_originals
        ) == 1

        original = (
            matching_originals[0]
        )

        assert (
            duplicate.source_event_id
            == original.event_id
        )


def test_late_event_passes_schema_and_exceeds_threshold() -> None:
    config = ProducerConfig(
        late_threshold_minutes=30
    )

    records = BadEventGenerator(
        config
    ).generate()

    late_record = next(
        record
        for record in records
        if (
            record.scenario
            is EventScenario.LATE
        )
    )

    event = (
        RetailPaymentEvent
        .model_validate_json(
            late_record.value
        )
    )

    delay = (
        event.producer_time
        - event.event_time
    )

    assert delay > timedelta(
        minutes=(
            config
            .late_threshold_minutes
        )
    )


def test_malformed_event_cannot_be_parsed_as_json() -> None:
    records = BadEventGenerator(
        ProducerConfig()
    ).generate()

    malformed_record = next(
        record
        for record in records
        if (
            record.scenario
            is EventScenario.MALFORMED
        )
    )

    with pytest.raises(
        json.JSONDecodeError
    ):
        json.loads(
            malformed_record.value
        )


def test_negative_amount_is_json_but_fails_schema() -> None:
    records = BadEventGenerator(
        ProducerConfig()
    ).generate()

    record = next(
        item
        for item in records
        if (
            item.scenario
            is EventScenario
            .NEGATIVE_AMOUNT
        )
    )

    payload = json.loads(
        record.value
    )

    assert (
        payload["amount"]
        == "-1000.00"
    )

    with pytest.raises(
        ValidationError
    ):
        (
            RetailPaymentEvent
            .model_validate_json(
                record.value
            )
        )


def test_unsupported_version_is_json_but_fails_schema() -> None:
    records = BadEventGenerator(
        ProducerConfig()
    ).generate()

    record = next(
        item
        for item in records
        if (
            item.scenario
            is EventScenario
            .UNSUPPORTED_SCHEMA_VERSION
        )
    )

    payload = json.loads(
        record.value
    )

    assert (
        payload["schema_version"]
        == "99.0"
    )

    with pytest.raises(
        ValidationError
    ):
        (
            RetailPaymentEvent
            .model_validate_json(
                record.value
            )
        )


def test_total_rate_greater_than_one_is_rejected() -> None:
    with pytest.raises(
        ValidationError,
        match="sum of scenario rates",
    ):
        ProducerConfig(
            duplicate_rate=0.40,
            late_event_rate=0.40,
            malformed_rate=0.40,
            negative_amount_rate=0.0,
            unsupported_schema_version_rate=0.0,
        )


def test_duplicate_requires_valid_original() -> None:
    with pytest.raises(
        ValidationError,
        match=(
            "duplicate generation "
            "requires"
        ),
    ):
        ProducerConfig(
            duplicate_rate=1.0,
            late_event_rate=0.0,
            malformed_rate=0.0,
            negative_amount_rate=0.0,
            unsupported_schema_version_rate=0.0,
        )