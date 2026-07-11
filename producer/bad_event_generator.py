"""Deterministic controlled-event generator used by the Kafka producer."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from producer.config import ProducerConfig
from producer.schemas import (
    PAYMENT_EVENT_TYPES,
    Currency,
    EventType,
    RetailPaymentEvent,
)


class EventScenario(StrEnum):
    """Các scenario dữ liệu được producer hỗ trợ."""

    VALID = "valid"
    DUPLICATE = "duplicate"
    LATE = "late"
    MALFORMED = "malformed"
    NEGATIVE_AMOUNT = "negative_amount"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"


@dataclass(frozen=True, slots=True)
class GeneratedRecord:
    """Một Kafka record đã sẵn sàng để gửi.

    Attributes:
        scenario:
            Loại dữ liệu được tạo.

        key:
            Kafka message key dạng bytes.

        value:
            Kafka message value dạng bytes.

        event_id:
            Event ID của record gốc, nếu generator biết được.

        source_event_id:
            Event ID của original record đối với duplicate.
            Các scenario khác để None.
    """

    scenario: EventScenario
    key: bytes
    value: bytes
    event_id: UUID | None
    source_event_id: UUID | None = None


def calculate_scenario_counts(
    config: ProducerConfig,
) -> dict[EventScenario, int]:
    """Chuyển các rate thành số lượng record chính xác.

    Các scenario loại trừ nhau.

    Phần dư sau khi tính các controlled-event count
    được đưa vào valid_count để tổng luôn bằng data_volume.
    """

    duplicate_count = int(
        config.data_volume * config.duplicate_rate
    )
    late_count = int(
        config.data_volume * config.late_event_rate
    )
    malformed_count = int(
        config.data_volume * config.malformed_rate
    )
    negative_count = int(
        config.data_volume * config.negative_amount_rate
    )
    unsupported_count = int(
        config.data_volume
        * config.unsupported_schema_version_rate
    )

    valid_count = config.data_volume - (
        duplicate_count
        + late_count
        + malformed_count
        + negative_count
        + unsupported_count
    )

    return {
        EventScenario.VALID: valid_count,
        EventScenario.DUPLICATE: duplicate_count,
        EventScenario.LATE: late_count,
        EventScenario.MALFORMED: malformed_count,
        EventScenario.NEGATIVE_AMOUNT: negative_count,
        EventScenario.UNSUPPORTED_SCHEMA_VERSION: (
            unsupported_count
        ),
    }


class BadEventGenerator:
    """Sinh controlled events theo ProducerConfig."""

    _EVENT_TYPES: tuple[EventType, ...] = tuple(EventType)
    _CURRENCIES: tuple[Currency, ...] = tuple(Currency)

    def __init__(self, config: ProducerConfig) -> None:
        self._config = config

    def generate(self) -> list[GeneratedRecord]:
        """Sinh toàn bộ dataset theo cấu hình.

        Một Random instance mới được tạo ở mỗi lần gọi.
        Vì vậy, cùng config và cùng seed sẽ sinh cùng output.
        """

        rng = random.Random(self._config.random_seed)
        counts = calculate_scenario_counts(self._config)

        # Duplicate chưa được thêm vào đây vì duplicate phải
        # tham chiếu tới một valid original record đã tồn tại.
        non_duplicate_scenarios: list[EventScenario] = (
            [EventScenario.VALID]
            * counts[EventScenario.VALID]
            + [EventScenario.LATE]
            * counts[EventScenario.LATE]
            + [EventScenario.MALFORMED]
            * counts[EventScenario.MALFORMED]
            + [EventScenario.NEGATIVE_AMOUNT]
            * counts[EventScenario.NEGATIVE_AMOUNT]
            + [EventScenario.UNSUPPORTED_SCHEMA_VERSION]
            * counts[
                EventScenario.UNSUPPORTED_SCHEMA_VERSION
            ]
        )

        # Shuffle có kiểm soát bằng seeded RNG.
        rng.shuffle(non_duplicate_scenarios)

        # Nếu cần duplicate, phải bảo đảm có ít nhất
        # một valid original xuất hiện trước.
        if counts[EventScenario.DUPLICATE] > 0:
            valid_position = non_duplicate_scenarios.index(
                EventScenario.VALID
            )

            non_duplicate_scenarios[0], non_duplicate_scenarios[
                valid_position
            ] = (
                non_duplicate_scenarios[valid_position],
                non_duplicate_scenarios[0],
            )

        records: list[GeneratedRecord] = []
        ordinary_valid_records: list[GeneratedRecord] = []

        for base_index, scenario in enumerate(
            non_duplicate_scenarios
        ):
            record = self._generate_record(
                rng=rng,
                base_index=base_index,
                scenario=scenario,
            )

            records.append(record)

            if scenario is EventScenario.VALID:
                ordinary_valid_records.append(record)

        # Tạo duplicate bằng cách sao chép nguyên key và value
        # từ một ordinary valid record đã xuất hiện trước đó.
        for _ in range(counts[EventScenario.DUPLICATE]):
            source_record = rng.choice(
                ordinary_valid_records
            )

            duplicate_record = GeneratedRecord(
                scenario=EventScenario.DUPLICATE,
                key=source_record.key,
                value=source_record.value,
                event_id=source_record.event_id,
                source_event_id=source_record.event_id,
            )

            # Duplicate chỉ được chèn sau original.
            original_position = records.index(source_record)

            insert_position = rng.randint(
                original_position + 1,
                len(records),
            )

            records.insert(
                insert_position,
                duplicate_record,
            )

        if len(records) != self._config.data_volume:
            raise RuntimeError(
                "generator produced an unexpected number "
                "of records: "
                f"expected={self._config.data_volume}, "
                f"actual={len(records)}"
            )

        return records

    def _generate_record(
        self,
        *,
        rng: random.Random,
        base_index: int,
        scenario: EventScenario,
    ) -> GeneratedRecord:
        """Sinh một record không phải duplicate."""

        forced_event_type: EventType | None = None

        # Negative amount chỉ có ý nghĩa với payment event,
        # nên ép base event thành payment_authorized.
        if scenario is EventScenario.NEGATIVE_AMOUNT:
            forced_event_type = (
                EventType.PAYMENT_AUTHORIZED
            )

        event = self._build_valid_event(
            rng=rng,
            base_index=base_index,
            late=scenario is EventScenario.LATE,
            forced_event_type=forced_event_type,
        )

        # Valid và late đều phải pass Pydantic schema.
        if scenario in {
            EventScenario.VALID,
            EventScenario.LATE,
        }:
            return GeneratedRecord(
                scenario=scenario,
                key=event.kafka_key(),
                value=event.kafka_value(),
                event_id=event.event_id,
            )

        if scenario is EventScenario.MALFORMED:
            valid_value = event.kafka_value()

            # model_dump_json() trả về JSON object kết thúc
            # bằng ký tự "}". Cắt byte cuối làm JSON bị lỗi.
            malformed_value = valid_value[:-1]

            return GeneratedRecord(
                scenario=scenario,
                key=event.kafka_key(),
                value=malformed_value,
                event_id=event.event_id,
            )

        # Chuyển valid event thành dictionary JSON-safe.
        # Không sửa trực tiếp Pydantic object gốc.
        payload = json.loads(event.kafka_value())

        if scenario is EventScenario.NEGATIVE_AMOUNT:
            payload["amount"] = "-1000.00"

        elif (
            scenario
            is EventScenario.UNSUPPORTED_SCHEMA_VERSION
        ):
            payload["schema_version"] = "99.0"

        else:
            raise ValueError(
                f"unsupported scenario: {scenario}"
            )

        return GeneratedRecord(
            scenario=scenario,
            key=event.kafka_key(),
            value=self._encode_json(payload),
            event_id=event.event_id,
        )

    def _build_valid_event(
        self,
        *,
        rng: random.Random,
        base_index: int,
        late: bool,
        forced_event_type: EventType | None = None,
    ) -> RetailPaymentEvent:
        """Sinh một base event hợp lệ trước khi corrupt."""

        event_type = (
            forced_event_type
            if forced_event_type is not None
            else rng.choice(self._EVENT_TYPES)
        )

        event_id = self._deterministic_uuid(rng)

        order_id = self._build_order_id(
            rng,
            base_index,
        )

        store_id = self._build_store_id(
            rng,
            base_index,
        )

        # Không dùng datetime.now().
        # Timestamp được tạo từ base_event_time cố định.
        event_time = (
            self._config.base_event_time
            + timedelta(
                seconds=(
                    base_index * 60
                    + rng.randint(0, 59)
                )
            )
        )

        if late:
            # Luôn lớn hơn late_threshold_minutes.
            producer_delay_minutes = (
                self._config.late_threshold_minutes
                + rng.randint(1, 120)
            )

            producer_time = (
                event_time
                + timedelta(
                    minutes=producer_delay_minutes
                )
            )

        else:
            producer_time = (
                event_time
                + timedelta(
                    seconds=rng.randint(0, 120)
                )
            )

        payment_id: str | None = None
        amount: Decimal | None = None
        currency: Currency | None = None

        if event_type in PAYMENT_EVENT_TYPES:
            payment_id = (
                f"payment-{base_index:08d}-"
                f"{rng.randint(0, 9999):04d}"
            )

            # Sinh amount dương và có đúng hai chữ số
            # phần thập phân.
            amount = (
                Decimal(
                    rng.randint(
                        100_000,
                        50_000_000,
                    )
                )
                / Decimal("100")
            ).quantize(Decimal("0.01"))

            currency = rng.choice(self._CURRENCIES)

        event_data = {
            "event_id": event_id,
            "event_type": event_type,
            "order_id": order_id,
            "payment_id": payment_id,
            "customer_id": (
                f"customer-"
                f"{rng.randint(1, 10_000):06d}"
            ),
            "store_id": store_id,
            "amount": amount,
            "currency": currency,
            "event_time": event_time,
            "producer_time": producer_time,
            "schema_version": "1.0",
            "idempotency_key": f"idem-{event_id}",
            "source": "synthetic-retail-producer",
        }

        # Mọi base event đều phải qua Pydantic.
        return RetailPaymentEvent.model_validate(
            event_data
        )

    def _build_order_id(
        self,
        rng: random.Random,
        base_index: int,
    ) -> str:
        """Tạo order_id, hỗ trợ hot-order skew."""

        if (
            self._config.skew_mode == "hot_order"
            and rng.random() < 0.80
        ):
            return "order-hot-000001"

        return (
            f"order-{base_index:08d}-"
            f"{rng.randint(0, 9999):04d}"
        )

    def _build_store_id(
        self,
        rng: random.Random,
        base_index: int,
    ) -> str:
        """Tạo store_id, hỗ trợ hot-store skew."""

        if (
            self._config.skew_mode == "hot_store"
            and rng.random() < 0.80
        ):
            return "store-hot-001"

        return (
            f"store-{(base_index % 100) + 1:03d}-"
            f"{rng.randint(0, 999):03d}"
        )

    @staticmethod
    def _deterministic_uuid(
        rng: random.Random,
    ) -> UUID:
        """Tạo UUIDv4-shaped value bằng seeded RNG."""

        return UUID(
            int=rng.getrandbits(128),
            version=4,
        )

    @staticmethod
    def _encode_json(
        payload: dict[str, object],
    ) -> bytes:
        """Serialize bad payload thành JSON ổn định."""

        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")