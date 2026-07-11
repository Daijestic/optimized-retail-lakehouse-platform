"""Configuration for the deterministic synthetic Kafka producer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)


class ProducerConfig(BaseModel):
    """Cấu hình dùng chung cho event generator và Kafka producer.

    Quy ước:
    - data_volume là tổng số record cuối cùng được tạo.
    - Mỗi record chỉ thuộc một scenario.
    - Số lượng từng scenario được tính bằng:
      int(data_volume * scenario_rate)
    - Phần còn lại là valid event bình thường.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    # Reproducibility
    random_seed: int = 42
    data_volume: int = Field(
        default=100,
        gt=0,
    )

    # Kafka connection
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        min_length=1,
    )
    kafka_topic: str = Field(
        default="retail-payment-events",
        min_length=1,
    )
    client_id: str = Field(
        default="synthetic-retail-producer",
        min_length=1,
    )

    # Controlled-event rates
    duplicate_rate: float = Field(
        default=0.10,
        ge=0.0,
        le=1.0,
    )
    late_event_rate: float = Field(
        default=0.10,
        ge=0.0,
        le=1.0,
    )
    malformed_rate: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
    )
    negative_amount_rate: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
    )
    unsupported_schema_version_rate: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
    )

    # Optional data skew for later experiments
    skew_mode: Literal[
        "none",
        "hot_order",
        "hot_store",
    ] = "none"

    # Fixed timestamp origin — không dùng datetime.now()
    base_event_time: AwareDatetime = datetime(
        2026,
        7,
        6,
        0,
        0,
        0,
        tzinfo=timezone.utc,
    )

    # Event được xem là late nếu:
    # producer_time - event_time > late_threshold_minutes
    late_threshold_minutes: int = Field(
        default=30,
        gt=0,
    )

    # Thời gian tối đa chờ Kafka giao hết record
    flush_timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        le=300.0,
    )

    @property
    def error_rate(self) -> float:
        """Tổng tỷ lệ các scenario không phải valid bình thường.

        Tên error_rate được giữ để phù hợp run summary của roadmap.
        Duplicate và late không nhất thiết là dữ liệu sai cú pháp.
        """

        return (
            self.duplicate_rate
            + self.late_event_rate
            + self.malformed_rate
            + self.negative_amount_rate
            + self.unsupported_schema_version_rate
        )

    @model_validator(mode="after")
    def validate_scenario_rates(self) -> Self:
        """Kiểm tra tổng rate và kế hoạch duplicate có thực hiện được."""

        if self.error_rate > 1.0 + 1e-12:
            raise ValueError(
                "the sum of scenario rates must be less than or equal to 1.0"
            )

        duplicate_count = int(
            self.data_volume * self.duplicate_rate
        )
        late_count = int(
            self.data_volume * self.late_event_rate
        )
        malformed_count = int(
            self.data_volume * self.malformed_rate
        )
        negative_amount_count = int(
            self.data_volume * self.negative_amount_rate
        )
        unsupported_version_count = int(
            self.data_volume
            * self.unsupported_schema_version_rate
        )

        valid_count = self.data_volume - (
            duplicate_count
            + late_count
            + malformed_count
            + negative_amount_count
            + unsupported_version_count
        )

        if duplicate_count > 0 and valid_count < 1:
            raise ValueError(
                "duplicate generation requires at least one ordinary "
                "valid original record in the output"
            )

        return self