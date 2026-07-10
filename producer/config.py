from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ProducerConfig(BaseModel):
    """Cấu hình producer dùng từ Ngày 6."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    random_seed: int = 42
    data_volume: int = Field(default=100, gt=0)

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        min_length=1,
    )
    kafka_topic: str = Field(
        default="retail-payment-events",
        min_length=1,
    )

    duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    late_event_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    malformed_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    negative_amount_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
    )
    unsupported_schema_version_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
    )

    skew_mode: Literal[
        "none",
        "hot_order",
        "hot_store",
    ] = "none"