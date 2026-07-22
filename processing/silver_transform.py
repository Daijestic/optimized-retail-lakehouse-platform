"""Parse Bronze records and split them into valid and invalid candidates.

Day 2 scope:
- read Bronze JSONL with an explicit schema;
- validate the authoritative value_base64 payload;
- parse valid event payloads into typed Spark columns;
- preserve source metadata for audit;
- split rows into valid and invalid DataFrames.

This module does not:
- write DLQ records;
- deduplicate event_id;
- classify late events;
- write Delta or MinIO output.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from quality.validation_rules import validate_bronze_record


# ---------------------------------------------------------------------------
# Bronze schema
# ---------------------------------------------------------------------------

KAFKA_HEADER_SCHEMA = T.StructType(
    [
        T.StructField("name", T.StringType(), nullable=True),
        T.StructField("value_base64", T.StringType(), nullable=True),
    ]
)

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField(
            "record_version",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "ingestion_run_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "ingestion_batch_number",
            T.IntegerType(),
            nullable=True,
        ),
        T.StructField(
            "ingestion_batch_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "ingestion_time",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "processing_date",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "source_record_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "source_topic",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "source_partition",
            T.IntegerType(),
            nullable=True,
        ),
        T.StructField(
            "source_offset",
            T.LongType(),
            nullable=True,
        ),
        T.StructField(
            "kafka_timestamp_type",
            T.IntegerType(),
            nullable=True,
        ),
        T.StructField(
            "kafka_timestamp_ms",
            T.LongType(),
            nullable=True,
        ),
        T.StructField(
            "key_base64",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "value_base64",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "headers",
            T.ArrayType(
                KAFKA_HEADER_SCHEMA,
                containsNull=True,
            ),
            nullable=True,
        ),
        T.StructField(
            "payload_parse_status",
            T.StringType(),
            nullable=True,
        ),
        # Các field dưới đây chỉ là best-effort metadata của Bronze.
        # Silver không dùng chúng làm source of truth.
        T.StructField(
            "event_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "event_type",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "event_time",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "producer_time",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "schema_version",
            T.StringType(),
            nullable=True,
        ),
    ]
)


# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------

EVENT_SCHEMA = T.StructType(
    [
        T.StructField(
            "event_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "event_type",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "order_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "payment_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "customer_id",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "store_id",
            T.StringType(),
            nullable=True,
        ),
        # Data contract lưu amount dưới dạng decimal string.
        # Việc cast sang DecimalType có thể thực hiện ở bước sau.
        T.StructField(
            "amount",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "currency",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "event_time",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "producer_time",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "schema_version",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "idempotency_key",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "source",
            T.StringType(),
            nullable=True,
        ),
    ]
)


# ---------------------------------------------------------------------------
# Validation UDF contract
# ---------------------------------------------------------------------------

VALIDATION_RESULT_SCHEMA = T.StructType(
    [
        T.StructField(
            "is_valid",
            T.BooleanType(),
            nullable=False,
        ),
        T.StructField(
            "reason_code",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "reason_detail",
            T.StringType(),
            nullable=True,
        ),
        T.StructField(
            "parsed_event_json",
            T.StringType(),
            nullable=True,
        ),
    ]
)


BRONZE_AUDIT_COLUMNS: tuple[str, ...] = (
    "record_version",
    "ingestion_run_id",
    "ingestion_batch_number",
    "ingestion_batch_id",
    "ingestion_time",
    "processing_date",
    "source_record_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "kafka_timestamp_type",
    "kafka_timestamp_ms",
    "key_base64",
    "value_base64",
    "headers",
    "payload_parse_status",
)

REQUIRED_BRONZE_COLUMNS = frozenset(
    BRONZE_SCHEMA.fieldNames()
)


@dataclass(frozen=True, slots=True)
class SilverTransformResult:
    """Hai nhánh output sau bước parse và validation."""

    valid_df: DataFrame
    invalid_df: DataFrame


def read_bronze_jsonl(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """Đọc Bronze JSONL bằng explicit schema.

    Args:
        spark: SparkSession đang hoạt động.
        path: Local path, file URI hoặc object-storage URI.

    Returns:
        Bronze DataFrame theo BRONZE_SCHEMA.
    """

    if not isinstance(path, str) or not path.strip():
        raise ValueError(
            "Bronze path must be a non-empty string"
        )

    return (
        spark.read
        .schema(BRONZE_SCHEMA)
        .json(path)
    )


def _validate_value_base64(
    value_base64: Any,
) -> tuple[
    bool,
    str | None,
    str | None,
    str | None,
]:
    """Adapter giữa Spark UDF và validator của Ngày 1."""

    result = validate_bronze_record(
        {"value_base64": value_base64}
    )

    reason_code = (
        result.reason_code.value
        if result.reason_code is not None
        else None
    )

    parsed_event_json = (
        json.dumps(
            result.parsed_event,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if result.parsed_event is not None
        else None
    )

    return (
        result.is_valid,
        reason_code,
        result.reason_detail,
        parsed_event_json,
    )


_VALIDATE_VALUE_BASE64_UDF = F.udf(
    _validate_value_base64,
    VALIDATION_RESULT_SCHEMA,
)


def _validate_required_bronze_columns(
    bronze_df: DataFrame,
) -> None:
    """Fail sớm khi DataFrame không đúng Bronze contract."""

    actual_columns = set(bronze_df.columns)

    missing_columns = sorted(
        REQUIRED_BRONZE_COLUMNS - actual_columns
    )

    if missing_columns:
        missing_text = ", ".join(missing_columns)

        raise ValueError(
            "Missing required Bronze columns: "
            f"{missing_text}"
        )


def _build_valid_candidates(
    validated_df: DataFrame,
) -> DataFrame:
    """Parse và flatten các event đã vượt validation."""

    valid_with_event = (
        validated_df
        .filter(F.col("_validation.is_valid"))
        .withColumn(
            "_event",
            F.from_json(
                F.col(
                    "_validation.parsed_event_json"
                ),
                EVENT_SCHEMA,
            ),
        )
    )

    audit_columns = [
        F.col(column_name)
        for column_name in BRONZE_AUDIT_COLUMNS
    ]

    event_columns = [
        F.col(
            f"_event.{field_name}"
        ).alias(field_name)
        for field_name in EVENT_SCHEMA.fieldNames()
    ]

    return valid_with_event.select(
        *audit_columns,
        *event_columns,
    )


def _build_invalid_candidates(
    validated_df: DataFrame,
) -> DataFrame:
    """Giữ raw evidence và reason cho record invalid."""

    audit_columns = [
        F.col(column_name)
        for column_name in BRONZE_AUDIT_COLUMNS
    ]

    return (
        validated_df
        .filter(~F.col("_validation.is_valid"))
        .select(
            *audit_columns,
            F.col(
                "_validation.reason_code"
            ).alias("reason_code"),
            F.col(
                "_validation.reason_detail"
            ).alias("reason_detail"),
        )
    )


def transform_bronze_dataframe(
    bronze_df: DataFrame,
) -> SilverTransformResult:
    """Parse Bronze và tách valid/invalid candidates.

    Hàm này là pure DataFrame transformation:
    - không gọi count/collect;
    - không ghi dữ liệu;
    - không commit checkpoint;
    - không thực hiện deduplication;
    - không phân loại late events.

    Args:
        bronze_df: DataFrame theo Bronze envelope contract.

    Returns:
        SilverTransformResult gồm valid_df và invalid_df.
    """

    _validate_required_bronze_columns(bronze_df)

    validated_df = bronze_df.withColumn(
        "_validation",
        _VALIDATE_VALUE_BASE64_UDF(
            F.col("value_base64")
        ),
    )

    valid_df = _build_valid_candidates(
        validated_df
    )

    invalid_df = _build_invalid_candidates(
        validated_df
    )

    return SilverTransformResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
    )