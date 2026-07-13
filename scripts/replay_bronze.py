"""Replay immutable Bronze records for one processing date.

The replay command is read-only with respect to MinIO Bronze and Kafka:

- list partitioned Bronze objects;
- validate object keys and metadata;
- validate Bronze JSONL envelopes;
- preserve each Bronze row unchanged;
- write a deterministic local JSONL replay artifact;
- create a separate replay manifest.

It does not reset Kafka offsets, publish to Kafka, modify Bronze,
deduplicate records or apply Silver business validation.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from ingestion.bronze_writer import (
    BronzeWriterConfig,
    create_s3_client,
    validate_processing_date,
)
from logging_config import configure_logging, log_event


LOGGER = logging.getLogger("scripts.replay_bronze")


@dataclass(frozen=True, slots=True)
class BronzeObjectReference:
    """Coordinates parsed from one partitioned Bronze object key."""

    bucket: str
    key: str
    processing_date: str
    topic: str
    partition: int
    start_offset: int
    end_offset: int
    size_bytes: int

    @property
    def expected_record_count(self) -> int:
        return self.end_offset - self.start_offset + 1


@dataclass(frozen=True, slots=True)
class ReplayPartitionSummary:
    """Replay counts for one Kafka topic-partition."""

    topic: str
    partition: int
    first_offset: int
    last_offset: int
    object_count: int
    record_count: int


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Final result of one Bronze replay execution."""

    replay_run_id: str
    processing_date: str
    bucket: str
    source_prefix: str
    output_path: str
    manifest_path: str
    object_count: int
    record_count: int
    output_size_bytes: int
    output_sha256: str
    partitions: tuple[ReplayPartitionSummary, ...]
    status: str


def build_date_prefix(
    *,
    root_prefix: str,
    processing_date: str,
) -> str:
    """Build the object-listing prefix for one processing date."""

    valid_date = validate_processing_date(
        processing_date
    )

    return (
        f"{root_prefix.strip('/')}/"
        f"processing_date={valid_date}/"
    )


def parse_bronze_object_key(
    *,
    bucket: str,
    key: str,
    size_bytes: int,
    root_prefix: str,
) -> BronzeObjectReference:
    """Parse and validate the canonical Day 5 Bronze object key."""

    clean_prefix = re.escape(
        root_prefix.strip("/")
    )

    pattern = re.compile(
        rf"^{clean_prefix}/"
        r"processing_date="
        r"(?P<processing_date>\d{4}-\d{2}-\d{2})/"
        r"topic=(?P<topic>[^/]+)/"
        r"partition=(?P<partition>\d{5})/"
        r"offsets="
        r"(?P<start_offset>\d{20})-"
        r"(?P<end_offset>\d{20})"
        r"\.jsonl$"
    )

    match = pattern.fullmatch(key)

    if match is None:
        raise ValueError(
            "Unexpected Bronze object key layout: "
            f"{key}"
        )

    processing_date = validate_processing_date(
        match.group("processing_date")
    )
    topic = match.group("topic")
    partition = int(match.group("partition"))
    start_offset = int(
        match.group("start_offset")
    )
    end_offset = int(
        match.group("end_offset")
    )

    if end_offset < start_offset:
        raise ValueError(
            "Bronze object end offset is smaller "
            f"than start offset: {key}"
        )

    if size_bytes <= 0:
        raise ValueError(
            f"Bronze object is empty: {key}"
        )

    return BronzeObjectReference(
        bucket=bucket,
        key=key,
        processing_date=processing_date,
        topic=topic,
        partition=partition,
        start_offset=start_offset,
        end_offset=end_offset,
        size_bytes=size_bytes,
    )


def list_replay_objects(
    *,
    s3: BaseClient,
    bucket: str,
    root_prefix: str,
    processing_date: str,
    topic_filter: str | None = None,
    partition_filter: int | None = None,
) -> list[BronzeObjectReference]:
    """List all Bronze JSONL objects for a processing date."""

    date_prefix = build_date_prefix(
        root_prefix=root_prefix,
        processing_date=processing_date,
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    references: list[
        BronzeObjectReference
    ] = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=date_prefix,
    ):
        for item in page.get("Contents", []):
            key = str(item["Key"])

            if not key.endswith(".jsonl"):
                continue

            reference = parse_bronze_object_key(
                bucket=bucket,
                key=key,
                size_bytes=int(item["Size"]),
                root_prefix=root_prefix,
            )

            if (
                reference.processing_date
                != processing_date
            ):
                raise RuntimeError(
                    "Listed object processing date "
                    "does not match requested date: "
                    f"{reference.key}"
                )

            if (
                topic_filter is not None
                and reference.topic != topic_filter
            ):
                continue

            if (
                partition_filter is not None
                and reference.partition
                != partition_filter
            ):
                continue

            references.append(reference)

    references.sort(
        key=lambda item: (
            item.topic,
            item.partition,
            item.start_offset,
            item.end_offset,
            item.key,
        )
    )

    if not references:
        raise FileNotFoundError(
            "No partitioned Bronze objects found for "
            f"processing_date={processing_date}, "
            f"prefix={date_prefix}"
        )

    return references


def validate_object_ranges(
    references: Sequence[
        BronzeObjectReference
    ],
    *,
    allow_gaps: bool,
) -> None:
    """Reject overlapping or unexpectedly discontinuous offset ranges."""

    grouped: dict[
        tuple[str, int],
        list[BronzeObjectReference],
    ] = defaultdict(list)

    for reference in references:
        grouped[
            (
                reference.topic,
                reference.partition,
            )
        ].append(reference)

    for (
        topic,
        partition,
    ), partition_objects in grouped.items():
        ordered = sorted(
            partition_objects,
            key=lambda item: (
                item.start_offset,
                item.end_offset,
            ),
        )

        previous: (
            BronzeObjectReference | None
        ) = None

        for current in ordered:
            if previous is None:
                previous = current
                continue

            if (
                current.start_offset
                <= previous.end_offset
            ):
                raise RuntimeError(
                    "Overlapping Bronze offset ranges: "
                    f"topic={topic}, "
                    f"partition={partition}, "
                    f"previous={previous.start_offset}-"
                    f"{previous.end_offset}, "
                    f"current={current.start_offset}-"
                    f"{current.end_offset}"
                )

            expected_start = (
                previous.end_offset + 1
            )

            if (
                not allow_gaps
                and current.start_offset
                != expected_start
            ):
                raise RuntimeError(
                    "Gap detected between Bronze objects: "
                    f"topic={topic}, "
                    f"partition={partition}, "
                    f"expected_start={expected_start}, "
                    f"actual_start="
                    f"{current.start_offset}"
                )

            previous = current


def validate_object_metadata(
    *,
    s3: BaseClient,
    reference: BronzeObjectReference,
) -> None:
    """Verify object-level metadata before reading the JSONL body."""

    response = s3.head_object(
        Bucket=reference.bucket,
        Key=reference.key,
    )

    actual_size = int(
        response["ContentLength"]
    )

    if actual_size != reference.size_bytes:
        raise RuntimeError(
            "Bronze object size changed between "
            "LIST and HEAD: "
            f"key={reference.key}, "
            f"listed={reference.size_bytes}, "
            f"actual={actual_size}"
        )

    metadata = response.get(
        "Metadata",
        {},
    )

    expected_metadata = {
        "processing-date": (
            reference.processing_date
        ),
        "source-topic": reference.topic,
        "source-partition": str(
            reference.partition
        ),
        "start-offset": str(
            reference.start_offset
        ),
        "end-offset": str(
            reference.end_offset
        ),
        "record-version": "bronze-raw-v1",
    }

    for name, expected in (
        expected_metadata.items()
    ):
        actual = metadata.get(name)

        if actual != expected:
            raise RuntimeError(
                "Bronze object metadata mismatch: "
                f"key={reference.key}, "
                f"field={name}, "
                f"expected={expected}, "
                f"actual={actual}"
            )


def validate_optional_base64(
    value: Any,
    *,
    field_name: str,
    object_key: str,
    source_offset: int,
) -> None:
    """Ensure an optional Bronze byte field contains valid Base64."""

    if value is None:
        return

    if not isinstance(value, str):
        raise RuntimeError(
            f"{field_name} must be string or null: "
            f"key={object_key}, "
            f"source_offset={source_offset}"
        )

    try:
        base64.b64decode(
            value,
            validate=True,
        )
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid Base64 in {field_name}: "
            f"key={object_key}, "
            f"source_offset={source_offset}"
        ) from exc


def validate_bronze_row(
    row: Any,
    *,
    reference: BronzeObjectReference,
    expected_offset: int,
) -> None:
    """Validate the technical Bronze envelope without business rules."""

    if not isinstance(row, dict):
        raise RuntimeError(
            "Bronze JSONL row must be an object: "
            f"key={reference.key}, "
            f"expected_offset={expected_offset}"
        )

    expected_values: dict[str, Any] = {
        "record_version": "bronze-raw-v1",
        "processing_date": (
            reference.processing_date
        ),
        "source_topic": reference.topic,
        "source_partition": (
            reference.partition
        ),
        "source_offset": expected_offset,
        "source_record_id": (
            f"{reference.topic}:"
            f"{reference.partition}:"
            f"{expected_offset}"
        ),
    }

    for field_name, expected in (
        expected_values.items()
    ):
        actual = row.get(field_name)

        if actual != expected:
            raise RuntimeError(
                "Bronze row metadata mismatch: "
                f"key={reference.key}, "
                f"field={field_name}, "
                f"expected={expected}, "
                f"actual={actual}"
            )

    validate_optional_base64(
        row.get("key_base64"),
        field_name="key_base64",
        object_key=reference.key,
        source_offset=expected_offset,
    )

    validate_optional_base64(
        row.get("value_base64"),
        field_name="value_base64",
        object_key=reference.key,
        source_offset=expected_offset,
    )


def read_and_validate_object(
    *,
    s3: BaseClient,
    reference: BronzeObjectReference,
) -> list[bytes]:
    """Read one object and return unchanged validated JSONL rows."""

    response = s3.get_object(
        Bucket=reference.bucket,
        Key=reference.key,
    )

    body = response["Body"]

    try:
        content = body.read()
    finally:
        body.close()

    lines = [
        line
        for line in content.splitlines()
        if line.strip()
    ]

    if (
        len(lines)
        != reference.expected_record_count
    ):
        raise RuntimeError(
            "Bronze object row count does not "
            "match offset range: "
            f"key={reference.key}, "
            f"expected="
            f"{reference.expected_record_count}, "
            f"actual={len(lines)}"
        )

    for index, line in enumerate(lines):
        expected_offset = (
            reference.start_offset + index
        )

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Invalid outer Bronze JSONL envelope: "
                f"key={reference.key}, "
                f"line={index + 1}"
            ) from exc

        validate_bronze_row(
            row,
            reference=reference,
            expected_offset=expected_offset,
        )

    return lines


def write_json_atomic(
    path: Path,
    payload: dict[str, Any],
) -> None:
    """Write a local JSON document through a temporary file."""

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        f".{path.name}.tmp"
    )

    temporary.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    os.replace(
        temporary,
        path,
    )


def replay_processing_date(
    *,
    s3: BaseClient,
    bucket: str,
    root_prefix: str,
    processing_date: str,
    output_path: Path,
    replay_run_id: str,
    overwrite: bool,
    allow_gaps: bool,
    topic_filter: str | None = None,
    partition_filter: int | None = None,
) -> ReplayResult:
    """Replay and validate one processing-date partition."""

    if output_path.exists() and not overwrite:
        raise FileExistsError(
            "Replay output already exists. "
            "Use --overwrite to replace it: "
            f"{output_path}"
        )

    references = list_replay_objects(
        s3=s3,
        bucket=bucket,
        root_prefix=root_prefix,
        processing_date=processing_date,
        topic_filter=topic_filter,
        partition_filter=partition_filter,
    )

    validate_object_ranges(
        references,
        allow_gaps=allow_gaps,
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = output_path.with_name(
        f".{output_path.name}."
        f"{replay_run_id}.tmp"
    )

    output_digest = hashlib.sha256()
    output_size = 0
    record_count = 0

    partition_states: dict[
        tuple[str, int],
        dict[str, int],
    ] = {}

    try:
        with temporary_path.open("wb") as output:
            for reference in references:
                validate_object_metadata(
                    s3=s3,
                    reference=reference,
                )

                lines = read_and_validate_object(
                    s3=s3,
                    reference=reference,
                )

                state_key = (
                    reference.topic,
                    reference.partition,
                )

                state = partition_states.setdefault(
                    state_key,
                    {
                        "first_offset": (
                            reference.start_offset
                        ),
                        "last_offset": (
                            reference.end_offset
                        ),
                        "object_count": 0,
                        "record_count": 0,
                    },
                )

                state["first_offset"] = min(
                    state["first_offset"],
                    reference.start_offset,
                )
                state["last_offset"] = max(
                    state["last_offset"],
                    reference.end_offset,
                )
                state["object_count"] += 1
                state["record_count"] += len(
                    lines
                )

                for line in lines:
                    serialized = line + b"\n"

                    output.write(serialized)
                    output_digest.update(
                        serialized
                    )

                    output_size += len(
                        serialized
                    )
                    record_count += 1

        os.replace(
            temporary_path,
            output_path,
        )

    except Exception:
        temporary_path.unlink(
            missing_ok=True
        )
        raise

    partition_summaries = tuple(
        ReplayPartitionSummary(
            topic=topic,
            partition=partition,
            first_offset=state[
                "first_offset"
            ],
            last_offset=state[
                "last_offset"
            ],
            object_count=state[
                "object_count"
            ],
            record_count=state[
                "record_count"
            ],
        )
        for (
            topic,
            partition,
        ), state in sorted(
            partition_states.items()
        )
    )

    manifest_path = output_path.with_suffix(
        output_path.suffix
        + ".manifest.json"
    )

    result = ReplayResult(
        replay_run_id=replay_run_id,
        processing_date=processing_date,
        bucket=bucket,
        source_prefix=build_date_prefix(
            root_prefix=root_prefix,
            processing_date=processing_date,
        ),
        output_path=str(output_path),
        manifest_path=str(manifest_path),
        object_count=len(references),
        record_count=record_count,
        output_size_bytes=output_size,
        output_sha256=(
            output_digest.hexdigest()
        ),
        partitions=partition_summaries,
        status="success",
    )

    manifest_payload = asdict(result)

    manifest_payload["partitions"] = [
        asdict(summary)
        for summary in partition_summaries
    ]

    write_json_atomic(
        manifest_path,
        manifest_payload,
    )

    return result


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the Bronze replay CLI."""

    parser = argparse.ArgumentParser(
        description=(
            "Replay partitioned Bronze records "
            "for one UTC processing date."
        )
    )

    parser.add_argument(
        "--processing-date",
        required=True,
        help="UTC date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Optional replay run ID. "
            "Generated automatically when omitted."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Local output JSONL path. "
            "Default: artifacts/replay/"
            "processing_date=<date>/"
            "bronze-replay.jsonl"
        ),
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Optional source-topic filter.",
    )
    parser.add_argument(
        "--partition",
        type=int,
        default=None,
        help="Optional Kafka partition filter.",
    )
    parser.add_argument(
        "--allow-gaps",
        action="store_true",
        help=(
            "Allow gaps between object offset ranges. "
            "Overlaps always fail."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing local replay output.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "List the replay plan without "
            "downloading object bodies."
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
    """Command-line entry point."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    configure_logging(
        args.log_level
    )

    try:
        processing_date = (
            validate_processing_date(
                args.processing_date
            )
        )

        if (
            args.partition is not None
            and args.partition < 0
        ):
            parser.error(
                "--partition must be zero "
                "or greater"
            )

        replay_run_id = (
            args.run_id.strip()
            if args.run_id
            else f"replay-{uuid4()}"
        )

        if not replay_run_id:
            parser.error(
                "--run-id must not be empty"
            )

        writer_config = (
            BronzeWriterConfig.from_environment()
        )

        s3 = create_s3_client(
            writer_config
        )

        references = list_replay_objects(
            s3=s3,
            bucket=writer_config.bucket,
            root_prefix=writer_config.prefix,
            processing_date=processing_date,
            topic_filter=args.topic,
            partition_filter=args.partition,
        )

        validate_object_ranges(
            references,
            allow_gaps=args.allow_gaps,
        )

        if args.dry_run:
            print(
                json.dumps(
                    {
                        "replay_run_id": (
                            replay_run_id
                        ),
                        "processing_date": (
                            processing_date
                        ),
                        "bucket": (
                            writer_config.bucket
                        ),
                        "object_count": len(
                            references
                        ),
                        "objects": [
                            asdict(reference)
                            for reference
                            in references
                        ],
                        "status": "dry_run",
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )

            return 0

        output_path = (
            args.output
            if args.output is not None
            else Path(
                "artifacts",
                "replay",
                (
                    "processing_date="
                    f"{processing_date}"
                ),
                "bronze-replay.jsonl",
            )
        )

        log_event(
            LOGGER,
            logging.INFO,
            "bronze_replay_started",
            replay_run_id=replay_run_id,
            processing_date=processing_date,
            bucket=writer_config.bucket,
            prefix=writer_config.prefix,
            output_path=str(output_path),
        )

        result = replay_processing_date(
            s3=s3,
            bucket=writer_config.bucket,
            root_prefix=writer_config.prefix,
            processing_date=processing_date,
            output_path=output_path,
            replay_run_id=replay_run_id,
            overwrite=args.overwrite,
            allow_gaps=args.allow_gaps,
            topic_filter=args.topic,
            partition_filter=args.partition,
        )

        log_event(
            LOGGER,
            logging.INFO,
            "bronze_replay_completed",
            **asdict(result),
        )

        print(
            json.dumps(
                asdict(result),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )

        return 0

    except (
        BotoCoreError,
        ClientError,
        FileExistsError,
        FileNotFoundError,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        log_event(
            LOGGER,
            logging.ERROR,
            "bronze_replay_failed",
            error_type=type(exc).__name__,
            error=str(exc),
            status="failed",
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())