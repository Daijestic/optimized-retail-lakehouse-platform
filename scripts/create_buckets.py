from __future__ import annotations

import logging
import os
import sys
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET = "lakehouse"


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def create_s3_client() -> BaseClient:
    endpoint = os.getenv("MINIO_ENDPOINT", DEFAULT_ENDPOINT)

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=required_env("MINIO_ROOT_USER"),
        aws_secret_access_key=required_env("MINIO_ROOT_PASSWORD"),
        region_name=DEFAULT_REGION,
        config=Config(
            signature_version="s3v4",
            retries={
                "max_attempts": 5,
                "mode": "standard",
            },
            connect_timeout=5,
            read_timeout=10,
            s3={
                "addressing_style": "path",
            },
        ),
    )


def ensure_bucket(client: BaseClient, bucket_name: str) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)

        logger.info(
            "Bucket already exists: %s",
            bucket_name,
        )
        return

    except ClientError as exc:
        error_code = (
            exc.response.get("Error", {})
            .get("Code", "")
        )

        if error_code not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise

    logger.info("Creating bucket: %s", bucket_name)

    client.create_bucket(
        Bucket=bucket_name,
    )

    logger.info(
        "Bucket created successfully: %s",
        bucket_name,
    )


def smoke_test(
    client: BaseClient,
    bucket_name: str,
) -> None:
    key = "_healthcheck/day02.txt"

    expected_content = (
        b"minio-community-smoke-test"
    )

    logger.info(
        "Uploading smoke test object..."
    )

    client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=expected_content,
        ContentType="text/plain",
    )

    logger.info(
        "Downloading smoke test object..."
    )

    response: dict[str, Any] = client.get_object(
        Bucket=bucket_name,
        Key=key,
    )

    actual_content = response["Body"].read()

    if actual_content != expected_content:
        raise RuntimeError(
            "Smoke test failed: object content mismatch."
        )

    logger.info(
        "Smoke test passed: s3://%s/%s",
        bucket_name,
        key,
    )


def main() -> int:
    bucket_name = os.getenv(
        "MINIO_BUCKET",
        DEFAULT_BUCKET,
    )

    try:
        logger.info(
            "Connecting to MinIO..."
        )

        client = create_s3_client()

        ensure_bucket(
            client,
            bucket_name,
        )

        smoke_test(
            client,
            bucket_name,
        )

    except (
        ValueError,
        RuntimeError,
        EndpointConnectionError,
        BotoCoreError,
        ClientError,
    ) as exc:

        logger.exception(
            "MinIO initialization failed: %s",
            exc,
        )

        return 1

    logger.info(
        "MinIO initialization completed successfully."
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())