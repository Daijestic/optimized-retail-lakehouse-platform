"""Shared structured logging configuration for local project scripts."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID


def _json_default(value: object) -> str:
    """Chuyển các object thường gặp thành chuỗi JSON-safe."""

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(
        value,
        (
            UUID,
            Decimal,
            Path,
            Enum,
        ),
    ):
        return str(value)

    return repr(value)


class JsonFormatter(logging.Formatter):
    """Format mỗi LogRecord thành một JSON object trên một dòng."""

    def format(
        self,
        record: logging.LogRecord,
    ) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        context = getattr(
            record,
            "context",
            None,
        )

        if isinstance(context, dict):
            payload.update(context)

        if record.exc_info:
            payload["exception"] = (
                self.formatException(
                    record.exc_info
                )
            )

        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            default=_json_default,
        )


def configure_logging(
    level: str = "INFO",
) -> None:
    """Cấu hình root logger cho command-line application."""

    numeric_level = getattr(
        logging,
        level.upper(),
        None,
    )

    if not isinstance(
        numeric_level,
        int,
    ):
        raise ValueError(
            f"invalid log level: {level}"
        )

    # StreamHandler mặc định ghi ra stderr.
    # Nhờ vậy stdout có thể dành cho JSONL khi dry-run.
    handler = logging.StreamHandler(
        sys.stderr
    )
    handler.setFormatter(
        JsonFormatter()
    )

    root_logger = logging.getLogger()

    # Tránh bị nhân đôi handler khi setup nhiều lần trong test.
    root_logger.handlers.clear()
    root_logger.setLevel(
        numeric_level
    )
    root_logger.addHandler(
        handler
    )

    logging.captureWarnings(True)


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    **context: object,
) -> None:
    """Ghi một structured log event."""

    logger.log(
        level,
        message,
        extra={
            "context": context,
        },
    )