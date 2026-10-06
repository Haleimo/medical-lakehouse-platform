"""
Structured logging for the pipeline.

Every pipeline component should call `get_logger(__name__)` instead of using
`print()`. Logs are emitted as single-line JSON so they can be shipped to any
log aggregator (Loki, ELK, CloudWatch) without a custom parser, and are also
readable in a terminal.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src.utils.config import get_settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Allow callers to pass structured fields via `extra={"fields": {...}}`
        extra_fields = getattr(record, "fields", None)
        if extra_fields:
            payload.update(extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_CONFIGURED = False


def _configure_root() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    settings = get_settings()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.setLevel(settings.log_level.upper())
    root.handlers = [handler]
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    _configure_root()
    return logging.getLogger(name)


def log_with_fields(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Helper to attach structured fields to a log line.

    Example:
        log_with_fields(logger, logging.INFO, "batch complete",
                         batch_id=batch_id, valid_records=10, rejected_records=1)
    """
    logger.log(level, message, extra={"fields": fields})
