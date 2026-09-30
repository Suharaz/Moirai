"""Structured JSON logging with mandatory redaction (every service calls `configure_logging`)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

from hdt.core.ids import utc_timestamp
from hdt.vault.redact import RedactionFilter

_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line; extra fields are emitted as top-level keys."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            # wall-clock time of the log call (never the business clock, which is simulated in replay)
            "ts": utc_timestamp(datetime.fromtimestamp(record.created, UTC)),
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exc"] = record.exc_text
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(service: str, level: str = "INFO", stream: TextIO | None = None) -> logging.Handler:
    """Install one JSON handler with the redaction filter on the root logger (idempotent)."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_hdt_handler", False):
            root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.addFilter(RedactionFilter())
    handler.setFormatter(JsonFormatter(service))
    handler._hdt_handler = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)
    # Third-party HTTP clients log full URLs (query strings may hold signatures); keep them quiet.
    for noisy in ("httpx", "httpcore", "websockets", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return handler
