"""Structured JSON logging utilities shared across all pipeline services."""

import json
import logging
import os
from logging import LogRecord


class JsonFormatter(logging.Formatter):
    """Formats log records as JSON for structured log aggregation."""

    def format(self, record: LogRecord) -> str:
        """Serialize log record to a single-line JSON string."""
        log_data: dict[str, object] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Merge any structured fields passed via extra={...}.
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            ):
                log_data[key] = value

        return json.dumps(log_data)


def get_logger(name: str) -> logging.Logger:
    """Return a structured JSON logger for the given module name.

    Reads LOG_LEVEL from the environment (default: INFO).
    Handlers are only attached once to avoid duplicate log lines.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False

    return logger
