"""Unit tests for the structured logger."""

import json
import logging

from shared.logger import JsonFormatter, get_logger


class TestJsonFormatter:
    def test_formats_to_valid_json(self) -> None:
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Hello world",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "Hello world"
        assert parsed["service"] == "test"
        assert "timestamp" in parsed

    def test_extra_fields_included(self) -> None:
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Kafka error",
            args=(),
            exc_info=None,
        )
        record.topic = "market-data-raw"
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed.get("topic") == "market-data-raw"


class TestGetLogger:
    def test_returns_logger_instance(self) -> None:
        logger = get_logger("my.module")
        assert isinstance(logger, logging.Logger)

    def test_does_not_duplicate_handlers(self) -> None:
        logger = get_logger("duplicate.test")
        count_before = len(logger.handlers)
        get_logger("duplicate.test")
        assert len(logger.handlers) == count_before
