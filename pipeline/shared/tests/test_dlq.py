"""Unit tests for the shared DLQ/retry helpers."""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException
from quixstreams.models.messagecontext import MessageContext
from quixstreams.models.rows import Row
from quixstreams.models.serializers import SerializationContext
from quixstreams.models.serializers.exceptions import IgnoreMessage

from shared.dlq import (
    DlqPublisher,
    build_dlq_error_handler,
    build_safe_json_deserializer,
    call_with_retries,
)
from shared.exceptions import DatabaseError, RetryExhaustedError

_logger = logging.getLogger("test")


def _row(topic: str = "market-data-raw", key: str | None = "BTCUSDT", value: object = None) -> Row:
    return Row(
        value=value if value is not None else {"symbol": "BTCUSDT"},
        key=key,
        timestamp=0,
        context=MessageContext(topic=topic, partition=0, offset=1, size=10),
        headers=None,
    )


class TestCallWithRetries:
    def test_returns_on_first_success(self) -> None:
        fn = MagicMock(return_value="ok")
        result = call_with_retries(fn, retryable_exceptions=(DatabaseError,))
        assert result == "ok"
        assert fn.call_count == 1

    def test_retries_then_succeeds(self) -> None:
        fn = MagicMock(side_effect=[DatabaseError("x"), DatabaseError("x"), "ok"])
        with patch("shared.dlq.time.sleep"):
            result = call_with_retries(fn, retryable_exceptions=(DatabaseError,), max_attempts=3)
        assert result == "ok"
        assert fn.call_count == 3

    def test_exhausts_and_raises_retry_exhausted(self) -> None:
        fn = MagicMock(side_effect=DatabaseError("still down"))
        with patch("shared.dlq.time.sleep"), pytest.raises(RetryExhaustedError) as exc_info:
            call_with_retries(fn, retryable_exceptions=(DatabaseError,), max_attempts=3)
        assert fn.call_count == 3
        assert isinstance(exc_info.value.__cause__, DatabaseError)

    def test_non_retryable_exception_propagates_immediately(self) -> None:
        fn = MagicMock(side_effect=RuntimeError("bug"))
        with pytest.raises(RuntimeError):
            call_with_retries(fn, retryable_exceptions=(DatabaseError,), max_attempts=3)
        assert fn.call_count == 1


class TestDlqPublisher:
    def test_publish_produces_envelope(self) -> None:
        producer = MagicMock()
        dlq = DlqPublisher(producer)

        dlq.publish(
            source_topic="trade-signals",
            key="sig-1",
            raw_value='{"symbol": "BTCUSDT"}',
            error=ValueError("bad payload"),
            attempts=2,
        )

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "trade-signals.dlq"
        assert call_kwargs["key"] == "sig-1"
        envelope = json.loads(call_kwargs["value"])
        assert envelope["source_topic"] == "trade-signals"
        assert envelope["error_type"] == "ValueError"
        assert envelope["attempts"] == 2

    def test_publish_failure_is_logged_and_swallowed(self) -> None:
        producer = MagicMock()
        producer.produce.side_effect = KafkaException("broker down")
        dlq = DlqPublisher(producer)

        dlq.publish(
            source_topic="trade-signals",
            key="sig-1",
            raw_value="{}",
            error=ValueError("bad payload"),
        )  # must not raise


class TestBuildDlqErrorHandler:
    def test_allow_listed_exception_is_dlqd_and_suppressed(self) -> None:
        producer = MagicMock()
        dlq = DlqPublisher(producer)
        handler = build_dlq_error_handler(dlq, safe_exceptions=(ValueError,), service="test-svc")

        row = _row(topic="market-data-raw", key="BTCUSDT", value={"price": 1.0})
        suppressed = handler(ValueError("bad"), row, _logger)

        assert suppressed is True
        producer.produce.assert_called_once()
        assert producer.produce.call_args.kwargs["topic"] == "market-data-raw.dlq"

    def test_non_allow_listed_exception_is_not_suppressed(self) -> None:
        """Proves fail-fast survives: a bug-shaped exception isn't DLQ'd, and
        the caller (Quix Streams) is told to re-raise, crashing the process."""
        producer = MagicMock()
        dlq = DlqPublisher(producer)
        handler = build_dlq_error_handler(dlq, safe_exceptions=(ValueError,), service="test-svc")

        row = _row()
        suppressed = handler(RuntimeError("real bug"), row, _logger)

        assert suppressed is False
        producer.produce.assert_not_called()

    def test_missing_row_logs_and_suppresses_without_publishing(self) -> None:
        producer = MagicMock()
        dlq = DlqPublisher(producer)
        handler = build_dlq_error_handler(dlq, safe_exceptions=(ValueError,), service="test-svc")

        suppressed = handler(ValueError("bad"), None, _logger)

        assert suppressed is True
        producer.produce.assert_not_called()


class TestBuildSafeJsonDeserializer:
    """Covers the gap `on_processing_error` can't see: quixstreams'
    `Application` try/except only wraps a row's `.apply()`/`.to_topic()`
    pipeline, never `Topic.row_deserialize()` itself — a message with
    unparseable JSON raises there and crashes the whole consumer before
    `on_processing_error` runs (confirmed live against a real deployment,
    not just read from the source). `build_safe_json_deserializer` closes
    that gap using quixstreams' own `IgnoreMessage` escape hatch instead.
    """

    def test_valid_json_deserializes_normally(self) -> None:
        producer = MagicMock()
        dlq = DlqPublisher(producer)
        deserialize = build_safe_json_deserializer(dlq, service="test-svc")

        ctx = SerializationContext(topic="trade-signals", field="value")
        result = deserialize(b'{"symbol": "BTCUSDT"}', ctx)

        assert result == {"symbol": "BTCUSDT"}
        producer.produce.assert_not_called()

    def test_malformed_json_quarantines_and_raises_ignore_message(self) -> None:
        producer = MagicMock()
        dlq = DlqPublisher(producer)
        deserialize = build_safe_json_deserializer(dlq, service="test-svc")

        ctx = SerializationContext(topic="trade-signals", field="value")

        with pytest.raises(IgnoreMessage):
            deserialize(b"{not valid json at all", ctx)

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "trade-signals.dlq"
        envelope = json.loads(call_kwargs["value"])
        assert envelope["source_topic"] == "trade-signals"
        assert envelope["raw_value"] == "{not valid json at all"

    def test_malformed_json_does_not_crash_row_deserialize(self) -> None:
        """The integration-level proof: feeding the safe deserializer through
        Topic.row_deserialize (the actual call site that used to crash the
        process) returns None — Quix Streams' documented "skip this message"
        signal — instead of raising.
        """
        from quixstreams.models.topics import Topic

        producer = MagicMock()
        dlq = DlqPublisher(producer)
        topic = Topic(
            name="trade-signals",
            value_deserializer=build_safe_json_deserializer(dlq, service="test-svc"),
        )

        message = MagicMock()
        message.headers.return_value = None
        message.topic.return_value = "trade-signals"
        message.partition.return_value = 0
        message.offset.return_value = 1
        message.key.return_value = None
        message.value.return_value = b"{not valid json at all"
        message.timestamp.return_value = (1, 0)
        message.leader_epoch.return_value = None
        message.__len__.return_value = 20

        result = topic.row_deserialize(message)

        assert result is None
        producer.produce.assert_called_once()
