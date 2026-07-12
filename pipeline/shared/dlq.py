"""Dead-letter-queue and retry helpers for Quix Streams pipeline services.

Quix Streams' `Application` wraps each row's `.apply()`/`.to_topic()` pipeline
in a broad try/except (see `quixstreams/app.py::_process_message`) and hands
the already-caught exception to an `on_processing_error`/`on_producer_error`
callback we supply — that's what `build_dlq_error_handler` below is for. Our
code here never writes `except Exception` itself — the handler only does
`isinstance` dispatch against an explicit, caller-supplied allow-list of
exception types considered safe to quarantine; anything else it declines to
suppress, so the framework re-raises and the process crashes loud (the
intended behavior for a genuine bug).

That try/except does NOT cover `Topic.row_deserialize()` (raw bytes -> Row) —
a message with unparseable JSON raises there, outside the guarded zone, and
crashes the whole consumer before `on_processing_error` ever gets a chance to
run (confirmed against quixstreams 3.24.0's actual `_process_message`/
`_run_dataframe` source, not assumed). `build_safe_json_deserializer` below
closes that gap using Quix Streams' own supported escape hatch — raising
`IgnoreMessage` from inside a deserializer, which `row_deserialize()` catches
and turns into a clean "nothing to process this tick" rather than a crash.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, TypeVar

from confluent_kafka import KafkaException, Producer
from pydantic import BaseModel

from shared.exceptions import RetryExhaustedError
from shared.logger import get_logger
from shared.metrics import dlq_messages_total, retry_attempts_total

if TYPE_CHECKING:
    # Only the Quix-Streams-specific pieces below need this — candle_aggregator
    # and signal_router. exchange_connector uses DlqPublisher/call_with_retries
    # from this same module directly (no Quix Streams Application), so this
    # import must stay type-checking-only or exchange_connector's container
    # (whose requirements.txt intentionally has no quixstreams dependency)
    # fails at startup with ModuleNotFoundError.
    from quixstreams.models import Row
    from quixstreams.models.serializers import SerializationContext
    from quixstreams.models.serializers.base import Deserializer

_logger = get_logger(__name__)

T = TypeVar("T")

ProcessingOrProducerErrorCallback = Callable[[Exception, "Row | None", logging.Logger], bool]


class DlqEnvelope(BaseModel):
    """Payload published to `{source_topic}.dlq` for a quarantined message."""

    source_topic: str
    key: str | None
    raw_value: str
    error_type: str
    error_message: str
    attempts: int
    failed_at: datetime


class DlqPublisher:
    """Publishes quarantined messages to their source topic's `.dlq` companion."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def publish(
        self,
        *,
        source_topic: str,
        key: str | None,
        raw_value: str,
        error: Exception,
        attempts: int = 1,
    ) -> None:
        """Best-effort publish of a DlqEnvelope to `{source_topic}.dlq`.

        The narrow `except KafkaException` here is the end of the line for a
        DLQ-publish failure — there's nowhere further to route it, so it logs
        and drops rather than raising back into the caller's error handler.
        """
        envelope = DlqEnvelope(
            source_topic=source_topic,
            key=key,
            raw_value=raw_value,
            error_type=type(error).__name__,
            error_message=str(error),
            attempts=attempts,
            failed_at=datetime.now(tz=timezone.utc),
        )
        try:
            self._producer.produce(
                topic=f"{source_topic}.dlq",
                key=key,
                value=envelope.model_dump_json(),
            )
            self._producer.poll(0)
        except KafkaException as exc:
            _logger.error(
                "Failed to publish to DLQ — message dropped.",
                extra={"source_topic": source_topic, "key": key, "error": str(exc)},
            )


def call_with_retries(
    fn: Callable[[], T],
    *,
    retryable_exceptions: tuple[type[Exception], ...],
    max_attempts: int = 3,
    base_delay_seconds: float = 0.5,
    service: str = "",
    operation: str = "",
) -> T:
    """Run `fn`, retrying on a caller-supplied, explicit tuple of exception types.

    Never catches a bare `Exception` — only the types the caller opts in.
    Raises RetryExhaustedError (wrapping the last failure) once `max_attempts`
    is exhausted, for the caller (or a Quix Streams error handler further up
    the stack) to route to a DLQ.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except retryable_exceptions as exc:
            last_exc = exc
            retry_attempts_total.labels(service=service, operation=operation).inc()
            if attempt < max_attempts:
                _logger.warning(
                    "Retrying after failure.",
                    extra={
                        "service": service,
                        "operation": operation,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                time.sleep(base_delay_seconds * attempt)

    raise RetryExhaustedError(f"{operation} failed after {max_attempts} attempts.") from last_exc


def _stringify_key(key: object) -> str | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def build_dlq_error_handler(
    dlq: DlqPublisher,
    safe_exceptions: tuple[type[Exception], ...],
    service: str,
) -> ProcessingOrProducerErrorCallback:
    """Build a Quix Streams error callback usable for both `on_processing_error`
    and `on_producer_error` — both are handed `(exc, row, logger)` and both
    read the original message off `row.topic`/`row.key`/`row.value`, which is
    unchanged from the row consumed off the *input* topic even at the
    producer-error stage (Quix Streams carries the original row context
    through `.apply()`/`.to_topic()`), so one handler covers both hooks.
    """

    def _handler(exc: Exception, row: Row | None, logger: logging.Logger) -> bool:
        if not isinstance(exc, safe_exceptions):
            return False

        if row is None:
            # A producer-level failure with no associated message (e.g. a
            # bare poll() error) — nothing to route to a DLQ; log and move on
            # rather than crashing the whole service over a transient hiccup.
            _logger.warning(
                "Producer-level error with no associated message.",
                extra={"service": service, "error": str(exc)},
            )
            return True

        dlq_messages_total.labels(
            service=service, source_topic=row.topic, error_type=type(exc).__name__
        ).inc()
        dlq.publish(
            source_topic=row.topic,
            key=_stringify_key(row.key),
            raw_value=json.dumps(row.value, default=str),
            error=exc,
        )
        return True

    return _handler


def build_safe_json_deserializer(dlq: DlqPublisher, service: str) -> Deserializer:
    """Build a `value_deserializer` for `app.topic(...)` that quarantines a
    message with unparseable JSON instead of crashing the consumer.

    See the module docstring: `on_processing_error` never sees a
    deserialization failure, so this has to be handled at the deserializer
    level itself, using Quix Streams' own `IgnoreMessage` escape hatch.
    `key=None` in the DLQ envelope here isn't a shortcut — a deserializer
    only receives the raw value bytes and a `SerializationContext` (topic/
    field/headers), not the message key, so there is no key available to
    thread through even in principle at this stage. A real `Deserializer`
    subclass (rather than a bare callable) is required here — Quix Streams'
    `Application.topic(value_deserializer=...)` type-checks for one, even
    though `row_deserialize` only ever calls it, never does an isinstance
    check itself.
    """
    from quixstreams.models.serializers.base import Deserializer as _DeserializerBase
    from quixstreams.models.serializers.exceptions import IgnoreMessage

    class _SafeJsonDeserializer(_DeserializerBase):
        def __call__(self, value: bytes, ctx: SerializationContext) -> object:
            try:
                return json.loads(value)
            except (ValueError, TypeError) as exc:
                _logger.error(
                    "Malformed message failed JSON deserialization — quarantining.",
                    extra={"service": service, "source_topic": ctx.topic, "error": str(exc)},
                )
                dlq_messages_total.labels(
                    service=service, source_topic=ctx.topic, error_type=type(exc).__name__
                ).inc()
                dlq.publish(
                    source_topic=ctx.topic,
                    key=None,
                    raw_value=value.decode("utf-8", errors="replace"),
                    error=exc,
                )
                raise IgnoreMessage(str(exc)) from exc

    # quixstreams' Deserializer.__init__ is untyped (*args/**kwargs, no
    # annotations) — this is a gap in the library's own stubs, not ours.
    return _SafeJsonDeserializer()  # type: ignore[no-untyped-call]
