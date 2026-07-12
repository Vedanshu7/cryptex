"""Candle Aggregator — consumes raw ticks and emits OHLCV candles.

Built on Quix Streams' tumbling-window aggregation instead of hand-rolled tick
buffers. Ticks are keyed by symbol (set by the exchange connector), so the
window state is naturally partitioned per symbol with no explicit group_by.

Windows use the default "key" closing strategy: a window closes (and the
candle is emitted) when a later tick for the *same* symbol arrives past the
window boundary — the same "closes on next tick" semantics the previous
hand-rolled buffer implementation had.
"""

import os
from datetime import datetime, timezone
from typing import Any

from quixstreams import Application
from quixstreams.dataframe.windows import First, Last, Max, Min, Sum
from quixstreams.models.timestamps import TimestampType

from shared.db_client import get_db_connection
from shared.dlq import (
    DlqPublisher,
    build_dlq_error_handler,
    build_safe_json_deserializer,
    call_with_retries,
)
from shared.exceptions import DatabaseError, RetryExhaustedError
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.metrics import candles_published
from shared.models import Candle

_logger = get_logger(__name__)

SERVICE_NAME = "candle-aggregator"

TIMEFRAME_MINUTES: int = int(os.getenv("CANDLE_TIMEFRAME_MINUTES", "5"))
KAFKA_BROKERS: str = os.getenv("KAFKA_BROKERS", "kafka:9092")
INPUT_TOPIC = "market-data-raw"
OUTPUT_TOPIC = "market-data-candles"
CONSUMER_GROUP = "candle-aggregator"


def _build_candle(symbol: str, window: dict[str, Any], timeframe_minutes: int) -> Candle:
    """Build a Candle from a closed tumbling-window aggregation result.

    Args:
        symbol: Kafka message key for the window (ticks are keyed by symbol).
        window: A Quix Streams `.final()` record from a *named multi-aggregation*
            `.agg(...)` call — the named fields are flat alongside "start"/"end"
            (e.g. {"start", "end", "open", "high", "low", "close", "volume"}).
            Note: a *single* unnamed aggregation (e.g. `.sum()`) nests its result
            under a "value" key instead — that shape does not apply here.
        timeframe_minutes: Configured candle timeframe, used for the output label.
    """
    return Candle(
        symbol=symbol,
        open=window["open"],
        high=window["high"],
        low=window["low"],
        close=window["close"],
        volume=window["volume"],
        opened_at=datetime.fromtimestamp(window["start"] / 1000, tz=timezone.utc),
        closed_at=datetime.fromtimestamp(window["end"] / 1000, tz=timezone.utc),
        timeframe=f"{timeframe_minutes}m",
    )


def _do_persist(candle: Candle) -> None:
    """Insert a completed candle into the TimescaleDB candles hypertable."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO candles
                (time, symbol, open, high, low, close, volume, timeframe)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                candle.opened_at,
                candle.symbol,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.timeframe,
            ),
        )


def _persist_candle(candle: Candle) -> None:
    """Insert a completed candle, retrying transient DB failures with backoff.

    Raises:
        RetryExhaustedError: The DB write failed on every attempt. Left to
            propagate — the caller runs inside a Quix Streams `.apply()`, so
            this reaches the framework's own try/except and gets routed to
            `on_processing_error` (see build_application()), rather than
            being silently swallowed.
    """
    call_with_retries(
        lambda: _do_persist(candle),
        retryable_exceptions=(DatabaseError,),
        max_attempts=3,
        service=SERVICE_NAME,
        operation="persist_candle",
    )


def _on_window_closed(
    window: dict[str, Any],
    key: str,
    timestamp_ms: int,
    headers: list[tuple[str, bytes]] | None,
) -> dict[str, Any]:
    """Reshape a closed window into a Candle, persist it, and return its Kafka payload."""
    candle = _build_candle(symbol=key, window=window, timeframe_minutes=TIMEFRAME_MINUTES)
    _persist_candle(candle)

    candles_published.labels(symbol=candle.symbol, timeframe=candle.timeframe).inc()
    _logger.info(
        "Candle published.",
        extra={
            "symbol": candle.symbol,
            "open": candle.open,
            "close": candle.close,
            "volume": candle.volume,
            "timeframe": candle.timeframe,
        },
    )
    return candle.model_dump(mode="json")


def _extract_tick_timestamp(
    value: dict[str, Any],
    headers: list[tuple[str, bytes]] | None,
    timestamp_ms: int,
    timestamp_type: TimestampType,
) -> int:
    """Window by the tick's own event time, not Kafka's produce/broker timestamp.

    Matches the previous hand-rolled implementation, which floored
    `tick.timestamp` (from the JSON payload) rather than any Kafka-assigned
    timestamp — important if ticks are ever replayed or arrive slightly
    out of order relative to when they were produced.
    """
    return int(datetime.fromisoformat(value["timestamp"]).timestamp() * 1000)


def build_application() -> Application:
    """Wire the Quix Streams topology: raw ticks -> tumbling window -> candles."""
    dlq = DlqPublisher(KafkaClientFactory.create_producer(f"{SERVICE_NAME}-dlq"))
    error_handler = build_dlq_error_handler(
        dlq, safe_exceptions=(RetryExhaustedError,), service=SERVICE_NAME
    )

    app = Application(
        broker_address=KAFKA_BROKERS,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="latest",
        on_processing_error=error_handler,
        on_producer_error=error_handler,
    )

    input_topic = app.topic(
        INPUT_TOPIC,
        key_deserializer="str",
        value_deserializer=build_safe_json_deserializer(dlq, SERVICE_NAME),
        timestamp_extractor=_extract_tick_timestamp,
    )
    output_topic = app.topic(OUTPUT_TOPIC, key_serializer="str", value_serializer="json")

    sdf = app.dataframe(input_topic)
    sdf = (
        sdf.tumbling_window(duration_ms=TIMEFRAME_MINUTES * 60_000)
        .agg(
            open=First("price"),
            high=Max("price"),
            low=Min("price"),
            close=Last("price"),
            volume=Sum("volume"),
        )
        .final()
    )
    sdf = sdf.apply(_on_window_closed, metadata=True)
    sdf.to_topic(output_topic, key=lambda candle: candle["symbol"])

    return app


def run() -> None:
    """Start the candle aggregator, consuming indefinitely."""
    _logger.info("Candle aggregator started.", extra={"timeframe": TIMEFRAME_MINUTES})
    build_application().run()


if __name__ == "__main__":
    from shared.metrics import start_metrics_server

    start_metrics_server()
    run()
