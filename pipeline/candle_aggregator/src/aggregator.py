"""Candle Aggregator — consumes raw ticks and emits OHLCV candles.

Maintains per-symbol tick buffers. When a tick falls outside the current
window, the buffered ticks are aggregated into a candle and published to
the market-data-candles topic. The buffer is then reset for the new window.
"""

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaError, Message

from shared.exceptions import KafkaPublishError
from shared.db_client import get_db_connection
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.metrics import candles_published
from shared.models import Candle, Tick

_logger = get_logger(__name__)

TIMEFRAME_MINUTES: int = int(os.getenv("CANDLE_TIMEFRAME_MINUTES", "5"))
INPUT_TOPIC = "market-data-raw"
OUTPUT_TOPIC = "market-data-candles"
CONSUMER_GROUP = "candle-aggregator"


class CandleAggregator:
    """Aggregates raw ticks into OHLCV candles and publishes them to Kafka."""

    def __init__(self) -> None:
        self._consumer = KafkaClientFactory.create_consumer(
            group_id=CONSUMER_GROUP,
            topics=[INPUT_TOPIC],
        )
        self._producer = KafkaClientFactory.create_producer("candle-aggregator")
        self._tick_buffers: dict[str, list[Tick]] = defaultdict(list)
        self._window_start: dict[str, datetime] = {}

    def run(self) -> None:
        """Consume ticks indefinitely and emit candles when windows close."""
        _logger.info("Candle aggregator started.", extra={"timeframe": TIMEFRAME_MINUTES})

        while True:
            msg: Message | None = self._consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                err: KafkaError = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                _logger.error(
                    "Kafka consumer error.",
                    extra={"error": str(err), "topic": INPUT_TOPIC},
                )
                continue

            raw_value = msg.value()
            if raw_value is None:
                continue

            tick = self._deserialize(raw_value)
            if tick is not None:
                self._process_tick(tick)

    def _process_tick(self, tick: Tick) -> None:
        """Add tick to its symbol buffer and emit a candle if the window closed."""
        symbol = tick.symbol

        if symbol not in self._window_start:
            self._window_start[symbol] = _floor_to_window(tick.timestamp, TIMEFRAME_MINUTES)

        window_end = self._window_start[symbol] + timedelta(minutes=TIMEFRAME_MINUTES)

        if tick.timestamp >= window_end:
            if self._tick_buffers[symbol]:
                candle = self._build_candle(symbol)
                self._publish_candle(candle)

            self._tick_buffers[symbol] = []
            self._window_start[symbol] = _floor_to_window(tick.timestamp, TIMEFRAME_MINUTES)

        self._tick_buffers[symbol].append(tick)

    def _build_candle(self, symbol: str) -> Candle:
        """Build an OHLCV candle from the current tick buffer for a symbol."""
        ticks = self._tick_buffers[symbol]
        prices = [t.price for t in ticks]

        return Candle(
            symbol=symbol,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=sum(t.volume for t in ticks),
            opened_at=self._window_start[symbol],
            closed_at=ticks[-1].timestamp,
            timeframe=f"{TIMEFRAME_MINUTES}m",
        )

    def _publish_candle(self, candle: Candle) -> None:
        """Publish a completed candle to Kafka and persist to TimescaleDB."""
        try:
            self._producer.produce(
                topic=OUTPUT_TOPIC,
                key=candle.symbol,
                value=candle.model_dump_json(),
            )
            self._producer.flush()
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
        except Exception as exc:
            raise KafkaPublishError(
                f"Failed to publish candle for {candle.symbol}."
            ) from exc

        # Persist to TimescaleDB for the signal_pipeline to query.
        self._persist_candle(candle)

    def _persist_candle(self, candle: Candle) -> None:
        """Insert a completed candle into the TimescaleDB candles hypertable."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
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
        except Exception as exc:
            # Log but don't crash — Kafka delivery already succeeded.
            _logger.error(
                "Failed to persist candle to TimescaleDB.",
                extra={"symbol": candle.symbol, "error": str(exc)},
            )

    def _deserialize(self, raw: bytes) -> Tick | None:
        """Deserialize Kafka message bytes into a Tick model.

        Returns None on parse failure so run() can skip and continue rather
        than crashing the service on a single malformed message.
        """
        try:
            return Tick.model_validate_json(raw)
        except Exception as exc:
            _logger.warning(
                "Failed to deserialize tick — skipping message.",
                extra={"error": str(exc)},
            )
            return None


if __name__ == "__main__":
    from shared.metrics import start_metrics_server
    start_metrics_server()
    CandleAggregator().run()


def _floor_to_window(dt: datetime, window_minutes: int) -> datetime:
    """Floor a datetime to the nearest timeframe window boundary.

    E.g. 09:47 with window=5 → 09:45.
    """
    floored_minute = (dt.minute // window_minutes) * window_minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0, tzinfo=dt.tzinfo or timezone.utc)
