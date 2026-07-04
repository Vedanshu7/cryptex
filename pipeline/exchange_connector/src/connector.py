"""Exchange Connector — streams raw ticks from Binance and publishes to Kafka.

Connects to the Binance @trade WebSocket stream for the symbols in the current
trading universe (shared/universe.py), normalizes each message, and publishes
to the market-data-raw topic. Reconnects automatically on connection drops —
and also on a universe version change, so widening the symbol set takes effect
without restarting the service.
"""

import asyncio
import json
import os

import websockets
import websockets.exceptions

from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import Tick
from shared.universe import get_universe

from .normalizer import normalize
from .producer import TickProducer

_logger = get_logger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
EXCHANGE = "binance"
RECONNECT_DELAY_SECONDS = 5
UNIVERSE_POLL_SECONDS = int(os.getenv("UNIVERSE_POLL_SECONDS", "60"))


class ExchangeConnector:
    """Connects to Binance WebSocket streams and publishes ticks to Kafka."""

    def __init__(self) -> None:
        producer = KafkaClientFactory.create_producer("exchange-connector")
        self._tick_producer = TickProducer(producer)

    async def run(self) -> None:
        """Start streaming ticks, reconnecting on failure or universe change."""
        _logger.info("Exchange connector starting.")

        while True:
            universe_version = get_universe().version
            url = self._build_stream_url()
            _logger.info("Connecting with current universe.", extra={"url": url})

            try:
                await self._stream(url, universe_version)
            except websockets.exceptions.ConnectionClosed as exc:
                _logger.warning(
                    "WebSocket connection closed, reconnecting.",
                    extra={"reason": str(exc), "delay": RECONNECT_DELAY_SECONDS},
                )
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "Unexpected error in exchange connector.",
                    extra={"error": str(exc), "delay": RECONNECT_DELAY_SECONDS * 2},
                )
                await asyncio.sleep(RECONNECT_DELAY_SECONDS * 2)

    async def _stream(self, url: str, universe_version: int) -> None:
        """Connect to WebSocket and consume messages until closed or the
        universe version changes — whichever happens first.
        """
        async with websockets.connect(url) as ws:  # type: ignore[attr-defined]
            _logger.info("Connected to Binance WebSocket.")

            consume_task = asyncio.create_task(self._consume(ws))
            watch_task = asyncio.create_task(self._watch_for_universe_change(universe_version))

            try:
                await asyncio.wait([consume_task, watch_task], return_when=asyncio.FIRST_COMPLETED)
            finally:
                consume_task.cancel()
                watch_task.cancel()

            if watch_task.done() and not watch_task.cancelled():
                _logger.info("Universe changed — reconnecting to apply new symbol set.")

    async def _consume(self, ws: "websockets.WebSocketClientProtocol") -> None:
        """Consume messages from an open WebSocket until it closes."""
        async for raw_message in ws:
            self._handle_message(raw_message)

    async def _watch_for_universe_change(self, universe_version: int) -> None:
        """Poll the universe file and return as soon as its version changes."""
        while get_universe().version == universe_version:
            await asyncio.sleep(UNIVERSE_POLL_SECONDS)

    def _handle_message(self, raw: str) -> None:
        """Parse, normalize and publish a single WebSocket message."""
        try:
            data: dict[str, object] = json.loads(raw)
        except json.JSONDecodeError as exc:
            _logger.warning("Invalid JSON from WebSocket.", extra={"error": str(exc)})
            return

        tick: Tick | None = normalize(data)
        if tick is not None:
            self._tick_producer.publish(tick)

    def _build_stream_url(self) -> str:
        """Build a multi-stream WebSocket URL for symbols assigned to Binance.

        Only symbols from regions whose exchange is "binance" are included —
        this connector ingests exclusively from Binance regardless of how
        many regions in the universe reference other exchanges.
        """
        symbols = get_universe().symbols_for_exchange(EXCHANGE)
        streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
        return f"{BINANCE_WS_URL}/{streams}"


if __name__ == "__main__":
    from shared.metrics import start_metrics_server

    start_metrics_server()
    connector = ExchangeConnector()
    asyncio.run(connector.run())
