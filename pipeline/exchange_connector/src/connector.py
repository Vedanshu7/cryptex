"""Exchange Connector — streams raw ticks from Binance and publishes to Kafka.

Connects to the Binance @trade WebSocket stream for configured symbols,
normalizes each message, and publishes to the market-data-raw topic.
Reconnects automatically on connection drops.
"""

import asyncio
import json
import os

import websockets
import websockets.exceptions

from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import Tick

from .normalizer import normalize
from .producer import TickProducer

_logger = get_logger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOLS: list[str] = os.getenv("TRADE_SYMBOLS", "btcusdt,ethusdt").split(",")
RECONNECT_DELAY_SECONDS = 5


class ExchangeConnector:
    """Connects to Binance WebSocket streams and publishes ticks to Kafka."""

    def __init__(self) -> None:
        producer = KafkaClientFactory.create_producer("exchange-connector")
        self._tick_producer = TickProducer(producer)

    async def run(self) -> None:
        """Start streaming ticks, reconnecting on failure indefinitely."""
        url = self._build_stream_url()
        _logger.info("Exchange connector starting.", extra={"url": url})

        while True:
            try:
                await self._stream(url)
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

    async def _stream(self, url: str) -> None:
        """Connect to WebSocket and process incoming messages until closed."""
        async with websockets.connect(url) as ws:  # type: ignore[attr-defined]
            _logger.info("Connected to Binance WebSocket.")
            async for raw_message in ws:
                self._handle_message(raw_message)

    def _handle_message(self, raw: str) -> None:
        """Parse, normalize and publish a single WebSocket message."""
        try:
            data: dict[str, object] = json.loads(raw)
        except json.JSONDecodeError as exc:
            _logger.warning(
                "Invalid JSON from WebSocket.", extra={"error": str(exc)}
            )
            return

        tick: Tick | None = normalize(data)
        if tick is not None:
            self._tick_producer.publish(tick)

    def _build_stream_url(self) -> str:
        """Build a multi-stream WebSocket URL for all configured symbols."""
        streams = "/".join(f"{symbol.lower()}@trade" for symbol in SYMBOLS)
        return f"{BINANCE_WS_URL}/{streams}"


if __name__ == "__main__":
    from shared.metrics import start_metrics_server
    start_metrics_server()
    connector = ExchangeConnector()
    asyncio.run(connector.run())
