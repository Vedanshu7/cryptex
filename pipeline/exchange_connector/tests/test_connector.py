"""Unit tests for the exchange connector's universe-driven behavior."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from exchange_connector.src.connector import ExchangeConnector
from shared.exceptions import KafkaPublishError
from shared.universe import Universe


def _universe(version: int, symbols: list[str]) -> Universe:
    return Universe.model_validate(
        {"version": version, "regions": {"tokyo": {"exchange": "binance", "symbols": symbols}}}
    )


@pytest.fixture()
def connector() -> ExchangeConnector:
    with patch("exchange_connector.src.connector.KafkaClientFactory") as mock_factory:
        mock_factory.create_producer.return_value = MagicMock()
        return ExchangeConnector()


class TestBuildStreamUrl:
    def test_builds_url_from_binance_symbols_only(self, connector: ExchangeConnector) -> None:
        universe = Universe.model_validate(
            {
                "version": 1,
                "regions": {
                    "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT", "ETHUSDT"]},
                    "eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]},
                },
            }
        )

        with patch("exchange_connector.src.connector.get_universe", return_value=universe):
            url = connector._build_stream_url()

        assert "btcusdt@trade" in url
        assert "ethusdt@trade" in url
        # Deribit's symbol must never leak into the Binance stream URL.
        assert "btc-perpetual" not in url.lower()

    def test_reflects_widened_universe(self, connector: ExchangeConnector) -> None:
        with patch(
            "exchange_connector.src.connector.get_universe",
            return_value=_universe(2, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
        ):
            url = connector._build_stream_url()

        assert "solusdt@trade" in url


class TestHandleMessage:
    def test_publish_failure_routes_to_dlq_instead_of_raising(
        self, connector: ExchangeConnector
    ) -> None:
        raw = json.dumps({"s": "BTCUSDT", "p": "100.0", "q": "1.0", "T": 1_704_102_300_000})
        connector._tick_producer.publish = MagicMock(  # type: ignore[method-assign]
            side_effect=KafkaPublishError("delivery failed")
        )
        connector._dlq.publish = MagicMock()  # type: ignore[method-assign]

        with patch(
            "exchange_connector.src.connector.normalize",
            return_value=MagicMock(symbol="BTCUSDT", model_dump_json=lambda: "{}"),
        ):
            connector._handle_message(raw)  # must not raise

        connector._dlq.publish.assert_called_once()
        assert connector._dlq.publish.call_args.kwargs["source_topic"] == "market-data-raw"


class TestWatchForUniverseChange:
    @pytest.mark.asyncio
    async def test_returns_once_version_changes(self, connector: ExchangeConnector) -> None:
        versions = iter([1, 1, 2])

        def _fake_universe() -> Universe:
            return _universe(next(versions), ["BTCUSDT"])

        with (
            patch("exchange_connector.src.connector.get_universe", side_effect=_fake_universe),
            patch("exchange_connector.src.connector.UNIVERSE_POLL_SECONDS", 0),
        ):
            await asyncio.wait_for(
                connector._watch_for_universe_change(universe_version=1), timeout=1.0
            )
