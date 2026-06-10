"""Unit tests for the OHLCV candle aggregator."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from candle_aggregator.src.aggregator import CandleAggregator, _floor_to_window
from shared.models import Tick


def _tick(symbol: str, price: float, volume: float, ts: datetime) -> Tick:
    return Tick(symbol=symbol, price=price, volume=volume, timestamp=ts)


def _utc(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour, minute, second, tzinfo=timezone.utc)


class TestFloorToWindow:
    def test_already_on_boundary(self) -> None:
        dt = _utc(9, 45)
        assert _floor_to_window(dt, 5) == dt

    def test_floors_to_nearest_5_min(self) -> None:
        dt = _utc(9, 47, 30)
        assert _floor_to_window(dt, 5) == _utc(9, 45)

    def test_floors_to_nearest_1_min(self) -> None:
        dt = _utc(9, 47, 30)
        assert _floor_to_window(dt, 1) == _utc(9, 47)


@pytest.fixture()
def aggregator() -> CandleAggregator:
    with (
        patch("candle_aggregator.src.aggregator.KafkaClientFactory") as mock_factory,
    ):
        mock_factory.create_consumer.return_value = MagicMock()
        mock_factory.create_producer.return_value = MagicMock()
        agg = CandleAggregator()
    return agg


class TestCandleAggregator:
    def test_ticks_buffered_within_window(self, aggregator: CandleAggregator) -> None:
        t0 = _utc(9, 45)
        aggregator._process_tick(_tick("BTCUSDT", 50_000.0, 0.1, t0))
        aggregator._process_tick(_tick("BTCUSDT", 50_100.0, 0.2, _utc(9, 46)))
        assert len(aggregator._tick_buffers["BTCUSDT"]) == 2

    def test_candle_emitted_when_window_closes(self, aggregator: CandleAggregator) -> None:
        published: list[object] = []

        def capture_publish(candle: object) -> None:
            published.append(candle)

        aggregator._publish_candle = capture_publish  # type: ignore[method-assign]

        t0 = _utc(9, 45)
        aggregator._process_tick(_tick("BTCUSDT", 50_000.0, 0.1, t0))
        aggregator._process_tick(_tick("BTCUSDT", 50_100.0, 0.2, _utc(9, 46)))
        # Tick outside window triggers emit.
        aggregator._process_tick(_tick("BTCUSDT", 50_200.0, 0.3, _utc(9, 50)))

        assert len(published) == 1

    def test_ohlcv_values_correct(self, aggregator: CandleAggregator) -> None:
        captured = None

        def capture(candle: object) -> None:
            nonlocal captured
            captured = candle

        aggregator._publish_candle = capture  # type: ignore[method-assign]

        aggregator._process_tick(_tick("BTCUSDT", 100.0, 1.0, _utc(9, 45)))
        aggregator._process_tick(_tick("BTCUSDT", 120.0, 2.0, _utc(9, 46)))
        aggregator._process_tick(_tick("BTCUSDT", 90.0,  1.5, _utc(9, 47)))
        aggregator._process_tick(_tick("BTCUSDT", 110.0, 1.0, _utc(9, 48)))
        # Cross window boundary to trigger emit.
        aggregator._process_tick(_tick("BTCUSDT", 115.0, 0.5, _utc(9, 50)))

        assert captured is not None
        from shared.models import Candle
        candle: Candle = captured
        assert candle.open == 100.0
        assert candle.high == 120.0
        assert candle.low == 90.0
        assert candle.close == 110.0
        assert candle.volume == pytest.approx(5.5)

    def test_buffer_resets_after_window_close(self, aggregator: CandleAggregator) -> None:
        aggregator._publish_candle = MagicMock()  # type: ignore[method-assign]

        aggregator._process_tick(_tick("BTCUSDT", 100.0, 1.0, _utc(9, 45)))
        aggregator._process_tick(_tick("BTCUSDT", 105.0, 1.0, _utc(9, 50)))  # new window

        assert len(aggregator._tick_buffers["BTCUSDT"]) == 1

    def test_partial_window_not_emitted_mid_stream(self, aggregator: CandleAggregator) -> None:
        published: list[object] = []
        aggregator._publish_candle = published.append  # type: ignore[method-assign]

        aggregator._process_tick(_tick("BTCUSDT", 100.0, 1.0, _utc(9, 45)))
        aggregator._process_tick(_tick("BTCUSDT", 101.0, 1.0, _utc(9, 46)))

        assert len(published) == 0

    def test_independent_symbol_buffers(self, aggregator: CandleAggregator) -> None:
        aggregator._process_tick(_tick("BTCUSDT", 100.0, 1.0, _utc(9, 45)))
        aggregator._process_tick(_tick("ETHUSDT", 2_000.0, 5.0, _utc(9, 45)))

        assert len(aggregator._tick_buffers["BTCUSDT"]) == 1
        assert len(aggregator._tick_buffers["ETHUSDT"]) == 1
