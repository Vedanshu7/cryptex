"""Unit tests for the OHLCV candle aggregator.

Tests target the pure reshape/persist functions directly rather than the Quix
Streams topology wiring (`build_application`) — that wiring needs a live
Kafka broker to exercise meaningfully and is left to integration testing.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from quixstreams.models.timestamps import TimestampType

from candle_aggregator.src.aggregator import (
    _build_candle,
    _extract_tick_timestamp,
    _on_window_closed,
    _persist_candle,
)
from shared.exceptions import DatabaseError, RetryExhaustedError
from shared.models import Candle


def _utc(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour, minute, second, tzinfo=UTC)


def _window(
    start_ms: int,
    end_ms: int,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> dict[str, Any]:
    return {
        "start": start_ms,
        "end": end_ms,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class TestBuildCandle:
    def test_ohlcv_values_correct(self) -> None:
        window = _window(
            start_ms=1_704_102_300_000,  # 2024-01-01 09:45:00 UTC
            end_ms=1_704_102_600_000,  # 2024-01-01 09:50:00 UTC
            open_=100.0,
            high=120.0,
            low=90.0,
            close=110.0,
            volume=5.5,
        )

        candle = _build_candle(symbol="BTCUSDT", window=window, timeframe_minutes=5)

        assert candle.symbol == "BTCUSDT"
        assert candle.open == 100.0
        assert candle.high == 120.0
        assert candle.low == 90.0
        assert candle.close == 110.0
        assert candle.volume == pytest.approx(5.5)
        assert candle.timeframe == "5m"

    def test_window_boundaries_become_opened_and_closed_at(self) -> None:
        window = _window(
            start_ms=1_704_102_300_000,
            end_ms=1_704_102_600_000,
            open_=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=1.0,
        )

        candle = _build_candle(symbol="ETHUSDT", window=window, timeframe_minutes=5)

        assert candle.opened_at == _utc(9, 45)
        assert candle.closed_at == _utc(9, 50)

    def test_different_timeframe_label(self) -> None:
        window = _window(
            start_ms=0, end_ms=60_000, open_=1.0, high=1.0, low=1.0, close=1.0, volume=1.0
        )

        candle = _build_candle(symbol="BTCUSDT", window=window, timeframe_minutes=1)

        assert candle.timeframe == "1m"


class TestPersistCandle:
    def _candle(self) -> Candle:
        return Candle(
            symbol="BTCUSDT",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=2.0,
            opened_at=_utc(9, 45),
            closed_at=_utc(9, 50),
            timeframe="5m",
        )

    def test_persists_via_db_connection(self) -> None:
        mock_conn = MagicMock()
        with patch("candle_aggregator.src.aggregator.get_db_connection") as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            _persist_candle(self._candle())

        mock_conn.cursor.return_value.__enter__.return_value.execute.assert_called_once()

    def test_db_error_retries_then_raises_retry_exhausted(self) -> None:
        with (
            patch("candle_aggregator.src.aggregator.get_db_connection") as mock_get_conn,
            patch("shared.dlq.time.sleep"),
        ):
            mock_get_conn.side_effect = DatabaseError("db unavailable")
            with pytest.raises(RetryExhaustedError):
                _persist_candle(self._candle())

        assert mock_get_conn.call_count == 3

    def test_non_database_error_is_not_retried(self) -> None:
        with patch("candle_aggregator.src.aggregator.get_db_connection") as mock_get_conn:
            mock_get_conn.side_effect = RuntimeError("unexpected bug")
            with pytest.raises(RuntimeError):
                _persist_candle(self._candle())

        assert mock_get_conn.call_count == 1


class TestExtractTickTimestamp:
    def test_uses_tick_event_time_not_kafka_timestamp(self) -> None:
        tick_value = {
            "symbol": "BTCUSDT",
            "price": 100.0,
            "volume": 1.0,
            "timestamp": "2024-01-01T09:45:00Z",
        }
        kafka_broker_timestamp_ms = 0  # deliberately different from the tick's own time

        extracted = _extract_tick_timestamp(
            tick_value, None, kafka_broker_timestamp_ms, TimestampType.TIMESTAMP_CREATE_TIME
        )

        assert extracted == int(_utc(9, 45).timestamp() * 1000)


class TestOnWindowClosed:
    def test_reshapes_persists_and_returns_json_payload(self) -> None:
        window = _window(
            start_ms=1_704_102_300_000,
            end_ms=1_704_102_600_000,
            open_=100.0,
            high=120.0,
            low=90.0,
            close=110.0,
            volume=5.5,
        )

        with patch("candle_aggregator.src.aggregator._persist_candle") as mock_persist:
            payload = _on_window_closed(window, "BTCUSDT", 1_704_102_600_000, None)

        mock_persist.assert_called_once()
        assert payload["symbol"] == "BTCUSDT"
        assert payload["open"] == 100.0
        assert payload["close"] == 110.0
        assert payload["timeframe"] == "5m"
