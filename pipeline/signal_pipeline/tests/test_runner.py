"""Unit tests for the signal pipeline runner."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from shared.exceptions import DatabaseError
from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.model import SignalGenerator
from signal_pipeline.src.runner import _fetch_candles, _generate_and_publish

from .test_features import _rising_candles


def _fake_signal() -> TradeSignal:
    now = datetime.now(tz=timezone.utc)
    return TradeSignal(
        id="sig-test", symbol="BTCUSDT", side=TradeSide.BUY,
        confidence=0.8, generated_at=now, expires_at=now + timedelta(seconds=90),
    )


class TestGenerateAndPublish:
    def test_skips_when_no_candles(self) -> None:
        gen, prod = MagicMock(spec=SignalGenerator), MagicMock()
        with patch("signal_pipeline.src.runner._fetch_candles", return_value=[]):
            _generate_and_publish("BTCUSDT", gen, prod)
        gen.predict.assert_not_called()

    def test_publishes_when_candles_available(self) -> None:
        gen       = MagicMock(spec=SignalGenerator)
        gen.predict.return_value = _fake_signal()
        gen.trained_on_real      = True
        prod = MagicMock()

        with patch("signal_pipeline.src.runner._fetch_candles",
                   return_value=_rising_candles(60)):
            _generate_and_publish("BTCUSDT", gen, prod)

        gen.predict.assert_called_once()
        prod.produce.assert_called_once()
        prod.flush.assert_called_once()

    def test_skips_when_insufficient_for_features(self) -> None:
        gen, prod = MagicMock(spec=SignalGenerator), MagicMock()
        with patch("signal_pipeline.src.runner._fetch_candles",
                   return_value=_rising_candles(10)):
            _generate_and_publish("BTCUSDT", gen, prod)
        gen.predict.assert_not_called()


class TestFetchCandles:
    def test_returns_empty_on_db_error(self) -> None:
        with patch("signal_pipeline.src.runner.get_db_connection",
                   side_effect=DatabaseError("no db")):
            assert _fetch_candles("BTCUSDT", limit=100) == []
