"""Unit tests for the signal pipeline runner."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from shared.exceptions import DatabaseError
from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.base import BaseSignalSource
from signal_pipeline.src.llm_source import LLMSignalSource
from signal_pipeline.src.model import MLSignalSource
from signal_pipeline.src.runner import _build_sources, _fetch_candles, _generate_and_publish

from .test_features import _rising_candles


def _fake_signal(source: str = "ml") -> TradeSignal:
    now = datetime.now(tz=timezone.utc)
    return TradeSignal(
        id="sig-test", symbol="BTCUSDT", side=TradeSide.BUY,
        confidence=0.8, generated_at=now, expires_at=now + timedelta(seconds=90),
        source=source,
    )


class TestGenerateAndPublish:
    def test_skips_when_no_candles(self) -> None:
        src  = MagicMock(spec=BaseSignalSource)
        prod = MagicMock()
        with patch("signal_pipeline.src.runner._fetch_candles", return_value=[]):
            _generate_and_publish("BTCUSDT", src, prod)
        src.predict.assert_not_called()

    def test_publishes_when_candles_available(self) -> None:
        src = MagicMock(spec=BaseSignalSource)
        src.predict.return_value = _fake_signal("ml")
        prod = MagicMock()

        with patch("signal_pipeline.src.runner._fetch_candles",
                   return_value=_rising_candles(60)):
            _generate_and_publish("BTCUSDT", src, prod)

        src.predict.assert_called_once()
        prod.produce.assert_called_once()
        prod.flush.assert_called_once()

    def test_skips_when_insufficient_for_features(self) -> None:
        src  = MagicMock(spec=BaseSignalSource)
        prod = MagicMock()
        with patch("signal_pipeline.src.runner._fetch_candles",
                   return_value=_rising_candles(10)):
            _generate_and_publish("BTCUSDT", src, prod)
        src.predict.assert_not_called()

    def test_predict_receives_candles_and_features(self) -> None:
        src = MagicMock(spec=BaseSignalSource)
        src.predict.return_value = _fake_signal("llm")
        prod = MagicMock()
        candles = _rising_candles(60)

        with patch("signal_pipeline.src.runner._fetch_candles", return_value=candles):
            _generate_and_publish("BTCUSDT", src, prod)

        call_args = src.predict.call_args
        assert call_args[0][0] == "BTCUSDT"      # symbol
        assert call_args[0][1] is candles         # candles passed through


class TestBuildSources:
    def test_ml_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("signal_pipeline.src.runner.SIGNAL_SOURCE", "ml")
        sources = _build_sources()
        assert set(sources.keys()) == {"ml"}
        assert isinstance(sources["ml"], MLSignalSource)

    def test_llm_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("signal_pipeline.src.runner.SIGNAL_SOURCE", "llm")
        sources = _build_sources()
        assert set(sources.keys()) == {"llm"}
        assert isinstance(sources["llm"], LLMSignalSource)

    def test_both(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("signal_pipeline.src.runner.SIGNAL_SOURCE", "both")
        sources = _build_sources()
        assert set(sources.keys()) == {"ml", "llm"}

    def test_invalid_raises_value_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("signal_pipeline.src.runner.SIGNAL_SOURCE", "invalid")
        with pytest.raises(ValueError, match="SIGNAL_SOURCE"):
            _build_sources()


class TestFetchCandles:
    def test_returns_empty_on_db_error(self) -> None:
        with patch("signal_pipeline.src.runner.get_db_connection",
                   side_effect=DatabaseError("no db")):
            assert _fetch_candles("BTCUSDT", limit=100) == []
