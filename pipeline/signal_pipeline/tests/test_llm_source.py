"""Unit tests for LLMSignalSource — all tests mock the LLM client, no network calls."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.llm_source import LLMSignalSource

from .test_features import _rising_candles
from signal_pipeline.src.features import build_features


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def candles_and_features():
    candles  = _rising_candles(60)
    features = build_features(candles)
    assert features is not None
    return candles, features


@pytest.fixture
def source_with_mock_client(monkeypatch) -> tuple[LLMSignalSource, MagicMock]:
    """LLMSignalSource with a pre-set mock Anthropic client (skips warmup I/O)."""
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    src = LLMSignalSource()
    mock_client = MagicMock()
    src._client = mock_client
    return src, mock_client


# ── Helpers ───────────────────────────────────────────────────────────────────


def _tool_use_response(
    tool_name: str,
    tool_id: str,
    tool_input: dict,
    stop_reason: str = "tool_use",
) -> MagicMock:
    """Build a mock Anthropic response containing a single tool_use block."""
    block = MagicMock()
    block.type  = "tool_use"
    block.name  = tool_name
    block.id    = tool_id
    block.input = tool_input

    response = MagicMock()
    response.stop_reason = stop_reason
    response.content     = [block]
    return response


def _end_turn_response(text: str = "Analysis complete.") -> MagicMock:
    """Build a mock Anthropic response that stops without a tool call."""
    block = MagicMock()
    block.type = "text"
    block.text = text

    response = MagicMock()
    response.stop_reason = "end_turn"
    response.content     = [block]
    return response


# ── Agent loop ────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestAgentLoop:
    def test_emit_signal_on_first_turn_returns_buy(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "BUY", "confidence": 0.85, "reasoning": "Strong bullish trend."},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.BUY
        assert result.confidence == pytest.approx(0.85)
        assert result.source == "llm"
        assert mock_client.messages.create.call_count == 1

    def test_tool_call_then_emit_signal(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.side_effect = [
            _tool_use_response("get_candles", "tu_1", {"symbol": "BTCUSDT", "limit": 20}),
            _tool_use_response(
                "emit_signal", "tu_2",
                {"side": "SELL", "confidence": 0.72, "reasoning": "Bearish momentum."},
            ),
        ]
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.SELL
        assert result.confidence == pytest.approx(0.72)
        assert mock_client.messages.create.call_count == 2

    def test_multi_tool_calls_before_emit(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.side_effect = [
            _tool_use_response("get_technical_indicators", "tu_1", {"symbol": "BTCUSDT"}),
            _tool_use_response("analyze_volatility", "tu_2", {"symbol": "BTCUSDT"}),
            _tool_use_response(
                "emit_signal", "tu_3",
                {"side": "HOLD", "confidence": 0.48, "reasoning": "Mixed signals."},
            ),
        ]
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD
        assert mock_client.messages.create.call_count == 3

    def test_loop_exhausted_returns_hold(
        self, source_with_mock_client, candles_and_features, monkeypatch
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        monkeypatch.setattr("signal_pipeline.src.llm_source.LLM_MAX_LOOP_ITERATIONS", 2)
        # Always returns a non-emit tool call — loop never terminates naturally.
        mock_client.messages.create.return_value = _tool_use_response(
            "get_candles", "tu_x", {"symbol": "BTCUSDT", "limit": 10}
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD
        assert result.confidence == pytest.approx(0.1)
        assert mock_client.messages.create.call_count == 2

    def test_end_turn_without_emit_signal_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _end_turn_response()
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD
        assert result.confidence == pytest.approx(0.1)

    def test_api_exception_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.side_effect = RuntimeError("network error")
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD


# ── Tool execution ────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestToolExecution:
    def test_get_candles_returns_ohlcv_list(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        raw = src._execute_tool(
            "get_candles", {"symbol": "BTCUSDT", "limit": 5}, candles, features
        )
        data = json.loads(raw)
        assert isinstance(data, list)
        assert len(data) == 5
        keys = {"open", "high", "low", "close", "volume"}
        assert keys.issubset(data[0].keys())

    def test_get_candles_respects_limit(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        raw = src._execute_tool(
            "get_candles", {"symbol": "BTCUSDT", "limit": 3}, candles, features
        )
        assert len(json.loads(raw)) == 3

    def test_get_technical_indicators_has_all_fields(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        raw = src._execute_tool(
            "get_technical_indicators", {"symbol": "BTCUSDT"}, candles, features
        )
        data = json.loads(raw)
        for key in ("ema_cross", "rsi_14", "macd", "macd_signal", "macd_hist",
                    "return_1", "return_3", "volatility_21", "return_z",
                    "volume_ratio", "hl_range"):
            assert key in data, f"Missing indicator: {key}"

    def test_analyze_volatility_has_expected_keys(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        raw = src._execute_tool(
            "analyze_volatility", {"symbol": "BTCUSDT"}, candles, features
        )
        data = json.loads(raw)
        assert "price_mean" in data
        assert "price_stdev" in data
        assert "price_range_pct" in data
        assert "momentum_5bar_pct" in data
        assert "current_close" in data

    def test_unknown_tool_returns_error_json(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        raw = src._execute_tool("nonexistent_tool", {}, candles, features)
        data = json.loads(raw)
        assert "error" in data


# ── Retry behaviour ───────────────────────────────────────────────────────────


@pytest.mark.unit
class TestRetryBehaviour:
    def test_retries_on_rate_limit_then_succeeds(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features

        class RateLimitError(Exception):
            pass

        call_count = 0
        good_response = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "BUY", "confidence": 0.75, "reasoning": "test"},
        )

        def mock_create(*args: object, **kwargs: object) -> object:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RateLimitError("rate limited")
            return good_response

        mock_client.messages.create.side_effect = mock_create

        with patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert call_count == 3
        assert result.side == TradeSide.BUY

    def test_returns_hold_after_max_retries_exhausted(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features

        class RateLimitError(Exception):
            pass

        mock_client.messages.create.side_effect = RateLimitError("always rate limited")

        with patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert result.side == TradeSide.HOLD
        assert result.source == "llm"

    def test_non_rate_limit_error_not_retried(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features

        call_count = 0

        def mock_create(*args: object, **kwargs: object) -> object:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("network down")

        mock_client.messages.create.side_effect = mock_create

        with patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert call_count == 1  # no retry for non-rate-limit errors
        assert result.side == TradeSide.HOLD


# ── Warmup / client availability ─────────────────────────────────────────────


@pytest.mark.unit
class TestWarmup:
    def test_missing_anthropic_key_leaves_client_none(self, monkeypatch) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "anthropic")
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        src = LLMSignalSource()
        src.warmup()
        assert src._client is None

    def test_missing_openai_key_leaves_client_none(self, monkeypatch) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        src = LLMSignalSource()
        src.warmup()
        assert src._client is None

    def test_unknown_provider_leaves_client_none(self, monkeypatch) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "gemini")
        src = LLMSignalSource()
        src.warmup()
        assert src._client is None

    def test_predict_with_no_client_returns_hold(
        self, monkeypatch, candles_and_features
    ) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "anthropic")
        src = LLMSignalSource()
        src._client = None
        candles, features = candles_and_features
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD
        assert result.source == "llm"
        assert result.confidence == pytest.approx(0.1)


# ── Signal field invariants ───────────────────────────────────────────────────


@pytest.mark.unit
class TestSignalFields:
    def test_source_field_is_llm(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "BUY", "confidence": 0.8, "reasoning": "test"},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.source == "llm"

    def test_symbol_preserved(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "HOLD", "confidence": 0.4, "reasoning": "test"},
        )
        result = src.predict("ETHUSDT", candles, features)
        assert result.symbol == "ETHUSDT"

    def test_expires_after_generated(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "BUY", "confidence": 0.7, "reasoning": "test"},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.expires_at > result.generated_at

    def test_unique_signal_ids(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "SELL", "confidence": 0.65, "reasoning": "test"},
        )
        r1 = src.predict("BTCUSDT", candles, features)
        r2 = src.predict("BTCUSDT", candles, features)
        assert r1.id != r2.id

    def test_confidence_clamped_above_1(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "BUY", "confidence": 1.5, "reasoning": "test"},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.BUY
        assert result.confidence == pytest.approx(1.0)

    def test_confidence_clamped_below_0(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "SELL", "confidence": -0.2, "reasoning": "test"},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.SELL
        assert result.confidence == pytest.approx(0.0)

    def test_invalid_side_in_emit_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        mock_client.messages.create.return_value = _tool_use_response(
            "emit_signal", "tu_1",
            {"side": "MAYBE", "confidence": 0.5, "reasoning": "test"},
        )
        result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD

    def test_confidence_in_valid_range(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, mock_client = source_with_mock_client
        candles, features = candles_and_features
        for side, conf in [("BUY", 0.9), ("SELL", 0.6), ("HOLD", 0.45)]:
            mock_client.messages.create.return_value = _tool_use_response(
                "emit_signal", "tu_x",
                {"side": side, "confidence": conf, "reasoning": "test"},
            )
            result = src.predict("BTCUSDT", candles, features)
            assert 0.0 <= result.confidence <= 1.0
