"""Unit tests for LLMSignalSource — all tests mock the LLM client, no network calls."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from shared.models import TradeSide, TradeSignal
from signal_pipeline.src.features import FEATURE_COLS, build_features
from signal_pipeline.src.llm_source import LLMSignalSource

from .test_features import _rising_candles


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def candles_and_features():
    candles  = _rising_candles(60)
    features = build_features(candles)
    assert features is not None
    return candles, features


@pytest.fixture
def source_with_mock_client(monkeypatch) -> tuple[LLMSignalSource, MagicMock]:
    """LLMSignalSource with a pre-set mock client (skips warmup I/O)."""
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    src = LLMSignalSource()
    mock_client = MagicMock()
    src._client = mock_client
    return src, mock_client


def _make_llm_response(side: str, confidence: float, reasoning: str = "test signal") -> str:
    return json.dumps({"side": side, "confidence": confidence, "reasoning": reasoning})


# ── Prompt building ───────────────────────────────────────────────────────────


@pytest.mark.unit
class TestPromptBuilding:
    def test_prompt_contains_symbol(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        prompt = src._build_prompt("BTCUSDT", candles, features)
        assert "BTCUSDT" in prompt

    def test_prompt_contains_candle_rows(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        prompt = src._build_prompt("BTCUSDT", candles, features)
        assert "| 1 |" in prompt

    def test_prompt_limits_candles_to_window(
        self, monkeypatch, candles_and_features
    ) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "anthropic")
        monkeypatch.setattr("signal_pipeline.src.llm_source.LLM_CANDLE_WINDOW", 5)
        src = LLMSignalSource()
        src._client = MagicMock()
        candles, features = candles_and_features
        prompt = src._build_prompt("BTCUSDT", candles, features)
        assert "| 5 |" in prompt
        assert "| 6 |" not in prompt

    def test_prompt_contains_all_feature_names(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        prompt = src._build_prompt("BTCUSDT", candles, features)
        assert "EMA Cross" in prompt
        assert "RSI (14)" in prompt
        assert "MACD" in prompt
        assert "Volume Ratio" in prompt


# ── Response parsing ──────────────────────────────────────────────────────────


@pytest.mark.unit
class TestResponseParsing:
    def test_parses_buy_signal(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("BUY", 0.85)):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.BUY
        assert result.confidence == pytest.approx(0.85)
        assert result.source == "llm"

    def test_parses_sell_signal(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("SELL", 0.72)):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.SELL

    def test_parses_hold_signal(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("HOLD", 0.45)):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD

    def test_malformed_json_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry", return_value="not valid json at all"):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD
        assert result.confidence == pytest.approx(0.1)

    def test_missing_side_key_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=json.dumps({"confidence": 0.8})):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD

    def test_missing_confidence_key_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=json.dumps({"side": "BUY"})):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD

    def test_invalid_side_value_returns_hold(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=json.dumps({"side": "MAYBE", "confidence": 0.5})):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.HOLD

    def test_confidence_clamped_above_1(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=json.dumps({"side": "BUY", "confidence": 1.5})):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.BUY
        assert result.confidence == pytest.approx(1.0)

    def test_confidence_clamped_below_0(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=json.dumps({"side": "SELL", "confidence": -0.2})):
            result = src.predict("BTCUSDT", candles, features)
        assert result.side == TradeSide.SELL
        assert result.confidence == pytest.approx(0.0)


# ── Retry behaviour ───────────────────────────────────────────────────────────


@pytest.mark.unit
class TestRetryBehaviour:
    def test_retries_on_rate_limit_then_succeeds(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features

        # Rate limit error whose class name matches the retry condition.
        class RateLimitError(Exception):
            pass

        good_response = _make_llm_response("BUY", 0.75)
        call_count = 0

        def side_effect(prompt: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RateLimitError("rate limited")
            return good_response

        with patch.object(src, "_call_provider", side_effect=side_effect), \
             patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert call_count == 3
        assert result.side == TradeSide.BUY

    def test_returns_hold_after_max_retries_exhausted(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features

        class RateLimitError(Exception):
            pass

        with patch.object(src, "_call_provider", side_effect=RateLimitError("always")), \
             patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert result.side == TradeSide.HOLD
        assert result.source == "llm"

    def test_non_rate_limit_error_not_retried(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features

        call_count = 0

        def side_effect(prompt: str) -> str:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("network down")

        with patch.object(src, "_call_provider", side_effect=side_effect), \
             patch("signal_pipeline.src.llm_source.time.sleep"):
            result = src.predict("BTCUSDT", candles, features)

        assert call_count == 1   # no retry for non-rate-limit error
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
    def test_source_field_is_llm(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("BUY", 0.8)):
            result = src.predict("BTCUSDT", candles, features)
        assert result.source == "llm"

    def test_symbol_preserved(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("HOLD", 0.4)):
            result = src.predict("ETHUSDT", candles, features)
        assert result.symbol == "ETHUSDT"

    def test_expires_after_generated(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("BUY", 0.7)):
            result = src.predict("BTCUSDT", candles, features)
        assert result.expires_at > result.generated_at

    def test_unique_signal_ids(self, source_with_mock_client, candles_and_features) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        with patch.object(src, "_call_with_retry",
                          return_value=_make_llm_response("SELL", 0.65)):
            r1 = src.predict("BTCUSDT", candles, features)
            r2 = src.predict("BTCUSDT", candles, features)
        assert r1.id != r2.id

    def test_confidence_in_valid_range(
        self, source_with_mock_client, candles_and_features
    ) -> None:
        src, _ = source_with_mock_client
        candles, features = candles_and_features
        for side, conf in [("BUY", 0.9), ("SELL", 0.6), ("HOLD", 0.45)]:
            with patch.object(src, "_call_with_retry",
                              return_value=_make_llm_response(side, conf)):
                result = src.predict("BTCUSDT", candles, features)
            assert 0.0 <= result.confidence <= 1.0
