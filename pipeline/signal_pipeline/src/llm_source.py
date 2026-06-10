"""LLM-backed agentic signal source.

Uses Claude's native tool_use API for multi-step ReAct reasoning, giving the
agent active control over what market data it inspects before producing a trade
signal. OpenAI's function-calling API follows the same loop pattern.

Agent lifecycle per predict() call:
    1. Send initial task: "Analyse {symbol} and emit a trade signal."
    2. Agent calls tools to fetch candles, indicators, or volatility stats.
    3. When ready, the agent calls emit_signal — the only way to produce a
       structured output (enforced by the tool schema).
    4. If the loop exhausts its iteration budget, return a safe HOLD fallback.

Supported providers (LLM_PROVIDER env var):
    anthropic  — requires anthropic>=0.25 and ANTHROPIC_API_KEY
    openai     — requires openai>=1.0 and OPENAI_API_KEY
"""

from __future__ import annotations

import json
import os
import statistics
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from shared.logger import get_logger
from shared.models import Candle, TradeSide, TradeSignal

from .base import BaseSignalSource
from .features import FEATURE_COLS

_logger = get_logger(__name__)

LLM_PROVIDER: str              = os.getenv("LLM_PROVIDER", "anthropic").lower()
LLM_CANDLE_WINDOW: int         = int(os.getenv("LLM_CANDLE_WINDOW", "20"))
LLM_MAX_RETRIES: int           = int(os.getenv("LLM_MAX_RETRIES", "3"))
LLM_RETRY_BASE_DELAY: float    = float(os.getenv("LLM_RETRY_BASE_DELAY", "2.0"))
LLM_MAX_LOOP_ITERATIONS: int   = int(os.getenv("LLM_MAX_LOOP_ITERATIONS", "8"))
LLM_MAX_TOKENS: int            = int(os.getenv("LLM_MAX_TOKENS", "4096"))
SIGNAL_TTL_SECONDS: int        = int(os.getenv("SIGNAL_TTL_SECONDS", "90"))

_DEFAULT_MODELS: dict[str, str] = {
    "anthropic": "claude-opus-4-8",
    "openai":    "gpt-4o",
}

# ── Tool definitions ──────────────────────────────────────────────────────────

_TOOLS: list[dict[str, Any]] = [
    {
        "name": "get_candles",
        "description": (
            "Fetch recent OHLCV candlestick data for the symbol. "
            "Each candle covers one 5-minute bar (oldest first)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
                "limit": {
                    "type": "integer",
                    "description": "Number of most-recent candles to return (1–100)",
                    "minimum": 1,
                    "maximum": 100,
                },
            },
            "required": ["symbol", "limit"],
        },
    },
    {
        "name": "get_technical_indicators",
        "description": (
            "Return the latest computed technical indicators: EMA cross, RSI (14), "
            "MACD and histogram, 1-bar and 3-bar returns, 21-bar volatility, "
            "return z-score, volume ratio, and H-L range."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "analyze_volatility",
        "description": (
            "Return a statistical summary of recent price volatility: mean, standard "
            "deviation, price range (%), and 5-bar momentum (%) over the last 20 candles."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Trading pair, e.g. BTCUSDT"},
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "emit_signal",
        "description": (
            "Emit the final trade signal. Call this exactly once after reviewing "
            "sufficient market data. This is the only way to produce a trading decision."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "side": {
                    "type": "string",
                    "enum": ["BUY", "SELL", "HOLD"],
                    "description": (
                        "Trade direction. Prefer HOLD when signals are mixed or "
                        "conviction is low."
                    ),
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": (
                        "Signal confidence 0.0–1.0. "
                        "Use >= 0.7 for high conviction. "
                        "Use < 0.5 only with HOLD."
                    ),
                },
                "reasoning": {
                    "type": "string",
                    "description": "1-2 sentence explanation of the trading decision.",
                },
            },
            "required": ["side", "confidence", "reasoning"],
        },
    },
]

# OpenAI wraps the same schema in a "function" envelope
_OPENAI_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    }
    for t in _TOOLS
]

_INITIAL_MESSAGE = (
    "You are a quantitative trading analyst making a real-time trading decision.\n\n"
    "Symbol: {symbol}\n\n"
    "Use the available tools to fetch the market data you need — candle history, "
    "technical indicators, and volatility analysis. Reason step-by-step about trend "
    "direction (EMA cross, MACD), momentum (RSI, return z-score), and volume "
    "confirmation. When you have enough information, call emit_signal with your "
    "final BUY, SELL, or HOLD decision."
)


class LLMSignalSource(BaseSignalSource):
    """Agentic signal source driven by Claude's tool_use API (or OpenAI function calling)."""

    def __init__(self) -> None:
        self._provider: str      = LLM_PROVIDER
        self._model: str         = os.getenv("LLM_MODEL", _DEFAULT_MODELS.get(self._provider, ""))
        self._client: Any | None = None

    # ── BaseSignalSource interface ────────────────────────────────────────────

    def warmup(self) -> None:
        """Instantiate the SDK client and validate API key presence."""
        if self._provider == "anthropic":
            self._warmup_anthropic()
        elif self._provider == "openai":
            self._warmup_openai()
        else:
            _logger.warning(
                "Unknown LLM provider — LLM source disabled.",
                extra={"provider": self._provider},
            )

    def predict(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> TradeSignal:
        """Run the agentic tool-use loop and return a TradeSignal.

        Falls back to a low-confidence HOLD on any failure.
        """
        if self._client is None:
            _logger.warning(
                "LLM client not available — returning HOLD fallback.",
                extra={"symbol": symbol},
            )
            return self._make_hold_signal(symbol)

        try:
            if self._provider == "anthropic":
                return self._run_anthropic_loop(symbol, candles, features)
            return self._run_openai_loop(symbol, candles, features)
        except Exception as exc:
            _logger.error(
                "Agent loop raised unexpected error — HOLD fallback.",
                extra={"symbol": symbol, "error": str(exc)},
            )
            return self._make_hold_signal(symbol)

    # ── Warmup helpers ────────────────────────────────────────────────────────

    def _warmup_anthropic(self) -> None:
        key = os.getenv("ANTHROPIC_API_KEY")
        if not key:
            _logger.warning("ANTHROPIC_API_KEY not set — LLM source disabled.")
            return
        try:
            import anthropic  # type: ignore[import-untyped]

            self._client = anthropic.Anthropic(api_key=key)
            _logger.info(
                "LLM source ready.",
                extra={"provider": "anthropic", "model": self._model},
            )
        except ImportError:
            _logger.warning("anthropic package not installed — LLM source disabled.")

    def _warmup_openai(self) -> None:
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            _logger.warning("OPENAI_API_KEY not set — LLM source disabled.")
            return
        try:
            import openai  # type: ignore[import-untyped]

            self._client = openai.OpenAI(api_key=key)
            _logger.info(
                "LLM source ready.",
                extra={"provider": "openai", "model": self._model},
            )
        except ImportError:
            _logger.warning("openai package not installed — LLM source disabled.")

    # ── Anthropic agentic loop ────────────────────────────────────────────────

    def _run_anthropic_loop(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> TradeSignal:
        """ReAct tool-use loop using Anthropic's messages API."""
        messages: list[dict[str, Any]] = [
            {"role": "user", "content": _INITIAL_MESSAGE.format(symbol=symbol)},
        ]

        for iteration in range(LLM_MAX_LOOP_ITERATIONS):
            try:
                response = self._call_with_retry(
                    lambda: self._call_anthropic(messages)
                )
            except Exception as exc:
                _logger.error(
                    "Anthropic API error — HOLD fallback.",
                    extra={"symbol": symbol, "error": str(exc), "iteration": iteration},
                )
                return self._make_hold_signal(symbol)

            emit_block = None
            other_blocks: list[Any] = []
            for block in response.content:
                if getattr(block, "type", None) == "tool_use":
                    if block.name == "emit_signal":
                        emit_block = block
                    else:
                        other_blocks.append(block)

            if emit_block is not None:
                _logger.info(
                    "Agent emitted signal.",
                    extra={"symbol": symbol, "iterations": iteration + 1},
                )
                return self._signal_from_emit(symbol, emit_block.input)

            if response.stop_reason != "tool_use" or not other_blocks:
                _logger.warning(
                    "Agent stopped without emit_signal — HOLD fallback.",
                    extra={"symbol": symbol, "stop_reason": response.stop_reason},
                )
                return self._make_hold_signal(symbol)

            messages.append({"role": "assistant", "content": response.content})
            messages.append({
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": self._execute_tool(block.name, block.input, candles, features),
                    }
                    for block in other_blocks
                ],
            })

        _logger.warning(
            "Agent loop exceeded max iterations — HOLD fallback.",
            extra={"symbol": symbol},
        )
        return self._make_hold_signal(symbol)

    def _call_anthropic(self, messages: list[dict[str, Any]]) -> Any:
        """Build and dispatch a single Anthropic messages.create call."""
        kwargs: dict[str, Any] = {
            "model":      self._model,
            "max_tokens": LLM_MAX_TOKENS,
            "tools":      _TOOLS,
            "messages":   messages,
        }
        if "opus" in self._model:
            kwargs["thinking"] = {"type": "adaptive"}
        return self._client.messages.create(**kwargs)  # type: ignore[union-attr]

    # ── OpenAI agentic loop ───────────────────────────────────────────────────

    def _run_openai_loop(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> TradeSignal:
        """Function-calling loop using OpenAI's chat completions API."""
        messages: list[dict[str, Any]] = [
            {"role": "user", "content": _INITIAL_MESSAGE.format(symbol=symbol)},
        ]

        for iteration in range(LLM_MAX_LOOP_ITERATIONS):
            try:
                response = self._call_with_retry(
                    lambda: self._client.chat.completions.create(  # type: ignore[union-attr]
                        model=self._model,
                        max_tokens=LLM_MAX_TOKENS,
                        tools=_OPENAI_TOOLS,
                        tool_choice="auto",
                        messages=messages,
                    )
                )
            except Exception as exc:
                _logger.error(
                    "OpenAI API error — HOLD fallback.",
                    extra={"symbol": symbol, "error": str(exc), "iteration": iteration},
                )
                return self._make_hold_signal(symbol)

            msg = response.choices[0].message
            tool_calls = msg.tool_calls or []

            emit_call = None
            other_calls: list[Any] = []
            for tc in tool_calls:
                if tc.function.name == "emit_signal":
                    emit_call = tc
                else:
                    other_calls.append(tc)

            if emit_call is not None:
                _logger.info(
                    "Agent emitted signal.",
                    extra={"symbol": symbol, "iterations": iteration + 1},
                )
                return self._signal_from_emit(symbol, json.loads(emit_call.function.arguments))

            if not tool_calls:
                _logger.warning(
                    "Agent stopped without emit_signal — HOLD fallback.",
                    extra={"symbol": symbol},
                )
                return self._make_hold_signal(symbol)

            messages.append({
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in tool_calls
                ],
            })
            for tc in other_calls:
                result = self._execute_tool(
                    tc.function.name,
                    json.loads(tc.function.arguments),
                    candles,
                    features,
                )
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

        _logger.warning(
            "Agent loop exceeded max iterations — HOLD fallback.",
            extra={"symbol": symbol},
        )
        return self._make_hold_signal(symbol)

    # ── Retry wrapper ─────────────────────────────────────────────────────────

    def _call_with_retry(self, api_call: Callable[[], Any]) -> Any:
        """Call api_call() with exponential backoff on RateLimitError."""
        last_exc: Exception | None = None
        for attempt in range(LLM_MAX_RETRIES):
            try:
                return api_call()
            except Exception as exc:
                if type(exc).__name__ == "RateLimitError":
                    last_exc = exc
                    delay = LLM_RETRY_BASE_DELAY * (2 ** attempt)
                    _logger.warning(
                        "LLM rate limit — retrying.",
                        extra={"attempt": attempt + 1, "delay_seconds": delay},
                    )
                    time.sleep(delay)
                else:
                    raise
        raise last_exc or RuntimeError("LLM call failed after all retries.")

    # ── Tool implementations ──────────────────────────────────────────────────

    def _execute_tool(
        self,
        name: str,
        tool_input: dict[str, Any],
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> str:
        """Dispatch a tool call and return a JSON string result."""
        if name == "get_candles":
            return self._tool_get_candles(tool_input, candles)
        if name == "get_technical_indicators":
            return self._tool_get_indicators(features)
        if name == "analyze_volatility":
            return self._tool_analyze_volatility(candles)
        return json.dumps({"error": f"Unknown tool: {name}"})

    def _tool_get_candles(
        self,
        tool_input: dict[str, Any],
        candles: list[Candle],
    ) -> str:
        limit = min(int(tool_input.get("limit", LLM_CANDLE_WINDOW)), 100)
        window = candles[-limit:]
        return json.dumps([
            {
                "open":   round(c.open, 2),
                "high":   round(c.high, 2),
                "low":    round(c.low, 2),
                "close":  round(c.close, 2),
                "volume": round(c.volume, 4),
            }
            for c in window
        ])

    def _tool_get_indicators(self, features: pd.DataFrame) -> str:
        latest = features[FEATURE_COLS].tail(1).iloc[0]
        return json.dumps({
            "ema_cross":     round(float(latest["ema_cross"]), 4),
            "rsi_14":        round(float(latest["rsi_14"]), 2),
            "macd":          round(float(latest["macd"]), 4),
            "macd_signal":   round(float(latest["macd_signal"]), 4),
            "macd_hist":     round(float(latest["macd_hist"]), 4),
            "return_1":      round(float(latest["return_1"]), 4),
            "return_3":      round(float(latest["return_3"]), 4),
            "volatility_21": round(float(latest["volatility_21"]), 6),
            "return_z":      round(float(latest["return_z"]), 2),
            "volume_ratio":  round(float(latest["volume_ratio"]), 2),
            "hl_range":      round(float(latest["hl_range"]), 4),
        })

    def _tool_analyze_volatility(self, candles: list[Candle]) -> str:
        window = candles[-20:]
        closes = [c.close for c in window]
        if len(closes) < 2:
            return json.dumps({"error": "Insufficient candle data for volatility analysis"})
        price_mean  = statistics.mean(closes)
        price_stdev = statistics.stdev(closes)
        price_range = (max(closes) - min(closes)) / price_mean * 100 if price_mean else 0.0
        momentum_5  = (
            (closes[-1] - closes[-5]) / closes[-5] * 100
            if len(closes) >= 5 and closes[-5]
            else 0.0
        )
        return json.dumps({
            "price_mean":        round(price_mean, 2),
            "price_stdev":       round(price_stdev, 2),
            "price_range_pct":   round(price_range, 3),
            "momentum_5bar_pct": round(momentum_5, 3),
            "current_close":     closes[-1],
        })

    # ── Signal construction ───────────────────────────────────────────────────

    def _signal_from_emit(self, symbol: str, tool_input: dict[str, Any]) -> TradeSignal:
        """Construct a TradeSignal from the emit_signal tool call input."""
        side_str   = str(tool_input.get("side", "HOLD")).upper()
        confidence = float(tool_input.get("confidence", 0.1))

        if side_str not in ("BUY", "SELL", "HOLD"):
            _logger.warning(
                "emit_signal has invalid side value — HOLD fallback.",
                extra={"symbol": symbol, "side": side_str},
            )
            return self._make_hold_signal(symbol)

        clamped = float(max(0.0, min(1.0, confidence)))
        now = datetime.now(tz=timezone.utc)
        return TradeSignal(
            id           = str(uuid.uuid4()),
            symbol       = symbol,
            side         = TradeSide(side_str),
            confidence   = clamped,
            generated_at = now,
            expires_at   = now + timedelta(seconds=SIGNAL_TTL_SECONDS),
            source       = "llm",
        )

    def _make_hold_signal(self, symbol: str) -> TradeSignal:
        """Return a low-confidence HOLD signal as the safe fallback."""
        now = datetime.now(tz=timezone.utc)
        return TradeSignal(
            id           = str(uuid.uuid4()),
            symbol       = symbol,
            side         = TradeSide.HOLD,
            confidence   = 0.1,
            generated_at = now,
            expires_at   = now + timedelta(seconds=SIGNAL_TTL_SECONDS),
            source       = "llm",
        )
