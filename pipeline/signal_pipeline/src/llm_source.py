"""LLM-backed signal source.

Sends recent candle data and computed feature values to a language model
and parses its structured JSON response into a TradeSignal.

Supported providers (LLM_PROVIDER env var):
  anthropic  — requires anthropic>=0.25 and ANTHROPIC_API_KEY
  openai     — requires openai>=1.0 and OPENAI_API_KEY

If the API key is missing or the SDK is not installed, warmup() logs a
warning and disables the source. predict() then returns a low-confidence
HOLD signal as a safe fallback.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from shared.logger import get_logger
from shared.models import Candle, TradeSide, TradeSignal

from .base import BaseSignalSource
from .features import FEATURE_COLS

_logger = get_logger(__name__)

LLM_PROVIDER: str            = os.getenv("LLM_PROVIDER", "anthropic").lower()
LLM_CANDLE_WINDOW: int       = int(os.getenv("LLM_CANDLE_WINDOW", "20"))
LLM_MAX_RETRIES: int         = int(os.getenv("LLM_MAX_RETRIES", "3"))
LLM_RETRY_BASE_DELAY: float  = float(os.getenv("LLM_RETRY_BASE_DELAY", "2.0"))
SIGNAL_TTL_SECONDS: int      = int(os.getenv("SIGNAL_TTL_SECONDS", "90"))

_DEFAULT_MODELS: dict[str, str] = {
    "anthropic": "claude-3-5-haiku-20241022",
    "openai":    "gpt-4o-mini",
}


class LLMSignalSource(BaseSignalSource):
    """Signal source backed by an LLM API (Anthropic Claude or OpenAI GPT)."""

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
        """Call the LLM and parse its JSON response into a TradeSignal.

        Falls back to a low-confidence HOLD on any API or parsing failure.
        """
        if self._client is None:
            _logger.warning(
                "LLM client not available — returning HOLD fallback.",
                extra={"symbol": symbol},
            )
            return self._make_hold_signal(symbol)

        prompt = self._build_prompt(symbol, candles, features)

        try:
            raw_text = self._call_with_retry(prompt)
        except Exception as exc:
            _logger.error(
                "LLM API call failed after retries — returning HOLD fallback.",
                extra={"symbol": symbol, "error": str(exc)},
            )
            return self._make_hold_signal(symbol)

        return self._parse_response(symbol, raw_text)

    # ── Private: warmup helpers ───────────────────────────────────────────────

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

    # ── Private: prompt builder ───────────────────────────────────────────────

    def _build_prompt(
        self,
        symbol: str,
        candles: list[Candle],
        features: pd.DataFrame,
    ) -> str:
        """Build the analysis prompt sent to the LLM."""
        window = candles[-LLM_CANDLE_WINDOW:]
        candle_rows = "\n".join(
            f"| {i + 1} "
            f"| {c.open:.2f} "
            f"| {c.high:.2f} "
            f"| {c.low:.2f} "
            f"| {c.close:.2f} "
            f"| {c.volume:.3f} |"
            for i, c in enumerate(window)
        )

        latest = features[FEATURE_COLS].tail(1).iloc[0]

        return (
            f"You are a quantitative trading analyst. Analyze the following market data "
            f"for {symbol} and generate a trade signal. "
            f"Respond ONLY with valid JSON — no markdown, no explanation outside the JSON.\n\n"
            f"## Recent Candles (last {len(window)} × 5-minute bars, oldest first)\n\n"
            f"| # | Open | High | Low | Close | Volume |\n"
            f"|---|------|------|-----|-------|--------|\n"
            f"{candle_rows}\n\n"
            f"## Latest Technical Indicators (computed on full {len(candles)} candle history)\n\n"
            f"| Indicator       | Value      |\n"
            f"|-----------------|------------|\n"
            f"| EMA Cross       | {latest['ema_cross']:.4f} |\n"
            f"| RSI (14)        | {latest['rsi_14']:.2f} |\n"
            f"| MACD            | {latest['macd']:.4f} |\n"
            f"| MACD Signal     | {latest['macd_signal']:.4f} |\n"
            f"| MACD Histogram  | {latest['macd_hist']:.4f} |\n"
            f"| 1-Bar Return    | {latest['return_1']:.4f} |\n"
            f"| 3-Bar Return    | {latest['return_3']:.4f} |\n"
            f"| Volatility (21) | {latest['volatility_21']:.6f} |\n"
            f"| Return Z-Score  | {latest['return_z']:.2f} |\n"
            f"| Volume Ratio    | {latest['volume_ratio']:.2f} |\n"
            f"| H-L Range       | {latest['hl_range']:.4f} |\n\n"
            f"## Instructions\n\n"
            f"Based on the above data, determine whether to BUY, SELL, or HOLD {symbol}.\n"
            f"Consider trend direction (EMA cross, MACD), momentum (RSI, return z-score), "
            f"and volume confirmation (volume ratio).\n\n"
            f"Respond with exactly this JSON structure:\n"
            f'{{"side": "BUY" | "SELL" | "HOLD", '
            f'"confidence": <float 0.0 to 1.0>, '
            f'"reasoning": "<one sentence explanation>"}}\n\n'
            f"Rules:\n"
            f"- confidence >= 0.7 means high conviction; < 0.5 should be HOLD\n"
            f"- If signals are mixed or ambiguous, prefer HOLD with confidence 0.4-0.5\n"
            f"- Do not output anything except the JSON object"
        )

    # ── Private: API call ─────────────────────────────────────────────────────

    def _call_with_retry(self, prompt: str) -> str:
        """Call the LLM API with exponential backoff on rate-limit errors."""
        last_exc: Exception | None = None
        for attempt in range(LLM_MAX_RETRIES):
            try:
                return self._call_provider(prompt)
            except Exception as exc:
                if type(exc).__name__ == "RateLimitError":
                    last_exc = exc
                    delay = LLM_RETRY_BASE_DELAY * (2**attempt)
                    _logger.warning(
                        "LLM rate limit — retrying.",
                        extra={"attempt": attempt + 1, "delay_seconds": delay},
                    )
                    time.sleep(delay)
                else:
                    raise  # non-retryable; propagate immediately
        raise last_exc or RuntimeError("LLM call failed after all retries.")

    def _call_provider(self, prompt: str) -> str:
        """Dispatch the API call to the configured provider."""
        if self._provider == "anthropic":
            response = self._client.messages.create(  # type: ignore[union-attr]
                model=self._model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return str(response.content[0].text)  # type: ignore[union-attr]
        # openai
        response = self._client.chat.completions.create(  # type: ignore[union-attr]
            model=self._model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return str(response.choices[0].message.content)  # type: ignore[union-attr]

    # ── Private: response parsing ─────────────────────────────────────────────

    def _parse_response(self, symbol: str, raw_text: str) -> TradeSignal:
        """Parse LLM JSON response into a TradeSignal; falls back to HOLD on error."""
        try:
            data: dict[str, Any] = json.loads(raw_text.strip())
        except json.JSONDecodeError:
            _logger.warning(
                "LLM returned invalid JSON — using HOLD fallback.",
                extra={"symbol": symbol, "raw_snippet": raw_text[:120]},
            )
            return self._make_hold_signal(symbol)

        side_str   = data.get("side")
        confidence = data.get("confidence")

        if side_str is None or confidence is None:
            _logger.warning(
                "LLM response missing required fields — using HOLD fallback.",
                extra={"symbol": symbol, "keys_present": list(data.keys())},
            )
            return self._make_hold_signal(symbol)

        if side_str not in ("BUY", "SELL", "HOLD"):
            _logger.warning(
                "LLM returned unrecognised side value — using HOLD fallback.",
                extra={"symbol": symbol, "side": side_str},
            )
            return self._make_hold_signal(symbol)

        if not isinstance(confidence, (int, float)):
            _logger.warning(
                "LLM confidence is not a number — using HOLD fallback.",
                extra={"symbol": symbol, "confidence": confidence},
            )
            return self._make_hold_signal(symbol)

        clamped = float(max(0.0, min(1.0, confidence)))
        if clamped != float(confidence):
            _logger.warning(
                "LLM confidence out of [0, 1] — clamped.",
                extra={"symbol": symbol, "original": confidence, "clamped": clamped},
            )

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
