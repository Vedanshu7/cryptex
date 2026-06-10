"""Signal pipeline runner.

Concurrent async tasks per active signal source:
  - ML source  : prediction every SIGNAL_INTERVAL_SECONDS (default 300 s)
                 + daily retrain loop (RETRAIN_INTERVAL_HOURS)
  - LLM source : prediction every LLM_SIGNAL_INTERVAL_SECONDS (default 900 s)
                 (no retrain — the LLM model is managed by the provider)

Select active sources via SIGNAL_SOURCE env var:
  ml   — LightGBM only (default)
  llm  — LLM agent only
  both — both run independently on their own cadences

On startup each source's warmup() is called before any prediction loop begins.
"""

import asyncio
import os

from shared.db_client import get_db_connection
from shared.exceptions import DatabaseError
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.metrics import signal_confidence as signal_confidence_metric, start_metrics_server
from shared.models import Candle

from .base import BaseSignalSource
from .features import build_features
from .llm_source import LLMSignalSource
from .model import MLSignalSource

_logger = get_logger(__name__)

SYMBOLS: list[str]          = os.getenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT").upper().split(",")
SIGNAL_SOURCE: str          = os.getenv("SIGNAL_SOURCE", "ml").lower()
INTERVAL_SECONDS: int       = int(os.getenv("SIGNAL_INTERVAL_SECONDS", "300"))
LLM_INTERVAL_SECONDS: int   = int(os.getenv("LLM_SIGNAL_INTERVAL_SECONDS", "900"))
RETRAIN_HOURS: int          = int(os.getenv("RETRAIN_INTERVAL_HOURS", "24"))
CANDLE_LOOKBACK: int        = 100
TRAINING_LOOKBACK_DAYS: int = int(os.getenv("TRAINING_LOOKBACK_DAYS", "30"))
# 5-min candles × 288/day × N days
TRAINING_LOOKBACK_CANDLES: int = 288 * TRAINING_LOOKBACK_DAYS


def _build_sources() -> dict[str, BaseSignalSource]:
    """Instantiate signal sources based on SIGNAL_SOURCE env var."""
    sources: dict[str, BaseSignalSource] = {}
    if SIGNAL_SOURCE in ("ml", "both"):
        sources["ml"] = MLSignalSource()
    if SIGNAL_SOURCE in ("llm", "both"):
        sources["llm"] = LLMSignalSource()
    if not sources:
        raise ValueError(
            f"Unknown SIGNAL_SOURCE={SIGNAL_SOURCE!r}. Use 'ml', 'llm', or 'both'."
        )
    _logger.info("Signal sources configured.", extra={"active": list(sources.keys())})
    return sources


async def run() -> None:
    """Warm up all sources then start prediction and retrain loops."""
    _logger.info(
        "Signal pipeline starting.",
        extra={
            "symbols":        SYMBOLS,
            "signal_source":  SIGNAL_SOURCE,
            "ml_interval_s":  INTERVAL_SECONDS,
            "llm_interval_s": LLM_INTERVAL_SECONDS,
            "retrain_hours":  RETRAIN_HOURS,
        },
    )

    sources  = _build_sources()
    producer = KafkaClientFactory.create_producer("signal-pipeline")

    for src in sources.values():
        src.warmup()

    tasks = []
    if "ml" in sources:
        tasks.append(_prediction_loop(sources["ml"], producer, INTERVAL_SECONDS))
        tasks.append(_retrain_loop(sources["ml"]))
    if "llm" in sources:
        tasks.append(_prediction_loop(sources["llm"], producer, LLM_INTERVAL_SECONDS))

    await asyncio.gather(*tasks)


async def _prediction_loop(
    source: BaseSignalSource,
    producer: object,
    interval: int,
) -> None:
    """Publish a signal for each symbol every *interval* seconds."""
    while True:
        for symbol in SYMBOLS:
            _generate_and_publish(symbol, source, producer)
        await asyncio.sleep(interval)


async def _retrain_loop(source: BaseSignalSource) -> None:
    """Retrain the source on fresh DB candles every RETRAIN_HOURS."""
    # Retrain immediately on first startup so we exit warm-up mode ASAP.
    _do_retrain(source)

    while True:
        await asyncio.sleep(RETRAIN_HOURS * 3_600)
        _do_retrain(source)


def _do_retrain(source: BaseSignalSource) -> None:
    """Fetch training candles for all symbols and call source.retrain()."""
    _logger.info("Starting scheduled model retrain.",
                 extra={"lookback_days": TRAINING_LOOKBACK_DAYS})

    candles_by_symbol: dict[str, list[Candle]] = {}
    for symbol in SYMBOLS:
        candles = _fetch_candles(symbol, limit=TRAINING_LOOKBACK_CANDLES)
        if candles:
            candles_by_symbol[symbol] = candles

    if not candles_by_symbol:
        _logger.warning("No candles available for retrain — staying on current model.")
        return

    source.retrain(candles_by_symbol)


def _generate_and_publish(
    symbol: str,
    source: BaseSignalSource,
    producer: object,
) -> None:
    """Fetch prediction candles, build features, predict, publish to Kafka."""
    candles = _fetch_candles(symbol, limit=CANDLE_LOOKBACK)

    if len(candles) < 26:
        _logger.warning(
            "Insufficient candle history — skipping signal generation.",
            extra={"symbol": symbol, "count": len(candles)},
        )
        return

    features = build_features(candles)
    if features is None:
        return

    signal = source.predict(symbol, candles, features)

    producer.produce(  # type: ignore[union-attr]
        topic="trade-signals",
        key=signal.symbol,
        value=signal.model_dump_json(),
    )
    producer.flush()  # type: ignore[union-attr]

    signal_confidence_metric.labels(
        symbol=signal.symbol,
        side=signal.side.value,
    ).observe(signal.confidence)

    _logger.info(
        "Signal published.",
        extra={
            "symbol":     signal.symbol,
            "side":       signal.side.value,
            "confidence": signal.confidence,
            "source":     signal.source,
        },
    )


def _fetch_candles(symbol: str, limit: int) -> list[Candle]:
    """Query TimescaleDB for the most recent *limit* candles for *symbol*."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT time, symbol, open, high, low, close, volume, timeframe
                    FROM candles
                    WHERE symbol = %s
                    ORDER BY time DESC
                    LIMIT %s
                    """,
                    (symbol, limit),
                )
                rows = cur.fetchall()
    except DatabaseError as exc:
        _logger.error(
            "Database unavailable — skipping candle fetch.",
            extra={"symbol": symbol, "error": str(exc)},
        )
        return []

    return [
        Candle(
            symbol    = row[1],
            open      = float(row[2]),
            high      = float(row[3]),
            low       = float(row[4]),
            close     = float(row[5]),
            volume    = float(row[6]),
            opened_at = row[0],
            closed_at = row[0],
            timeframe = row[7],
        )
        for row in reversed(rows)  # oldest first for feature engineering
    ]


if __name__ == "__main__":
    start_metrics_server()
    asyncio.run(run())
