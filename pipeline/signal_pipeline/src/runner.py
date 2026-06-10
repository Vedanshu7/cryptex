"""Signal pipeline runner.

Two concurrent async tasks:
  1. Prediction loop  — every SIGNAL_INTERVAL_SECONDS (default 300 = 5 min)
                        generate and publish a signal per symbol.
  2. Daily retrain    — every RETRAIN_INTERVAL_HOURS (default 24)
                        fetch the last TRAINING_LOOKBACK_DAYS of real candles
                        from TimescaleDB and retrain the LightGBM model.

On startup the model runs in warm-up mode (synthetic labels) until the first
retrain completes. Predictions are emitted regardless — callers can use the
`trained_on_real` flag to decide whether to act on warm-up signals.
"""

import asyncio
import os

from shared.db_client import get_db_connection
from shared.exceptions import DatabaseError
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.metrics import signal_confidence as signal_confidence_metric, start_metrics_server
from shared.models import Candle

from .features import build_features
from .model import SignalGenerator

_logger = get_logger(__name__)

SYMBOLS: list[str]         = os.getenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT").upper().split(",")
INTERVAL_SECONDS: int      = int(os.getenv("SIGNAL_INTERVAL_SECONDS", "300"))
RETRAIN_HOURS: int         = int(os.getenv("RETRAIN_INTERVAL_HOURS", "24"))
CANDLE_LOOKBACK: int       = 100
TRAINING_LOOKBACK_DAYS: int = int(os.getenv("TRAINING_LOOKBACK_DAYS", "30"))
# 5-min candles × 288/day × N days
TRAINING_LOOKBACK_CANDLES: int = 288 * TRAINING_LOOKBACK_DAYS


async def run() -> None:
    """Start prediction and retrain loops concurrently."""
    _logger.info(
        "Signal pipeline starting.",
        extra={
            "symbols":          SYMBOLS,
            "interval_seconds": INTERVAL_SECONDS,
            "retrain_hours":    RETRAIN_HOURS,
        },
    )

    generator = SignalGenerator()
    producer  = KafkaClientFactory.create_producer("signal-pipeline")

    await asyncio.gather(
        _prediction_loop(generator, producer),
        _retrain_loop(generator),
    )


async def _prediction_loop(generator: SignalGenerator, producer: object) -> None:
    """Publish a signal for each symbol every INTERVAL_SECONDS."""
    while True:
        for symbol in SYMBOLS:
            _generate_and_publish(symbol, generator, producer)
        await asyncio.sleep(INTERVAL_SECONDS)


async def _retrain_loop(generator: SignalGenerator) -> None:
    """Retrain the model on fresh DB candles every RETRAIN_HOURS."""
    # Retrain immediately on first startup so we exit warm-up mode ASAP.
    _do_retrain(generator)

    while True:
        await asyncio.sleep(RETRAIN_HOURS * 3_600)
        _do_retrain(generator)


def _do_retrain(generator: SignalGenerator) -> None:
    """Fetch training candles for all symbols and retrain the model."""
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

    generator.retrain(candles_by_symbol)


def _generate_and_publish(
    symbol: str,
    generator: SignalGenerator,
    producer: object,
) -> None:
    """Fetch prediction candles, build features, predict, publish."""
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

    signal = generator.predict(symbol, features)

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
            "symbol":           signal.symbol,
            "side":             signal.side.value,
            "confidence":       signal.confidence,
            "trained_on_real":  generator.trained_on_real,
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
            symbol   = row[1],
            open     = float(row[2]),
            high     = float(row[3]),
            low      = float(row[4]),
            close    = float(row[5]),
            volume   = float(row[6]),
            opened_at = row[0],
            closed_at = row[0],
            timeframe = row[7],
        )
        for row in reversed(rows)  # oldest first for feature engineering
    ]


if __name__ == "__main__":
    start_metrics_server()
    asyncio.run(run())
