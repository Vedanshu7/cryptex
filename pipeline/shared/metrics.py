"""Shared Prometheus metrics helpers.

Each service calls start_metrics_server() in its main entry point.
Metrics are exposed on port 8000 at GET /metrics.
"""

import os
import threading

from prometheus_client import Counter, Histogram, start_http_server

_METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
_started = False
_lock = threading.Lock()


def start_metrics_server() -> None:
    """Start the Prometheus HTTP server on METRICS_PORT (default 8000).

    Safe to call multiple times — starts only once per process.
    """
    global _started  # noqa: PLW0603
    with _lock:
        if not _started:
            start_http_server(_METRICS_PORT)
            _started = True


# ── Exchange Connector ────────────────────────────────────────────────────────

ticks_ingested = Counter(
    "exchange_connector_ticks_total",
    "Total raw ticks received from exchange WebSocket.",
    labelnames=["symbol"],
)

# ── Candle Aggregator ─────────────────────────────────────────────────────────

candles_published = Counter(
    "candle_aggregator_candles_total",
    "Total OHLCV candles published to Kafka.",
    labelnames=["symbol", "timeframe"],
)

# ── Signal Router ─────────────────────────────────────────────────────────────

orders_routed = Counter(
    "signal_router_orders_routed_total",
    "Total order requests routed to tenants.",
    labelnames=["tenant_id", "symbol", "side"],
)

signals_discarded_stale = Counter(
    "signal_router_stale_signals_total",
    "Signals discarded because they had expired.",
    labelnames=["symbol"],
)

# ── Signal Pipeline ───────────────────────────────────────────────────────────

signal_confidence = Histogram(
    "signal_pipeline_signal_confidence",
    "Distribution of signal confidence scores.",
    buckets=[0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0],
    labelnames=["symbol", "side"],
)
