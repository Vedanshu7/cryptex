"""Signal Router — routes trade signals to matching tenants as order requests.

Built on Quix Streams: consumes trade-signals and, for each fresh signal,
expands it into one order-request payload per matching tenant via
`.apply(..., expand=True)`. Each payload is routed to its tenant's regional
topic through a dynamic `to_topic()` topic selector — the signal router is the
last central component, so everything downstream stays within the region.

Stale signals (past their expires_at) are discarded before the tenant lookup.
"""

import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from quixstreams import Application
from quixstreams.models.topics import Topic

from shared.logger import get_logger
from shared.metrics import (
    orders_routed,
    signals_discarded_region_mismatch,
    signals_discarded_stale,
    tenant_universe_mismatches,
)
from shared.models import OrderRequest, TradeSignal
from shared.universe import get_universe
from shared.universe_consistency import find_tenant_universe_mismatches

from .tenant_config import get_matching_tenants

_logger = get_logger(__name__)

KAFKA_BROKERS: str = os.getenv("KAFKA_BROKERS", "kafka:9092")
INPUT_TOPIC = "trade-signals"
CONSUMER_GROUP = "signal-router"

# How often the background thread re-checks enabled tenant strategies against
# the live universe. Independent of signal traffic — see
# shared/universe_consistency.py for why this can't just piggyback on
# _region_supports_symbol's reactive, per-message check.
CONSISTENCY_CHECK_INTERVAL_SECONDS = int(os.getenv("CONSISTENCY_CHECK_INTERVAL_SECONDS", "300"))


def _is_stale(signal: TradeSignal) -> bool:
    """Return True if the signal's expiry time has passed."""
    return datetime.now(tz=timezone.utc) > signal.expires_at


def _regional_topic_name(region: str) -> str:
    """Topic name for a region's order-requests."""
    return f"{region}.order-requests"


def _to_order_request_payload(
    signal: TradeSignal,
    tenant: dict[str, object],
) -> dict[str, Any]:
    """Build the Kafka payload for a single tenant's order request.

    Includes a transient "_region" field used only to pick the destination
    topic in `to_topic()` — not part of the OrderRequest schema itself, but
    harmless to downstream consumers that deserialize with unknown fields
    ignored (OMS's System.Text.Json config does this).
    """
    order_request = OrderRequest(
        id=str(uuid.uuid4()),
        tenant_id=str(tenant["tenant_id"]),
        symbol=signal.symbol,
        side=signal.side,
        quantity=float(str(tenant["position_size"])),
        signal_id=signal.id,
        created_at=datetime.now(tz=timezone.utc),
    )

    orders_routed.labels(
        tenant_id=order_request.tenant_id,
        symbol=order_request.symbol,
        side=order_request.side.value,
    ).inc()

    _logger.info(
        "Order request published to regional topic.",
        extra={
            "tenant_id": order_request.tenant_id,
            "symbol": order_request.symbol,
            "side": order_request.side.value,
            "quantity": order_request.quantity,
            "region": str(tenant["region"]),
        },
    )

    payload = order_request.model_dump(mode="json")
    payload["_region"] = str(tenant["region"])
    return payload


def _region_supports_symbol(region: str, symbol: str) -> bool:
    """Check the universe for whether `region`'s exchange lists `symbol`.

    Regions are paired one-to-one with a specific exchange for latency
    reasons (see shared/universe.py) — that exchange may simply not list
    the signal's symbol (e.g. a Binance-only altcoin routed to a tenant
    whose region trades exclusively on Deribit).
    """
    if get_universe().has_symbol(region, symbol):
        return True

    _logger.warning(
        "Tenant's region does not support this symbol — order request skipped.",
        extra={"region": region, "symbol": symbol},
    )
    signals_discarded_region_mismatch.labels(region=region, symbol=symbol).inc()
    return False


def expand_to_order_requests(value: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn one trade signal into zero-or-more per-tenant order-request payloads.

    Returns an empty list (nothing published) for stale signals, symbols with
    no matching tenant strategy, or tenants whose region's exchange doesn't
    list the symbol.
    """
    signal = TradeSignal.model_validate(value)

    if _is_stale(signal):
        _logger.warning(
            "Stale signal discarded.",
            extra={"signal_id": signal.id, "symbol": signal.symbol},
        )
        signals_discarded_stale.labels(symbol=signal.symbol).inc()
        return []

    tenants = get_matching_tenants(signal.symbol)
    if not tenants:
        _logger.debug("No matching tenants for signal.", extra={"symbol": signal.symbol})
        return []

    tenants = [t for t in tenants if _region_supports_symbol(str(t["region"]), signal.symbol)]

    return [_to_order_request_payload(signal, tenant) for tenant in tenants]


def build_application() -> Application:
    """Wire the Quix Streams topology: trade-signals -> per-tenant order-requests."""
    app = Application(
        broker_address=KAFKA_BROKERS,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="latest",
    )

    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
    regional_topics: dict[str, Topic] = {
        region: app.topic(_regional_topic_name(region), value_serializer="json")
        for region in get_universe().region_names
    }

    def _select_regional_topic(
        value: dict[str, Any],
        key: str,
        timestamp: int,
        headers: list[tuple[str, bytes]] | None,
    ) -> Topic:
        """Look up (or lazily register) the Kafka Topic for a payload's region.

        Lazy-create covers a region added to the universe after this service
        started — the underlying Kafka topic must already exist (provisioned
        via infra/kafka/create-topics.sh or the OMS/EMS startup provisioner),
        this only registers the in-process Topic model for it.
        """
        region = value["_region"]
        if region not in regional_topics:
            regional_topics[region] = app.topic(
                _regional_topic_name(region), value_serializer="json"
            )
        return regional_topics[region]

    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(expand_to_order_requests, expand=True)
    sdf.to_topic(_select_regional_topic, key=lambda value: str(value["tenant_id"]))

    return app


def _refresh_tenant_universe_gauge() -> None:
    """Run one consistency-check pass and update the Prometheus gauge.

    Explicitly zeroes every known reason before setting counts so a mismatch
    that gets fixed disappears from the gauge on the next pass, rather than
    leaving a stale non-zero value behind.
    """
    mismatches = find_tenant_universe_mismatches(get_universe())

    counts_by_reason: dict[str, int] = {"unknown_region": 0, "symbol_not_in_region": 0}
    for m in mismatches:
        counts_by_reason[m.reason] = counts_by_reason.get(m.reason, 0) + 1
        _logger.warning(
            "Tenant strategy does not match current universe.",
            extra={
                "tenant_id": m.tenant_id,
                "tenant_name": m.tenant_name,
                "symbol": m.symbol,
                "region": m.region,
                "reason": m.reason,
            },
        )

    for reason, count in counts_by_reason.items():
        tenant_universe_mismatches.labels(reason=reason).set(count)


def _consistency_check_loop() -> None:
    """Background loop: refresh the tenant/universe consistency gauge periodically."""
    while True:
        try:
            _refresh_tenant_universe_gauge()
        except Exception as exc:
            # A failed check (e.g. DB briefly unavailable) shouldn't kill the
            # thread — just retry on the next interval.
            _logger.error(
                "Tenant/universe consistency check failed — will retry.",
                extra={"error": str(exc)},
            )
        time.sleep(CONSISTENCY_CHECK_INTERVAL_SECONDS)


def run() -> None:
    """Start the signal router, consuming indefinitely."""
    _logger.info("Signal router started.")

    threading.Thread(target=_consistency_check_loop, daemon=True).start()

    build_application().run()


if __name__ == "__main__":
    from shared.metrics import start_metrics_server

    start_metrics_server()
    run()
