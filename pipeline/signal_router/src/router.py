"""Signal Router — routes trade signals to matching tenants as order requests.

Consumes from trade-signals, looks up tenants that have an active strategy
for the signal's symbol, and publishes a per-tenant OrderRequest to order-requests.
Stale signals (past their expires_at) are discarded without publishing.
"""

import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaError

from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.metrics import orders_routed, signals_discarded_stale
from shared.models import OrderRequest, TradeSignal

from .tenant_config import get_matching_tenants

_logger = get_logger(__name__)

INPUT_TOPIC = "trade-signals"
OUTPUT_TOPIC = "order-requests"
CONSUMER_GROUP = "signal-router"


class SignalRouter:
    """Routes trade signals to matching tenants as per-tenant order requests."""

    def __init__(self) -> None:
        self._consumer = KafkaClientFactory.create_consumer(
            group_id=CONSUMER_GROUP,
            topics=[INPUT_TOPIC],
        )
        self._producer = KafkaClientFactory.create_producer("signal-router")

    def run(self) -> None:
        """Consume signals and route to tenants indefinitely."""
        _logger.info("Signal router started.")

        while True:
            msg = self._consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                err: KafkaError = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                _logger.error(
                    "Consumer error in signal router.",
                    extra={"error": str(err)},
                )
                continue

            raw = msg.value()
            if raw is None:
                continue

            signal = self._deserialize(raw)
            if signal is not None:
                self._route_signal(signal)

    def _route_signal(self, signal: TradeSignal) -> None:
        """Discard stale signals; route fresh ones to all matching tenants."""
        if _is_stale(signal):
            _logger.warning(
                "Stale signal discarded.",
                extra={"signal_id": signal.id, "symbol": signal.symbol},
            )
            signals_discarded_stale.labels(symbol=signal.symbol).inc()
            return

        tenants = get_matching_tenants(signal.symbol)
        if not tenants:
            _logger.debug(
                "No matching tenants for signal.",
                extra={"symbol": signal.symbol},
            )
            return

        for tenant in tenants:
            self._publish_order_request(signal, tenant)

    def _publish_order_request(
        self,
        signal: TradeSignal,
        tenant: dict[str, object],
    ) -> None:
        """Build and publish a per-tenant OrderRequest to Kafka."""
        order_request = OrderRequest(
            id=str(uuid.uuid4()),
            tenant_id=str(tenant["tenant_id"]),
            symbol=signal.symbol,
            side=signal.side,
            quantity=float(str(tenant["position_size"])),
            signal_id=signal.id,
            created_at=datetime.now(tz=timezone.utc),
        )

        self._producer.produce(
            topic=OUTPUT_TOPIC,
            key=order_request.tenant_id,
            value=order_request.model_dump_json(),
        )
        self._producer.flush()
        orders_routed.labels(
            tenant_id=order_request.tenant_id,
            symbol=order_request.symbol,
            side=order_request.side.value,
        ).inc()

        _logger.info(
            "Order request published.",
            extra={
                "tenant_id": order_request.tenant_id,
                "symbol": order_request.symbol,
                "side": order_request.side.value,
                "quantity": order_request.quantity,
            },
        )

    def _deserialize(self, raw: bytes) -> TradeSignal | None:
        """Deserialize Kafka message bytes into a TradeSignal model.

        Returns None on parse failure so run() can skip and continue rather
        than crashing the service on a single malformed message.
        """
        try:
            return TradeSignal.model_validate_json(raw)
        except Exception as exc:
            _logger.warning(
                "Failed to deserialize signal — skipping message.",
                extra={"error": str(exc)},
            )
            return None


def _is_stale(signal: TradeSignal) -> bool:
    """Return True if the signal's expiry time has passed."""
    return datetime.now(tz=timezone.utc) > signal.expires_at


if __name__ == "__main__":
    from shared.metrics import start_metrics_server
    start_metrics_server()
    SignalRouter().run()
