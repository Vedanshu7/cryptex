"""Thin Kafka publishing wrapper for the exchange connector."""

from confluent_kafka import KafkaException, Producer

from shared.exceptions import KafkaPublishError
from shared.logger import get_logger
from shared.metrics import ticks_ingested
from shared.models import Tick

_logger = get_logger(__name__)

KAFKA_TOPIC = "market-data-raw"


class TickProducer:
    """Publishes normalized Tick objects to the market-data-raw Kafka topic."""

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def publish(self, tick: Tick) -> None:
        """Serialize and deliver a tick to Kafka.

        Raises:
            KafkaPublishError: When delivery fails after retries.
        """
        try:
            self._producer.produce(
                topic=KAFKA_TOPIC,
                key=tick.symbol,
                value=tick.model_dump_json(),
                callback=self._on_delivery,
            )
            self._producer.poll(0)
            ticks_ingested.labels(symbol=tick.symbol).inc()
        except KafkaException as exc:
            _logger.error(
                "Failed to publish tick.",
                extra={"symbol": tick.symbol, "error": str(exc), "topic": KAFKA_TOPIC},
            )
            raise KafkaPublishError(
                f"Kafka publish failed for {tick.symbol}."
            ) from exc

    def flush(self) -> None:
        """Block until all queued messages are delivered."""
        self._producer.flush()

    def _on_delivery(self, err: object, msg: object) -> None:
        """Log delivery errors from the Kafka background thread."""
        if err is not None:
            _logger.error(
                "Kafka delivery failed.",
                extra={"error": str(err), "topic": KAFKA_TOPIC},
            )
