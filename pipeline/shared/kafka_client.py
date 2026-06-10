"""Shared Kafka producer and consumer factory.

All pipeline services use this factory to avoid duplicating Kafka configuration.
Configuration is read from the KAFKA_BROKERS environment variable.
"""

import os

from confluent_kafka import Consumer, Producer

from shared.logger import get_logger

_logger = get_logger(__name__)

_DEFAULT_BROKERS = "kafka:9092"


class KafkaClientFactory:
    """Factory for creating pre-configured Kafka producers and consumers."""

    @staticmethod
    def create_producer(client_id: str) -> Producer:
        """Create a Kafka producer with standard reliability settings.

        Args:
            client_id: Unique identifier shown in Kafka broker logs.

        Returns:
            Configured Producer instance. Caller owns the lifecycle.
        """
        brokers = os.getenv("KAFKA_BROKERS", _DEFAULT_BROKERS)
        config: dict[str, object] = {
            "bootstrap.servers": brokers,
            "client.id": client_id,
            "acks": "all",             # all ISR replicas must acknowledge.
            "retries": 3,
            "retry.backoff.ms": 500,
            "delivery.timeout.ms": 30_000,
        }
        _logger.info(
            "Creating Kafka producer.",
            extra={"client_id": client_id, "brokers": brokers},
        )
        return Producer(config)  # type: ignore[call-arg]

    @staticmethod
    def create_consumer(group_id: str, topics: list[str]) -> Consumer:
        """Create a Kafka consumer subscribed to the given topics.

        Args:
            group_id: Consumer group identifier for offset management.
            topics: List of topic names to subscribe to.

        Returns:
            Configured and subscribed Consumer instance.
        """
        brokers = os.getenv("KAFKA_BROKERS", _DEFAULT_BROKERS)
        config: dict[str, object] = {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "session.timeout.ms": 30_000,
        }
        consumer: Consumer = Consumer(config)  # type: ignore[call-arg]
        consumer.subscribe(topics)
        _logger.info(
            "Kafka consumer subscribed.",
            extra={"group_id": group_id, "topics": topics, "brokers": brokers},
        )
        return consumer
