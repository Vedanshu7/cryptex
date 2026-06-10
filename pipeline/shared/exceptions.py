"""Custom exception types for the trading pipeline."""


class KafkaPublishError(Exception):
    """Raised when a Kafka message cannot be delivered after retries."""


class DatabaseError(Exception):
    """Raised when a database operation fails unexpectedly."""


class SignalExpiredError(Exception):
    """Raised when a trade signal has passed its expiry time."""


class DeserializationError(Exception):
    """Raised when a Kafka message cannot be parsed into the expected model."""
