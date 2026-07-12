"""Custom exception types for the trading pipeline."""


class KafkaPublishError(Exception):
    """Raised when a Kafka message cannot be delivered after retries."""


class DatabaseError(Exception):
    """Raised when a database operation fails unexpectedly."""


class SignalExpiredError(Exception):
    """Raised when a trade signal has passed its expiry time."""


class DeserializationError(Exception):
    """Raised when a Kafka message cannot be parsed into the expected model."""


class TenantLookupError(Exception):
    """Raised when tenant matching fails with no cache to fall back on."""


class RetryExhaustedError(Exception):
    """Raised when call_with_retries exhausts its attempts. Wraps the last failure."""
