"""Shared Redis client factory.

All pipeline services that need caching (tenant-mapping cache, Kalman filter
state) use this factory to avoid duplicating Redis configuration.
Configuration is read from the REDIS_URL environment variable.
"""

import os
import threading

import redis

from shared.logger import get_logger

_logger = get_logger(__name__)

_DEFAULT_REDIS_URL = "redis://redis:6379/0"

_client: redis.Redis | None = None
_lock = threading.Lock()


class RedisClientFactory:
    """Factory for creating a pre-configured Redis client."""

    @staticmethod
    def create_client(client_name: str) -> redis.Redis:
        """Create a Redis client with standard connection settings.

        Args:
            client_name: Identifier used only for logging — Redis has no
                         server-side concept of a named client here.

        Returns:
            Configured redis.Redis instance. Caller owns the lifecycle.
        """
        url = os.getenv("REDIS_URL", _DEFAULT_REDIS_URL)
        _logger.info(
            "Creating Redis client.",
            extra={"client_name": client_name, "redis_url": url},
        )
        return redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )


def get_redis_client() -> redis.Redis:
    """Return the process-wide Redis client, creating it on first use."""
    global _client
    with _lock:
        if _client is None:
            _client = RedisClientFactory.create_client("shared")
        return _client
