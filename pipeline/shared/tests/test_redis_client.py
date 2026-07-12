"""Unit tests for the shared Redis client factory."""

from unittest.mock import patch

from shared import redis_client
from shared.redis_client import RedisClientFactory, get_redis_client


class TestRedisClientFactory:
    def test_uses_default_url_when_env_unset(self, monkeypatch: object) -> None:
        import os

        os.environ.pop("REDIS_URL", None)
        with patch("shared.redis_client.redis.Redis.from_url") as mock_from_url:
            RedisClientFactory.create_client("test-client")

        mock_from_url.assert_called_once()
        args, kwargs = mock_from_url.call_args
        assert args[0] == "redis://redis:6379/0"
        assert kwargs["decode_responses"] is True

    def test_uses_redis_url_env_var(self, monkeypatch: object) -> None:
        import os

        os.environ["REDIS_URL"] = "redis://custom-host:6380/1"
        try:
            with patch("shared.redis_client.redis.Redis.from_url") as mock_from_url:
                RedisClientFactory.create_client("test-client")

            args, _ = mock_from_url.call_args
            assert args[0] == "redis://custom-host:6380/1"
        finally:
            del os.environ["REDIS_URL"]


class TestGetRedisClient:
    def test_returns_singleton(self) -> None:
        redis_client._client = None
        try:
            with patch("shared.redis_client.RedisClientFactory.create_client") as mock_create:
                first = get_redis_client()
                second = get_redis_client()

            mock_create.assert_called_once()
            assert first is second
        finally:
            redis_client._client = None
