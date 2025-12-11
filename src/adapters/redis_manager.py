"""
Redis connection manager supporting multiple environments.

Allows switching between local and public Redis instances at runtime.
"""

import os
from dataclasses import dataclass
from enum import Enum

from redis.asyncio import Redis


class RedisEnvironment(str, Enum):
    LOCAL = "local"
    PUBLIC = "public"


@dataclass
class RedisConfig:
    host: str
    port: int
    password: str | None
    name: str


class RedisManager:
    """Manages Redis connections for different environments."""

    _instance = None
    _current_env: RedisEnvironment = RedisEnvironment.LOCAL
    _connections: dict[RedisEnvironment, Redis] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_config(cls, env: RedisEnvironment) -> RedisConfig:
        """Get Redis configuration for the specified environment."""
        if env == RedisEnvironment.LOCAL:
            return RedisConfig(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6380")),
                password=os.getenv("REDIS_PASSWORD") or None,
                name="Local Redis (Docker)",
            )
        else:
            host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
            port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
            # Show appropriate name based on whether tunnel is being used
            if host == "localhost":
                name = f"Public Redis (via IAP tunnel on port {port})"
            else:
                name = f"Public Redis (GCE VM at {host})"
            return RedisConfig(
                host=host, port=port, password=os.getenv("PUBLIC_REDIS_PASSWORD") or None, name=name
            )

    @classmethod
    def get_current_env(cls) -> RedisEnvironment:
        """Get the current Redis environment."""
        return cls._current_env

    @classmethod
    def set_current_env(cls, env: RedisEnvironment) -> None:
        """Set the current Redis environment."""
        cls._current_env = env
        # Clear cached connections when switching
        cls._connections.clear()

    @classmethod
    def get_redis(cls, env: RedisEnvironment | None = None) -> Redis:
        """Get Redis client for the specified or current environment."""
        if env is None:
            env = cls._current_env

        if env not in cls._connections:
            config = cls.get_config(env)
            cls._connections[env] = Redis(
                host=config.host,
                port=config.port,
                password=config.password,
                decode_responses=True,
                socket_timeout=10.0,
                socket_connect_timeout=5.0,
            )

        return cls._connections[env]

    @classmethod
    async def test_connection(cls, env: RedisEnvironment) -> dict:
        """Test connection to a Redis environment."""
        config = cls.get_config(env)
        try:
            client = Redis(
                host=config.host,
                port=config.port,
                password=config.password,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            await client.ping()  # type: ignore[misc]
            info = await client.info("server")
            dbsize = await client.dbsize()
            await client.aclose()
            return {
                "status": "connected",
                "host": config.host,
                "port": config.port,
                "name": config.name,
                "redis_version": info.get("redis_version", "unknown"),
                "dbsize": dbsize,
            }
        except Exception as e:
            return {
                "status": "error",
                "host": config.host,
                "port": config.port,
                "name": config.name,
                "error": str(e),
            }


# Convenience function for backwards compatibility
def get_redis() -> Redis:
    """Get the current Redis client."""
    return RedisManager.get_redis()
