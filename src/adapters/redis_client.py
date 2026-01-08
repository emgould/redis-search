"""
Redis client module - delegates to RedisManager for connection handling.
"""

from adapters.redis_manager import RedisManager, get_redis

# Re-export for backwards compatibility
__all__ = ["get_redis", "RedisManager"]
