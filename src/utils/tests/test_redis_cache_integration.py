"""
Integration tests for redis_cache.py - runs against local Redis instance.

These tests require a local Redis instance running on localhost:6379.
This is ensured by firebase/start-local-redis.sh called via dev-start.sh.

Test scenarios:
- Basic Read/Write/Delete operations
- Expiration handling (TTL)
- Version mismatch invalidation
- Complex object serialization (Pickle)
- Decorator functionality (Function & Method)
- Concurrent cache access (no thundering herd prevention in sync version)
"""

import asyncio
import os
import time
from dataclasses import dataclass

import pytest

# Ensure Redis config points to localhost for tests
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"
os.environ["ENABLE_CACHE_FOR_TESTS"] = "1"

# Force-enable cache since module may have already been imported
from utils import redis_cache as _redis_module

_redis_module.DISABLE_CACHE = False

from utils.redis_cache import CacheEntry, RedisCache

# Use a unique prefix for tests to avoid collisions
TEST_PREFIX = "test_redis_"


# =============================================================================
# Module-level dataclasses for pickling tests
# =============================================================================

@dataclass
class TestModel:
    id: int
    name: str
    tags: list


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def redis_cache():
    """Standard RedisCache instance for tests."""
    cache = RedisCache(
        prefix=TEST_PREFIX,
        defaultTTL=3600,
        verbose=True
    )
    # Ensure clean slate (SYNCHRONOUS)
    cache.clear()
    yield cache
    cache.clear()
    cache.close()


@pytest.fixture
def redis_cache_short_ttl():
    """RedisCache with short TTL for expiration tests."""
    cache = RedisCache(
        prefix=f"{TEST_PREFIX}ttl_",
        defaultTTL=1,  # 1 second
        verbose=True
    )
    cache.clear()
    yield cache
    cache.clear()
    cache.close()


# =============================================================================
# Basic Operations Tests
# =============================================================================

def test_add_and_read(redis_cache):
    """Test basic add and read operations."""
    test_data = {"key": "value", "list": [1, 2, 3]}
    key = "test_key_1"

    # Add to cache (SYNCHRONOUS)
    entry = redis_cache.add(
        data=test_data,
        cache_key=key,
        funcName="test_func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={}
    )

    assert entry is not None
    assert entry.data == test_data

    # Read back (SYNCHRONOUS)
    result = redis_cache.read(key)
    assert result is not None
    assert result.data == test_data
    assert result.source == "redis"
    print(f"✅ Basic read/write successful: {result.data}")


def test_remove(redis_cache):
    """Test removing an item from cache."""
    key = "test_remove_key"
    redis_cache.add(
        data="data",
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={}
    )

    # Verify it's there
    assert redis_cache.read(key) is not None

    # Remove
    redis_cache.remove(key)

    # Verify it's gone
    assert redis_cache.read(key) is None
    print("✅ Remove operation successful")


def test_complex_objects(redis_cache):
    """Test caching complex objects (Dataclasses)."""
    model = TestModel(id=1, name="RedisTest", tags=["fast", "in-memory"])
    key = "complex_obj"

    redis_cache.add(
        data=model,
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={}
    )

    result = redis_cache.read(key)
    assert result is not None
    assert isinstance(result.data, TestModel)
    assert result.data.id == 1
    assert result.data.tags == ["fast", "in-memory"]
    print("✅ Complex object serialization successful")


# =============================================================================
# Expiration Tests
# =============================================================================

def test_expiration(redis_cache_short_ttl):
    """Test that items expire from Redis automatically."""
    key = "expiring_key"

    redis_cache_short_ttl.add(
        data="quick_data",
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 1,  # 1 second absolute expiry
        kwargs={}
    )

    # Should exist immediately
    assert redis_cache_short_ttl.read(key) is not None

    # Wait for expiration
    print("⏳ Waiting for expiration (2s)...")
    time.sleep(2.1)

    # Should be gone (Redis returns None)
    result = redis_cache_short_ttl.read(key)
    assert result is None
    print("✅ Expiration successful - item purged")


# =============================================================================
# Versioning Tests
# =============================================================================

def test_version_mismatch(redis_cache):
    """Test that version mismatch causes a cache miss."""
    key = "version_test"

    # Inject a cache entry with an old version manually
    entry = CacheEntry()
    entry.data = "old_data"
    entry.version = "0.0.0" # Old version
    entry.expiry = int(time.time() + 3600)
    entry.key = key

    import pickle
    payload = pickle.dumps(entry)
    # Access private redis client to force inject bad data
    # Must use full key including prefix manually since we bypass .add()
    full_key = redis_cache._full_key(key)
    redis_cache._redis.set(full_key, payload)

    # Read with current version (RELEASE_VERSION)
    # Should detect mismatch, delete, and return None
    result = redis_cache.read(key)

    assert result is None

    # Verify it was deleted from Redis
    raw = redis_cache._redis.get(full_key)
    assert raw is None
    print("✅ Version mismatch correctly invalidated cache")


# =============================================================================
# Decorator Tests
# =============================================================================

@pytest.mark.asyncio
async def test_function_decorator(redis_cache):
    """Test using @RedisCache.use_cache as a function decorator."""

    call_count = 0

    @RedisCache.use_cache(redis_cache, prefix="func_deco")
    async def expensive_function(arg1, arg2):
        nonlocal call_count
        call_count += 1
        return f"{arg1}-{arg2}"

    # First call
    res1 = await expensive_function("hello", "world")
    assert res1 == "hello-world"
    assert call_count == 1

    # Second call (should be cached)
    res2 = await expensive_function("hello", "world")
    assert res2 == "hello-world"
    assert call_count == 1 # Still 1

    # Different args
    res3 = await expensive_function("foo", "bar")
    assert res3 == "foo-bar"
    assert call_count == 2

    print("✅ Function decorator working correctly")


@pytest.mark.asyncio
async def test_method_decorator(redis_cache):
    """Test using @RedisCache.use_cache on a class method."""

    class Service:
        def __init__(self):
            self.call_count = 0

        @RedisCache.use_cache(redis_cache, prefix="method_deco")
        async def fetch_data(self, id: int):
            self.call_count += 1
            return {"id": id, "ts": time.time()}

    service = Service()

    # First call
    data1 = await service.fetch_data(100)
    assert service.call_count == 1

    # Second call
    data2 = await service.fetch_data(100)
    assert service.call_count == 1
    assert data1 == data2

    # Different instance, same logic?
    # Logic uses func name + args. 'self' is the first arg.
    # Standard Cache v2 logic includes 'self' in args if it's not a classmethod on Cache.
    # The decorator implementation in RedisCache calls `get_cache_key`.
    # If the method is an instance method, `self` is passed as first arg.
    # `get_cache_key` calls `getArgs`.
    # `isClassMethod=True` (default) strips the first arg (assumed to be cls or self).
    # So different instances of Service should share cache if `id` is same!

    service2 = Service()
    data3 = await service2.fetch_data(100)
    # Should use cache because `self` is ignored in key generation by default logic
    assert service2.call_count == 0
    assert data3 == data1

    print("✅ Class method decorator working correctly (ignoring self)")


@pytest.mark.asyncio
async def test_concurrent_calls(redis_cache):
    """Test that concurrent calls for the same key work correctly.

    Note: With synchronous Redis, there's no thundering herd protection.
    Multiple concurrent calls will each execute the function if the cache
    is empty. This is acceptable because:
    1. Redis is fast enough (~1ms) that duplicate fetches are rare
    2. The complexity of async locks was causing event loop issues
    """

    execution_count = 0

    @RedisCache.use_cache(redis_cache, prefix="concurrent")
    async def slow_function(idx):
        nonlocal execution_count
        execution_count += 1
        await asyncio.sleep(0.1)  # Simulate work
        return idx

    # Launch 5 concurrent calls
    tasks = [slow_function(99) for _ in range(5)]

    start = time.time()
    results = await asyncio.gather(*tasks)
    duration = time.time() - start

    # Without thundering herd protection, first call caches, others may or may not hit cache
    # depending on timing. The important thing is it doesn't crash!
    assert all(r == 99 for r in results)

    # Verify the cache works for subsequent calls
    subsequent_count = execution_count
    result = await slow_function(99)
    assert result == 99
    assert execution_count == subsequent_count  # No additional execution, cache hit

    print(f"✅ Concurrent calls completed: {execution_count} executions for 5 calls in {duration:.2f}s")


if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v", "-s"])
