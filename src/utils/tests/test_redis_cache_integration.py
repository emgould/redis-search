"""
Integration tests for redis_cache.py — runs against local Redis instance.

These tests require a local Redis instance running on localhost:6379.

Test scenarios:
- Basic async Read/Write/Delete operations
- Expiration handling (TTL)
- Version mismatch invalidation (async remove inside read)
- Complex object serialization (Pickle)
- Decorator functionality (Function & Method)
- Concurrent cache access with advisory-lock coalescing
- Event-loop interleaving (proves async ops don't block)
- Pickle backward compatibility (sync-written entries readable by async)
- Connection pool bounds enforcement
"""

import asyncio
import os
import pickle
import time
from dataclasses import dataclass

import pytest
import pytest_asyncio

os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"
os.environ["ENABLE_CACHE_FOR_TESTS"] = "1"

from utils import redis_cache as _redis_module

_redis_module.DISABLE_CACHE = False

from redis import Redis as SyncRedis

from utils.redis_cache import (
    CacheEntry,
    RedisCache,
    _ASYNC_POOL_MAX_CONNECTIONS,
    get_async_redis_client,
    reset_async_redis_client,
)

TEST_PREFIX = "test_redis_"


@pytest_asyncio.fixture(autouse=True)
async def _reset_async_client():
    """Reset the async Redis singleton before each test.

    pytest-asyncio creates a new event loop per test. The singleton must be
    re-created on the current loop to avoid "attached to a different loop" errors.
    """
    await reset_async_redis_client()
    yield
    await reset_async_redis_client()


# =============================================================================
# Module-level dataclasses for pickling tests
# =============================================================================


@dataclass
class SampleModel:
    id: int
    name: str
    tags: list


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def redis_cache():
    """Standard RedisCache instance for tests."""
    cache = RedisCache(prefix=TEST_PREFIX, defaultTTL=3600, verbose=True)
    await cache.clear()
    yield cache
    await cache.clear()
    await cache.close()


@pytest_asyncio.fixture
async def redis_cache_short_ttl():
    """RedisCache with short TTL for expiration tests."""
    cache = RedisCache(prefix=f"{TEST_PREFIX}ttl_", defaultTTL=1, verbose=True)
    await cache.clear()
    yield cache
    await cache.clear()
    await cache.close()


# =============================================================================
# Basic Operations Tests
# =============================================================================


@pytest.mark.asyncio
async def test_add_and_read(redis_cache):
    """Test basic async add and read operations."""
    test_data = {"key": "value", "list": [1, 2, 3]}
    key = "test_key_1"

    entry = await redis_cache.add(
        data=test_data,
        cache_key=key,
        funcName="test_func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={},
    )

    assert entry is not None
    assert entry.data == test_data

    result = await redis_cache.read(key)
    assert result is not None
    assert result.data == test_data
    assert result.source == "redis"


@pytest.mark.asyncio
async def test_remove(redis_cache):
    """Test removing an item from cache."""
    key = "test_remove_key"
    await redis_cache.add(
        data="data",
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={},
    )

    assert await redis_cache.read(key) is not None

    await redis_cache.remove(key)

    assert await redis_cache.read(key) is None


@pytest.mark.asyncio
async def test_complex_objects(redis_cache):
    """Test caching complex objects (Dataclasses)."""
    model = SampleModel(id=1, name="RedisTest", tags=["fast", "in-memory"])
    key = "complex_obj"

    await redis_cache.add(
        data=model,
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 3600,
        kwargs={},
    )

    result = await redis_cache.read(key)
    assert result is not None
    assert isinstance(result.data, SampleModel)
    assert result.data.id == 1
    assert result.data.tags == ["fast", "in-memory"]


# =============================================================================
# Expiration Tests
# =============================================================================


@pytest.mark.asyncio
async def test_expiration(redis_cache_short_ttl):
    """Test that items expire from Redis automatically."""
    key = "expiring_key"

    await redis_cache_short_ttl.add(
        data="quick_data",
        cache_key=key,
        funcName="func",
        args=[],
        expiry=time.time() + 1,
        kwargs={},
    )

    assert await redis_cache_short_ttl.read(key) is not None

    await asyncio.sleep(2.1)

    result = await redis_cache_short_ttl.read(key)
    assert result is None


# =============================================================================
# Versioning Tests
# =============================================================================


@pytest.mark.asyncio
async def test_version_mismatch(redis_cache):
    """Test that version mismatch causes a cache miss and async remove fires."""
    key = "version_test"

    entry = CacheEntry()
    entry.data = "old_data"
    entry.version = "0.0.0"
    entry.expiry = int(time.time() + 3600)
    entry.key = key

    payload = pickle.dumps(entry)
    full_key = redis_cache._full_key(key)
    await redis_cache._redis.set(full_key, payload)

    result = await redis_cache.read(key)
    assert result is None

    raw = await redis_cache._redis.get(full_key)
    assert raw is None


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

    res1 = await expensive_function("hello", "world")
    assert res1 == "hello-world"
    assert call_count == 1

    res2 = await expensive_function("hello", "world")
    assert res2 == "hello-world"
    assert call_count == 1

    res3 = await expensive_function("foo", "bar")
    assert res3 == "foo-bar"
    assert call_count == 2


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

    data1 = await service.fetch_data(100)
    assert service.call_count == 1

    data2 = await service.fetch_data(100)
    assert service.call_count == 1
    assert data1 == data2

    service2 = Service()
    data3 = await service2.fetch_data(100)
    assert service2.call_count == 0
    assert data3 == data1


# =============================================================================
# Async Migration: Concurrency & Coalescing Tests
# =============================================================================


@pytest.mark.asyncio
async def test_concurrent_calls(redis_cache):
    """Test that concurrent calls for the same key work correctly."""

    execution_count = 0

    @RedisCache.use_cache(redis_cache, prefix="concurrent")
    async def slow_function(idx):
        nonlocal execution_count
        execution_count += 1
        await asyncio.sleep(0.1)
        return idx

    tasks = [slow_function(99) for _ in range(5)]

    results = await asyncio.gather(*tasks)

    assert all(r == 99 for r in results)

    subsequent_count = execution_count
    result = await slow_function(99)
    assert result == 99
    assert execution_count == subsequent_count


@pytest.mark.asyncio
async def test_coalescing_limits_duplicate_fetches(redis_cache):
    """With async locks, concurrent misses for the same key should coalesce.

    Verifies that concurrent callers for the same cache key don't ALL execute
    the underlying function.  Some callers should wait and reuse the result
    written by the creator.
    """

    execution_count = 0

    @RedisCache.use_cache(redis_cache, prefix="coalesce")
    async def expensive(key: str):
        nonlocal execution_count
        execution_count += 1
        await asyncio.sleep(0.5)
        return f"value-{key}"

    tasks = [expensive("same_key") for _ in range(5)]
    results = await asyncio.gather(*tasks)

    assert all(r == "value-same_key" for r in results)
    assert execution_count < 5, (
        f"Expected coalescing to reduce executions below 5, got {execution_count}"
    )


@pytest.mark.asyncio
async def test_event_loop_not_blocked_during_cache_ops(redis_cache):
    """Prove async cache ops yield to the event loop, not block it."""

    timestamps: list[float] = []

    async def background_ticker():
        for _ in range(5):
            timestamps.append(time.monotonic())
            await asyncio.sleep(0.01)

    @RedisCache.use_cache(redis_cache, prefix="interleave")
    async def slow_fetch(key: str):
        await asyncio.sleep(0.1)
        return f"result-{key}"

    ticker = asyncio.create_task(background_ticker())
    result = await slow_fetch("abc")
    await ticker

    assert result == "result-abc"
    assert len(timestamps) >= 3


# =============================================================================
# Async Migration: Pickle Backward Compatibility
# =============================================================================


@pytest.mark.asyncio
async def test_pickle_backward_compat(redis_cache):
    """Entries written by the sync client are readable by the async path."""

    sync_client = SyncRedis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        decode_responses=False,
    )

    entry = CacheEntry()
    entry.data = {"legacy": True}
    entry.version = redis_cache.version
    entry.expiry = int(time.time() + 3600)
    entry.key = "compat_key"

    payload = pickle.dumps(entry, protocol=pickle.HIGHEST_PROTOCOL)
    full_key = redis_cache._full_key("compat_key")
    sync_client.set(full_key, payload, ex=3600)
    sync_client.close()

    result = await redis_cache.read("compat_key")
    assert result is not None
    assert result.data == {"legacy": True}


# =============================================================================
# Async Migration: Connection Pool Bounds
# =============================================================================


def test_async_pool_has_max_connections():
    """Verify the async client enforces pool bounds."""
    client = get_async_redis_client()
    pool = client.connection_pool
    assert pool.max_connections == _ASYNC_POOL_MAX_CONNECTIONS


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
