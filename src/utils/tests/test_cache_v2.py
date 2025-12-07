"""
Integration tests for cache_v2.py - runs against Firebase emulator.

These tests require the Firebase emulator to be running:
    cd firebase && ./dev-start.sh

Test scenarios:
- Memory cache operations
- Firestore cache read/write
- Document too large for Firestore (>1MB) - falls back gracefully
- Expiration handling
- Version mismatch
- Decorator functionality
"""

import asyncio
import os
import time
from dataclasses import dataclass

import pytest

from utils.cache_v2 import RELEASE_VERSION, Cache, CacheEntry

# Ensure we're using the emulator
os.environ.setdefault("FIRESTORE_EMULATOR_HOST", "localhost:8080")

# Enable cache for these tests (ENVIRONMENT=test normally disables cache)
os.environ["ENABLE_CACHE_FOR_TESTS"] = "1"

# Force-enable cache since module may have already been imported with DISABLE_CACHE=True
from utils import cache_v2 as _cache_module

_cache_module.DISABLE_CACHE = False

# Test collection name - separate from production cache
TEST_COLLECTION = "cache_test"


def clear_test_collection():
    """Clear the test Firestore collection before tests run."""
    from google.cloud import firestore

    try:
        db = firestore.Client()
        docs = db.collection(TEST_COLLECTION).limit(500).stream()
        deleted = 0
        for doc in docs:
            doc.reference.delete()
            deleted += 1
        if deleted > 0:
            print(f"🧹 Cleared {deleted} documents from {TEST_COLLECTION}")
    except Exception as e:
        print(f"⚠️ Could not clear test collection: {e}")


# Clear test collection at module load
clear_test_collection()


# =============================================================================
# Module-level dataclasses for pickling tests
# (Local classes inside functions can't be pickled)
# =============================================================================


@dataclass
class TestModel:
    """Simple model for testing."""

    id: int
    name: str
    tags: list


@dataclass
class InnerModel:
    """Inner model for nested testing."""

    value: int


@dataclass
class OuterModel:
    """Outer model with nested inner model."""

    name: str
    inner: InnerModel


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cache_memory_only():
    """Cache with only memory (no persistent storage)."""
    cache = Cache(
        prefix="test_mem",
        defaultTTL=3600,
        use_firestore=False,
        use_cloud_storage=False,
        verbose=True,
    )
    yield cache
    cache.clear_memory_cache()


@pytest.fixture
def cache_with_firestore():
    """Cache with Firestore enabled (uses emulator)."""
    cache = Cache(
        prefix="test_fs",
        defaultTTL=3600,
        use_firestore=True,
        use_cloud_storage=False,
        verbose=True,
        collection=TEST_COLLECTION,
    )
    yield cache
    cache.clear_memory_cache()


@pytest.fixture
def cache_short_ttl():
    """Cache with short TTL for expiration tests."""
    cache = Cache(
        prefix="test_ttl",
        defaultTTL=1,  # 1 second TTL
        use_firestore=True,
        use_cloud_storage=False,
        verbose=True,
        collection=TEST_COLLECTION,
    )
    yield cache
    cache.clear_memory_cache()


# =============================================================================
# Memory Cache Tests
# =============================================================================


class TestMemoryCache:
    """Basic memory cache operations."""

    @pytest.mark.asyncio
    async def test_add_and_read(self, cache_memory_only):
        """Add data and read it back from memory."""
        test_data = {"name": "test", "value": 123, "nested": {"a": [1, 2, 3]}}

        entry = await cache_memory_only.add(
            data=test_data,
            cache_key="mem_test_1",
            funcName="test_func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )

        assert entry is not None
        assert entry.data == test_data

        # Read back from memory
        result = await cache_memory_only.read("mem_test_1")
        assert result is not None
        assert result.data == test_data
        print(f"✅ Memory cache read/write works: {result.data}")

    @pytest.mark.asyncio
    async def test_cache_miss(self, cache_memory_only):
        """Cache miss returns None."""
        result = await cache_memory_only.read("nonexistent_key_12345")
        assert result is None
        print("✅ Cache miss correctly returns None")

    @pytest.mark.asyncio
    async def test_clear_memory(self, cache_memory_only):
        """Clearing memory cache works."""
        await cache_memory_only.add(
            data="test",
            cache_key="clear_test",
            funcName="func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )

        assert len(cache_memory_only.cache) > 0
        cache_memory_only.clear_memory_cache()
        assert len(cache_memory_only.cache) == 0
        print("✅ Memory cache cleared successfully")


# =============================================================================
# Firestore Integration Tests (against emulator)
# =============================================================================


class TestFirestoreIntegration:
    """Tests that actually hit Firestore emulator with full data verification."""

    @pytest.mark.asyncio
    async def test_firestore_write_and_verify(self):
        """Write to Firestore and verify data is persisted correctly."""
        cache = Cache(
            prefix="fs_write",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
            collection=TEST_COLLECTION,
        )

        call_count = 0
        test_data = {"message": "Hello Firestore!", "value": 42}

        @Cache.use_cache(cache, prefix="write")
        async def get_data():
            nonlocal call_count
            call_count += 1
            return test_data.copy()

        # First call - writes to Firestore
        result1 = await get_data()
        assert result1 == test_data
        assert call_count == 1

        await cache.wait_for_pending_writes(timeout=5.0)

        # Clear memory and verify Firestore has the data
        cache.clear_memory_cache()

        # Second call - should restore from Firestore
        result2 = await get_data()
        assert result2 == test_data, "Data from Firestore should match original"
        assert call_count == 1, "Should NOT re-execute - data from Firestore"

        print(f"✅ Firestore write verified: {result2}")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_firestore_preserves_complex_nested_data(self):
        """Verify Firestore preserves complex nested structures exactly."""
        cache = Cache(
            prefix="fs_complex",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
            collection=TEST_COLLECTION,
        )

        call_count = 0
        complex_data = {
            "users": [
                {"id": 1, "name": "Alice", "tags": ["admin", "active"], "score": 95.5},
                {"id": 2, "name": "Bob", "tags": ["user"], "score": 87.3},
            ],
            "metadata": {
                "version": "1.0",
                "nested": {"deep": {"value": True, "count": 0}},
            },
            "numbers": [1, 2, 3, 4, 5],
            "flags": {"enabled": True, "debug": False},
        }

        @Cache.use_cache(cache, prefix="complex")
        async def get_complex():
            nonlocal call_count
            call_count += 1
            # Return a copy to ensure we're testing the cached version
            import copy

            return copy.deepcopy(complex_data)

        # First call
        result1 = await get_complex()
        assert result1 == complex_data
        assert call_count == 1

        await cache.wait_for_pending_writes(timeout=5.0)
        cache.clear_memory_cache()

        # Second call - from Firestore
        result2 = await get_complex()

        # Detailed verification
        assert result2 == complex_data, "Complex data should match exactly"
        assert result2["users"][0]["name"] == "Alice"
        assert result2["users"][0]["tags"] == ["admin", "active"]
        assert result2["users"][1]["score"] == 87.3
        assert result2["metadata"]["nested"]["deep"]["value"] is True
        assert result2["numbers"] == [1, 2, 3, 4, 5]
        assert call_count == 1, "Should restore from Firestore"

        print("✅ Complex nested data preserved exactly through Firestore")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_firestore_multiple_keys_independent(self):
        """Verify multiple cache keys are stored and retrieved independently."""
        cache = Cache(
            prefix="fs_multi",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
            collection=TEST_COLLECTION,
        )

        call_count = 0

        @Cache.use_cache(cache, prefix="item")
        async def get_item(item_id):
            nonlocal call_count
            call_count += 1
            return {"id": item_id, "name": f"Item {item_id}", "price": item_id * 10}

        # Cache multiple items
        item1 = await get_item(1)
        item2 = await get_item(2)
        item3 = await get_item(3)
        assert call_count == 3

        await cache.wait_for_pending_writes(timeout=5.0)
        cache.clear_memory_cache()

        # Restore in different order
        restored3 = await get_item(3)
        restored1 = await get_item(1)
        restored2 = await get_item(2)

        assert call_count == 3, "All items should restore from Firestore"
        assert restored1 == item1
        assert restored2 == item2
        assert restored3 == item3
        assert restored1["name"] == "Item 1"
        assert restored2["price"] == 20
        assert restored3["id"] == 3

        print("✅ Multiple independent keys stored and retrieved correctly")
        cache.clear_memory_cache()


# =============================================================================
# Document Size Limit Tests (>1MB)
# =============================================================================


class TestFirestoreSizeLimit:
    """Test behavior when data exceeds Firestore's 1MB limit."""

    @pytest.mark.asyncio
    async def test_large_document_stays_in_memory_only(self):
        """
        Documents >1MB should be cached in memory but NOT in Firestore.
        Verify data integrity in memory cache.
        """
        cache = Cache(
            prefix="large_mem",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
        )

        call_count = 0
        # Create data larger than 1MB (~1.5MB)
        large_content = "x" * 1_500_000

        @Cache.use_cache(cache, prefix="large")
        async def get_large_data():
            nonlocal call_count
            call_count += 1
            return {"content": large_content, "size": len(large_content)}

        # First call - executes and caches to memory
        result1 = await get_large_data()
        assert result1["content"] == large_content
        assert result1["size"] == 1_500_000
        assert call_count == 1

        await cache.wait_for_pending_writes(timeout=5.0)

        # Second call - should hit memory cache
        result2 = await get_large_data()
        assert result2 == result1, "Memory cached data should match"
        assert call_count == 1, "Should use memory cache"

        print(f"✅ Large document ({len(large_content):,} bytes) cached in memory correctly")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_large_document_not_restored_after_memory_clear(self):
        """Large documents should NOT be recoverable from Firestore after memory clear."""
        cache = Cache(
            prefix="large_nofs",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
        )

        call_count = 0
        large_content = "y" * 1_500_000

        @Cache.use_cache(cache, prefix="bigdata")
        async def get_big_data():
            nonlocal call_count
            call_count += 1
            return {"data": large_content}

        # First call
        result1 = await get_big_data()
        assert result1["data"] == large_content
        assert call_count == 1

        await cache.wait_for_pending_writes(timeout=5.0)

        # Clear memory - data should be lost (not in Firestore)
        cache.clear_memory_cache()

        # Second call - should execute function again (no Firestore backup)
        result2 = await get_big_data()
        assert result2["data"] == large_content
        assert call_count == 2, "Function should execute again - large data not in Firestore"

        print("✅ Large document correctly NOT recoverable from Firestore")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_medium_document_roundtrips_through_firestore(self):
        """Documents under 1MB should roundtrip through Firestore correctly."""
        cache = Cache(
            prefix="medium_fs",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
            collection=TEST_COLLECTION,
        )

        call_count = 0
        # ~500KB raw - fits in Firestore after encoding
        medium_content = "z" * 500_000

        @Cache.use_cache(cache, prefix="medium")
        async def get_medium_data():
            nonlocal call_count
            call_count += 1
            return {"content": medium_content, "size": len(medium_content)}

        # First call
        result1 = await get_medium_data()
        assert result1["content"] == medium_content
        assert result1["size"] == 500_000
        assert call_count == 1

        await cache.wait_for_pending_writes(timeout=5.0)

        # Clear memory to force Firestore restore
        cache.clear_memory_cache()

        # Second call - should restore from Firestore
        result2 = await get_medium_data()
        assert result2 == result1, "Firestore-restored data should match exactly"
        assert result2["content"] == medium_content
        assert call_count == 1, "Should restore from Firestore, not re-execute"

        print(f"✅ Medium document ({len(medium_content):,} bytes) roundtrips through Firestore")
        cache.clear_memory_cache()


# =============================================================================
# Expiration Tests
# =============================================================================


class TestExpiration:
    """Test cache expiration behavior."""

    @pytest.mark.asyncio
    async def test_expired_entry_not_returned(self, cache_short_ttl):
        """Expired entries should not be returned."""
        cache_key = f"expiry_test_{int(time.time())}"

        await cache_short_ttl.add(
            data="will_expire",
            cache_key=cache_key,
            funcName="test_func",
            args=[],
            expiry=time.time() + 1,  # 1 second
            kwargs={},
        )

        # Immediate read should work
        result1 = await cache_short_ttl.read(cache_key)
        assert result1 is not None
        print(f"✅ Immediate read works: {result1.data}")

        # Wait for expiration
        await asyncio.sleep(2)

        # Read after expiration should return None
        result2 = await cache_short_ttl.read(cache_key)
        assert result2 is None, "Expired entry should return None"
        print("✅ Expired entry correctly returns None")

    @pytest.mark.asyncio
    async def test_no_expiration_flag(self, cache_short_ttl):
        """noExpiration=True should bypass expiry check."""
        cache_key = f"no_expiry_test_{int(time.time())}"

        await cache_short_ttl.add(
            data="never_expire",
            cache_key=cache_key,
            funcName="test_func",
            args=[],
            expiry=time.time() + 1,
            kwargs={},
        )

        await asyncio.sleep(2)

        # With noExpiration=True, should still return data
        result = await cache_short_ttl.read(cache_key, noExpiration=True)
        assert result is not None
        assert result.data == "never_expire"
        print("✅ noExpiration flag bypasses expiry check")


# =============================================================================
# Version Mismatch Tests
# =============================================================================


class TestVersionMismatch:
    """Test cache version checking."""

    @pytest.mark.asyncio
    async def test_version_mismatch_invalidates_cache(self, cache_with_firestore):
        """Entries with wrong version should be rejected."""
        cache_key = f"version_test_{int(time.time())}"

        # Manually create an entry with wrong version
        entry = CacheEntry()
        entry.data = "old_version_data"
        entry.expiry = time.time() + 3600
        entry.key = cache_key
        entry.version = "0.0.0"  # Wrong version
        entry.size = 100

        cache_with_firestore.cache[cache_key] = entry

        # Read should reject due to version mismatch
        result = await cache_with_firestore.read(cache_key)
        assert result is None, "Version mismatch should invalidate entry"
        assert cache_key not in cache_with_firestore.cache
        print(f"✅ Version mismatch correctly invalidates cache (expected: {RELEASE_VERSION})")


# =============================================================================
# Empty Data Filtering Tests
# =============================================================================


class TestEmptyDataFiltering:
    """Test that empty/trivial data is not cached."""

    @pytest.mark.asyncio
    async def test_none_not_cached(self, cache_memory_only):
        """None data should not be cached."""
        await cache_memory_only.add(
            data=None,
            cache_key="none_test",
            funcName="func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )
        assert "none_test" not in cache_memory_only.cache
        print("✅ None data correctly not cached")

    @pytest.mark.asyncio
    async def test_empty_list_not_cached(self, cache_memory_only):
        """Empty list should not be cached."""
        await cache_memory_only.add(
            data=[],
            cache_key="empty_list_test",
            funcName="func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )
        assert "empty_list_test" not in cache_memory_only.cache
        print("✅ Empty list correctly not cached")

    @pytest.mark.asyncio
    async def test_empty_dict_not_cached(self, cache_memory_only):
        """Empty dict should not be cached."""
        await cache_memory_only.add(
            data={},
            cache_key="empty_dict_test",
            funcName="func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )
        assert "empty_dict_test" not in cache_memory_only.cache
        print("✅ Empty dict correctly not cached")

    @pytest.mark.asyncio
    async def test_non_empty_data_cached(self, cache_memory_only):
        """Non-empty data should be cached."""
        await cache_memory_only.add(
            data={"key": "value"},
            cache_key="non_empty_test",
            funcName="func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )
        assert "non_empty_test" in cache_memory_only.cache
        print("✅ Non-empty data correctly cached")


# =============================================================================
# Decorator Tests
# =============================================================================


class TestDecorator:
    """Test @Cache.use_cache decorator with data verification."""

    @pytest.mark.asyncio
    async def test_decorator_caches_to_memory(self):
        """Decorator caches to memory and returns identical data."""
        cache = Cache(
            prefix="deco_mem",
            defaultTTL=3600,
            use_firestore=False,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
        )

        call_count = 0
        expected_data = {"computed": 42, "nested": {"values": [1, 2, 3]}}

        @Cache.use_cache(cache, prefix="deco")
        async def expensive_operation(value):
            nonlocal call_count
            call_count += 1
            return {"computed": value * 2, "nested": {"values": [1, 2, 3]}}

        # First call - executes function
        result1 = await expensive_operation(21)
        assert result1 == expected_data, "First call should return expected data"
        assert call_count == 1, "Function should execute on first call"

        # Second call - should return cached data
        result2 = await expensive_operation(21)
        assert result2 == expected_data, "Cached data should match original"
        assert result2 == result1, "Cached result should equal first result"
        assert call_count == 1, "Function should NOT execute on cache hit"

        print(f"✅ Memory cache returns identical data: {result2}")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_decorator_caches_to_firestore_and_restores(self):
        """Decorator writes to Firestore and restores correctly after memory clear."""
        cache = Cache(
            prefix="deco_fs",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            verbose=True,
            collection=TEST_COLLECTION,
        )

        call_count = 0
        expected_data = {
            "user_id": 123,
            "name": "Test User",
            "preferences": {"theme": "dark", "notifications": True},
        }

        @Cache.use_cache(cache, prefix="user")
        async def get_user_data(user_id):
            nonlocal call_count
            call_count += 1
            return {
                "user_id": user_id,
                "name": "Test User",
                "preferences": {"theme": "dark", "notifications": True},
            }

        # First call - executes function and writes to Firestore
        result1 = await get_user_data(123)
        assert result1 == expected_data, "First call should return expected data"
        assert call_count == 1

        # Wait for Firestore write to complete
        await cache.wait_for_pending_writes(timeout=5.0)

        # Clear memory cache to force Firestore read
        cache.clear_memory_cache()
        assert len(cache.cache) == 0, "Memory cache should be empty"

        # Second call - should restore from Firestore
        result2 = await get_user_data(123)
        assert result2 == expected_data, "Firestore-restored data should match original"
        assert result2 == result1, "Restored data should equal first result"
        assert call_count == 1, "Function should NOT execute - data from Firestore"

        print(f"✅ Firestore roundtrip returns identical data: {result2}")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_decorator_different_args_different_cache(self):
        """Different arguments produce different cache entries with correct data."""
        cache = Cache(
            prefix="args_test",
            defaultTTL=3600,
            use_firestore=True,
            use_cloud_storage=False,
            isClassMethod=False,
            collection=TEST_COLLECTION,
        )

        call_count = 0

        @Cache.use_cache(cache, prefix="mult")
        async def multiply(x):
            nonlocal call_count
            call_count += 1
            return {"input": x, "result": x * 10}

        # Call with different args
        result1 = await multiply(5)
        result2 = await multiply(7)
        assert call_count == 2, "Two unique args = two function calls"

        # Verify data is correct
        assert result1 == {"input": 5, "result": 50}
        assert result2 == {"input": 7, "result": 70}

        # Call again - should use cache with correct data
        result1_cached = await multiply(5)
        result2_cached = await multiply(7)
        assert call_count == 2, "Cached calls should not execute function"
        assert result1_cached == result1, "Cached data for arg=5 should match"
        assert result2_cached == result2, "Cached data for arg=7 should match"

        print("✅ Different args cached independently with correct data")
        cache.clear_memory_cache()

    @pytest.mark.asyncio
    async def test_decorator_no_cache_flag(self):
        """no_cache=True bypasses cache entirely."""
        cache = Cache(
            prefix="no_cache_test",
            defaultTTL=3600,
            use_firestore=False,
            use_cloud_storage=False,
            isClassMethod=False,
        )

        call_count = 0

        @Cache.use_cache(cache, prefix="nc")
        async def get_timestamp():
            nonlocal call_count
            call_count += 1
            return {"count": call_count}

        r1 = await get_timestamp(no_cache=True)
        r2 = await get_timestamp(no_cache=True)
        r3 = await get_timestamp(no_cache=True)

        assert call_count == 3, "no_cache should execute function each time"
        assert r1 == {"count": 1}
        assert r2 == {"count": 2}
        assert r3 == {"count": 3}
        print(f"✅ no_cache=True bypasses cache (calls: {call_count})")
        cache.clear_memory_cache()


# =============================================================================
# Pydantic/Dataclass Tests
# =============================================================================


class TestComplexObjects:
    """Test caching of complex Python objects."""

    @pytest.mark.asyncio
    async def test_dataclass_caching(self, cache_with_firestore):
        """Dataclasses should be cacheable (using module-level class)."""
        # Using module-level TestModel so it can be pickled
        model = TestModel(id=1, name="Test", tags=["a", "b"])
        cache_key = f"dataclass_test_{int(time.time())}"

        await cache_with_firestore.add(
            data=model,
            cache_key=cache_key,
            funcName="test_func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )

        await cache_with_firestore.wait_for_pending_writes(timeout=5.0)
        cache_with_firestore.clear_memory_cache()

        result = await cache_with_firestore.read(cache_key)
        assert result is not None
        assert result.data.id == 1
        assert result.data.name == "Test"
        assert result.data.tags == ["a", "b"]
        print(f"✅ Dataclass cached and restored: {result.data}")

    @pytest.mark.asyncio
    async def test_nested_dataclass_caching(self, cache_with_firestore):
        """Nested dataclasses should be cacheable (using module-level classes)."""
        # Using module-level OuterModel and InnerModel so they can be pickled
        model = OuterModel(name="outer", inner=InnerModel(value=42))
        cache_key = f"nested_dc_test_{int(time.time())}"

        await cache_with_firestore.add(
            data=model,
            cache_key=cache_key,
            funcName="test_func",
            args=[],
            expiry=time.time() + 3600,
            kwargs={},
        )

        await cache_with_firestore.wait_for_pending_writes(timeout=5.0)
        cache_with_firestore.clear_memory_cache()

        result = await cache_with_firestore.read(cache_key)
        assert result is not None
        assert result.data.name == "outer"
        assert result.data.inner.value == 42
        print("✅ Nested dataclass cached and restored")


# =============================================================================
# Cache Key Generation Tests
# =============================================================================


class TestCacheKeyGeneration:
    """Test cache key generation."""

    def test_consistent_key_generation(self, cache_memory_only):
        """Same inputs should produce same key."""
        key1 = cache_memory_only.get_cache_key("func", args=["self", "arg1"])
        key2 = cache_memory_only.get_cache_key("func", args=["self", "arg1"])
        assert key1 == key2
        print(f"✅ Consistent key generation: {key1}")

    def test_different_args_different_keys(self, cache_memory_only):
        """Different args should produce different keys."""
        key1 = cache_memory_only.get_cache_key("func", args=["self", "arg1"])
        key2 = cache_memory_only.get_cache_key("func", args=["self", "arg2"])
        assert key1 != key2
        print("✅ Different args = different keys")

    def test_reserved_kwargs_ignored(self, cache_memory_only):
        """Reserved kwargs should not affect key."""
        key1 = cache_memory_only.get_cache_key("func", kwargs={"param": "value"})
        key2 = cache_memory_only.get_cache_key(
            "func",
            kwargs={"param": "value", "expiry": 999, "no_cache": True, "shared": True},
        )
        assert key1 == key2
        print("✅ Reserved kwargs correctly ignored in key generation")


# =============================================================================
# Run tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
