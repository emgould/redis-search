"""
Simplified cache for Firebase Functions.

Architecture: Memory → Firestore → GCS (fallback for >1MB or Firestore failures)

No disk cache - Firestore latency (~5-20ms same region) is fast enough,
and it persists across cold starts unlike /tmp.
"""

import asyncio
import copy
import dataclasses
import datetime
import gc
import logging
import os
import pickle
import sys
import time
import traceback
from dataclasses import dataclass
from hashlib import md5
from typing import Any, cast

from google.cloud import storage as gcs_storage  # type: ignore[attr-defined]

from utils.get_logger import get_logger

from .firestore_cache import FirestoreCache

# Define version constant for Firebase Functions
RELEASE_VERSION = "1.3.4"
# Increase the maximum recursion depth
sys.setrecursionlimit(10**6)

FIRESTORE_COLLECTION = os.getenv("CACHE_FIRESTORE_COLLECTION", "cache")

# Cache is disabled in test environment unless explicitly enabled via ENABLE_CACHE_FOR_TESTS
DISABLE_CACHE = (
    os.getenv("ENVIRONMENT", "").lower() == "test"
    and os.getenv("ENABLE_CACHE_FOR_TESTS", "").lower() != "1"
)


def disable_cache():
    global DISABLE_CACHE
    DISABLE_CACHE = True


@dataclass
class CacheEntry:
    expiry: int = int(datetime.datetime.now().timestamp())
    data: Any = None
    size: int = 0
    data_type: str = ""
    key: str = ""
    function: str = ""
    args: list[Any] = dataclasses.field(default_factory=list)
    source: str = ""  # "memory" | "firestore" | "gcs"
    version: str = "0.0"

    def to_dict(self):
        return {
            field.name: getattr(self, field.name) for field in self.__dataclass_fields__.values()
        }


class Cache:
    """
    Simplified async cache with memory + Firestore + GCS fallback.

    Cache hierarchy:
    1. Memory (microseconds, lost on cold start)
    2. Firestore (5-20ms, persists across cold starts, 1MB limit)
    3. GCS (20-100ms, fallback for >1MB objects or Firestore failures)
    """

    conn = None
    persist = False
    allow_empty = False

    def __init__(
        self,
        defaultTTL=3600,
        cache_max_memory=134217728,
        verbose=False,
        isClassMethod=True,
        prefix="",
        persist=False,
        allow_empty=False,
        version=RELEASE_VERSION,
        use_cloud_storage=False,
        use_firestore=True,  # Default to True now
        collection: str = FIRESTORE_COLLECTION,
    ):
        self.defaultTTL = defaultTTL
        self.cache_max_memory = cache_max_memory
        self.cache_curr_memory = 0
        self.prefix = prefix
        self.collection = collection
        self.cache: dict[str, CacheEntry] = {}
        level = logging.WARNING if not verbose else logging.DEBUG
        logger_name = f"cache.{prefix}" if prefix else "cache"
        self.logging = get_logger(logger_name, level=level)
        self.verbose = verbose
        self.isClassMethod = isClassMethod
        self.version_check = True
        self.version = version
        self.CacheLocks: dict[str, asyncio.Lock] = {}
        self._background_tasks: set[asyncio.Task] = set()
        self.disableCache = False

        # Storage configuration
        self.persist = persist
        self.use_firestore = use_firestore
        self.fire_cache: FirestoreCache | None = None

        self.use_cloud_storage = use_cloud_storage
        self.gcs_client: gcs_storage.Client | None = None
        self.gcs_available = False

        if self.use_cloud_storage:
            # Disable GCS in emulator/test environments
            is_emulator_or_test = (
                os.getenv("FIRESTORE_EMULATOR_HOST")
                or os.getenv("FIREBASE_AUTH_EMULATOR_HOST")
                or os.getenv("FUNCTIONS_EMULATOR")
                or os.getenv("ENVIRONMENT", "").lower() == "test"
            )

            if is_emulator_or_test:
                self.use_cloud_storage = False
                self.logging.info("🧪 Emulator/test environment - Cloud Storage disabled")
            else:
                self.bucket_name = "media-circle-cache"
                self.logging.info(f"☁️  Cloud Storage backup enabled: {self.bucket_name}")

        # Initialize Firestore eagerly to avoid late initialization
        if self.use_firestore:
            self._init_firestore_cache()

        self.logging.info(
            f"Cache initialized: prefix={prefix}, firestore={self.use_firestore}, gcs={use_cloud_storage}"
        )

    def _init_firestore_cache(self) -> bool:
        """Initialize FirestoreCache. Called eagerly in __init__."""
        if self.fire_cache is not None:
            return True

        try:
            self.fire_cache = FirestoreCache(
                collection=self.collection,
                prefix=self.prefix,
                version=self.version,
            )
            self.logging.info(
                f"FirestoreCache initialized: collection={self.collection}, "
                f"prefix={self.prefix}, version={self.version}"
            )
            return True
        except Exception as e:
            self.logging.warning(f"FirestoreCache init failed: {e}. Falling back to GCS.")
            self.use_firestore = False
            self.fire_cache = None
            return False

    def _ensure_firestore_cache(self) -> bool:
        """Check if Firestore cache is available."""
        if not self.use_firestore:
            return False
        if self.fire_cache is None:
            return self._init_firestore_cache()
        return True

    def _ensure_gcs_client(self) -> bool:
        """Lazily initialize GCS client on first use."""
        if not self.use_cloud_storage:
            return False

        if self.gcs_client is not None:
            return True

        try:
            self.gcs_client = gcs_storage.Client()
            self.gcs_available = True
            self.logging.info("GCS client initialized")
            return True
        except Exception as e:
            self.logging.warning(f"GCS client init failed: {e}")
            self.use_cloud_storage = False
            return False

    def _track_task(self, task: asyncio.Task) -> None:
        """Track a background task and clean up when complete."""
        self._background_tasks.add(task)

        def remove_task(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                self.logging.debug(f"Background task cancelled: {t}")
            elif t.exception() is not None:
                self.logging.debug(f"Background task failed: {t.exception()}")

        task.add_done_callback(remove_task)

    async def wait_for_pending_writes(self, timeout: float | None = None) -> None:
        """Wait for all pending write tasks to complete."""
        if not self._background_tasks:
            return

        tasks = list(self._background_tasks)
        if timeout:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=timeout
                )
            except TimeoutError:
                self.logging.warning(f"Timeout waiting for {len(tasks)} pending writes")
        else:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_pending_write_count(self) -> int:
        """Get number of pending write tasks."""
        completed = {t for t in self._background_tasks if t.done()}
        self._background_tasks -= completed
        return len(self._background_tasks)

    def clear_memory_cache(self):
        """Clear the in-memory cache."""
        self.cache.clear()
        self.cache_curr_memory = 0
        gc.collect()
        self.logging.info("Memory cache cleared")

    def getArgs(self, args):
        if self.isClassMethod:
            ret = args[1:]
        else:
            ret = args
        return [str(arg) for arg in ret]

    def check_for_override(self):
        return False

    def get_cache_key(self, fn: str, prefix: str = "", args=None, kwargs=None) -> str:
        """Generate a unique cache key for a function call."""
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []

        cache_key = fn
        reserved_keywords = [
            "expiry",
            "shared",
            "mutable",
            "no_cache",
            "no_cache_update",
            "timeout",
        ]
        passed_keywords = [k for k in kwargs if k not in reserved_keywords]
        func_keywords = {k: v for k, v in kwargs.items() if k in passed_keywords}
        func_args = self.getArgs(args)

        if "cache_name" in kwargs:
            cache_key = kwargs["cache_name"]
        elif len(func_keywords.keys()) + len(func_args) > 0:
            cache_key = md5(
                str.encode(f"{cache_key}_{str(func_keywords) + '-'.join(func_args)}")
            ).hexdigest()

        cache_key = (
            f"{prefix}_{cache_key}" if prefix and not cache_key.startswith(prefix) else cache_key
        )
        if len(cache_key) > 80:
            cache_key = cache_key[0:80] + "-" + md5(str.encode(cache_key)).hexdigest()

        cache_key = cache_key.replace(",", "-")
        return cache_key

    def clear_locks(self):
        """Clear all locks."""
        for lock_name in self.CacheLocks:
            self.CacheLocks[lock_name].release()

    async def check_cache(self, cache_key: str, **kwargs):
        """Check if a key exists in cache."""
        if "expiry" in kwargs:
            del kwargs["expiry"]

        if "no_cache" in kwargs:
            if kwargs["no_cache"]:
                return None
            del kwargs["no_cache"]

        if self.disableCache:
            return None

        noExpiration = self.defaultTTL == -1
        cachedEntry = await self.read(cache_key, noExpiration)
        return cachedEntry.data if cachedEntry is not None else None

    @classmethod
    def use_cache(cls, instance, prefix="", expiry_override=None):
        """Decorator to cache function results with thundering herd prevention."""

        # Single-flight pattern: track in-flight requests to prevent thundering herd
        _inflight: dict[str, asyncio.Future] = {}
        _inflight_lock = asyncio.Lock()

        def decorator(func):
            async def inner1(*args, **kwargs):
                # Early exit if cache disabled
                cacheDisabled = kwargs.pop("no_cache", False)
                if DISABLE_CACHE or instance.disableCache or cacheDisabled:
                    return await func(*args, **kwargs)

                # Generate cache key
                cache_key = instance.get_cache_key(
                    fn=func.__name__, prefix=prefix, args=args, kwargs=kwargs
                )

                # Handle expiry
                if "expiry" in kwargs and kwargs["expiry"] is None:
                    del kwargs["expiry"]

                if "expiry" in kwargs:
                    expiry = time.time() + kwargs.pop("expiry")
                elif instance.defaultTTL == -1:
                    expiry = -1
                else:
                    expiry = time.time() + instance.defaultTTL

                mutable = kwargs.pop("mutable", True)
                kwargs.pop("shared", False)  # Remove but don't use - we handle locking internally
                cacheUpdateDisabled = kwargs.pop("no_cache_update", False)

                noExpiration = expiry == -1

                try:
                    # Try to read from cache first (fast path)
                    cachedEntry = await instance.read(cache_key, noExpiration, mutable)
                    if cachedEntry is not None:
                        instance.logging.debug(f"Cache hit: {cache_key}")
                        return cachedEntry.data

                    # Cache miss - check if another request is already fetching this key
                    async with _inflight_lock:
                        if cache_key in _inflight:
                            # Another request is already fetching - wait for it
                            instance.logging.debug(f"Waiting for inflight: {cache_key}")
                            future = _inflight[cache_key]
                        else:
                            # We're the first - create a future for others to wait on
                            future = asyncio.get_event_loop().create_future()
                            _inflight[cache_key] = future

                    # If we found an existing future, wait for its result
                    if (
                        future.done()
                        or cache_key in _inflight
                        and _inflight[cache_key] is not future
                    ):
                        try:
                            result = await future
                            instance.logging.debug(f"Got inflight result: {cache_key}")
                            return result
                        except Exception:
                            # If the original request failed, we'll try ourselves
                            pass

                    # We need to fetch the data
                    try:
                        instance.logging.debug(f"Cache miss: {cache_key}")
                        data = await func(*args, **kwargs)

                        if not cacheUpdateDisabled:
                            await instance.add(
                                data,
                                cache_key,
                                funcName=func.__name__,
                                args=args[1:],
                                expiry=expiry,
                                kwargs=kwargs,
                            )

                        # Resolve the future for waiting requests
                        if not future.done():
                            future.set_result(data)

                        return data

                    except Exception as e:
                        # Reject the future so waiting requests know to retry
                        if not future.done():
                            future.set_exception(e)
                        raise

                    finally:
                        # Clean up inflight tracking
                        async with _inflight_lock:
                            if cache_key in _inflight and _inflight[cache_key] is future:
                                del _inflight[cache_key]

                except asyncio.exceptions.CancelledError:
                    instance.logging.debug(f"Cache {cache_key} cancelled")
                    return None

                except RuntimeError as err:
                    error_msg = str(err)
                    if "cannot schedule" in error_msg or "Event loop is closed" in error_msg:
                        instance.logging.warning(f"Event loop shutdown: {cache_key}")
                        return None
                    raise

                except Exception as err:
                    instance.logging.warning(f"Cache error {cache_key}: {err}")
                    instance.logging.warning(traceback.format_exc())
                    return None

            return inner1

        return decorator

    def log(self, msg: str):
        self.logging.debug(msg)

    async def remove(self, key: str):
        """Remove an entry from all cache layers."""
        if key in self.cache:
            self.cache_curr_memory -= self.cache[key].size
            del self.cache[key]

        # Remove from Firestore
        if self._ensure_firestore_cache():
            try:
                assert self.fire_cache is not None
                await self.fire_cache.delete(key)
            except Exception as e:
                self.logging.warning(f"Failed to delete from Firestore: {e}")

        # Remove from GCS
        if self._ensure_gcs_client():
            try:

                def _sync_delete():
                    assert self.gcs_client is not None
                    bucket = self.gcs_client.bucket(self.bucket_name)
                    blob = bucket.blob(f"{self.prefix}/{key}")
                    blob.delete()

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _sync_delete)
            except Exception:
                pass  # Ignore GCS deletion errors

    def filter_empty(self, entry):
        """Filter out empty or trivially small data."""
        data = entry.data
        if (
            data is None
            or (isinstance(data, list) and data == [])
            or (isinstance(data, str) and data == "")
            or (isinstance(data, bytes) and data == b"")
            or (isinstance(data, int) and data == 0)
            or (isinstance(data, float) and data == 0)
            or (isinstance(data, dict) and len(data) == 0)
            or entry.size < 20
        ):
            return None
        return entry

    async def add(self, data, cache_key, funcName, args, expiry, kwargs):
        """Add data to cache (memory + persistent storage)."""
        try:
            entry = CacheEntry()
            entry.size = sys.getsizeof(data)
            expiry = time.time() + self.defaultTTL if expiry == -1 else expiry
            entry.expiry = time.time() + expiry if expiry < 31536000 else expiry
            entry.data_type = str(type(data))
            entry.function = funcName

            # Filter out unpicklable args
            filtered_args = []
            for arg in args:
                if callable(arg) and hasattr(arg, "__self__"):
                    continue
                if hasattr(arg, "__class__"):
                    class_name = arg.__class__.__name__
                    module_name = getattr(arg.__class__, "__module__", "")
                    if class_name == "ClientSession" and "aiohttp" in module_name:
                        continue
                    if hasattr(arg, "_context") or hasattr(arg, "__context__"):
                        continue
                filtered_args.append(arg)
            entry.args = filtered_args
            entry.key = cache_key
            entry.version = self.version
            entry.data = data

            # Skip empty data
            if not self.filter_empty(entry):
                return entry

            if hasattr(data, "error") and data.error:
                return entry

            # Add to memory cache
            if self.cache_curr_memory + entry.size < self.cache_max_memory:
                if kwargs.get("mutable", False):
                    self.cache[cache_key] = entry
                else:
                    self.cache[cache_key] = copy.deepcopy(entry)
                self.cache_curr_memory += entry.size
                self.log(f"Added to memory: {cache_key}")
            else:
                self.log(f"Memory limit exceeded, not caching: {cache_key}")

            # Write to persistent storage in background thread (non-blocking)
            # Uses threading instead of asyncio tasks to avoid event loop dependency
            # This prevents worker crashes while keeping responses fast
            import threading

            def _background_write():
                """Write to storage in a separate thread with its own event loop."""
                try:
                    # Create new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        new_loop.run_until_complete(self._write_to_storage(entry))
                    finally:
                        # Don't close immediately - let Firestore client finish
                        # The loop will be cleaned up when thread exits
                        pass
                except Exception as e:
                    self.logging.warning(f"Background Firestore write failed for {entry.key}: {e}")

            # Start write in daemon thread (won't block function exit)
            write_thread = threading.Thread(target=_background_write, daemon=True)
            write_thread.start()

        except Exception as e:
            self.logging.warning(f"Cache add error: {e}")

        return entry

    async def _write_to_storage(self, entry: CacheEntry):
        """Write cache entry to persistent storage (Firestore with GCS fallback)."""
        key = entry.key

        # Try Firestore first
        if self._ensure_firestore_cache():
            try:
                assert self.fire_cache is not None
                await self.fire_cache.write_to_store(key, entry)
                return  # Success - Firestore handled it
            except RuntimeError as e:
                if "cannot schedule" in str(e) or "Event loop is closed" in str(e):
                    self.logging.warning(f"Event loop shutdown, skipping storage write: {key}")
                    return
                raise
            except Exception as e:
                self.logging.warning(f"Firestore write failed for {key}: {e}, falling back to GCS")
                # Fall through to GCS

        # GCS fallback (for >1MB objects or Firestore failures)
        if self._ensure_gcs_client():
            try:
                # Serialize the entry
                cache_data = pickle.dumps(entry)

                def _sync_upload():
                    assert self.gcs_client is not None
                    bucket = self.gcs_client.bucket(self.bucket_name)
                    blob = bucket.blob(f"{self.prefix}/{key}")
                    blob.upload_from_string(cache_data, timeout=60)

                loop = asyncio.get_event_loop()
                await asyncio.wait_for(loop.run_in_executor(None, _sync_upload), timeout=60.0)
                self.log(f"Written to GCS: {key}")
            except Exception as e:
                self.logging.warning(f"GCS write failed for {key}: {e}")

    async def read(self, key: str, noExpiration: bool = False, mutable: bool = True):
        """Read from cache (memory first, then persistent storage)."""
        # Check memory cache first
        if key not in self.cache:
            self.log(f"{key} not in memory, checking storage")
            cacheEntry = await self._restore_from_storage(key)
        else:
            cacheEntry = self.cache[key]

        if cacheEntry is None:
            return None

        if hasattr(cacheEntry.data, "error") and cacheEntry.data.error:
            return None

        # Check if data is empty
        if not self.allow_empty:
            cacheEntry = self.filter_empty(cacheEntry)
            if cacheEntry is None:
                return None

        if self.check_for_override():
            noExpiration = True

        # Version check
        if not noExpiration and self.version_check and cacheEntry.version != self.version:
            self.logging.info(f"Version mismatch for {key}: {cacheEntry.version} != {self.version}")
            if key in self.cache:
                del self.cache[key]
            # Clean up storage
            asyncio.create_task(self._delete_from_storage(key))
            return None

        # Normalize expiry
        cacheEntry.expiry = (
            time.time() + cacheEntry.expiry if cacheEntry.expiry < 31536000 else cacheEntry.expiry
        )

        # Check expiration
        if not noExpiration and cacheEntry.expiry < time.time():
            self.logging.info(f"Cache expired: {key}")
            if key in self.cache:
                del self.cache[key]
            asyncio.create_task(self._delete_from_storage(key))
            return None

        # Check memory limit
        if self.cache_max_memory < self.cache_curr_memory + cacheEntry.size:
            self.log("Memory limit hit, clearing cache")
            self.clear_memory_cache()

        # Only increment memory if this is a new key (avoid double-counting)
        if key not in self.cache:
            self.cache_curr_memory += cacheEntry.size
        self.cache[key] = cacheEntry

        if mutable:
            return self.cache[key]
        else:
            return copy.deepcopy(self.cache[key])

    async def _restore_from_storage(self, key: str) -> CacheEntry | None:
        """Restore cache entry from persistent storage."""
        # Try Firestore first
        if self._ensure_firestore_cache():
            try:
                assert self.fire_cache is not None
                result = await self.fire_cache.get(key)
                if result is not None and isinstance(result, CacheEntry):
                    self.logging.debug(f"Restored from Firestore: {key}")
                    return cast(CacheEntry, result)
                # Firestore returned None - this is a cache miss, don't fall through to GCS
                # GCS is only for objects >1MB that can't fit in Firestore
                self.logging.debug(f"Not found in Firestore: {key}")
                return None
            except RuntimeError as e:
                if "cannot schedule" in str(e) or "Event loop is closed" in str(e):
                    return None
                raise
            except Exception as e:
                self.logging.warning(f"Firestore read failed for {key}: {e}")
                # Fall through to GCS only on Firestore failure

        # GCS fallback - only used when Firestore fails or is disabled
        if self._ensure_gcs_client():
            try:

                def _sync_download() -> CacheEntry | None:
                    assert self.gcs_client is not None
                    bucket = self.gcs_client.bucket(self.bucket_name)
                    blob = bucket.blob(f"{self.prefix}/{key}")
                    # Check if blob exists first to avoid timeout on 404
                    if not blob.exists():
                        return None
                    cache_data = blob.download_as_bytes(timeout=10, retry=None)
                    if cache_data:
                        loaded = pickle.loads(cache_data)
                        if isinstance(loaded, CacheEntry):
                            return loaded
                    return None

                loop = asyncio.get_event_loop()
                entry = await asyncio.wait_for(
                    loop.run_in_executor(None, _sync_download), timeout=15.0
                )
                if entry:
                    self.logging.debug(f"Restored from GCS: {key}")
                    return entry
            except TimeoutError:
                self.logging.warning(f"GCS download timeout for {key}")
            except Exception as e:
                # Only log as warning if it's not a NotFound (expected for cache miss)
                error_str = str(e)
                if "NotFound" in error_str or "404" in error_str:
                    self.logging.debug(f"Not found in GCS: {key}")
                else:
                    self.logging.debug(f"GCS read failed for {key}: {e}")

        return None

    async def _delete_from_storage(self, key: str):
        """Delete cache entry from persistent storage."""
        # Delete from Firestore
        if self._ensure_firestore_cache():
            try:
                assert self.fire_cache is not None
                await self.fire_cache.delete(key)
            except Exception as e:
                self.logging.warning(f"Firestore delete failed for {key}: {e}")

        # Delete from GCS
        if self._ensure_gcs_client():
            try:

                def _sync_delete():
                    assert self.gcs_client is not None
                    bucket = self.gcs_client.bucket(self.bucket_name)
                    blob = bucket.blob(f"{self.prefix}/{key}")
                    blob.delete()

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _sync_delete)
            except Exception:
                pass  # Ignore GCS deletion errors

    @classmethod
    def for_firebase_functions(
        cls,
        prefix="",
        defaultTTL=3600,
        cache_max_memory=134217728,
        use_cloud_storage=False,
        use_firestore=True,
        verbose=False,
        **kwargs,
    ):
        """
        Create a Cache instance optimized for Firebase Functions.

        Args:
            prefix: Cache prefix for organizing entries
            defaultTTL: Default time-to-live in seconds (default: 1 hour)
            cache_max_memory: Maximum memory usage in bytes (default: 128MB)
            use_firestore: Enable Firestore cache (default: True)
            use_cloud_storage: Enable GCS backup for >1MB objects (default: False)
            verbose: Enable verbose logging (default: False)

        Returns:
            Cache: Configured cache instance
        """
        return cls(
            defaultTTL=defaultTTL,
            cache_max_memory=cache_max_memory,
            verbose=verbose,
            prefix=prefix,
            use_firestore=use_firestore,
            use_cloud_storage=use_cloud_storage,
            **kwargs,
        )

    def _is_firebase_functions(self):
        """Detect if running in Firebase Functions environment."""
        return (
            os.getenv("K_SERVICE") is not None
            or os.getenv("FUNCTION_NAME") is not None
            or os.getenv("FUNCTION_TARGET") is not None
        )


# Example usage:
#
# cache = Cache.for_firebase_functions(prefix="my_api", use_firestore=True)
#
# @RedisCache.use_cache(cache, prefix="search")
# async def expensive_search(query: str):
#     return await do_expensive_work(query)
