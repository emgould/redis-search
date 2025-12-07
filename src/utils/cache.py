import asyncio
import copy
import dataclasses
import datetime
import gc  # Add this import at the top
import json
import logging
import os
import pickle
import shutil  # Add this import at the top
import sys
import time
import traceback
from dataclasses import dataclass
from hashlib import md5
from typing import Any

import aiofiles  # type: ignore[import-untyped]
from filelock import AsyncFileLock
from google.cloud import storage as gcs_storage  # type: ignore[attr-defined]

from utils.get_logger import get_logger

# Define version constant for Firebase Functions
RELEASE_VERSION = "1.3.4"
# Increase the maximum recursion depth
sys.setrecursionlimit(10**6)

# Cache is disabled in test environment unless explicitly enabled via ENABLE_CACHE_FOR_TESTS
# This allows integration tests to use cache when --n flag is used
DISABLE_CACHE = (
    os.getenv("ENVIRONMENT", "").lower() == "test"
    and os.getenv("ENABLE_CACHE_FOR_TESTS", "").lower() != "1"
)


def disable_cache():
    global DISABLE_CACHE
    DISABLE_CACHE = True


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        # Handle Pydantic models
        if hasattr(o, "model_dump"):
            return o.model_dump()
        return super().default(o)


@dataclass
class CacheEntry:
    expiry: int = int(datetime.datetime.now().timestamp())
    data: Any = None
    size: int = 0
    data_type: str = ""
    filename: str = ""
    function: str = ""
    args: list[Any] = dataclasses.field(default_factory=list)
    source: str = ""  # "memory" | "disk"
    version: str = "0.0"

    def to_dict(self):
        return {
            field.name: getattr(self, field.name) for field in self.__dataclass_fields__.values()
        }


"""
  Caching:
  This is a self updating asynchrounous cache that utilitizes both in memory caching and file system based caching.
  read_file_cache and write_file_cache are the main controllers for utilizing the cache. Caching is managed on the
  read side. In memory cache is checked first, then file system and if not available then returns an empty dataframe.
  Expirations are not tied to the files themselves.   When data is being requested from the cache the timestamp of the
  file is compared to the desired expiration and will return an empty dataframe if it expired. This allows for the caching
  to be fully configurable at the time in which the attempt to read from cache is initiated.  The datamanager defaults
  the expiration to one day or 86700 seconds.

# A wrapper for utilizing the cache

Cache Wrapper: will cache with file and memory if avaialable for any
function wrapped in this decorator. Important usage note.
the cached manager has special reserved props.
    shared: True or False, if shared is True then the data is not copied out of
            the memory cache. So the memory cache memory is being 'shared'
    expiry: This operates like all expiry options in the dataManager.
            if not specified defaults will be used for accessing the file cache
    no_cache: do not use the cache, force execution of the function

It is also important to note that the cache needs to operate on passed keyword arguments NOT
dataclasses.  It uses the arguments to generate a "fingerprint" for the function/args which is
used to identify the object in the cache(or not)
"""

CacheLocks: dict = {}
# Thread pool executor for running synchronous FileLock operations without blocking the event loop


class Cache:
    conn = None
    persist = False
    allow_empty = False  # Allow cache to persist if data is empty

    def __init__(
        self,
        defaultTTL=3600,
        cache_max_memory=134217728,
        verbose=False,
        location=None,
        isClassMethod=True,
        prefix="",
        persist=False,
        allow_empty=False,
        version=RELEASE_VERSION,
        use_cloud_storage=False,
        skip_file_lock=False,  # "Unsafe mode" - skip file locking for high-concurrency scenarios
    ):
        self.defaultTTL = defaultTTL
        self.cache_max_memory = cache_max_memory
        self.cache_curr_memory = 0
        self.prefix = prefix
        self.skip_file_lock = skip_file_lock

        if location is not None:
            self.base_location = location
        else:
            # Firebase Functions only allows writes to /tmp directory
            self.base_location = os.path.join("/tmp", "cache", prefix)

        self.cache_location = f"{self.base_location}"
        self.cache: dict[str, CacheEntry] = {}
        level = logging.WARNING if not verbose else logging.DEBUG
        # Use unique logger name per cache instance to avoid conflicts
        logger_name = f"cache.{prefix}" if prefix else "cache"
        self.logging = get_logger(logger_name, level=level)
        self.verbose = verbose
        self.isClassMethod = isClassMethod
        self.version_check = True
        self.version = version
        self.CacheLocks = {}
        os.makedirs(self.cache_location, exist_ok=True)
        self.logging.info(f"Caching enabled to: {self.cache_location}")
        self.disableCache = False

        # GCS Configuration
        self.persist = persist
        self.use_cloud_storage = False  # Disable for all clients
        self.gcs_client: gcs_storage.Client | None = None
        self.gcs_available = False

        if self.use_cloud_storage:
            # Check for emulator environment variables
            firestore_emulator = os.getenv("FIRESTORE_EMULATOR_HOST")
            auth_emulator = os.getenv("FIREBASE_AUTH_EMULATOR_HOST")
            functions_emulator = os.getenv("FUNCTIONS_EMULATOR")
            # Also check for common emulator indicators
            gcloud_project = os.getenv("GCLOUD_PROJECT", "")
            is_gcloud_local = "localhost" in gcloud_project or "127.0.0.1" in gcloud_project

            # Check for test environment
            is_test_env = os.getenv("ENVIRONMENT", "").lower() == "test"

            is_emulator_or_test = (
                firestore_emulator
                or auth_emulator
                or functions_emulator
                or is_gcloud_local
                or is_test_env
            )

            if is_emulator_or_test:
                self.use_cloud_storage = False
                if is_test_env:
                    self.logging.info("🧪 Test environment detected - Cloud Storage disabled")
                else:
                    self.logging.info(
                        f"🔧 Emulator mode detected - Cloud Storage disabled. "
                        f"(FIRESTORE_EMULATOR_HOST={firestore_emulator}, "
                        f"FIREBASE_AUTH_EMULATOR_HOST={auth_emulator}, "
                        f"FUNCTIONS_EMULATOR={functions_emulator})"
                    )
            else:
                self.bucket_name = "media-circle-cache"  # Or the correct bucket name from config
                # Don't initialize GCS client here - do it lazily on first use to avoid blocking startup
                self.logging.info(f"☁️  Cloud Storage backup enabled for bucket: {self.bucket_name}")

    def _ensure_gcs_client(self) -> bool:
        """
        Lazily initialize GCS client on first use to avoid blocking startup.
        Returns True if client is available, False otherwise.
        """
        if not self.use_cloud_storage:
            return False

        if self.gcs_client is not None:
            return True

        try:
            self.gcs_client = gcs_storage.Client()
            self.gcs_available = True
            self.logging.info("GCS client initialized successfully")
            return True
        except Exception as e:
            self.logging.warning(f"Failed to initialize GCS client: {e}. Cloud Storage disabled.")
            self.use_cloud_storage = False
            return False

    def clear_memory_cache(self):
        """Clear the in-memory cache while preserving file cache."""
        # Clear the cache dictionary
        self.cache.clear()  # Using clear() is slightly more efficient than reassignment
        self.cache_curr_memory = 0

        # Force garbage collection to ensure memory is freed
        gc.collect()

        self.logging.info("Memory cache cleared and garbage collection performed")

    def clear_disk_cache(self):
        """Clear the disk cache by removing and recreating the cache directory."""
        if os.path.exists(self.cache_location):
            shutil.rmtree(self.cache_location)
        os.makedirs(self.cache_location, exist_ok=True)
        self.logging.info("Disk cache cleared and directory recreated")

    def getArgs(self, args):
        if self.isClassMethod:
            ret = args[1:]
        else:
            ret = args
        return [str(arg) for arg in ret]

    def check_for_override(self):
        return False

    def get_cache_name(self, fn: str, prefix: str = "", args=None, kwargs=None):
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        cacheName = fn

        reserved_keywards = [
            "expiry",
            "shared",
            "mutable",
            "no_cache",
            "no_cache_update",
            "timeout",
        ]
        passed_keywards = [string for string in kwargs if string not in reserved_keywards]
        func_keywards = {k: v for k, v in kwargs.items() if k in passed_keywards}
        func_args = self.getArgs(args)
        if "cache_name" in kwargs:
            cacheName = kwargs["cache_name"]
            # del kwargs["cache_name"]

        else:
            if len(func_keywards.keys()) + len(func_args) > 0:
                cacheName = md5(
                    str.encode(f"{cacheName}_{str(func_keywards) + '-'.join(func_args)}")
                ).hexdigest()

        cacheName = (
            f"{prefix}_{cacheName}" if prefix and not cacheName.startswith(prefix) else cacheName
        )
        if len(cacheName) > 80:
            cacheName = cacheName[0:80] + "-" + md5(str.encode(cacheName)).hexdigest()

        if not cacheName.endswith(".pkl"):
            cacheName = cacheName + ".pkl"

        cacheName = cacheName.replace(",", "-")
        return cacheName

    """
    A utility to clear all locks
    """

    def clear_locks(self):
        for lock_name in self.CacheLocks:
            self.CacheLocks[lock_name].release()

    """
    A utility function that allows one to check the cache directly
    """

    async def check_cache(self, cache_name: str, **kwargs):
        if not cache_name.endswith(".pkl"):
            cache_name = cache_name + ".pkl"

        if "expiry" in kwargs:
            expiry = kwargs["expiry"]
            del kwargs["expiry"]
        else:
            expiry = time.time() + self.defaultTTL

        # See if we are forcing use of cache, no expiration
        if self.defaultTTL == -1:
            expiry = -1

        if "no_cache" in kwargs:
            cacheDisabled = kwargs["no_cache"]
            del kwargs["no_cache"]
        else:
            cacheDisabled = False
        noExpiration = expiry == -1

        if self.disableCache or cacheDisabled:
            return None
        else:
            cachedEntry: CacheEntry = await self.read(cache_name, noExpiration)
        return cachedEntry.data if cachedEntry is not None else None

    @classmethod
    def use_cache(cls, instance, prefix="", expiry_override=None):
        def decorator(func):
            async def inner1(*args, **kwargs):
                # Early check for cache disable flags - skip all cache logic if disabled
                # This optimization makes mocked tests much faster by avoiding expensive operations
                cacheDisabled = False
                if "no_cache" in kwargs:
                    cacheDisabled = kwargs["no_cache"]
                    del kwargs["no_cache"]

                if DISABLE_CACHE or instance.disableCache or cacheDisabled:
                    # Cache is disabled - execute function directly without any cache overhead
                    return await func(*args, **kwargs)

                # Cache is enabled - proceed with full cache logic
                cacheName = instance.get_cache_name(
                    fn=func.__name__, prefix=prefix, args=args, kwargs=kwargs
                )

                # Expiration has a bunch of levels of control
                # 1st priority as expiry kwarg
                # 2nd uses cache's defaultTTL
                #     if -1 then the cache never expires
                # Handle default expiry=None
                if "expiry" in kwargs and kwargs["expiry"] is None:
                    del kwargs["expiry"]

                if "expiry" in kwargs:
                    expiry = time.time() + kwargs["expiry"]
                    del kwargs["expiry"]
                elif instance.defaultTTL == -1:
                    expiry = -1
                else:
                    expiry = time.time() + instance.defaultTTL

                if "shared" in kwargs:
                    shared = kwargs["shared"]
                    del kwargs["shared"]
                else:
                    shared = False

                if "mutable" in kwargs:
                    mutable = kwargs["mutable"]
                    del kwargs["mutable"]
                else:
                    mutable = True

                if "no_cache_update" in kwargs:
                    cacheUpdateDisabled = kwargs["no_cache_update"]
                    del kwargs["no_cache_update"]
                else:
                    cacheUpdateDisabled = False
                # Multi-worker locking
                if shared:
                    lock = instance.CacheLocks.get(cacheName, asyncio.Lock())
                    instance.CacheLocks[cacheName] = lock

                # if defaultTTL is set to -1 then no expiration for
                # reading cache, if exists then restored
                noExpiration = expiry == -1

                if shared:
                    await lock.acquire()

                try:
                    # Cache is enabled (we already checked above), so read from cache
                    cachedEntry = await instance.read(cacheName, noExpiration, mutable)
                    if cachedEntry is None:
                        instance.log(f"Cache: {cacheName}: Uncached.Executing Function")
                        data = await func(*args, **kwargs)
                        # add to cache, adds to mem, async to disk, do not wait
                        if not cacheUpdateDisabled:
                            instance.logging.debug(f"Cache {cacheName}: Adding to cache...")
                            cachedEntry = await instance.add(
                                data,
                                cacheName,
                                funcName=func.__name__,
                                args=args[1:],
                                expiry=expiry,
                                kwargs=kwargs,
                            )
                            instance.logging.debug(f"Cache {cacheName}: Added to cache")
                        else:
                            cachedEntry = CacheEntry()
                            cachedEntry.data = data
                    else:
                        instance.logging.info(f"Cache: {cacheName}: Resolved From Cache")

                    if shared:
                        lock.release()
                    if cacheName in instance.CacheLocks:
                        del instance.CacheLocks[cacheName]

                    return cachedEntry.data
                except asyncio.exceptions.CancelledError:
                    instance.logging.info(f"Cache {cacheName} execution was cancelled")
                    return None

                except Exception as err:
                    instance.logging.warning(
                        f"Cache {cacheName} observed execution error: {str(err)}"
                    )
                    stack_msg = traceback.format_exc()
                    instance.logging.warning(f"Stack trace:\n{stack_msg}\n")
                    # Handle uncaught exception, still releasing the lock
                    if shared:
                        lock.release()
                        del instance.CacheLocks[cacheName]
                    return None

            return inner1

        return decorator

    def log(self, msg: str):
        self.logging.info(msg)

    async def remove(self, name, file: bool = True):
        if name not in self.cache:
            return
        self.cache_curr_memory = self.cache_curr_memory - self.cache[name].size
        if file:
            try:
                if os.path.exists(self.cache[name].filename):
                    os.remove(self.cache[name].filename)
            except Exception:
                self.log(
                    f"Attempted to delete: {self.cache[name].filename} but was unable to. File may already be deleted"
                )
        del self.cache[name]

    def filter_empty(self, entry):
        # Prevent empty or small data caching
        data = entry.data
        if (
            data is None
            or isinstance(data, list)
            and data == []
            or isinstance(data, str)
            and data == ""
            or isinstance(data, bytes)
            and data == b""
            or isinstance(data, int)
            and data == 0
            or isinstance(data, float)
            and data == 0
            or isinstance(data, dict)
            and len(data) == 0
            or entry.size < 20
        ):
            return None
        return entry

    async def add(self, data, cacheName, funcName, args, expiry, kwargs):
        # Ensure GCS initialization is started (fallback if not started in __init__)
        try:
            entry: CacheEntry = CacheEntry()
            entry.size = sys.getsizeof(data)
            # Expiry of minus 1 means never expire but that is a a read condition not a write condition
            expiry = time.time() + self.defaultTTL if expiry == -1 else expiry
            # Expirations can be passed as an offset as opposed toa fixed time.
            entry.expiry = time.time() + expiry if expiry < 31536000 else expiry
            entry.data_type = str(type(data))
            entry.function = funcName
            # Filter out any args that are bound functions or unpicklable objects (like aiohttp sessions)
            filtered_args = []
            for arg in args:
                # Skip callable objects with __self__ (bound methods)
                if callable(arg) and hasattr(arg, "__self__"):
                    continue
                # Skip aiohttp.ClientSession and similar async objects that contain context variables
                if hasattr(arg, "__class__"):
                    class_name = arg.__class__.__name__
                    module_name = getattr(arg.__class__, "__module__", "")
                    # Filter out aiohttp sessions and other async objects that can't be pickled
                    if class_name == "ClientSession" and "aiohttp" in module_name:
                        continue
                    # Skip objects that have context variables (common in async libraries)
                    if hasattr(arg, "_context") or hasattr(arg, "__context__"):
                        continue
                filtered_args.append(arg)
            entry.args = filtered_args
            entry.filename = f"{cacheName}"
            entry.version = self.version

            try:
                entry.data = data
            except TypeError:
                entry.data = data

            # Prevent empty or small data caching
            if not self.filter_empty(entry):
                return entry

            if hasattr(data, "error") and data.error:
                return entry

            # Turn this off, if empty its empty...cache expiry should control
            # if isinstance(data, list) and len(data) == 0:
            #     return entry
            if self.cache_curr_memory + entry.size < self.cache_max_memory:
                if kwargs.get("mutable", False):
                    self.log(f"Adding to cache: {cacheName} as mutable")
                    self.cache[cacheName] = entry
                else:
                    self.log(f"Adding to cache: {cacheName} as immutable")
                    self.cache[cacheName] = copy.deepcopy(entry)

                self.cache_curr_memory = self.cache_curr_memory + entry.size
            else:
                self.log(f"Allocated cache memory exceeded. Not added to cache:{cacheName}")

            # Write to file asynchronously...don't wait
            # asyncio.create_task(self.write_to_file(cacheEntry=entry))
            await self.write_to_file(cacheEntry=entry)

        except Exception as e:
            self.logging.warning(f"Unable to update  file cache. Encountered error:  {e}")

        return entry

    async def write_to_file(self, cacheEntry, no_storage=False):
        fullpath = f"{self.cache_location}/{cacheEntry.filename}"

        # Ensure the base cache location exists first
        try:
            os.makedirs(self.cache_location, exist_ok=True)
            # Verify directory was created
            if not os.path.exists(self.cache_location):
                raise OSError(f"Failed to create cache directory: {self.cache_location}")
        except OSError as e:
            self.logging.error(f"Failed to create cache directory {self.cache_location}: {e}")
            return

        # Ensure the directory for the cache file exists before creating lock file
        cache_file_dir = os.path.dirname(fullpath)
        try:
            os.makedirs(cache_file_dir, exist_ok=True)
            if not os.path.exists(cache_file_dir):
                raise OSError(f"Failed to create cache file directory: {cache_file_dir}")
        except OSError as e:
            self.logging.error(f"Failed to create cache file directory {cache_file_dir}: {e}")
            return

        lockfile = f"{fullpath}.lock"
        lockfile_dir = os.path.dirname(lockfile)
        # Ensure lock file directory exists - critical for AsyncFileLock
        try:
            os.makedirs(lockfile_dir, exist_ok=True)
            if not os.path.exists(lockfile_dir):
                raise OSError(f"Failed to create lock file directory: {lockfile_dir}")
        except OSError as e:
            self.logging.error(f"Failed to create lock file directory {lockfile_dir}: {e}")
            return

        # Check for stale lock file (older than 5 seconds) and remove it
        # Nothing we're pickling locally should take longer than 5 seconds
        if os.path.exists(lockfile):
            lock_age = time.time() - os.path.getmtime(lockfile)
            if lock_age > 5:
                self.logging.warning(f"Removing stale lock file {lockfile} (age: {lock_age:.1f}s)")
                try:
                    os.remove(lockfile)
                except Exception as e:
                    self.logging.warning(f"Failed to remove stale lock file: {e}")

        # Serialize the cache entry to bytes
        cache_data: bytes | None = None
        try:
            cache_data = pickle.dumps(cacheEntry)
        except (TypeError, AttributeError, pickle.PicklingError) as e:
            error_msg = str(e)
            # Check if this is a context variable error
            if "contextvars" in error_msg.lower() or "context" in error_msg.lower():
                self.logging.warning(
                    f"=====> Warning Unable to pickle cache entry for {cacheEntry.filename}. "
                    f"Contains unpicklable context variables. Error: {e}. "
                    f"Attempting to clean args..."
                )
                # Try to clean args that might contain context variables
                try:
                    # Re-filter args to ensure no context variables slipped through
                    cleaned_args = []
                    for arg in cacheEntry.args:
                        if hasattr(arg, "__class__"):
                            class_name = arg.__class__.__name__
                            module_name = getattr(arg.__class__, "__module__", "")
                            if class_name == "ClientSession" and "aiohttp" in module_name:
                                continue
                            if hasattr(arg, "_context") or hasattr(arg, "__context__"):
                                continue
                        cleaned_args.append(arg)
                    cacheEntry.args = cleaned_args
                    # Try to pickle again after cleaning args
                    try:
                        cache_data = pickle.dumps(cacheEntry)
                        self.logging.info(
                            f"Successfully pickled cache entry for {cacheEntry.filename} after cleaning args"
                        )
                    except Exception:
                        # If pickling still fails after cleaning args, fall through to data cleaning
                        cache_data = None
                except Exception:
                    pass  # If cleaning args fails, try data cleaning

            # If we still don't have cache_data, try cleaning the data via JSON round-trip
            if cache_data is None:
                # Fallback: If pickle still fails despite our checks, try cleaning and retry
                self.logging.warning(
                    f"=====> Warning Unable to pickle cache entry for {cacheEntry.filename}. "
                    f"Error: {e}. Attempting fallback JSON serialization..."
                )
                try:
                    # Clean the data by round-tripping through JSON
                    if cacheEntry.data is not None:
                        cacheEntry.data = json.loads(
                            json.dumps(cacheEntry.data, cls=EnhancedJSONEncoder)
                        )

                    # Try to pickle again with cleaned data
                    cache_data = pickle.dumps(cacheEntry)
                    self.logging.info(
                        f"Successfully cleaned and pickled cache entry for {cacheEntry.filename}"
                    )
                except Exception as clean_error:
                    # If cleaning fails, log and skip caching this entry
                    self.logging.warning(
                        f"=====> Warning Unable to clean cache entry for {cacheEntry.filename}. "
                        f"Error: {clean_error}. Skipping cache write."
                    )
                    return
                # If we reach here, cache_data should be set (either from line 562, 588, or 613)
                # If JSON cleaning failed, we would have returned above

        # At this point, cache_data must be set:
        # - Either the try block succeeded (line 562)
        # - Or the exception handler set it (line 588 or 613)
        # - Or we returned early (line 623) when it was None
        # Type narrowing: mypy knows cache_data cannot be None here
        # Removed unreachable defensive check - cache_data is guaranteed to be set at this point

        # "Unsafe mode" - skip file locking for high-concurrency scenarios
        # where lock contention causes more problems than potential write collisions
        if self.skip_file_lock:
            try:
                async with aiofiles.open(fullpath, "wb") as file:
                    await file.write(cache_data)
            except Exception as e:
                self.logging.warning(f"Error writing cache file {fullpath} (no lock): {e}")
                return
        else:
            # Final safety check: ensure lock file directory exists right before creating lock
            # This prevents race conditions where directory might be deleted between creation and lock acquisition
            if not os.path.exists(lockfile_dir):
                try:
                    os.makedirs(lockfile_dir, exist_ok=True)
                except OSError as e:
                    self.logging.error(
                        f"Failed to create lock file directory {lockfile_dir} before lock creation: {e}"
                    )
                    return

            # Use native async file lock
            lock = AsyncFileLock(lockfile, timeout=3)

            try:
                # This is fully non-blocking and safe for concurrent writers
                async with lock, aiofiles.open(fullpath, "wb") as file:
                    await file.write(cache_data)
            except (OSError, FileNotFoundError) as e:
                # Handle race condition where directory was removed between creation and lock acquisition
                # Check if this is a "No such file or directory" error related to the lock file
                error_str = str(e)
                if (
                    "No such file or directory" in error_str
                    or lockfile in error_str
                    or (hasattr(e, "errno") and e.errno == 2)
                ):
                    self.logging.debug(
                        f"Lock file directory missing during lock acquisition, recreating: {lockfile_dir}"
                    )
                    try:
                        os.makedirs(lockfile_dir, exist_ok=True)
                        # Create a new lock instance for retry (old one may be in bad state)
                        retry_lock = AsyncFileLock(lockfile, timeout=3)
                        # Retry the lock acquisition
                        async with retry_lock, aiofiles.open(fullpath, "wb") as file:
                            await file.write(cache_data)
                    except Exception as retry_error:
                        self.logging.warning(
                            f"Failed to acquire lock after directory recreation: {retry_error}"
                        )
                        raise
                else:
                    # Not a directory-related error, re-raise
                    raise

            except TimeoutError:
                # AsyncFileLock raises filelock.Timeout on timeout
                self.logging.warning(
                    f"AsyncFileLock acquisition timed out for {lockfile}, "
                    f"removing stale lock and writing without lock"
                )
                try:
                    if os.path.exists(lockfile):
                        os.remove(lockfile)
                except Exception as e:
                    self.logging.warning(f"Failed to remove stale lock file after timeout: {e}")

                # Fallback: best-effort write without lock (as in your original logic)
                try:
                    async with aiofiles.open(fullpath, "wb") as file:
                        await file.write(cache_data)
                except Exception as e:
                    self.logging.warning(f"Error writing cache file {fullpath} after timeout: {e}")
                    return

            except Exception as e:
                self.logging.warning(f"Error writing cache file {fullpath}: {e}")
                self.logging.warning(traceback.format_exc())
                return

        # Upload to GCS after file is written and lock is released
        if not no_storage and self._ensure_gcs_client():
            # Retry logic for GCS uploads to handle transient connection issues
            max_retries = 3
            retry_delay = 0.5  # Start with 500ms delay
            upload_success = False

            for attempt in range(max_retries):
                try:
                    # Use google-cloud-storage (synchronous) with run_in_executor for async compatibility
                    def _sync_upload():
                        assert self.gcs_client is not None  # Type narrowing for mypy
                        bucket = self.gcs_client.bucket(self.bucket_name)
                        blob = bucket.blob(f"{self.prefix}/{cacheEntry.filename}")
                        # upload_from_string handles bytes or str
                        blob.upload_from_string(cache_data, timeout=60)

                    loop = asyncio.get_event_loop()
                    await asyncio.wait_for(loop.run_in_executor(None, _sync_upload), timeout=60.0)
                    upload_success = True
                    self.log(f"{fullpath} written to disk and uploaded to GCS")
                    break
                except TimeoutError:
                    self.logging.warning(
                        f"GCS upload timeout for {cacheEntry.filename} (attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (2**attempt))  # Exponential backoff
                except Exception as e:
                    error_msg = str(e)
                    # Check if it's a connection error that might be retryable
                    is_retryable = any(
                        keyword in error_msg.lower()
                        for keyword in [
                            "disconnected",
                            "timeout",
                            "connection",
                            "network",
                            "temporary",
                        ]
                    )

                    if is_retryable and attempt < max_retries - 1:
                        self.logging.warning(
                            f"GCS upload failed for {cacheEntry.filename} (attempt {attempt + 1}/{max_retries}): {e}. Retrying..."
                        )
                        await asyncio.sleep(retry_delay * (2**attempt))  # Exponential backoff
                    else:
                        # Non-retryable error or last attempt
                        self.logging.warning(f"Failed to upload {cacheEntry.filename} to GCS: {e}")
                        break

            if not upload_success:
                self.log(
                    f"{fullpath} written to disk (GCS upload failed after {max_retries} attempts)"
                )
        else:
            self.log(f"{fullpath} written to disk")

        # Best-effort cleanup of lock file (AsyncFileLock should release, but
        # you were explicitly deleting the lock file before, so we keep that behavior)
        try:
            if os.path.exists(lockfile):
                os.remove(lockfile)
        except FileNotFoundError:
            pass
        except Exception:
            # Don't crash the cache on cleanup failure
            pass

    async def read(self, name, noExpiration: bool = False, mutable: bool = True):
        if name not in self.cache:
            self.log(f"{name} not in memory.Checking files")
            cacheEntry = await self.restore_from_file(name)
        else:
            cacheEntry = self.cache[name]

        if cacheEntry is None:
            return None

        if hasattr(cacheEntry.data, "error") and cacheEntry.data.error:
            return None

        # Check if data is empty
        if not self.allow_empty:
            cacheEntry = self.filter_empty(cacheEntry)
            if cacheEntry is None:
                return None

        # If we have override, no expiration checking
        if self.check_for_override():
            noExpiration = True

        # Check if version has changed
        if not noExpiration and self.version_check and cacheEntry.version != self.version:
            self.logging.info(
                f"WARNING: Cached data {cacheEntry.filename} version:{cacheEntry.version} does not match current release: {self.version}. Removing and ignoring."
            )
            filename = f"{self.cache_location}/{name}"
            if os.path.exists(filename):
                os.remove(filename)
            if name in self.cache:
                del self.cache[name]
            return None

        cacheEntry.expiry = (
            time.time() + cacheEntry.expiry if cacheEntry.expiry < 31536000 else cacheEntry.expiry
        )

        # We are expired
        if not noExpiration and cacheEntry.expiry < time.time():
            self.logging.info(f"Read request for cached:{name} was not allowed. Expired.")
            try:
                fullpath = f"{self.cache_location}/{cacheEntry.filename}"
                if os.path.exists(fullpath):
                    os.remove(fullpath)
                if name in self.cache:
                    del self.cache[name]
                if self._ensure_gcs_client():
                    try:
                        # Use google-cloud-storage (synchronous) with run_in_executor for async compatibility
                        def _sync_delete():
                            assert self.gcs_client is not None  # Type narrowing for mypy
                            bucket = self.gcs_client.bucket(self.bucket_name)
                            blob = bucket.blob(f"{self.prefix}/{cacheEntry.filename}")
                            blob.delete()

                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, _sync_delete)
                    except Exception:
                        pass  # Ignore deletion errors

            except Exception:
                pass
            return None

        # Check if we have enough memory
        if self.cache_max_memory < self.cache_curr_memory + cacheEntry.size:
            self.log("Max Memory limit hit.Clearing existing memory cache")
            self.clear_memory_cache()

        self.cache_curr_memory += cacheEntry.size

        self.cache[name] = cacheEntry
        if mutable:
            return self.cache[name]
        else:
            return copy.deepcopy(self.cache[name])

    async def restore_from_file(self, name):
        filename = f"{self.cache_location}/{name}"
        entry = None

        # Ensure the base cache location exists first
        os.makedirs(self.cache_location, exist_ok=True)

        # Ensure the directory for the cache file exists before creating lock file
        cache_file_dir = os.path.dirname(filename)
        os.makedirs(cache_file_dir, exist_ok=True)

        # Try to restore from Firebase Storage if file doesn't exist locally
        # Double-check emulator/test mode as a safeguard (defense in depth)
        firestore_emulator = os.getenv("FIRESTORE_EMULATOR_HOST")
        auth_emulator = os.getenv("FIREBASE_AUTH_EMULATOR_HOST")
        functions_emulator = os.getenv("FUNCTIONS_EMULATOR")
        is_test_env = os.getenv("ENVIRONMENT", "").lower() == "test"
        is_emulator_or_test_runtime = (
            firestore_emulator or auth_emulator or functions_emulator or is_test_env
        )

        if (
            not os.path.exists(filename)
            and not is_emulator_or_test_runtime
            and self._ensure_gcs_client()
        ):
            self.logging.info(f"Attempting to restore {name} from Firebase Storage")

            try:
                # Use google-cloud-storage (synchronous) with run_in_executor for async compatibility
                def _sync_download():
                    assert self.gcs_client is not None  # Type narrowing for mypy
                    bucket = self.gcs_client.bucket(self.bucket_name)
                    blob = bucket.blob(f"{self.prefix}/{name}")
                    # download_as_bytes has built-in timeout handling
                    cache_data = blob.download_as_bytes(timeout=10)
                    if cache_data:
                        entry = pickle.loads(cache_data)
                        return entry
                    return None

                loop = asyncio.get_event_loop()
                entry = await asyncio.wait_for(
                    loop.run_in_executor(None, _sync_download), timeout=10.0
                )
                if entry:
                    # Write to local cache for future use
                    await self.write_to_file(entry, no_storage=True)
                    return entry
            except TimeoutError:
                # GCS download timed out - log and continue (will execute function instead)
                self.logging.warning(
                    f"GCS download for {name} timed out after 10 seconds, will execute function instead"
                )
            except Exception as e:
                # GCS download failed (404 or other error) - this is normal on first run
                self.logging.debug(f"Could not restore {name} from GCS: {e}")

        # If file doesn't exist locally and we didn't get it from cloud storage, return None early
        if not os.path.exists(filename):
            return None

        try:
            # No lock needed for reads - concurrent reads are safe
            # FileLock is only needed for writes to prevent corruption
            async with aiofiles.open(filename, "rb") as file:
                try:
                    cache_data = await file.read()
                    entry = pickle.loads(cache_data)
                except Exception:
                    # Handle corrupt file.
                    self.logging.warning("Existing file cache is older format. Skipping")
                    if os.path.exists(filename):
                        os.remove(filename)
        except FileNotFoundError:
            pass
        except Exception as e:
            self.logging.warning(f"Error restoring from file {name}: {e}")
            self.logging.warning(traceback.format_exc())
            return None

        return entry

    def get_last_update(self):
        try:  # can also be minutes, seconds, etc.
            self.last_updated = os.path.getmtime(
                f"{self.cache_location}/datamgr.txt"
            )  # filename is the path to the local file you are refreshing
        except Exception:
            self.last_updated = 0
        return self.last_updated

    def set_last_update(self):
        # Ensure the cache directory exists before creating file
        os.makedirs(self.cache_location, exist_ok=True)

        self.last_updated = time.time()
        with open(f"{self.cache_location}/datamgr.txt", "w") as file1:
            file1.write(str(self.last_updated))

    @classmethod
    def for_firebase_functions(
        cls,
        prefix="",
        defaultTTL=3600,
        cache_max_memory=134217728,
        use_cloud_storage=False,
        verbose=False,
        **kwargs,
    ):
        """
        Create a Cache instance optimized for Firebase Functions.

        Args:
            prefix: Cache prefix for organizing cache files
            defaultTTL: Default time-to-live in seconds (default: 1 hour)
            cache_max_memory: Maximum memory usage in bytes (default: 128MB)
            use_cloud_storage: Enable GCS backup using google-cloud-storage (default: False)
                - Uses google-cloud-storage for reliable GCS access
                - Local /tmp cache is sufficient for most use cases
                - Automatically disabled in emulator environments
                - Set to True if you need persistent cache across cold starts
            verbose: Enable verbose logging (default: False)
            **kwargs: Additional arguments passed to Cache constructor

        Returns:
            Cache: Configured cache instance for Firebase Functions
        """
        return cls(
            defaultTTL=defaultTTL,
            cache_max_memory=cache_max_memory,
            verbose=verbose,
            prefix=prefix,
            use_cloud_storage=use_cloud_storage,
            **kwargs,
        )

    def _is_firebase_functions(self):
        """
        Detect if running in Firebase Functions environment.

        Returns:
            bool: True if running in Firebase Functions/Cloud Run
        """
        return (
            os.getenv("K_SERVICE") is not None  # Cloud Run
            or os.getenv("FUNCTION_NAME") is not None  # Cloud Functions Gen1
            or os.getenv("FUNCTION_TARGET") is not None  # Cloud Functions Gen2
        )


# Example usage for Firebase Functions:
#
# # Basic usage (memory + disk only):
# cache = Cache.for_firebase_functions(prefix="my_function")
#
# # With GCS backup for persistence across cold starts:
# cache = Cache.for_firebase_functions(
#     prefix="api_cache",
#     defaultTTL=7200,  # 2 hours
#     cache_max_memory=67108864,  # 64MB
#     use_cloud_storage=True,  # Enable GCS backup via google-cloud-storage
#     verbose=False
# )
#
# # Use as decorator:
# @Cache.use_cache(cache, prefix="my_api")
# async def expensive_api_call(param1, param2):
#     # Your expensive operation here
#     return result
