import asyncio
import copy
import dataclasses
import datetime
import io
import logging
import os
import pickle
import random
import sys
import time
from dataclasses import dataclass
from hashlib import md5
from typing import Any, cast

from redis import Redis
from redis.exceptions import RedisError

# Try to import get_logger, fallback to standard logging
try:
    from utils.get_logger import get_logger
except ImportError:

    def get_logger(name: str, level=None, filename=None, app_name=None) -> logging.Logger:
        logger = logging.getLogger(name)
        if level is None:
            level = logging.INFO
        logger.setLevel(level)
        return logger


# Define version constant
RELEASE_VERSION = "1.3.4"

# Redis hash key for the shared version registry.
# Stores one version per cache prefix so all repos/processes sharing
# the same Redis instance agree on the effective version.
VERSION_REGISTRY_KEY = "__cache_versions__"

_logger = logging.getLogger("rediscache.versions")


_DEFAULT_NEW_PREFIX_VERSION = "1.0.0"

# Captured at module load time — used to calculate container age in alerts
_MODULE_LOAD_TIME = time.monotonic()

# Advisory lock for thundering-herd prevention (cache coalescing)
_LOCK_TTL_SECONDS = 120  # Long enough for slow fetches (SchedulesDirect ~60s)
_LOCK_MAX_WAIT = 12  # Seconds waiters poll before re-attempting lock
_LOCK_POLL_INITIAL = 0.05
_LOCK_POLL_MAX = 0.3
_LOCK_POLL_JITTER = 0.03


def _poll_sleep(attempt: int) -> float:
    """Jittered exponential backoff, capped. Spreads load, avoids stampede."""
    base = min(_LOCK_POLL_INITIAL * (1.5**attempt), _LOCK_POLL_MAX)
    return base + random.uniform(0, _LOCK_POLL_JITTER)


# Lua script: atomic compare-and-delete. Only removes the lock if the
# caller still owns it (token matches). Prevents deleting another
# owner's lock when the original creator's function outlives the TTL.
_RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


def get_cache_version(prefix: str) -> str:
    """Read the shared cache version for *prefix* from the Redis registry.

    If the prefix has never been registered, it is auto-registered with
    version ``1.0.0`` so it becomes immediately visible to all repos
    sharing this Redis instance.

    Returns ``RELEASE_VERSION`` as a fallback only when Redis is unreachable.
    Retries up to 2 times on timeout errors, resetting the singleton client
    between attempts so a dead TCP socket is replaced with a fresh connection.
    If all retries fail, an alert is sent because a version mismatch causes
    every cache read to silently fail.
    """
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            client = get_redis_client()
            raw = client.hget(VERSION_REGISTRY_KEY, prefix)
            if raw is None:
                # Auto-register new prefixes so they appear in the shared registry
                client.hset(VERSION_REGISTRY_KEY, prefix, _DEFAULT_NEW_PREFIX_VERSION)
                _logger.info(
                    "Auto-registered new cache prefix '%s' with version %s",
                    prefix,
                    _DEFAULT_NEW_PREFIX_VERSION,
                )
                return _DEFAULT_NEW_PREFIX_VERSION
            return raw.decode() if isinstance(raw, bytes) else str(raw)
        except RedisError as e:
            is_timeout = "timeout" in str(e).lower() or "timed out" in str(e).lower()
            if is_timeout and attempt < max_attempts - 1:
                _logger.warning(
                    "Redis timeout reading version for '%s' (attempt %d/%d): %s. "
                    "Resetting client and retrying...",
                    prefix,
                    attempt + 1,
                    max_attempts,
                    e,
                )
                reset_redis_client()
                time.sleep(1)  # Brief backoff before retry
            else:
                _logger.error("Redis error reading version for '%s': %s", prefix, e)
                _send_version_fallback_alert(prefix, e, attempts=attempt + 1)
                return RELEASE_VERSION
        except Exception as e:
            _logger.error("Error reading version for '%s': %s", prefix, e)
            _send_version_fallback_alert(prefix, e, attempts=attempt + 1)
            return RELEASE_VERSION

    return RELEASE_VERSION  # Satisfies type checker; unreachable if logic is correct


def _probe_redis_status() -> dict[str, str]:
    """Best-effort probe of Redis health for diagnostic metadata.

    Creates a fresh, short-timeout connection (separate from the singleton)
    to avoid interfering with normal operations. Returns a dict of status
    fields suitable for inclusion in alert metadata.
    """
    host = os.getenv("REDIS_HOST", "localhost")
    port_str = os.getenv("REDIS_PORT", "6379")
    password = os.getenv("REDIS_PASSWORD")

    try:
        port = int(port_str)
    except ValueError:
        return {"Redis Status": f"invalid port: {port_str}"}

    probe = Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=False,
        socket_timeout=3,
        socket_connect_timeout=3,
    )

    status: dict[str, str] = {}
    try:
        t0 = time.perf_counter()
        probe.ping()
        ping_ms = (time.perf_counter() - t0) * 1000
        status["Redis PING"] = f"OK ({ping_ms:.0f}ms)"
    except Exception as e:
        status["Redis PING"] = f"FAILED: {e}"
        return status  # No point trying INFO if PING fails

    try:
        info: dict[str, Any] = cast(dict[str, Any], probe.info("memory"))
        used_mb = int(info.get("used_memory", 0)) / (1024 * 1024)
        peak_mb = int(info.get("used_memory_peak", 0)) / (1024 * 1024)
        maxmem = info.get("maxmemory")
        max_str = f"{int(maxmem) / (1024**3):.1f}GB" if maxmem else "none"
        status["Redis Memory"] = f"used={used_mb:.0f}MB, peak={peak_mb:.0f}MB, max={max_str}"
    except Exception as e:
        status["Redis Memory"] = f"INFO failed: {e}"

    try:
        info_server: dict[str, Any] = cast(dict[str, Any], probe.info("server"))
        uptime_seconds = int(info_server.get("uptime_in_seconds", 0))
        uptime_days = info_server.get("uptime_in_days", 0)
        redis_version = info_server.get("redis_version", "unknown")
        status["Redis Version"] = str(redis_version)
        status["Redis Uptime"] = f"{uptime_days}d ({uptime_seconds}s)"
    except Exception:
        pass

    try:
        info_clients: dict[str, Any] = cast(dict[str, Any], probe.info("clients"))
        connected = info_clients.get("connected_clients")
        blocked = info_clients.get("blocked_clients")
        status["Redis Clients"] = f"connected={connected}, blocked={blocked}"
    except Exception:
        pass

    try:
        probe.close()
    except Exception:
        pass

    return status


def _send_version_fallback_alert(
    prefix: str, error: Exception, attempts: int = 1
) -> None:
    """Send an alert when cache version falls back to RELEASE_VERSION.

    This indicates Redis was unreachable when a cache prefix version was
    lazy-loaded. The container will use RELEASE_VERSION for the lifetime of
    the process, causing every cache read to fail the version check against
    entries written with the registry version — effectively disabling the
    cache silently.

    Args:
        prefix: The cache prefix that failed.
        error: The exception that caused the failure.
        attempts: Number of attempts made before giving up.
    """
    try:
        from utils.alerts import send_alert

        function_target = os.getenv("FUNCTION_TARGET", "unknown")
        k_service = os.getenv("K_SERVICE", "unknown")
        k_revision = os.getenv("K_REVISION", "unknown")

        # Container age since module load (proxy for cold-start detection)
        container_age_s = time.monotonic() - _MODULE_LOAD_TIME
        if container_age_s < 60:
            container_age_str = f"{container_age_s:.1f}s (cold start likely)"
        elif container_age_s < 3600:
            container_age_str = f"{container_age_s / 60:.1f}min"
        else:
            container_age_str = f"{container_age_s / 3600:.1f}hr"

        # Probe Redis health on a fresh connection for diagnostics
        redis_status = _probe_redis_status()

        metadata: dict[str, str] = {
            "Function Target": function_target,
            "Cloud Run Service": k_service,
            "Revision": k_revision,
            "Cache Prefix": prefix,
            "Attempts": str(attempts),
            "Container Age": container_age_str,
            "Redis Host": os.getenv("REDIS_HOST", "unknown"),
            "Redis Port": os.getenv("REDIS_PORT", "unknown"),
        }
        metadata.update(redis_status)

        send_alert(
            subject=f"CRITICAL: Redis cache version fallback for '{prefix}'",
            body=(
                f"Cache prefix '{prefix}' failed to read its version from Redis.\n\n"
                f"Error: {error}\n\n"
                f"Falling back to RELEASE_VERSION={RELEASE_VERSION}.\n"
                f"This will cause version mismatches and silent cache misses "
                f"for every request on this container instance.\n\n"
                f"Check Redis connectivity and memory pressure immediately."
            ),
            severity="critical",
            metadata=metadata,
        )
    except Exception as alert_err:
        _logger.error("Failed to send version fallback alert: %s", alert_err)


def set_cache_version(prefix: str, version: str) -> None:
    """Write *version* into the shared registry for *prefix*.

    Use this to bust a single cache prefix without a code deploy.
    All processes sharing this Redis will see the new version on their
    next ``RedisCache.read()`` call and treat older entries as stale.
    """
    client = get_redis_client()
    client.hset(VERSION_REGISTRY_KEY, prefix, version)


def get_all_cache_versions() -> dict[str, str]:
    """Return every prefix/version pair in the shared registry."""
    try:
        client = get_redis_client()
        raw = cast(dict[bytes, bytes], client.hgetall(VERSION_REGISTRY_KEY))
        return {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else str(v)
            )
            for k, v in raw.items()
        }
    except RedisError:
        return {}


# Cache is disabled in test environment unless explicitly enabled
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
    source: str = "redis"
    version: str = "0.0"

    def to_dict(self):
        return {
            field.name: getattr(self, field.name) for field in self.__dataclass_fields__.values()
        }


class _CacheEntryUnpickler(pickle.Unpickler):
    """Unpickler that resolves CacheEntry to the local class regardless of source module path.

    Pickle embeds the full module path (e.g. ``utils.redis_cache.CacheEntry``) when
    serializing.  If another repo has ``redis_cache.py`` at a different import path,
    the default unpickler raises ``ModuleNotFoundError``.  This subclass intercepts
    the class lookup and returns the local ``CacheEntry`` for any module, enabling
    cross-repo cache sharing.
    """

    def find_class(self, module: str, name: str) -> type:
        if name == "CacheEntry":
            return CacheEntry
        cls: type = super().find_class(module, name)
        return cls


# Singleton Redis client (synchronous)
_redis_client: Redis | None = None


def get_redis_client() -> Redis:
    """Singleton accessor for the synchronous Redis client."""
    global _redis_client
    if _redis_client is None:
        host = os.getenv("REDIS_HOST", "localhost")
        port_str = os.getenv("REDIS_PORT", "6379")
        password = os.getenv("REDIS_PASSWORD")

        try:
            port = int(port_str)
        except ValueError:
            raise RuntimeError(f"Invalid REDIS_PORT value: {port_str!r}")

        # Synchronous Redis client - no event loop issues!
        # decode_responses=False because we are storing pickled binary values
        _redis_client = Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=False,
            socket_timeout=5,  # 5 second socket timeout
            socket_connect_timeout=5,  # 5 second connection timeout
            retry_on_timeout=True,  # Retry on timeout
            health_check_interval=30,  # Check connection health every 30 seconds
        )
    return _redis_client


def reset_redis_client() -> None:
    """Close and discard the singleton Redis client.

    Call this when a connection failure is detected (e.g. timeout on a dead
    TCP socket). The next call to ``get_redis_client()`` will create a fresh
    connection, avoiding repeated failures on a stale socket.
    """
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
        _redis_client = None


class RedisCache:
    """
    Redis-backed cache implementation using SYNCHRONOUS Redis client.

    This eliminates all asyncio event loop issues while maintaining
    compatibility with async decorated functions. Redis operations
    are fast enough (~1ms) that blocking is acceptable.
    """

    def __init__(
        self,
        defaultTTL: int = 3600,
        prefix: str = "cache:",
        verbose: bool = False,
        isClassMethod: bool = True,
        # Accepted for compatibility but ignored/not used same way
        cache_max_memory: int = 0,
        persist: bool = False,
        allow_empty: bool = False,
        use_cloud_storage: bool = False,
        use_firestore: bool = False,
        **kwargs,
    ) -> None:
        # Redis client and version are lazy-loaded on first cache operation.
        # This avoids network calls during module import, which would block
        # Firebase CLI's function discovery (runs locally without VPC access).
        self._redis_instance: Redis | None = None
        self._version: str | None = None

        self.defaultTTL = defaultTTL
        self.prefix = prefix
        self.verbose = verbose
        self.isClassMethod = isClassMethod
        self.allow_empty = allow_empty

        # Logging setup
        level = logging.WARNING if not verbose else logging.DEBUG
        logger_name = f"rediscache.{prefix}" if prefix else "rediscache"
        self.logging = get_logger(logger_name, level=level)

        # Internal state matching Cache v2
        self.disableCache = False

        self.logging.info(f"RedisCache initialized (lazy): prefix={prefix}, ttl={defaultTTL}")

    @property
    def _redis(self) -> Redis:
        """Lazy accessor for the Redis client — connects on first use, not at import time."""
        if self._redis_instance is None:
            self._redis_instance = get_redis_client()
        return self._redis_instance

    @property
    def version(self) -> str:
        """Lazy accessor for the cache version — fetched from Redis on first use."""
        if self._version is None:
            self._version = get_cache_version(self.prefix)
        return self._version

    @version.setter
    def version(self, value: str) -> None:
        """Allow external code to set the version directly."""
        self._version = value

    @classmethod
    def for_firebase_functions(
        cls, prefix: str = "", defaultTTL: int = 3600, verbose: bool = False, **kwargs
    ):
        """
        Factory method to create a RedisCache instance optimized for Firebase Functions.
        Matches the signature of Cache.for_firebase_functions.
        """
        return cls(prefix=prefix, defaultTTL=defaultTTL, verbose=verbose, **kwargs)

    def getArgs(self, args):
        if self.isClassMethod:
            ret = args[1:]
        else:
            ret = args
        return [str(arg) for arg in ret]

    def check_for_override(self):
        return False

    def get_cache_key(self, fn: str, prefix: str = "", args=None, kwargs=None) -> str:
        """Generate a unique cache key for a function call. Identical to Cache v2."""
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

    def _full_key(self, key: str) -> str:
        """Apply instance prefix to the key for Redis storage."""
        if self.prefix and not key.startswith(self.prefix):
            return f"{self.prefix}:{key}"
        return key

    def add(
        self, data: Any, cache_key: str, funcName: str, args: list, expiry: int, kwargs: dict
    ) -> CacheEntry:
        """Add data to Redis cache (SYNCHRONOUS)."""
        try:
            # Use full key for Redis storage
            storage_key = self._full_key(cache_key)

            entry = CacheEntry()
            entry.size = sys.getsizeof(data)

            now = time.time()
            ttl_seconds = 0

            if expiry == -1:
                ttl_seconds = self.defaultTTL if self.defaultTTL != -1 else 0
            else:
                ttl_seconds = int(expiry - now)

            # Sanity check
            if ttl_seconds < 0:
                ttl_seconds = 1

            entry.expiry = int(expiry)
            entry.data_type = str(type(data))
            entry.function = funcName

            # Filter args (copied from v2)
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
            entry.source = "redis"

            # Skip empty data if configured
            if not self.allow_empty and self.filter_empty(entry) is None:
                return entry

            if hasattr(data, "error") and data.error:
                return entry

            # Serialize
            payload = pickle.dumps(entry, protocol=pickle.HIGHEST_PROTOCOL)

            # Set in Redis (SYNCHRONOUS - no await!)
            if ttl_seconds > 0:
                self._redis.set(storage_key, payload, ex=ttl_seconds)
            else:
                self._redis.set(storage_key, payload)

            self.logging.debug(f"Added to Redis: {storage_key} (ttl={ttl_seconds})")

            return entry

        except RedisError as e:
            error_msg = str(e)
            if "Timeout" in error_msg or "timeout" in error_msg.lower():
                self.logging.warning(
                    f"Redis add timeout for {cache_key}: {e}. "
                    f"Host: {os.getenv('REDIS_HOST', 'unknown')}:{os.getenv('REDIS_PORT', 'unknown')}"
                )
            else:
                self.logging.warning(f"Redis add error for {cache_key}: {e}")
            return CacheEntry()  # Return empty entry on failure to allow flow to continue
        except Exception as e:
            self.logging.warning(f"Redis add error for {cache_key}: {e}")
            return CacheEntry()  # Return empty entry on failure to allow flow to continue

    def read(self, key: str, noExpiration: bool = False, mutable: bool = True) -> CacheEntry | None:
        """Read from Redis cache (SYNCHRONOUS)."""
        try:
            storage_key = self._full_key(key)
            raw = self._redis.get(storage_key)  # SYNCHRONOUS - no await!
            if raw is None:
                self.logging.debug(f"Redis miss: {storage_key}")
                return None

            entry = _CacheEntryUnpickler(io.BytesIO(cast(bytes, raw))).load()
            if not isinstance(entry, CacheEntry):
                self.logging.warning(f"Invalid cache entry format for {key}")
                return None

            # Version check
            if not noExpiration and self.version and entry.version != self.version:
                self.logging.info(f"Version mismatch for {key}: {entry.version} != {self.version}")
                self.remove(key)
                return None

            # Check expiration
            if not noExpiration and entry.expiry != -1 and entry.expiry < time.time():
                self.logging.info(f"Cache logically expired: {key}")
                self.remove(key)
                return None

            # Empty check
            if not self.allow_empty and self.filter_empty(entry) is None:
                return None

            if mutable:
                return entry
            else:
                return copy.deepcopy(entry)

        except RedisError as e:
            error_msg = str(e)
            if "Timeout" in error_msg or "timeout" in error_msg.lower():
                self.logging.warning(
                    f"Redis read timeout for {key}: {e}. "
                    f"Host: {os.getenv('REDIS_HOST', 'unknown')}:{os.getenv('REDIS_PORT', 'unknown')}"
                )
            else:
                self.logging.warning(f"Redis read failed for {key}: {e}")
            return None
        except ModuleNotFoundError as e:
            # Stale pickled cache entry from an old module (e.g., media_manager).
            # Delete it so it won't warn again, and treat as a cache miss.
            self.logging.debug(f"Stale cache entry for {key} (old module): {e}")
            self.remove(key)
            return None
        except Exception as e:
            self.logging.warning(f"Redis read error for {key}: {e}")
            return None

    def remove(self, key: str):
        """Remove a key from Redis cache (SYNCHRONOUS)."""
        try:
            storage_key = self._full_key(key)
            self._redis.delete(storage_key)  # SYNCHRONOUS - no await!
        except RedisError as e:
            self.logging.warning(f"Redis delete failed for {key}: {e}")

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
        ):
            return None
        return entry

    def clear(self, include_locks: bool = False) -> None:
        """Clear the cache for this prefix (SYNCHRONOUS).

        Args:
            include_locks: If True, also remove advisory lock keys
                (``__lock__:{prefix}:*``) for this prefix.
        """
        if not self.prefix:
            self.logging.warning("Clear called without prefix - skipping for safety")
            return

        patterns = [f"{self.prefix}:*"]
        if include_locks:
            patterns.append(f"__lock__:{self.prefix}:*")

        for pattern in patterns:
            try:
                cursor = 0
                while True:
                    cursor, keys = cast(
                        tuple[int, list[bytes]],
                        self._redis.scan(cursor=cursor, match=pattern, count=100),
                    )
                    if keys:
                        self._redis.delete(*keys)
                    if cursor == 0:
                        break
            except RedisError as e:
                self.logging.warning(f"Redis clear failed for pattern {pattern}: {e}")

    @staticmethod
    def flush_all_caches(pattern: str = "cache:*") -> dict:
        """
        Flush all cache entries matching a pattern (SYNCHRONOUS).

        Args:
            pattern: Redis key pattern to match. Defaults to "cache:*" which matches
                     all cache entries. Use "*" for truly all keys (dangerous!).

        Returns:
            dict with status and count of deleted keys.
        """
        redis_client = get_redis_client()
        deleted_count = 0

        try:
            cursor = 0
            while True:
                cursor, keys = cast(
                    tuple[int, list[bytes]],
                    redis_client.scan(cursor=cursor, match=pattern, count=100),
                )
                if keys:
                    deleted_count += cast(int, redis_client.delete(*keys))
                if cursor == 0:
                    break

            return {
                "status": "success",
                "pattern": pattern,
                "deleted_count": deleted_count,
            }
        except RedisError as e:
            return {
                "status": "error",
                "pattern": pattern,
                "error": str(e),
                "deleted_count": deleted_count,
            }

    @staticmethod
    def get_cache_stats(pattern: str = "cache:*") -> dict:
        """
        Get statistics about cached keys matching a pattern.

        Args:
            pattern: Redis key pattern to match.

        Returns:
            dict with key count and sample keys.
        """
        redis_client = get_redis_client()

        try:
            cursor = 0
            total_keys = 0
            sample_keys: list[str] = []

            while True:
                cursor, keys = cast(
                    tuple[int, list[bytes]],
                    redis_client.scan(cursor=cursor, match=pattern, count=100),
                )
                total_keys += len(keys)
                if len(sample_keys) < 20:  # Collect up to 20 sample keys
                    for k in keys[: 20 - len(sample_keys)]:
                        sample_keys.append(k.decode() if isinstance(k, bytes) else str(k))
                if cursor == 0:
                    break

            return {
                "status": "success",
                "pattern": pattern,
                "total_keys": total_keys,
                "sample_keys": sample_keys,
                "version": RELEASE_VERSION,
            }
        except RedisError as e:
            return {
                "status": "error",
                "pattern": pattern,
                "error": str(e),
            }

    def close(self):
        """Close the Redis connection if one was established."""
        if self._redis_instance is not None:
            self._redis_instance.close()

    @classmethod
    def use_cache(cls, instance, prefix="", expiry_override=None):
        """Decorator to cache async function results.

        Uses SYNCHRONOUS Redis operations — no event loop issues.
        The decorated function remains async; cache read/write is sync.

        Advisory-lock coalescing: on cache miss only one caller fetches;
        concurrent callers wait and reuse the result.  Function errors
        propagate to the caller (never silently swallowed).
        """

        def decorator(func):
            async def inner1(*args, **kwargs):
                # no_cache: skip cache READ but still WRITE the result.
                # Use this for warmup / forced refresh scenarios.
                skipCacheRead = kwargs.pop("no_cache", False)

                # Global kill-switch — skip everything (read AND write)
                if DISABLE_CACHE or instance.disableCache:
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
                kwargs.pop("shared", False)
                cacheUpdateDisabled = kwargs.pop("no_cache_update", False)

                noExpiration = expiry == -1

                # ---- Helper: fetch, cache result, release lock on failure ----
                async def _fetch_and_cache(
                    lock_key: str | None = None, lock_token: str | None = None
                ) -> Any:
                    """Execute the wrapped function, write result to cache.

                    If *lock_key* and *lock_token* are provided the lock is
                    released (via Lua compare-and-delete) only on failure so
                    waiters can re-contend immediately.  On success the lock
                    TTL expires naturally — safe even if another caller
                    acquired between our SET and our return.
                    """
                    try:
                        data = await func(*args, **kwargs)
                        if not cacheUpdateDisabled:
                            instance.add(
                                data,
                                cache_key,
                                funcName=func.__name__,
                                args=args[1:],
                                expiry=expiry,
                                kwargs=kwargs,
                            )
                        return data
                    except Exception:
                        if lock_key and lock_token:
                            try:
                                instance._redis.eval(
                                    _RELEASE_LOCK_SCRIPT, 1, lock_key, lock_token
                                )
                            except RedisError:
                                pass
                        raise

                # ---- Phase 1: Cache read ----
                if not skipCacheRead:
                    cachedEntry = instance.read(cache_key, noExpiration, mutable)
                    if cachedEntry is not None:
                        instance.logging.debug(f"Cache hit: {cache_key}")
                        return cachedEntry.data

                # ---- Phase 2: Cache miss — coalescing via advisory lock ----
                storage_key = instance._full_key(cache_key)
                lock_key = f"__lock__:{storage_key}"
                lock_token = os.urandom(16).hex()

                try:
                    acquired = instance._redis.set(
                        lock_key, lock_token, nx=True, ex=_LOCK_TTL_SECONDS
                    )
                except RedisError as e:
                    instance.logging.warning(
                        f"Lock acquire failed for {cache_key}, "
                        f"proceeding without coalescing: {e}"
                    )
                    acquired = True  # Degrade: proceed as creator

                if acquired:
                    instance.logging.debug(f"Cache miss (creator): {cache_key}")
                    if cacheUpdateDisabled:
                        instance.logging.warning(
                            f"Creator has cache update disabled for {cache_key}; "
                            f"concurrent waiters will not benefit from coalescing"
                        )
                    return await _fetch_and_cache(
                        lock_key=lock_key, lock_token=lock_token
                    )

                # ---- Waiter: poll with jittered backoff ----
                instance.logging.debug(f"Cache miss (waiter): {cache_key}")
                waited = 0.0
                poll_attempt = 0
                while waited < _LOCK_MAX_WAIT:
                    cachedEntry = instance.read(cache_key, noExpiration, mutable)
                    if cachedEntry is not None:
                        return cachedEntry.data
                    interval = _poll_sleep(poll_attempt)
                    await asyncio.sleep(interval)
                    waited += interval
                    poll_attempt += 1

                # max_wait reached — re-attempt lock before fallback
                lock_token = os.urandom(16).hex()
                try:
                    acquired = instance._redis.set(
                        lock_key, lock_token, nx=True, ex=_LOCK_TTL_SECONDS
                    )
                except RedisError:
                    acquired = False

                if acquired:
                    instance.logging.debug(
                        f"Lock re-acquired for {cache_key} after max_wait, fetching"
                    )
                    return await _fetch_and_cache(
                        lock_key=lock_key, lock_token=lock_token
                    )

                # Reacquire failed — race: creator may have just finished
                cachedEntry = instance.read(cache_key, noExpiration, mutable)
                if cachedEntry is not None:
                    return cachedEntry.data

                # Brief extra poll
                brief_wait = min(2.0, _LOCK_MAX_WAIT)
                waited = 0.0
                while waited < brief_wait:
                    cachedEntry = instance.read(cache_key, noExpiration, mutable)
                    if cachedEntry is not None:
                        return cachedEntry.data
                    interval = _poll_sleep(0)
                    await asyncio.sleep(interval)
                    waited += interval

                # Last resort — direct fetch without lock
                cachedEntry = instance.read(cache_key, noExpiration, mutable)
                if cachedEntry is not None:
                    return cachedEntry.data

                instance.logging.warning(
                    f"Last-resort fetch for {cache_key} (no lock held); "
                    f"coalescing exhausted all fallback paths"
                )
                return await _fetch_and_cache()

            return inner1

        return decorator

    def log(self, msg: str):
        self.logging.debug(msg)
