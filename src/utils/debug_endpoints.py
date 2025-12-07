import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from firebase_functions import https_fn, options

from utils.redis_cache import RELEASE_VERSION, RedisCache

logger = logging.getLogger(__name__)

# Simple password protection for debug endpoints
DEBUG_ENDPOINT_PASSWORD = "mediacircle5450"


def _check_debug_password(req: https_fn.Request) -> https_fn.Response | None:
    """Check password for debug endpoints. Returns error response if invalid, None if valid."""
    password = req.args.get("password", "")
    if password != DEBUG_ENDPOINT_PASSWORD:
        return https_fn.Response(
            json.dumps({
                "status": "error",
                "error": "Invalid or missing password. Add ?password=<password> to proceed.",
            }),
            status=401,
            headers={"Content-Type": "application/json"},
        )
    return None


async def _test_redis_connectivity_async(req: https_fn.Request) -> dict[str, Any]:
    """Async implementation of Redis connectivity test."""
    tests: list[dict[str, Any]] = []
    results: dict[str, Any] = {
        "status": "started",
        "timestamp": datetime.now().isoformat(),
        "env": {
            "REDIS_HOST": os.getenv("REDIS_HOST"),
            "REDIS_PORT": os.getenv("REDIS_PORT"),
            "VPC_CONNECTOR": os.getenv(
                "VPC_CONNECTOR_NAME", "mc-vpc-connector"
            ),  # Not strictly env var, but for context
        },
        "tests": tests,
    }

    cache = None
    try:
        # 1. Initialization
        start_time = time.time()
        cache = RedisCache.for_firebase_functions(prefix="connectivity_test", defaultTTL=60)
        tests.append(
            {
                "name": "initialization",
                "status": "success",
                "duration_ms": (time.time() - start_time) * 1000,
            }
        )

        # 2. Write (SYNCHRONOUS - no await needed!)
        start_time = time.time()
        test_key = f"test_key_{int(time.time())}"
        test_data = {"message": "Hello from Cloud Functions!", "timestamp": time.time()}

        cache.add(
            data=test_data,
            cache_key=test_key,
            funcName="test_func",
            args=[],
            expiry=time.time() + 60,  # 1 minute from now (absolute timestamp)
            kwargs={},
        )
        tests.append(
            {
                "name": "write",
                "status": "success",
                "key": test_key,
                "duration_ms": (time.time() - start_time) * 1000,
            }
        )

        # 3. Read (SYNCHRONOUS - no await needed!)
        start_time = time.time()
        entry = cache.read(test_key)

        if entry and entry.data == test_data:
            tests.append(
                {
                    "name": "read",
                    "status": "success",
                    "data_match": True,
                    "duration_ms": (time.time() - start_time) * 1000,
                }
            )
        else:
            tests.append(
                {
                    "name": "read",
                    "status": "failed",
                    "error": "Data mismatch or cache miss",
                    "got": str(entry.data) if entry else "None",
                }
            )
            results["status"] = "failed"

        # 4. Remove (SYNCHRONOUS - no await needed!)
        start_time = time.time()
        cache.remove(test_key)

        # Verify removal
        entry_after = cache.read(test_key)
        if entry_after is None:
            tests.append(
                {
                    "name": "remove",
                    "status": "success",
                    "duration_ms": (time.time() - start_time) * 1000,
                }
            )
        else:
            tests.append(
                {"name": "remove", "status": "failed", "error": "Item still exists after remove"}
            )
            results["status"] = "failed"

        # 5. Connection Info (if possible/safe to expose)
        # Not easily available from high-level client without private method access

        if results["status"] == "started":
            results["status"] = "success"

    except Exception as e:
        logger.exception("Redis connectivity test failed")
        results["status"] = "error"
        results["error"] = str(e)
        results["traceback"] = str(e)  # Simplified for security, full trace in logs

    finally:
        if cache:
            cache.close()

    return results


@https_fn.on_request(
    memory=options.MemoryOption.MB_256,
    timeout_sec=30,
    min_instances=0,  # Don't keep warm, this is a diagnostic tool
    # VPC configuration is global in main.py, but explicit here doesn't hurt
    vpc_connector="mc-vpc-connector",
    vpc_connector_egress_settings=options.VpcEgressSetting.ALL_TRAFFIC,
)
def debug_redis_connectivity(req: https_fn.Request) -> https_fn.Response:
    """
    Diagnostic endpoint to verify Redis connectivity from Cloud Functions.

    Usage: GET /debug_redis_connectivity
    """
    # Security check: Ideally restrict this or rely on IAM invoker permissions.
    # For now, we'll allow it as it's a "debug" function likely protected by deployment policy
    # or obscure enough. Consider adding a secret query param if public.

    result = asyncio.run(_test_redis_connectivity_async(req))

    status_code = 200 if result["status"] == "success" else 500

    return https_fn.Response(
        json.dumps(result, default=str),
        status=status_code,
        headers={"Content-Type": "application/json"},
    )


@https_fn.on_request(
    memory=options.MemoryOption.MB_256,
    timeout_sec=30,
    min_instances=0,
    vpc_connector="mc-vpc-connector",
    vpc_connector_egress_settings=options.VpcEgressSetting.ALL_TRAFFIC,
)
def debug_cache_stats(req: https_fn.Request) -> https_fn.Response:
    """
    Get statistics about cached keys in Redis.

    Usage:
        GET /debug_cache_stats?password=<pw>                    - Stats for all cache keys
        GET /debug_cache_stats?password=<pw>&pattern=tmdb:*     - Stats for specific pattern
    """
    # Password check
    if error_response := _check_debug_password(req):
        return error_response

    pattern = req.args.get("pattern", "cache:*")

    result = RedisCache.get_cache_stats(pattern)
    result["current_version"] = RELEASE_VERSION
    result["timestamp"] = datetime.now().isoformat()

    status_code = 200 if result["status"] == "success" else 500

    return https_fn.Response(
        json.dumps(result, default=str),
        status=status_code,
        headers={"Content-Type": "application/json"},
    )


@https_fn.on_request(
    memory=options.MemoryOption.MB_256,
    timeout_sec=60,  # Longer timeout for potentially large flush operations
    min_instances=0,
    vpc_connector="mc-vpc-connector",
    vpc_connector_egress_settings=options.VpcEgressSetting.ALL_TRAFFIC,
)
def debug_cache_flush(req: https_fn.Request) -> https_fn.Response:
    """
    Flush/bust cache entries in Redis.

    Usage:
        POST /debug_cache_flush?password=<pw>&confirm=yes              - Flush all cache keys
        POST /debug_cache_flush?password=<pw>&pattern=tmdb:*&confirm=yes - Flush specific pattern

    Security: Requires password and confirm=yes to prevent accidental flushes.
    """
    # Password check
    if error_response := _check_debug_password(req):
        return error_response

    # Only allow POST for destructive operations
    if req.method != "POST":
        return https_fn.Response(
            json.dumps({
                "status": "error",
                "error": "Method not allowed. Use POST.",
                "usage": "POST /debug_cache_flush?pattern=cache:*&confirm=yes"
            }),
            status=405,
            headers={"Content-Type": "application/json"},
        )

    # Require confirmation
    confirm = req.args.get("confirm", "").lower()
    if confirm != "yes":
        return https_fn.Response(
            json.dumps({
                "status": "error",
                "error": "Confirmation required. Add ?confirm=yes to proceed.",
                "usage": "POST /debug_cache_flush?pattern=cache:*&confirm=yes"
            }),
            status=400,
            headers={"Content-Type": "application/json"},
        )

    pattern = req.args.get("pattern", "cache:*")

    # Log the flush operation
    logger.warning(f"Cache flush requested for pattern: {pattern}")

    result = RedisCache.flush_all_caches(pattern)
    result["timestamp"] = datetime.now().isoformat()
    result["current_version"] = RELEASE_VERSION

    status_code = 200 if result["status"] == "success" else 500

    return https_fn.Response(
        json.dumps(result, default=str),
        status=status_code,
        headers={"Content-Type": "application/json"},
    )
