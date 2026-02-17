#!/usr/bin/env python3
"""Test Redis connectivity and measure latency.

Useful for verifying reachability from local machine to public Redis via IAP tunnel.

Usage:
  # Test public Redis (requires `make tunnel` running - forwards localhost:6381 to vm)
  REDIS_HOST=localhost REDIS_PORT=6381 REDIS_PASSWORD='rCrwd3xMFhfoKhUF9by9' python scripts/test_redis_connectivity.py

  # Or source local.env PUBLIC vars and override REDIS_* for the tunnel endpoint:
  export REDIS_HOST=localhost REDIS_PORT=6381 REDIS_PASSWORD=rCrwd3xMFhfoKhUF9by9
  python scripts/test_redis_connectivity.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Add project root for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

host = os.getenv("REDIS_HOST", "localhost")
port = int(os.getenv("REDIS_PORT", "6379"))
password = os.getenv("REDIS_PASSWORD") or None


def run_tests() -> bool:
    """Run connectivity tests. Returns True if all pass."""
    from redis import Redis
    from redis.exceptions import RedisError

    print("=" * 60)
    print("Redis Connectivity Test")
    print("=" * 60)
    print(f"  Host:     {host}")
    print(f"  Port:     {port}")
    print(f"  Password: {'*' * 8 if password else '(none)'}")
    print()

    client = Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=False,
        socket_timeout=5,
        socket_connect_timeout=5,
    )

    ok = True

    # 1. PING
    print("1. PING...")
    try:
        t0 = time.perf_counter()
        client.ping()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"   OK ({elapsed_ms:.1f} ms)")
    except RedisError as e:
        print(f"   FAIL: {e}")
        ok = False
        return ok

    # 2. INFO (server)
    print("2. INFO server...")
    try:
        t0 = time.perf_counter()
        info = client.info("server")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"   OK ({elapsed_ms:.1f} ms)")
        ver = info.get(b"redis_version", info.get("redis_version", "?"))
        print(f"   Redis version: {ver.decode() if isinstance(ver, bytes) else ver}")
    except RedisError as e:
        print(f"   FAIL: {e}")
        ok = False

    # 3. INFO memory
    print("3. INFO memory...")
    try:
        t0 = time.perf_counter()
        info = client.info("memory")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        used = int(info.get(b"used_memory", info.get("used_memory", 0))) / (1024 * 1024)
        peak = int(info.get(b"used_memory_peak", info.get("used_memory_peak", 0))) / (
            1024 * 1024
        )
        maxmem = info.get(b"maxmemory", info.get("maxmemory"))
        max_str = f"{int(maxmem) / (1024**3):.1f} GB" if maxmem else "none"
        print(f"   OK ({elapsed_ms:.1f} ms)")
        print(f"   used_memory: {used:.1f} MB, peak: {peak:.1f} MB, maxmemory: {max_str}")
    except RedisError as e:
        print(f"   FAIL: {e}")
        ok = False

    # 4. Cache version read (tmdb_func)
    print("4. get_cache_version('tmdb_func')...")
    try:
        os.environ["REDIS_HOST"] = host
        os.environ["REDIS_PORT"] = str(port)
        if password:
            os.environ["REDIS_PASSWORD"] = password
        elif "REDIS_PASSWORD" in os.environ:
            del os.environ["REDIS_PASSWORD"]

        from utils.redis_cache import get_cache_version

        t0 = time.perf_counter()
        version = get_cache_version("tmdb_func")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"   OK ({elapsed_ms:.1f} ms) -> version {version}")
    except Exception as e:
        print(f"   FAIL: {e}")
        ok = False

    # 5. Latency sample (10 round-trips)
    print("5. Latency sample (10x PING)...")
    try:
        times = []
        for _ in range(10):
            t0 = time.perf_counter()
            client.ping()
            times.append((time.perf_counter() - t0) * 1000)
        avg = sum(times) / len(times)
        min_t, max_t = min(times), max(times)
        print(f"   avg: {avg:.1f} ms, min: {min_t:.1f} ms, max: {max_t:.1f} ms")
    except RedisError as e:
        print(f"   FAIL: {e}")
        ok = False

    client.close()
    return ok


if __name__ == "__main__":
    success = run_tests()
    print()
    print("=" * 60)
    print("PASS" if success else "FAIL")
    print("=" * 60)
    sys.exit(0 if success else 1)
