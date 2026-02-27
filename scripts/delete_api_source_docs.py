#!/usr/bin/env python3
"""Delete media documents that were re-inserted via the /api/media-details bug.

Identifies documents where _source == "api" and removes them.

Usage:
    python scripts/delete_api_source_docs.py              # dry-run (count only)
    python scripts/delete_api_source_docs.py --delete      # actually delete
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from redis.asyncio import Redis  # noqa: E402

from adapters.config import load_env  # noqa: E402


async def run(delete: bool, redis_host: str, redis_port: int, redis_password: str) -> None:
    r: Redis = Redis(  # type: ignore[type-arg]
        host=redis_host,
        port=redis_port,
        password=redis_password or None,
        decode_responses=True,
        socket_timeout=10.0,
        socket_connect_timeout=5.0,
    )

    await r.ping()  # type: ignore[misc]
    print(f"Connected to Redis at {redis_host}:{redis_port}")

    cursor: int = 0
    api_keys: list[str] = []
    scanned = 0

    while True:
        cursor, keys = await r.scan(cursor=cursor, match="media:*", count=500)  # type: ignore[misc]
        for key in keys:
            scanned += 1
            try:
                source: Any = await r.json().get(key, "$._source")  # type: ignore[misc]
                if isinstance(source, list) and source and source[0] == "api":
                    api_keys.append(key)
            except Exception:
                pass

        if cursor == 0:
            break

    print(f"Scanned {scanned} media:* keys")
    print(f"Found {len(api_keys)} documents with _source='api'")

    if not api_keys:
        print("Nothing to delete.")
        await r.aclose()  # type: ignore[misc]
        return

    if not delete:
        print("\nDry-run mode. Pass --delete to remove these keys.")
        if len(api_keys) <= 20:
            for k in api_keys:
                print(f"  {k}")
        else:
            for k in api_keys[:10]:
                print(f"  {k}")
            print(f"  ... and {len(api_keys) - 10} more")
        await r.aclose()  # type: ignore[misc]
        return

    deleted = 0
    batch_size = 100
    for i in range(0, len(api_keys), batch_size):
        batch = api_keys[i : i + batch_size]
        result = await r.delete(*batch)
        deleted += int(result)  # type: ignore[arg-type]

    print(f"Deleted {deleted} documents.")
    await r.aclose()  # type: ignore[misc]


def main() -> None:
    load_env()

    parser = argparse.ArgumentParser(description="Delete _source='api' media docs")
    parser.add_argument("--delete", action="store_true", help="Actually delete (default is dry-run)")
    args = parser.parse_args()

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6380"))
    password = os.getenv("REDIS_PASSWORD", "")

    print(f"Target: {host}:{port}  mode={'DELETE' if args.delete else 'DRY-RUN'}")
    asyncio.run(run(delete=args.delete, redis_host=host, redis_port=port, redis_password=password))


if __name__ == "__main__":
    main()
