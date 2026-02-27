#!/usr/bin/env python3
"""Rename legacy media Redis keys to canonical format.

Renames:
  media:tmdb_movie_{id} → media:tmdb_{id}
  media:tmdb_tv_{id}    → media:tmdb_{id}

Usage:
    python scripts/migrate_media_keys.py              # dry-run (report only)
    python scripts/migrate_media_keys.py --apply       # rename keys
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

LEGACY_RE = re.compile(r"^media:tmdb_(movie|tv)_(\d+)$")


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
        socket_timeout=10.0,
        socket_connect_timeout=5.0,
    )


async def migrate_keys(apply: bool) -> dict[str, Any]:
    stats: dict[str, int] = {
        "scanned": 0,
        "legacy_found": 0,
        "renamed": 0,
        "skipped_target_exists": 0,
        "errors": 0,
    }

    redis = _connect_redis()
    await redis.ping()  # type: ignore[misc]
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6380")
    print(f"Connected to Redis at {host}:{port}")

    cursor: int = 0

    while True:
        cursor, keys = await redis.scan(  # type: ignore[misc]
            cursor=cursor, match="media:tmdb_*", count=500
        )

        for key in keys:
            stats["scanned"] += 1
            m = LEGACY_RE.match(key)
            if not m:
                continue

            stats["legacy_found"] += 1
            tmdb_id = m.group(2)
            new_key = f"media:tmdb_{tmdb_id}"

            if not apply:
                continue

            try:
                exists = await redis.exists(new_key)
                if exists:
                    await redis.delete(key)
                    stats["skipped_target_exists"] += 1
                    continue

                await redis.rename(key, new_key)
                stats["renamed"] += 1
            except Exception as e:
                print(f"  Error renaming {key} → {new_key}: {e}")
                stats["errors"] += 1

        if stats["scanned"] % 10000 < 500:
            print(
                f"  Progress: scanned={stats['scanned']}, "
                f"legacy={stats['legacy_found']}, renamed={stats['renamed']}"
            )

        if cursor == 0:
            break

    await redis.aclose()  # type: ignore[misc]
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rename legacy media:tmdb_movie_*/media:tmdb_tv_* keys to media:tmdb_*"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually rename keys (default is dry-run)",
    )
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Mode: {mode}\n")

    t0 = time.time()
    result = asyncio.run(migrate_keys(apply=args.apply))
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print(f"[{mode}] Key Migration Summary")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
