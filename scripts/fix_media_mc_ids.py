#!/usr/bin/env python3
"""
Fix id and mc_id fields in the media index to match generate_mc_id format.

Scans all media:* documents and updates both id and mc_id from
"tmdb_movie_238" / "tmdb_tv_1396" to the canonical "tmdb_238" / "tmdb_1396"
format produced by generate_mc_id. Both fields are set to the same value.

Usage:
    # Dry run — report mismatches without writing
    python scripts/fix_media_mc_ids.py --dry-run

    # Fix all
    python scripts/fix_media_mc_ids.py

    # Fix with limit
    python scripts/fix_media_mc_ids.py --limit 100
"""

from __future__ import annotations

import argparse
import asyncio
import os
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

from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _correct_mc_id(source_id: str) -> str:
    """Generate the canonical mc_id: tmdb_{source_id}."""
    return f"tmdb_{source_id}"


async def fix_mc_ids(
    scan_count: int,
    limit: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    stats: dict[str, int] = {
        "scanned": 0,
        "checked": 0,
        "fixed": 0,
        "already_correct": 0,
        "missing_source_id": 0,
    }

    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info("Redis connected")

        cursor: int = 0

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)

            if not keys:
                if cursor == 0:
                    break
                continue

            stats["scanned"] += len(keys)

            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key, "$.source_id", "$.id", "$.mc_id")  # type: ignore[union-attr]
            raw_results: list[object] = await pipe.execute()

            write_pipe = redis.pipeline()
            batch_fixes = 0

            for key, raw in zip(keys, raw_results, strict=True):
                if not isinstance(raw, dict):
                    continue

                source_id_val = raw.get("$.source_id")
                id_val = raw.get("$.id")
                mc_id_val = raw.get("$.mc_id")

                source_id = source_id_val[0] if isinstance(source_id_val, list) and source_id_val else None
                current_id = id_val[0] if isinstance(id_val, list) and id_val else None
                current_mc_id = mc_id_val[0] if isinstance(mc_id_val, list) and mc_id_val else None

                if not source_id:
                    stats["missing_source_id"] += 1
                    continue

                stats["checked"] += 1
                correct = _correct_mc_id(str(source_id))

                if current_id == correct and current_mc_id == correct:
                    stats["already_correct"] += 1
                    continue

                if not dry_run:
                    write_pipe.json().set(key, "$.id", correct)  # type: ignore[union-attr]
                    write_pipe.json().set(key, "$.mc_id", correct)  # type: ignore[union-attr]
                    batch_fixes += 1

                stats["fixed"] += 1

            if batch_fixes > 0:
                await write_pipe.execute()

            if stats["scanned"] % 5000 < scan_count:
                logger.info(
                    "  Progress: scanned=%d, fixed=%d, already_correct=%d",
                    stats["scanned"],
                    stats["fixed"],
                    stats["already_correct"],
                )

            if limit is not None and stats["fixed"] >= limit:
                logger.info("Reached limit of %d fixes", limit)
                break

            if cursor == 0:
                break

    finally:
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, Any], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Fix id + mc_id — Summary")
    print("=" * 60)
    for k, v in stats.items():
        if isinstance(v, (int, float)):
            print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix id and mc_id fields in media index to canonical tmdb_{source_id} format"
    )
    parser.add_argument(
        "--scan-count", type=int, default=500,
        help="Keys per SCAN iteration (default 500)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max documents to fix",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report mismatches without writing",
    )

    args = parser.parse_args()

    t0 = time.time()
    result = await fix_mc_ids(
        scan_count=args.scan_count,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    elapsed = time.time() - t0
    _print_stats(result, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
