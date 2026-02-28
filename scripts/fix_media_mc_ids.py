#!/usr/bin/env python3
"""
Migrate media index keys and documents to the new mc_id format.

Old format: mc_id = tmdb_{source_id}       key = media:tmdb_{source_id}
New format: mc_id = tmdb_{mc_type}_{source_id}  key = media:tmdb_{mc_type}_{source_id}

For each media:* document:
  1. Read the full document
  2. Compute new mc_id from source, mc_type, source_id
  3. Mutate id and mc_id in the document
  4. Write the updated document to the new key
  5. Delete the old key

After migration, drop and rebuild idx:media.

Usage:
    # Dry run — report what would change
    python scripts/fix_media_mc_ids.py --dry-run

    # Migrate all
    python scripts/fix_media_mc_ids.py

    # Migrate with limit
    python scripts/fix_media_mc_ids.py --limit 100

    # Migrate + rebuild index
    python scripts/fix_media_mc_ids.py --rebuild-index
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402
from redis.commands.search.index_definition import IndexDefinition, IndexType  # noqa: E402

from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

MEDIA_TYPES_WITH_TYPE_IN_KEY = ("movie", "tv")


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _compute_new_mc_id(source: str, mc_type: str, source_id: str) -> str:
    """Compute the canonical mc_id with mc_type included for movie/tv."""
    if mc_type in MEDIA_TYPES_WITH_TYPE_IN_KEY:
        return f"{source}_{mc_type}_{source_id}"
    return f"{source}_{source_id}"


async def migrate_mc_ids(
    scan_count: int,
    limit: int | None,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "checked": 0,
        "migrated": 0,
        "already_correct": 0,
        "missing_fields": 0,
        "old_keys_deleted": 0,
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

            read_pipe = redis.pipeline()
            for key in keys:
                read_pipe.json().get(key)  # type: ignore[union-attr]
            docs: list[object] = await read_pipe.execute()

            write_pipe = redis.pipeline()
            batch_writes = 0

            for key, doc in zip(keys, docs, strict=True):
                if not isinstance(doc, dict):
                    continue

                source = doc.get("source")
                source_id = doc.get("source_id")
                mc_type = doc.get("mc_type")

                if not source or not source_id or not mc_type:
                    stats["missing_fields"] += 1
                    continue

                stats["checked"] += 1
                new_mc_id = _compute_new_mc_id(source, mc_type, str(source_id))
                new_key = f"media:{new_mc_id}"

                if key == new_key and doc.get("id") == new_mc_id and doc.get("mc_id") == new_mc_id:
                    stats["already_correct"] += 1
                    continue

                if not dry_run:
                    doc["id"] = new_mc_id
                    doc["mc_id"] = new_mc_id
                    write_pipe.json().set(new_key, "$", doc)  # type: ignore[union-attr]
                    if key != new_key:
                        write_pipe.delete(key)  # type: ignore[union-attr]
                        stats["old_keys_deleted"] += 1

                    batch_writes += 1

                stats["migrated"] += 1

            if batch_writes > 0:
                await write_pipe.execute()

            if stats["scanned"] % 5000 < scan_count:
                logger.info(
                    "  Progress: scanned=%d, migrated=%d, already_correct=%d",
                    stats["scanned"],
                    stats["migrated"],
                    stats["already_correct"],
                )

            if limit is not None and stats["migrated"] >= limit:
                logger.info("Reached limit of %d migrations", limit)
                break

            if cursor == 0:
                break

    finally:
        await redis.aclose()

    return stats


async def rebuild_media_index() -> None:
    """Drop and recreate idx:media so it re-indexes all media:* keys."""
    redis = _connect_redis()
    try:
        try:
            await redis.ft("idx:media").dropindex(delete_documents=False)
            logger.info("Dropped idx:media")
        except Exception as e:
            logger.warning("Could not drop idx:media (may not exist): %s", e)

        from web.app import INDEX_CONFIGS

        schema = INDEX_CONFIGS["media"]["schema"]
        definition = IndexDefinition(prefix=["media:"], index_type=IndexType.JSON)
        await redis.ft("idx:media").create_index(schema, definition=definition)
        logger.info("Recreated idx:media")
    finally:
        await redis.aclose()


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Migrate media keys — Summary")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate media:* keys to new mc_id format: tmdb_{mc_type}_{source_id}"
    )
    parser.add_argument(
        "--scan-count", type=int, default=500,
        help="Keys per SCAN iteration (default 500)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max documents to migrate",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would change without writing",
    )
    parser.add_argument(
        "--rebuild-index", action="store_true",
        help="Drop and recreate idx:media after migration",
    )

    args = parser.parse_args()

    t0 = time.time()
    result = await migrate_mc_ids(
        scan_count=args.scan_count,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    elapsed = time.time() - t0
    _print_stats(result, elapsed, args.dry_run)

    if args.rebuild_index and not args.dry_run:
        logger.info("Rebuilding idx:media ...")
        await rebuild_media_index()
        logger.info("Index rebuild complete")


if __name__ == "__main__":
    asyncio.run(main())
