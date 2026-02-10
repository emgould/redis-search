"""
Migrate search_title fields to strip apostrophes for consistent RediSearch tokenization.

Problem:
    RediSearch tokenizes apostrophes as word separators, so "It's Complicated"
    becomes tokens ["it", "s", "complicated"]. This prevents users from finding
    titles when they type "its complicated" (no apostrophe).

Solution:
    - Strip apostrophes from search_title (used for TEXT index search)
    - Preserve original title in a new 'title' field (for display)

Affected indexes:
    - idx:media (media:* keys) — movies and TV shows
    - idx:people (person:* keys) — actors, directors, etc.

Usage:
    # Dry run (no changes)
    python scripts/migrate_search_titles.py --dry-run

    # Execute migration
    python scripts/migrate_search_titles.py

    # Custom Redis connection
    python scripts/migrate_search_titles.py --redis-host localhost --redis-port 6380
"""

import argparse
import asyncio
import os
import re
import sys
import time
from collections.abc import Awaitable
from typing import cast

from dotenv import load_dotenv
from redis.asyncio import Redis

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.get_logger import get_logger

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

logger = get_logger(__name__)

# Regex matching the same pattern as normalize_search_title in core.normalize
_APOSTROPHE_RE = re.compile(r"[\u0027\u2018\u2019\u02BC]")  # ' ' ' ʼ

SCAN_BATCH = 10_000
WRITE_BATCH = 500


def strip_apostrophes(title: str) -> str:
    """Strip apostrophes from a title string."""
    return _APOSTROPHE_RE.sub("", title)


async def migrate_prefix(
    redis: Redis,  # type: ignore[type-arg]
    prefix: str,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Migrate all documents with the given key prefix.

    For each document:
    - Reads current search_title
    - If it contains apostrophes, sets title = original, search_title = stripped
    - If no apostrophes, sets title = search_title (for consistency) only if title missing

    Args:
        redis: Redis async client
        prefix: Key prefix to scan (e.g. "media:", "person:")
        dry_run: If True, report what would change without writing

    Returns:
        Dict with migration statistics
    """
    stats: dict[str, int] = {
        "scanned": 0,
        "updated": 0,
        "already_ok": 0,
        "title_added": 0,
        "errors": 0,
    }

    # Collect all keys
    keys: list[str] = []
    async for key in redis.scan_iter(match=f"{prefix}*", count=SCAN_BATCH):
        keys.append(key)

    stats["scanned"] = len(keys)
    logger.info(f"Found {len(keys):,} keys with prefix '{prefix}'")

    if not keys:
        return stats

    # Process in batches
    for batch_start in range(0, len(keys), WRITE_BATCH):
        batch_keys = keys[batch_start : batch_start + WRITE_BATCH]

        # Read search_title and title for all keys in batch
        pipe = redis.pipeline()
        for key in batch_keys:
            pipe.json().get(key, "$.search_title", "$.title")
        results = await pipe.execute()

        # Build update pipeline
        update_pipe = redis.pipeline() if not dry_run else None
        batch_updates = 0

        for key, result in zip(batch_keys, results, strict=True):
            try:
                if not result:
                    stats["errors"] += 1
                    continue

                # JSON.GET with multiple paths returns a dict like:
                # {"$.search_title": ["value"], "$.title": ["value"]}
                # Or a list if single path, depending on redis-py version
                if isinstance(result, dict):
                    search_title_list = result.get("$.search_title", [])
                    title_list = result.get("$.title", [])
                elif isinstance(result, list):
                    # Fallback: might be a flat list
                    search_title_list = result[:1] if result else []
                    title_list = result[1:2] if len(result) > 1 else []
                else:
                    stats["errors"] += 1
                    continue

                search_title = search_title_list[0] if search_title_list else None
                existing_title = title_list[0] if title_list else None

                if not search_title:
                    stats["errors"] += 1
                    continue

                normalized = strip_apostrophes(search_title)
                needs_update = normalized != search_title
                needs_title = existing_title is None

                if needs_update:
                    if dry_run:
                        if stats["updated"] < 10:
                            logger.info(
                                f"  [DRY RUN] {key}: "
                                f"'{search_title}' -> '{normalized}'"
                            )
                    else:
                        # Set title to original (for display), search_title to normalized
                        update_pipe.json().set(key, "$.title", search_title)  # type: ignore[union-attr]
                        update_pipe.json().set(key, "$.search_title", normalized)  # type: ignore[union-attr]
                        batch_updates += 1
                    stats["updated"] += 1
                elif needs_title:
                    # No apostrophes but title field missing — add for consistency
                    if not dry_run:
                        update_pipe.json().set(key, "$.title", search_title)  # type: ignore[union-attr]
                        batch_updates += 1
                    stats["title_added"] += 1
                else:
                    stats["already_ok"] += 1

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    logger.error(f"Error processing {key}: {e}")

        # Execute updates
        if update_pipe and batch_updates > 0:
            await update_pipe.execute()

        processed = min(batch_start + WRITE_BATCH, len(keys))
        logger.info(
            f"  Processed {processed:,}/{len(keys):,} "
            f"(updated={stats['updated']:,}, title_added={stats['title_added']:,})"
        )

    return stats


async def run_migration(
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    dry_run: bool = False,
) -> None:
    """Run the full migration across media and people indexes."""
    redis = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

    try:
        ping_result = await cast(Awaitable[bool], redis.ping())
        if not ping_result:
            raise ConnectionError("Redis ping failed")
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    mode = "[DRY RUN] " if dry_run else ""
    logger.info("=" * 60)
    logger.info(f"{mode}Search Title Apostrophe Migration")
    logger.info("=" * 60)

    start_time = time.time()

    try:
        # Migrate media documents (movies + TV)
        logger.info("")
        logger.info("--- Media documents (media:*) ---")
        media_stats = await migrate_prefix(redis, "media:", dry_run=dry_run)

        # Migrate people documents
        logger.info("")
        logger.info("--- People documents (person:*) ---")
        people_stats = await migrate_prefix(redis, "person:", dry_run=dry_run)

        # Summary
        elapsed = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"{mode}Migration Summary")
        logger.info("=" * 60)

        for label, s in [("Media", media_stats), ("People", people_stats)]:
            logger.info(f"  {label}:")
            logger.info(f"    Scanned:     {s['scanned']:,}")
            logger.info(f"    Updated:     {s['updated']:,}")
            logger.info(f"    Title added: {s['title_added']:,}")
            logger.info(f"    Already OK:  {s['already_ok']:,}")
            logger.info(f"    Errors:      {s['errors']:,}")

        total_updated = media_stats["updated"] + people_stats["updated"]
        total_title = media_stats["title_added"] + people_stats["title_added"]
        logger.info(f"  Total updated: {total_updated:,}")
        logger.info(f"  Total title added: {total_title:,}")
        logger.info(f"  Duration: {elapsed:.2f}s")

    finally:
        await redis.aclose()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate search_title fields to strip apostrophes"
    )
    parser.add_argument(
        "--redis-host",
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port (default: 6380)",
    )
    parser.add_argument(
        "--redis-password",
        default=os.getenv("REDIS_PASSWORD") or None,
        help="Redis password",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without writing",
    )

    args = parser.parse_args()

    asyncio.run(
        run_migration(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
