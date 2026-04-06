"""
Backfill ``title_compact`` on existing ``media:*`` Redis JSON documents.

``title_compact`` is a slug-style derived field: lowercase, strip all
non-alphanumeric characters (except spaces), collapse whitespace, then
remove spaces entirely.  It enables collapsed-token searches like
``goodwillhunting`` to match "Good Will Hunting".

Optionally issues ``FT.ALTER`` to add the new TEXT field to the live
``idx:media`` index before the data backfill.

Usage:
    # Dry run (no changes)
    python scripts/backfill_title_compact.py --dry-run

    # Alter index schema then backfill
    python scripts/backfill_title_compact.py --alter-index

    # Backfill only (index already altered)
    python scripts/backfill_title_compact.py

    # Custom Redis connection
    python scripts/backfill_title_compact.py --redis-host localhost --redis-port 6380
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.get_logger import get_logger

env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

logger = get_logger(__name__)

SCAN_BATCH = 10_000
WRITE_BATCH = 500

_NON_ALNUM_SPACE_RE = re.compile(r"[^a-z0-9\s]")
_MULTI_SPACE_RE = re.compile(r"\s+")


def compact_title(title: str) -> str:
    """Derive slug-style compact title (mirrors core.normalize.compact_title)."""
    s = title.lower().strip()
    s = _NON_ALNUM_SPACE_RE.sub("", s)
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    return s.replace(" ", "")


async def alter_index(redis: Redis) -> None:  # type: ignore[type-arg]
    """Add ``title_compact`` TEXT NOSTEM field to the live ``idx:media`` index."""
    try:
        await redis.execute_command(
            "FT.ALTER", "idx:media", "SCHEMA", "ADD",
            "$.title_compact", "AS", "title_compact", "TEXT", "NOSTEM",
        )
        logger.info("FT.ALTER idx:media — added title_compact TEXT NOSTEM")
    except Exception as e:
        msg = str(e)
        if "Duplicate" in msg or "already exists" in msg.lower():
            logger.info("title_compact already present in idx:media schema — skipping ALTER")
        else:
            raise


async def backfill(
    redis: Redis,  # type: ignore[type-arg]
    dry_run: bool = False,
) -> dict[str, int]:
    """Scan ``media:*`` and set ``$.title_compact`` on every document."""
    stats: dict[str, int] = {
        "scanned": 0,
        "updated": 0,
        "already_ok": 0,
        "errors": 0,
    }

    keys: list[str] = []
    async for key in redis.scan_iter(match="media:*", count=SCAN_BATCH):
        keys.append(key)

    stats["scanned"] = len(keys)
    logger.info(f"Found {len(keys):,} media keys")

    if not keys:
        return stats

    for batch_start in range(0, len(keys), WRITE_BATCH):
        batch_keys = keys[batch_start : batch_start + WRITE_BATCH]

        pipe = redis.pipeline()
        for key in batch_keys:
            pipe.json().get(key, "$.title", "$.search_title", "$.title_compact")
        results = await pipe.execute()

        update_pipe = redis.pipeline() if not dry_run else None
        batch_updates = 0

        for key, result in zip(batch_keys, results, strict=True):
            try:
                if not result:
                    stats["errors"] += 1
                    continue

                if isinstance(result, dict):
                    title_list = result.get("$.title", [])
                    search_title_list = result.get("$.search_title", [])
                    existing_compact_list = result.get("$.title_compact", [])
                elif isinstance(result, list):
                    title_list = result[:1] if result else []
                    search_title_list = result[1:2] if len(result) > 1 else []
                    existing_compact_list = result[2:3] if len(result) > 2 else []
                else:
                    stats["errors"] += 1
                    continue

                canonical = (
                    (title_list[0] if title_list else None)
                    or (search_title_list[0] if search_title_list else None)
                    or ""
                )
                if not canonical:
                    stats["errors"] += 1
                    continue

                new_compact = compact_title(canonical)
                existing_compact = existing_compact_list[0] if existing_compact_list else None

                if existing_compact == new_compact:
                    stats["already_ok"] += 1
                    continue

                if dry_run:
                    if stats["updated"] < 10:
                        logger.info(f"  [DRY RUN] {key}: '{canonical}' -> '{new_compact}'")
                else:
                    update_pipe.json().set(key, "$.title_compact", new_compact)  # type: ignore[union-attr]
                    batch_updates += 1
                stats["updated"] += 1

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    logger.error(f"Error processing {key}: {e}")

        if update_pipe and batch_updates > 0:
            await update_pipe.execute()

        processed = min(batch_start + WRITE_BATCH, len(keys))
        logger.info(
            f"  Processed {processed:,}/{len(keys):,} "
            f"(updated={stats['updated']:,}, already_ok={stats['already_ok']:,})"
        )

    return stats


async def run(
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    dry_run: bool = False,
    do_alter_index: bool = False,
) -> None:
    """Main entry point."""
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
    logger.info(f"{mode}title_compact Backfill")
    logger.info("=" * 60)

    start_time = time.time()

    try:
        if do_alter_index and not dry_run:
            await alter_index(redis)

        stats = await backfill(redis, dry_run=dry_run)

        elapsed = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"{mode}Backfill Summary")
        logger.info("=" * 60)
        logger.info(f"  Scanned:    {stats['scanned']:,}")
        logger.info(f"  Updated:    {stats['updated']:,}")
        logger.info(f"  Already OK: {stats['already_ok']:,}")
        logger.info(f"  Errors:     {stats['errors']:,}")
        logger.info(f"  Duration:   {elapsed:.2f}s")

    finally:
        await redis.aclose()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill title_compact on media:* documents"
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
    parser.add_argument(
        "--alter-index",
        action="store_true",
        help="Issue FT.ALTER to add title_compact to idx:media before backfill",
    )

    args = parser.parse_args()

    asyncio.run(
        run(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            dry_run=args.dry_run,
            do_alter_index=args.alter_index,
        )
    )


if __name__ == "__main__":
    main()
