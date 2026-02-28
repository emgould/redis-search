#!/usr/bin/env python3
"""
Phase 2: Recover media documents lost to key collisions.

Under the old mc_id format (tmdb_{source_id}), a movie and TV show sharing the
same TMDB numeric ID would collide on the same Redis key. The loser was silently
overwritten. Phase 1 migrated surviving docs to the new format
(tmdb_{mc_type}_{source_id}), but collision victims are gone from Redis entirely.

This script scans every media doc in the index, checks whether the opposite
media type (movie ↔ tv) exists for the same source_id, and if missing, fetches
it from TMDB, normalizes it, runs it through the same ETL filter pipeline, and
writes it to Redis if it passes.

Usage:
    # Dry run — report candidates without fetching or writing
    python scripts/recover_collision_victims.py --dry-run

    # Recover all
    python scripts/recover_collision_victims.py

    # Limit fetches (useful for testing)
    python scripts/recover_collision_victims.py --limit 50
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from contracts.models import MCType  # noqa: E402
from core.normalize import document_to_redis, normalize_document  # noqa: E402
from etl.tmdb_nightly_etl import TMDBChangesETL  # noqa: E402
from utils.genre_mapping import get_genre_mapping_with_fallback  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

OPPOSITE_TYPE: dict[str, str] = {"movie": "tv", "tv": "movie"}
FETCH_BATCH_SIZE = 20
SCAN_COUNT = 500


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


async def _find_candidates(redis: Redis) -> list[tuple[str, str]]:  # type: ignore[type-arg]
    """Scan all media docs and return (source_id, missing_mc_type) pairs."""
    candidates: list[tuple[str, str]] = []
    seen_source_ids: set[str] = set()
    cursor: int = 0

    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=SCAN_COUNT)

        if keys:
            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key, "$.source_id", "$.mc_type")  # type: ignore[union-attr]
            results: list[object] = await pipe.execute()

            check_pipe = redis.pipeline()
            checks: list[tuple[str, str]] = []

            for raw in results:
                if not isinstance(raw, dict):
                    continue
                sid_val = raw.get("$.source_id")
                type_val = raw.get("$.mc_type")
                source_id = str(sid_val[0]) if isinstance(sid_val, list) and sid_val else None
                mc_type = type_val[0] if isinstance(type_val, list) and type_val else None

                if not source_id or not mc_type or mc_type not in OPPOSITE_TYPE:
                    continue
                if source_id in seen_source_ids:
                    continue
                seen_source_ids.add(source_id)

                opposite = OPPOSITE_TYPE[mc_type]
                opposite_key = f"media:tmdb_{opposite}_{source_id}"
                check_pipe.exists(opposite_key)
                checks.append((source_id, opposite))

            if checks:
                exists_results: list[int] = await check_pipe.execute()
                for (source_id, opposite_type), exists in zip(checks, exists_results, strict=True):
                    if not exists:
                        candidates.append((source_id, opposite_type))

        if cursor == 0:
            break

    return candidates


async def _fetch_and_filter(
    tmdb_id: int,
    mc_type: MCType,
    etl: TMDBChangesETL,
    genre_mapping: dict[int, str],
) -> dict[str, Any] | None:
    """Fetch from TMDB, check ETL filter, normalize. Return redis doc or None."""
    try:
        details = await etl.get_media_details(tmdb_id=tmdb_id, media_type=mc_type)
    except Exception as e:
        logger.warning("TMDB fetch failed for %s %d: %s", mc_type.value, tmdb_id, e)
        return None

    if details is None:
        return None

    if hasattr(details, "model_dump"):
        item_dict: dict[str, Any] = details.model_dump(mode="json")
    elif isinstance(details, dict):
        item_dict = details
    else:
        return None

    if not item_dict or item_dict.get("status_code") == 404:
        return None
    if item_dict.get("error"):
        logger.debug("Enrichment error for %s %d: %s", mc_type.value, tmdb_id, item_dict["error"])
        return None

    item_dict["_media_type"] = mc_type.value
    item_dict["_tmdb_id"] = tmdb_id

    if not etl._passes_media_filter(item_dict):
        return None

    normalized = normalize_document(item_dict, genre_mapping=genre_mapping)
    if normalized is None:
        return None

    now_ts = int(datetime.now(UTC).timestamp())
    normalized.created_at = now_ts
    normalized.modified_at = now_ts
    normalized._source = "collision_recovery"

    return document_to_redis(normalized)


async def recover(
    limit: int | None,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "total_docs_scanned": 0,
        "candidates_found": 0,
        "fetched": 0,
        "passed_filter": 0,
        "written": 0,
        "fetch_failed": 0,
        "filtered_out": 0,
    }

    redis = _connect_redis()
    await redis.ping()  # type: ignore[misc]
    logger.info("Redis connected")

    logger.info("Scanning for collision candidates ...")
    candidates = await _find_candidates(redis)
    stats["candidates_found"] = len(candidates)
    logger.info("Found %d candidates to check", len(candidates))

    if dry_run or not candidates:
        await redis.aclose()
        return stats

    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
    etl = TMDBChangesETL(verbose=False)

    effective = candidates[:limit] if limit else candidates

    for batch_start in range(0, len(effective), FETCH_BATCH_SIZE):
        batch = effective[batch_start : batch_start + FETCH_BATCH_SIZE]

        tasks = []
        for source_id, mc_type_str in batch:
            mc_type = MCType.TV_SERIES if mc_type_str == "tv" else MCType.MOVIE
            tasks.append(_fetch_and_filter(int(source_id), mc_type, etl, genre_mapping))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        write_pipe = redis.pipeline()
        batch_writes = 0

        for (source_id, mc_type_str), result in zip(batch, results, strict=True):
            stats["fetched"] += 1

            if isinstance(result, BaseException):
                stats["fetch_failed"] += 1
                logger.warning("Exception for %s %s: %s", mc_type_str, source_id, result)
                continue

            if result is None:
                stats["filtered_out"] += 1
                continue

            stats["passed_filter"] += 1
            key = f"media:{result['mc_id']}"
            write_pipe.json().set(key, "$", result)  # type: ignore[union-attr]
            batch_writes += 1

        if batch_writes > 0:
            await write_pipe.execute()
            stats["written"] += batch_writes

        processed = min(batch_start + FETCH_BATCH_SIZE, len(effective))
        logger.info(
            "Progress: %d/%d fetched, %d written, %d filtered out",
            processed, len(effective), stats["written"], stats["filtered_out"],
        )

    await redis.aclose()
    return stats


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Recover Collision Victims — Summary")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recover media docs lost to key collisions by fetching the missing media type from TMDB"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max candidates to fetch from TMDB",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Only scan for candidates, don't fetch or write",
    )

    args = parser.parse_args()

    t0 = time.time()
    result = await recover(limit=args.limit, dry_run=args.dry_run)
    elapsed = time.time() - t0
    _print_stats(result, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
