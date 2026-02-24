#!/usr/bin/env python3
"""
Backfill media dates, timestamps, us_rating, and watch_providers into Redis.

Multi-phase backfill per the media-index-enhancements plan:

  Phase 1: From cache data — dates, director, created_at/modified_at
  Phase 2: us_rating — TMDB API call per existing Redis media doc
  Phase 2b: watch_providers — TMDB API call per existing Redis media doc
  Phase 2+2b: Combined us_rating + watch_providers in one pass
  Phase 3: Timestamps for non-media indexes (person, podcast, book, author)

Usage:
    # Phase 1: backfill from cache files
    python scripts/backfill_media_dates_and_timestamps.py --phase 1 --type movie
    python scripts/backfill_media_dates_and_timestamps.py --phase 1 --type tv --year-lte 2020 --dry-run

    # Phase 2: us_rating
    python scripts/backfill_media_dates_and_timestamps.py --phase 2 --limit 500

    # Phase 2b: watch_providers
    python scripts/backfill_media_dates_and_timestamps.py --phase 2b --limit 500

    # Phase 2+2b: combined
    python scripts/backfill_media_dates_and_timestamps.py --phase 2+2b --dry-run

    # Phase 3: timestamps for non-media
    python scripts/backfill_media_dates_and_timestamps.py --phase 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from adapters.config import load_env

load_env()

from redis.asyncio import Redis  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
from api.tmdb.get_providers import get_streaming_platform_summary_for_title  # noqa: E402
from contracts.models import MCSources, MCType  # noqa: E402
from core.normalize import (  # noqa: E402
    BACKFILL_DEFAULT_TS,
    document_to_redis,
    normalize_document,
    resolve_timestamps,
)
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

BATCH_SIZE = 100
API_DELAY = 0.25


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


# ---------------------------------------------------------------------------
# Phase 1: cache data → Redis (dates + timestamps + director)
# ---------------------------------------------------------------------------


def _discover_cache_files(media_type: str, data_dir: Path) -> list[Path]:
    subdir = data_dir / media_type
    if not subdir.exists():
        return []
    return sorted(subdir.glob("*.json"))


def _filter_files_by_year_lte(files: list[Path], year_lte: int) -> list[Path]:
    filtered: list[Path] = []
    for f in files:
        parts = f.stem.split("_")
        if len(parts) >= 4:
            try:
                file_year = int(parts[2])
                if file_year <= year_lte:
                    filtered.append(f)
            except ValueError:
                continue
    return filtered


def _load_cache_items(file_path: Path) -> list[dict[str, Any]]:
    with open(file_path) as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        return data.get("results", [])
    if isinstance(data, list):
        return data
    return []


async def phase1(
    media_type: str,
    year_lte: int | None,
    dry_run: bool,
) -> dict[str, int]:
    mc_type = MCType.MOVIE if media_type == "movie" else MCType.TV_SERIES
    data_dir = Path("data/us")

    files = _discover_cache_files(media_type, data_dir)
    if year_lte is not None:
        files = _filter_files_by_year_lte(files, year_lte)

    logger.info("Phase 1: %s — %d cache files", media_type, len(files))

    stats: dict[str, int] = {
        "files": 0,
        "candidates": 0,
        "loaded": 0,
        "skipped": 0,
        "errors": 0,
    }

    if dry_run:
        for fp in files:
            items = _load_cache_items(fp)
            stats["files"] += 1
            stats["candidates"] += len(items)
        logger.info("[DRY RUN] Phase 1 candidates: %d across %d files", stats["candidates"], stats["files"])
        return stats

    redis = _connect_redis()
    try:
        await redis.ping()  # type: ignore[misc]

        for fp in files:
            items = _load_cache_items(fp)
            stats["files"] += 1
            batch: list[tuple[str, dict[str, Any]]] = []

            for item in items:
                doc = normalize_document(item, source=MCSources.TMDB, mc_type=mc_type)
                if doc is None:
                    stats["skipped"] += 1
                    continue

                key = f"media:{doc.id}"
                doc._source = "backfill"
                redis_doc = document_to_redis(doc)
                batch.append((key, redis_doc))

                if len(batch) >= BATCH_SIZE:
                    loaded = await _flush_phase1_batch(redis, batch)
                    stats["loaded"] += loaded
                    stats["errors"] += len(batch) - loaded
                    batch = []

            if batch:
                loaded = await _flush_phase1_batch(redis, batch)
                stats["loaded"] += loaded
                stats["errors"] += len(batch) - loaded

            stats["candidates"] += len(items)
            if stats["files"] % 50 == 0:
                logger.info("  Processed %d files, %d docs loaded so far", stats["files"], stats["loaded"])

    finally:
        await redis.aclose()

    return stats


async def _flush_phase1_batch(
    redis: Redis,  # type: ignore[type-arg]
    batch: list[tuple[str, dict[str, Any]]],
) -> int:
    now_ts = int(datetime.now(UTC).timestamp())
    keys = [k for k, _ in batch]

    read_pipe = redis.pipeline()
    for key in keys:
        read_pipe.json().get(key)
    existing_docs: list[object] = await read_pipe.execute()

    write_pipe = redis.pipeline()
    for (key, redis_doc), existing in zip(batch, existing_docs, strict=True):
        existing_dict = existing if isinstance(existing, dict) else None
        created_at, modified_at, source_tag = resolve_timestamps(existing_dict, now_ts, source_tag="backfill")
        redis_doc["created_at"] = created_at
        redis_doc["modified_at"] = modified_at
        redis_doc["_source"] = source_tag
        write_pipe.json().set(key, "$", redis_doc)

    await write_pipe.execute()
    return len(batch)


# ---------------------------------------------------------------------------
# Phase 2: us_rating backfill
# ---------------------------------------------------------------------------


async def phase2(
    limit: int | None,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {"scanned": 0, "updated": 0, "failed": 0, "skipped": 0}
    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        service = TMDBService()

        keys: list[str] = []
        async for key in redis.scan_iter(match="media:*", count=1000):
            keys.append(key)
            if limit and len(keys) >= limit:
                break

        logger.info("Phase 2 (us_rating): %d keys to process", len(keys))
        if dry_run:
            stats["scanned"] = len(keys)
            logger.info("[DRY RUN] Would update up to %d docs", len(keys))
            return stats

        for key in keys:
            stats["scanned"] += 1
            try:
                doc = await redis.json().get(key)  # type: ignore[misc]
                if not isinstance(doc, dict):
                    stats["skipped"] += 1
                    continue

                source_id = doc.get("source_id")
                mc_type = doc.get("mc_type", "")
                if not source_id:
                    stats["skipped"] += 1
                    continue

                tmdb_id = int(source_id)
                result = await service.get_content_rating(tmdb_id, "US", mc_type)
                rating = result.get("rating") if result else None

                now_ts = int(datetime.now(UTC).timestamp())
                pipe = redis.pipeline()
                pipe.json().set(key, "$.us_rating", rating)
                pipe.json().set(key, "$.modified_at", now_ts)
                await pipe.execute()
                stats["updated"] += 1

            except Exception:
                logger.warning("Phase 2 failed for %s", key, exc_info=True)
                try:
                    now_ts = int(datetime.now(UTC).timestamp())
                    pipe = redis.pipeline()
                    pipe.json().set(key, "$.us_rating", None)
                    pipe.json().set(key, "$.modified_at", now_ts)
                    await pipe.execute()
                except Exception:
                    logger.error("Could not set null us_rating for %s", key, exc_info=True)
                stats["failed"] += 1

            await asyncio.sleep(API_DELAY)
            if stats["scanned"] % 200 == 0:
                logger.info("  Phase 2 progress: %d/%d", stats["scanned"], len(keys))

    finally:
        await redis.aclose()

    return stats


# ---------------------------------------------------------------------------
# Phase 2b: watch_providers backfill
# ---------------------------------------------------------------------------


async def phase2b(
    limit: int | None,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {"scanned": 0, "updated": 0, "failed": 0, "skipped": 0}
    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]

        keys: list[str] = []
        async for key in redis.scan_iter(match="media:*", count=1000):
            keys.append(key)
            if limit and len(keys) >= limit:
                break

        logger.info("Phase 2b (watch_providers): %d keys to process", len(keys))
        if dry_run:
            stats["scanned"] = len(keys)
            logger.info("[DRY RUN] Would update up to %d docs", len(keys))
            return stats

        for key in keys:
            stats["scanned"] += 1
            try:
                doc = await redis.json().get(key)  # type: ignore[misc]
                if not isinstance(doc, dict):
                    stats["skipped"] += 1
                    continue

                source_id = doc.get("source_id")
                mc_type = doc.get("mc_type", "")
                if not source_id or mc_type not in ("movie", "tv"):
                    stats["skipped"] += 1
                    continue

                tmdb_id = int(source_id)
                wp = await get_streaming_platform_summary_for_title(tmdb_id, mc_type, "US")

                now_ts = int(datetime.now(UTC).timestamp())
                pipe = redis.pipeline()
                pipe.json().set(key, "$.watch_providers", wp)
                pipe.json().set(key, "$.modified_at", now_ts)
                await pipe.execute()
                stats["updated"] += 1

            except Exception:
                logger.warning("Phase 2b failed for %s", key, exc_info=True)
                try:
                    now_ts = int(datetime.now(UTC).timestamp())
                    pipe = redis.pipeline()
                    pipe.json().set(key, "$.watch_providers", None)
                    pipe.json().set(key, "$.modified_at", now_ts)
                    await pipe.execute()
                except Exception:
                    logger.error("Could not set null watch_providers for %s", key, exc_info=True)
                stats["failed"] += 1

            await asyncio.sleep(API_DELAY)
            if stats["scanned"] % 200 == 0:
                logger.info("  Phase 2b progress: %d/%d", stats["scanned"], len(keys))

    finally:
        await redis.aclose()

    return stats


# ---------------------------------------------------------------------------
# Phase 2+2b: combined us_rating + watch_providers
# ---------------------------------------------------------------------------


async def phase2_combined(
    limit: int | None,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {"scanned": 0, "updated": 0, "failed": 0, "skipped": 0}
    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        service = TMDBService()

        keys: list[str] = []
        async for key in redis.scan_iter(match="media:*", count=1000):
            keys.append(key)
            if limit and len(keys) >= limit:
                break

        logger.info("Phase 2+2b (us_rating + watch_providers): %d keys", len(keys))
        if dry_run:
            stats["scanned"] = len(keys)
            logger.info("[DRY RUN] Would update up to %d docs", len(keys))
            return stats

        for key in keys:
            stats["scanned"] += 1
            try:
                doc = await redis.json().get(key)  # type: ignore[misc]
                if not isinstance(doc, dict):
                    stats["skipped"] += 1
                    continue

                source_id = doc.get("source_id")
                mc_type = doc.get("mc_type", "")
                if not source_id or mc_type not in ("movie", "tv"):
                    stats["skipped"] += 1
                    continue

                tmdb_id = int(source_id)

                rating_result, wp = await asyncio.gather(
                    service.get_content_rating(tmdb_id, "US", mc_type),
                    get_streaming_platform_summary_for_title(tmdb_id, mc_type, "US"),
                    return_exceptions=True,
                )

                us_rating: str | None = None
                if isinstance(rating_result, dict):
                    us_rating = rating_result.get("rating")  # type: ignore[assignment]
                elif isinstance(rating_result, BaseException):
                    logger.warning("get_content_rating failed for %s: %s", key, rating_result)

                wp_value: dict[str, Any] | None = None
                if isinstance(wp, dict):
                    wp_value = wp
                elif isinstance(wp, BaseException):
                    logger.warning("get_streaming_platform_summary failed for %s: %s", key, wp)

                now_ts = int(datetime.now(UTC).timestamp())
                pipe = redis.pipeline()
                pipe.json().set(key, "$.us_rating", us_rating)
                pipe.json().set(key, "$.watch_providers", wp_value)
                pipe.json().set(key, "$.modified_at", now_ts)
                await pipe.execute()
                stats["updated"] += 1

            except Exception:
                logger.warning("Phase 2+2b failed for %s", key, exc_info=True)
                stats["failed"] += 1

            await asyncio.sleep(API_DELAY)
            if stats["scanned"] % 200 == 0:
                logger.info("  Phase 2+2b progress: %d/%d", stats["scanned"], len(keys))

    finally:
        await redis.aclose()

    return stats


# ---------------------------------------------------------------------------
# Phase 3: timestamps for non-media indexes
# ---------------------------------------------------------------------------

NON_MEDIA_PREFIXES = ("person:*", "podcast:*", "book:*", "author:*")


async def phase3(dry_run: bool) -> dict[str, int]:
    stats: dict[str, int] = {"scanned": 0, "updated": 0, "already_set": 0, "errors": 0}
    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]

        for pattern in NON_MEDIA_PREFIXES:
            keys: list[str] = []
            async for key in redis.scan_iter(match=pattern, count=1000):
                keys.append(key)

            logger.info("Phase 3: %s — %d keys", pattern, len(keys))

            for batch_start in range(0, len(keys), BATCH_SIZE):
                batch_keys = keys[batch_start : batch_start + BATCH_SIZE]

                read_pipe = redis.pipeline()
                for key in batch_keys:
                    read_pipe.json().get(key, "$.created_at")
                results: list[object] = await read_pipe.execute()

                write_pipe = redis.pipeline()
                writes_in_batch = 0

                for key, result in zip(batch_keys, results, strict=True):
                    stats["scanned"] += 1
                    has_created_at = False
                    if isinstance(result, list) and result and result[0] is not None:
                        has_created_at = True

                    if has_created_at:
                        stats["already_set"] += 1
                        continue

                    if dry_run:
                        stats["updated"] += 1
                        continue

                    write_pipe.json().set(key, "$.created_at", BACKFILL_DEFAULT_TS)
                    write_pipe.json().set(key, "$.modified_at", BACKFILL_DEFAULT_TS)
                    writes_in_batch += 1
                    stats["updated"] += 1

                if write_pipe and writes_in_batch > 0 and not dry_run:
                    await write_pipe.execute()

    except Exception:
        logger.error("Phase 3 error", exc_info=True)
        stats["errors"] += 1
    finally:
        await redis.aclose()

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_stats(phase_name: str, stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}{phase_name} — Summary")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill media dates, timestamps, us_rating, and watch_providers"
    )
    parser.add_argument(
        "--phase",
        required=True,
        choices=["1", "2", "2b", "2+2b", "3"],
        help="Backfill phase to run",
    )
    parser.add_argument(
        "--type",
        dest="media_type",
        choices=["movie", "tv"],
        help="Media type (required for phase 1)",
    )
    parser.add_argument(
        "--year-lte",
        type=int,
        default=None,
        help="Only process cache files with year <= this value (phase 1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max documents to process (phases 2, 2b, 2+2b)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count candidates without writing to Redis",
    )

    args = parser.parse_args()

    if args.phase == "1" and not args.media_type:
        parser.error("--type is required for phase 1")

    t0 = time.time()

    if args.phase == "1":
        stats = await phase1(args.media_type, args.year_lte, args.dry_run)
        _print_stats("Phase 1 (cache data)", stats, time.time() - t0, args.dry_run)

    elif args.phase == "2":
        stats = await phase2(args.limit, args.dry_run)
        _print_stats("Phase 2 (us_rating)", stats, time.time() - t0, args.dry_run)

    elif args.phase == "2b":
        stats = await phase2b(args.limit, args.dry_run)
        _print_stats("Phase 2b (watch_providers)", stats, time.time() - t0, args.dry_run)

    elif args.phase == "2+2b":
        stats = await phase2_combined(args.limit, args.dry_run)
        _print_stats("Phase 2+2b (combined)", stats, time.time() - t0, args.dry_run)

    elif args.phase == "3":
        stats = await phase3(args.dry_run)
        _print_stats("Phase 3 (non-media timestamps)", stats, time.time() - t0, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
