#!/usr/bin/env python3
"""
Backfill ``movie_credit_ids`` / ``tv_credit_ids`` on existing ``person:*`` docs.

Fetches TMDB ``/person/{id}/combined_credits`` once per person, filters with the
same rules as search filmography hydrate, and writes popularity-sorted capped
ID lists onto the person JSON (timestamp-preserving upsert).

Skips people that already have both ID fields (resumable). Processes in
popularity-desc order so the most-searched people land first.

Usage:
    python scripts/backfill_person_credit_ids.py --dry-run
    python scripts/backfill_person_credit_ids.py --person-id 10297
    python scripts/backfill_person_credit_ids.py --limit 100 --concurrency 5
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections.abc import Awaitable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from api.tmdb.person import PERSON_CREDIT_ID_CAP, TMDBPersonService  # noqa: E402
from core.normalize import resolve_timestamps  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_COUNT = 5_000
PROCESS_LOG_INTERVAL = 50


def _connect_redis() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _has_credit_ids(doc: dict[str, Any]) -> bool:
    movie_ids = doc.get("movie_credit_ids")
    tv_ids = doc.get("tv_credit_ids")
    return isinstance(movie_ids, list) and isinstance(tv_ids, list)


def _person_tmdb_id(doc: dict[str, Any], key: str) -> int | None:
    source_id = doc.get("source_id")
    if source_id is not None:
        try:
            return int(source_id)
        except (TypeError, ValueError):
            pass
    # person:tmdb_person_10297 or person:person_10297
    tail = key.rsplit("_", maxsplit=1)
    if len(tail) == 2:
        try:
            return int(tail[1])
        except ValueError:
            return None
    return None


async def _scan_candidates(
    redis: Redis,
    *,
    person_id: int | None,
    limit: int | None,
) -> list[tuple[str, dict[str, Any], float]]:
    """Return (key, doc, popularity) for people missing credit IDs, popularity desc."""
    candidates: list[tuple[str, dict[str, Any], float]] = []
    skipped = 0
    scanned = 0

    if person_id is not None:
        keys = [f"person:tmdb_person_{person_id}", f"person:person_{person_id}"]
        for key in keys:
            raw = await cast(Awaitable[object], redis.json().get(key))
            if isinstance(raw, dict):
                if _has_credit_ids(raw):
                    return []
                pop = float(raw.get("popularity") or 0)
                candidates.append((key, raw, pop))
                break
        return candidates

    async for key in redis.scan_iter(match="person:*", count=SCAN_COUNT):
        scanned += 1
        raw = await cast(Awaitable[object], redis.json().get(key))
        if not isinstance(raw, dict):
            continue
        if _has_credit_ids(raw):
            skipped += 1
            continue
        pop = float(raw.get("popularity") or 0)
        candidates.append((key, raw, pop))

    candidates.sort(key=lambda item: item[2], reverse=True)
    if limit is not None:
        candidates = candidates[:limit]

    logger.info(
        "Scan complete: scanned=%s candidates=%s already_have_ids=%s",
        scanned,
        len(candidates),
        skipped,
    )
    return candidates


async def backfill(
    *,
    dry_run: bool,
    person_id: int | None,
    limit: int | None,
    concurrency: int,
) -> dict[str, int]:
    stats = {
        "candidates": 0,
        "updated": 0,
        "skipped_existing": 0,
        "skipped_no_id": 0,
        "errors": 0,
        "dry_run": 0,
    }

    redis = _connect_redis()
    service = TMDBPersonService()
    semaphore = asyncio.Semaphore(concurrency)

    try:
        candidates = await _scan_candidates(redis, person_id=person_id, limit=limit)
        stats["candidates"] = len(candidates)

        if dry_run:
            stats["dry_run"] = len(candidates)
            for key, _doc, pop in candidates[:20]:
                logger.info("dry-run candidate %s popularity=%s", key, pop)
            if len(candidates) > 20:
                logger.info("... and %s more", len(candidates) - 20)
            return stats

        async def process_one(key: str, doc: dict[str, Any]) -> None:
            nonlocal stats
            async with semaphore:
                if _has_credit_ids(doc):
                    stats["skipped_existing"] += 1
                    return

                tmdb_id = _person_tmdb_id(doc, key)
                if tmdb_id is None:
                    stats["skipped_no_id"] += 1
                    return

                try:
                    credit_ids = await service.get_person_combined_credits(
                        tmdb_id, limit=PERSON_CREDIT_ID_CAP
                    )
                    if credit_ids is None:
                        stats["errors"] += 1
                        logger.warning("No combined_credits response for %s", key)
                        return
                    movie_ids, tv_ids = credit_ids
                except Exception as e:
                    stats["errors"] += 1
                    logger.error("combined_credits failed for %s: %s", key, e)
                    return

                now_ts = int(datetime.now(UTC).timestamp())
                ca, ma, _ = resolve_timestamps(doc, now_ts)
                try:
                    await cast(
                        Awaitable[object],
                        redis.json().set(key, "$.movie_credit_ids", movie_ids),
                    )
                    await cast(
                        Awaitable[object],
                        redis.json().set(key, "$.tv_credit_ids", tv_ids),
                    )
                    await cast(
                        Awaitable[object],
                        redis.json().set(key, "$.created_at", ca),
                    )
                    await cast(
                        Awaitable[object],
                        redis.json().set(key, "$.modified_at", ma),
                    )
                    stats["updated"] += 1
                    if stats["updated"] % PROCESS_LOG_INTERVAL == 0:
                        logger.info(
                            "Progress: updated=%s errors=%s last=%s movies=%s tv=%s",
                            stats["updated"],
                            stats["errors"],
                            key,
                            len(movie_ids),
                            len(tv_ids),
                        )
                except Exception as e:
                    stats["errors"] += 1
                    logger.error("Redis write failed for %s: %s", key, e)

        await asyncio.gather(*(process_one(key, doc) for key, doc, _ in candidates))
        return stats
    finally:
        await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report candidates only")
    parser.add_argument(
        "--person-id",
        type=int,
        default=None,
        help="Backfill a single TMDB person id (e.g. 10297 for McConaughey)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max people to process")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("PERSON_API_CONCURRENCY", "5")),
        help="Concurrent TMDB combined_credits calls (default 5)",
    )
    args = parser.parse_args()

    if not args.dry_run and not os.getenv("TMDB_READ_TOKEN"):
        print("TMDB_READ_TOKEN is not set. Source config/local.env or load_secrets.", file=sys.stderr)
        sys.exit(1)

    start = time.perf_counter()
    stats = asyncio.run(
        backfill(
            dry_run=args.dry_run,
            person_id=args.person_id,
            limit=args.limit,
            concurrency=max(1, args.concurrency),
        )
    )
    elapsed = time.perf_counter() - start
    print("=" * 60)
    print("Person credit ID backfill")
    print("=" * 60)
    for key, value in stats.items():
        print(f"  {key}: {value}")
    print(f"  elapsed_s: {elapsed:.1f}")


if __name__ == "__main__":
    main()
