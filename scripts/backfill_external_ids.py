#!/usr/bin/env python3
"""
Backfill external IDs for Redis media docs where external_ids is missing.

Scans ``media:*`` Redis JSON docs, selects records with null or empty
``external_ids``, calls the lightweight TMDB ``/external_ids`` endpoint,
and patches only the ``$.external_ids`` and ``$.modified_at`` fields.

Usage:
    python scripts/backfill_external_ids.py --dry-run
    python scripts/backfill_external_ids.py --limit 500
    python scripts/backfill_external_ids.py --concurrency 20
    python scripts/backfill_external_ids.py --mc-type movie
    python scripts/backfill_external_ids.py --mc-type tv
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

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
from contracts.models import MCType  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_LOG_INTERVAL = 5000
PROCESS_LOG_INTERVAL = 500

MC_TYPE_TO_ENUM: dict[str, MCType] = {
    "movie": MCType.MOVIE,
    "tv": MCType.TV_SERIES,
}


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _extract_first_json_value(raw_value: object) -> object | None:
    """Normalize Redis JSON.MGET value shapes to a scalar or dict."""
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            return _extract_first_json_value(parsed)
        except json.JSONDecodeError:
            return raw_value
    if isinstance(raw_value, list):
        if not raw_value:
            return None
        return _extract_first_json_value(raw_value[0])
    return raw_value


def _is_missing_external_ids(value: object) -> bool:
    extracted = _extract_first_json_value(value)
    if extracted is None:
        return True
    if isinstance(extracted, dict):
        return len(extracted) == 0
    if isinstance(extracted, str):
        return extracted.strip() in ("", "{}", "null")
    return False


async def backfill(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    mc_type_filter: str | None,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "candidates": 0,
        "fetched": 0,
        "fetch_failed": 0,
        "fetch_empty": 0,
        "updated": 0,
        "already_has_external_ids": 0,
        "skipped_unknown_type": 0,
    }

    redis = _connect_redis()
    service = TMDBService()

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info(
            "Backfill start: scan_count=%d limit=%s concurrency=%d dry_run=%s mc_type_filter=%s",
            scan_count,
            limit,
            concurrency,
            dry_run,
            mc_type_filter,
        )

        cursor = 0
        candidates: list[tuple[str, int, MCType]] = []

        while True:
            cursor, keys = await redis.scan(
                cursor=cursor, match="media:*", count=scan_count
            )
            if not keys:
                if cursor == 0:
                    break
                continue

            stats["scanned"] += len(keys)
            if (
                stats["scanned"] - stats.get("_last_log_scanned", 0)
                >= SCAN_LOG_INTERVAL
                or cursor == 0
            ):
                stats["_last_log_scanned"] = stats["scanned"]
                logger.info(
                    "Scan progress: scanned=%d candidates=%d already_has=%d",
                    stats["scanned"],
                    len(candidates),
                    stats["already_has_external_ids"],
                )

            mc_types_raw = await redis.execute_command(
                "JSON.MGET", *keys, "$.mc_type"
            )
            source_ids_raw = await redis.execute_command(
                "JSON.MGET", *keys, "$.source_id"
            )
            ext_ids_raw = await redis.execute_command(
                "JSON.MGET", *keys, "$.external_ids"
            )

            if not isinstance(mc_types_raw, list):
                mc_types_raw = []
            if not isinstance(source_ids_raw, list):
                source_ids_raw = []
            if not isinstance(ext_ids_raw, list):
                ext_ids_raw = []

            for key, mc_type_val, source_id_val, ext_ids_val in zip(
                keys, mc_types_raw, source_ids_raw, ext_ids_raw, strict=True
            ):
                mc_type_str = _extract_first_json_value(mc_type_val)
                if not isinstance(mc_type_str, str):
                    continue

                mc_enum = MC_TYPE_TO_ENUM.get(mc_type_str)
                if mc_enum is None:
                    stats["skipped_unknown_type"] += 1
                    continue

                if mc_type_filter and mc_type_str != mc_type_filter:
                    continue

                if not _is_missing_external_ids(ext_ids_val):
                    stats["already_has_external_ids"] += 1
                    continue

                source_id = _extract_first_json_value(source_id_val)
                if source_id is None:
                    continue
                try:
                    tmdb_id = int(str(source_id))
                except (ValueError, TypeError):
                    continue

                candidates.append((key, tmdb_id, mc_enum))

            if limit is not None and len(candidates) >= limit:
                candidates = candidates[:limit]
                break

            if cursor == 0:
                break

        stats["candidates"] = len(candidates)
        logger.info("Candidate scan complete: candidates=%d", stats["candidates"])

        if not candidates:
            logger.info("No candidates found, nothing to backfill.")
            await redis.aclose()
            return stats

        for offset in range(0, len(candidates), concurrency):
            chunk = candidates[offset : offset + concurrency]

            fetch_tasks = [
                service.get_external_ids(tmdb_id, mc_enum)
                for _, tmdb_id, mc_enum in chunk
            ]
            results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            now_ts = int(datetime.now(UTC).timestamp())
            pipe = redis.pipeline()
            write_count = 0

            for (key, tmdb_id, _mc_enum), result in zip(
                chunk, results, strict=True
            ):
                if isinstance(result, BaseException):
                    stats["fetch_failed"] += 1
                    logger.warning(
                        "Fetch failed for tmdb_id=%s: %s", tmdb_id, result
                    )
                    continue

                if result is None or not result:
                    stats["fetch_empty"] += 1
                    continue

                stats["fetched"] += 1

                if dry_run:
                    stats["updated"] += 1
                    continue

                ext_ids: dict[str, Any] = result
                pipe.json().set(key, "$.external_ids", ext_ids)
                pipe.json().set(key, "$.modified_at", now_ts)
                write_count += 1

            if write_count > 0:
                await pipe.execute()
                stats["updated"] += write_count

            processed = min(offset + len(chunk), len(candidates))
            if (
                processed % max(concurrency, PROCESS_LOG_INTERVAL) < concurrency
                or processed == len(candidates)
            ):
                logger.info(
                    "Progress: handled=%d/%d fetched=%d updated=%d fetch_failed=%d",
                    processed,
                    len(candidates),
                    stats["fetched"],
                    stats["updated"],
                    stats["fetch_failed"],
                )

        logger.info(
            "Backfill complete: scanned=%d candidates=%d updated=%d",
            stats["scanned"],
            stats["candidates"],
            stats["updated"],
        )

    finally:
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "WRITE MODE"
    print(f"\n{'=' * 64}")
    print(f"External IDs Backfill ({mode})")
    print("=" * 64)
    for key in (
        "scanned",
        "candidates",
        "already_has_external_ids",
        "skipped_unknown_type",
        "fetched",
        "fetch_failed",
        "fetch_empty",
        "updated",
    ):
        print(f"  {key}: {stats.get(key, 0):,}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill media docs with missing external_ids from TMDB"
    )
    parser.add_argument(
        "--scan-count",
        type=int,
        default=500,
        help="Redis SCAN page size (default: 500)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max candidates to process",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Concurrent TMDB calls (default: 15)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover only; do not write Redis updates",
    )
    parser.add_argument(
        "--mc-type",
        choices=["movie", "tv"],
        default=None,
        help="Filter to a single media type (default: both)",
    )

    args = parser.parse_args()

    start = time.time()
    stats = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        mc_type_filter=args.mc_type,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
