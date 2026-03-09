#!/usr/bin/env python3
"""
Backfill movie release dates for Redis media docs where release_date is missing.

This script scans `media:*` Redis JSON docs, selects movie records with
`release_date == null` (or empty), re-fetches TMDB details, normalizes using
the shared pipeline, and writes only when a non-empty release_date is recovered.

Usage:
    python scripts/backfill_missing_movie_release_dates.py --dry-run
    python scripts/backfill_missing_movie_release_dates.py --limit 500
    python scripts/backfill_missing_movie_release_dates.py --concurrency 20
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
from typing import Any, cast

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from api.tmdb.core import TMDBService  # noqa: E402
from contracts.models import MCSources, MCType  # noqa: E402
from core.normalize import (  # noqa: E402
    document_to_redis,
    normalize_document,
    resolve_timestamps,
)
from utils.genre_mapping import get_genre_mapping_with_fallback  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_LOG_INTERVAL = 5000
PROCESS_LOG_INTERVAL = 500


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _extract_first_json_value(raw_value: object) -> object | None:
    """
    Normalize Redis JSON.MGET value shapes to a scalar.

    JSON.MGET for a JSONPath may return forms like:
    - None
    - [None]
    - ["movie"]
    - "[\"movie\"]" (stringified JSON)
    - "movie" (plain string)
    """
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


def _as_nonempty_string(value: object) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _is_missing_release_date(value: object) -> bool:
    extracted = _extract_first_json_value(value)
    if extracted is None:
        return True
    if isinstance(extracted, str):
        return extracted.strip() == ""
    return False


async def backfill(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    sync_media_manager: bool,
    mm_batch_size: int,
    no_mm_flush: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "movie_candidates": 0,
        "fetched": 0,
        "fetch_failed": 0,
        "normalize_failed": 0,
        "has_release_date": 0,
        "updated": 0,
        "already_had_release_date": 0,
        "mm_submitted": 0,
        "mm_queued": 0,
        "mm_skipped": 0,
        "mm_errors": 0,
    }

    redis = _connect_redis()
    service = TMDBService()
    mm_client: MediaManagerClient | None = None

    try:
        await redis.ping()  # type: ignore[misc]
        genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
        logger.info(
            "Backfill start: scan_count=%d limit=%s concurrency=%d dry_run=%s sync_media_manager=%s",
            scan_count,
            limit,
            concurrency,
            dry_run,
            sync_media_manager,
        )
        if sync_media_manager and not dry_run:
            mm_client = MediaManagerClient()
            await mm_client.health_check()

        cursor = 0
        candidates: list[tuple[str, int]] = []

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)
            if not keys:
                if cursor == 0:
                    break
                continue

            stats["scanned"] += len(keys)
            if (
                stats["scanned"] - stats.get("last_log_scanned", 0) >= SCAN_LOG_INTERVAL
                or cursor == 0
            ):
                stats["last_log_scanned"] = stats["scanned"]
                logger.info(
                    "Scan progress: scanned=%d movie_candidates=%d already_had_release_date=%d",
                    stats["scanned"],
                    len(candidates),
                    stats["already_had_release_date"],
                )

            mc_types = await redis.execute_command("JSON.MGET", *keys, "$.mc_type")
            source_ids = await redis.execute_command("JSON.MGET", *keys, "$.source_id")
            release_dates = await redis.execute_command("JSON.MGET", *keys, "$.release_date")

            if not isinstance(mc_types, list):
                mc_types = []
            if not isinstance(source_ids, list):
                source_ids = []
            if not isinstance(release_dates, list):
                release_dates = []

            for key, mc_type_raw, source_id_raw, release_date_raw in zip(
                keys, mc_types, source_ids, release_dates, strict=True
            ):
                mc_type = _as_nonempty_string(_extract_first_json_value(mc_type_raw))
                if mc_type != "movie":
                    continue

                if not _is_missing_release_date(release_date_raw):
                    stats["already_had_release_date"] += 1
                    continue

                source_id_str = _as_nonempty_string(_extract_first_json_value(source_id_raw))
                if source_id_str is None:
                    continue

                try:
                    tmdb_id = int(source_id_str)
                except ValueError:
                    continue

                candidates.append((key, tmdb_id))

            if limit is not None and len(candidates) >= limit:
                candidates = candidates[:limit]
                break

            if cursor == 0:
                break

        stats["movie_candidates"] = len(candidates)
        logger.info("Candidate scan complete: movie_candidates=%d", stats["movie_candidates"])

        for offset in range(0, len(candidates), concurrency):
            chunk = candidates[offset : offset + concurrency]
            fetch_tasks = [
                service.get_media_details(tmdb_id, MCType.MOVIE, no_cache=True)
                for _, tmdb_id in chunk
            ]
            results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            prepared: list[tuple[str, dict[str, object]]] = []

            for (key, tmdb_id), result in zip(chunk, results, strict=True):
                if isinstance(result, BaseException):
                    stats["fetch_failed"] += 1
                    logger.warning("Fetch failed for tmdb_id=%s: %s", tmdb_id, result)
                    continue

                if hasattr(result, "model_dump"):
                    dumped = result.model_dump(mode="json")
                    if not isinstance(dumped, dict):
                        stats["fetch_failed"] += 1
                        continue
                    item_dict = dumped
                elif isinstance(result, dict):
                    item_dict = result
                else:
                    stats["fetch_failed"] += 1
                    continue

                if not item_dict or item_dict.get("status_code") == 404 or item_dict.get("error"):
                    stats["fetch_failed"] += 1
                    continue

                stats["fetched"] += 1

                doc = normalize_document(
                    item_dict, source=MCSources.TMDB, mc_type=MCType.MOVIE, genre_mapping=genre_mapping
                )
                if doc is None:
                    stats["normalize_failed"] += 1
                    continue

                if not doc.release_date or not doc.release_date.strip():
                    continue

                stats["has_release_date"] += 1
                doc._source = "backfill"
                redis_doc = document_to_redis(doc)
                prepared.append((key, redis_doc))

            if not prepared:
                if (
                    offset > 0
                    and offset % max(concurrency, PROCESS_LOG_INTERVAL) == 0
                ):
                    logger.info(
                        "Process progress: handled=%d/%d fetched=%d has_release_date=%d updated=%d",
                        min(offset + len(chunk), len(candidates)),
                        len(candidates),
                        stats["fetched"],
                        stats["has_release_date"],
                        stats["updated"],
                    )
                continue

            if dry_run:
                stats["updated"] += len(prepared)
                continue

            now_ts = int(datetime.now(UTC).timestamp())
            read_pipe = redis.pipeline()
            for key, _ in prepared:
                read_pipe.json().get(key)
            existing_docs: list[object] = await read_pipe.execute()

            write_pipe = redis.pipeline()
            for (key, redis_doc), existing in zip(prepared, existing_docs, strict=True):
                existing_dict = existing if isinstance(existing, dict) else None
                created_at, modified_at, source_tag = resolve_timestamps(
                    existing_dict, now_ts, source_tag="backfill"
                )
                redis_doc["created_at"] = created_at
                redis_doc["modified_at"] = modified_at
                redis_doc["_source"] = source_tag
                write_pipe.json().set(key, "$", redis_doc)
            await write_pipe.execute()
            stats["updated"] += len(prepared)
            if (
                min(offset + len(chunk), len(candidates)) % PROCESS_LOG_INTERVAL < concurrency
                or min(offset + len(chunk), len(candidates)) == len(candidates)
            ):
                logger.info(
                    "Process progress: handled=%d/%d fetched=%d has_release_date=%d updated=%d",
                    min(offset + len(chunk), len(candidates)),
                    len(candidates),
                    stats["fetched"],
                    stats["has_release_date"],
                    stats["updated"],
                )

            if mm_client is not None:
                docs_for_mm = [redis_doc for _, redis_doc in prepared]
                for batch_start in range(0, len(docs_for_mm), mm_batch_size):
                    batch_slice = docs_for_mm[batch_start : batch_start + mm_batch_size]
                    try:
                        response = await mm_client.insert_docs(
                            cast(list[dict[str, Any]], batch_slice)
                        )
                    except Exception as error:
                        stats["mm_errors"] += len(batch_slice)
                        logger.warning("Media Manager insert-docs failed: %s", error)
                        continue

                    stats["mm_submitted"] += len(batch_slice)
                    stats["mm_queued"] += response["queued"]
                    stats["mm_skipped"] += response["skipped"]
                    stats["mm_errors"] += len(response["errors"])
                    for err in response["errors"]:
                        logger.warning("Media Manager insert-docs error: %s", err)

        if mm_client is not None and not no_mm_flush:
            await mm_client.poll_until_drained()
            await mm_client.flush()
        logger.info(
            "Backfill complete: scanned=%d movie_candidates=%d updated=%d",
            stats["scanned"],
            stats["movie_candidates"],
            stats["updated"],
        )

    finally:
        if mm_client is not None:
            await mm_client.close()
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "WRITE MODE"
    print(f"\n{'=' * 64}")
    print(f"Missing Movie Release Date Backfill ({mode})")
    print("=" * 64)
    for key in (
        "scanned",
        "movie_candidates",
        "already_had_release_date",
        "fetched",
        "fetch_failed",
        "normalize_failed",
        "has_release_date",
        "updated",
        "mm_submitted",
        "mm_queued",
        "mm_skipped",
        "mm_errors",
    ):
        print(f"  {key}: {stats.get(key, 0):,}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill movie docs with missing release_date from TMDB details"
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
        help="Max missing-release-date movie docs to process",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Number of concurrent TMDB calls (default: 15)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and fetch only; do not write Redis updates",
    )
    parser.add_argument(
        "--sync-media-manager",
        action="store_true",
        help="Submit updated docs to Media Manager /insert-docs in the same run",
    )
    parser.add_argument(
        "--mm-batch-size",
        type=int,
        default=100,
        help="Media Manager submit batch size (max 100, default 100)",
    )
    parser.add_argument(
        "--no-mm-flush",
        action="store_true",
        help="Skip Media Manager queue drain + flush at the end",
    )

    args = parser.parse_args()
    if args.mm_batch_size > 100:
        parser.error("--mm-batch-size must not exceed 100")

    start = time.time()
    stats = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        sync_media_manager=args.sync_media_manager,
        mm_batch_size=args.mm_batch_size,
        no_mm_flush=args.no_mm_flush,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
