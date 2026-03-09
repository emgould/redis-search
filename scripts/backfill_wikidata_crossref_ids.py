#!/usr/bin/env python3
"""
Backfill Wikidata cross-reference identifiers into Redis media documents.

Scans ``media:*`` Redis JSON docs, looks up each item in the local
``data/wikidata_tmdb_tms_crossref.json`` file (by mc_type + source_id),
and merges any new identifier keys (rt_id, metacritic_id, letterboxd_id,
justwatch_id, tcm_id) into ``$.external_ids``.  Existing keys are never
overwritten.

No TMDB API calls are made — this is a purely local file + Redis operation.

Usage:
    python scripts/backfill_wikidata_crossref_ids.py --dry-run
    python scripts/backfill_wikidata_crossref_ids.py --limit 500
    python scripts/backfill_wikidata_crossref_ids.py --mc-type movie
    python scripts/backfill_wikidata_crossref_ids.py --mc-type tv
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

from core.wikidata_crossref import get_crossref_ids, load_crossref, merge_crossref_ids  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_LOG_INTERVAL = 5000
BATCH_LOG_INTERVAL = 500


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


async def backfill(
    scan_count: int,
    limit: int | None,
    batch_size: int,
    dry_run: bool,
    mc_type_filter: str | None,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "enriched": 0,
        "no_crossref_match": 0,
        "no_new_keys": 0,
        "updated": 0,
        "skipped_unknown_type": 0,
    }

    crossref = load_crossref()
    if not crossref:
        logger.error("Crossref file is empty or missing — nothing to backfill.")
        return stats

    logger.info("Crossref loaded: %s entries", f"{len(crossref):,}")

    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info(
            "Backfill start: scan_count=%d limit=%s batch_size=%d dry_run=%s mc_type_filter=%s",
            scan_count,
            limit,
            batch_size,
            dry_run,
            mc_type_filter,
        )

        cursor = 0
        candidates: list[tuple[str, str, str, dict[str, Any]]] = []

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
                stats["scanned"] - stats.get("_last_log_scanned", 0) >= SCAN_LOG_INTERVAL
                or cursor == 0
            ):
                stats["_last_log_scanned"] = stats["scanned"]
                logger.info(
                    "Scan progress: scanned=%d candidates=%d",
                    stats["scanned"],
                    len(candidates),
                )

            mc_types_raw = await redis.execute_command("JSON.MGET", *keys, "$.mc_type")
            source_ids_raw = await redis.execute_command("JSON.MGET", *keys, "$.source_id")
            ext_ids_raw = await redis.execute_command("JSON.MGET", *keys, "$.external_ids")

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
                if not isinstance(mc_type_str, str) or mc_type_str not in ("movie", "tv"):
                    stats["skipped_unknown_type"] += 1
                    continue

                if mc_type_filter and mc_type_str != mc_type_filter:
                    continue

                source_id = _extract_first_json_value(source_id_val)
                if source_id is None:
                    continue

                source_id_str = str(source_id)

                crossref_ids = get_crossref_ids(mc_type_str, source_id_str)
                if crossref_ids is None:
                    stats["no_crossref_match"] += 1
                    continue

                existing_ext: dict[str, Any] | None = None
                extracted = _extract_first_json_value(ext_ids_val)
                if isinstance(extracted, dict):
                    existing_ext = extracted

                merged = merge_crossref_ids(existing_ext, crossref_ids)
                existing_keys = set(existing_ext) if existing_ext else set()
                new_keys = set(merged) - existing_keys
                if not new_keys:
                    stats["no_new_keys"] += 1
                    continue

                stats["enriched"] += 1
                candidates.append((key, mc_type_str, source_id_str, merged))

            if limit is not None and len(candidates) >= limit:
                candidates = candidates[:limit]
                break

            if cursor == 0:
                break

        logger.info(
            "Scan complete: scanned=%d enriched=%d no_match=%d no_new_keys=%d",
            stats["scanned"],
            stats["enriched"],
            stats["no_crossref_match"],
            stats["no_new_keys"],
        )

        if not candidates:
            logger.info("No candidates to update.")
            await redis.aclose()
            return stats

        if dry_run:
            stats["updated"] = len(candidates)
            logger.info("DRY RUN: would update %d documents", len(candidates))
            await redis.aclose()
            return stats

        now_ts = int(datetime.now(UTC).timestamp())

        for offset in range(0, len(candidates), batch_size):
            chunk = candidates[offset : offset + batch_size]
            pipe = redis.pipeline()

            for key, _mc_type, _source_id, merged_ext_ids in chunk:
                pipe.json().set(key, "$.external_ids", merged_ext_ids)
                pipe.json().set(key, "$.modified_at", now_ts)

            await pipe.execute()
            stats["updated"] += len(chunk)

            processed = min(offset + len(chunk), len(candidates))
            if processed % max(batch_size, BATCH_LOG_INTERVAL) < batch_size or processed == len(
                candidates
            ):
                logger.info(
                    "Write progress: %d/%d updated",
                    processed,
                    len(candidates),
                )

        logger.info(
            "Backfill complete: scanned=%d enriched=%d updated=%d",
            stats["scanned"],
            stats["enriched"],
            stats["updated"],
        )

    finally:
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "WRITE MODE"
    print(f"\n{'=' * 64}")
    print(f"Wikidata Crossref IDs Backfill ({mode})")
    print("=" * 64)
    for key in (
        "scanned",
        "enriched",
        "no_crossref_match",
        "no_new_keys",
        "skipped_unknown_type",
        "updated",
    ):
        print(f"  {key}: {stats.get(key, 0):,}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Wikidata crossref identifiers into Redis media docs"
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
        "--batch-size",
        type=int,
        default=200,
        help="Redis pipeline batch size (default: 200)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and count only; do not write Redis updates",
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
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        mc_type_filter=args.mc_type,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
