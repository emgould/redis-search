#!/usr/bin/env python3
"""
Push existing Redis media documents to Media Manager for FAISS indexing.

Iterates the idx:media index via SCAN, reads full JSON documents, and
POSTs them in batches to the Media Manager /insert-docs endpoint.

The SCAN/batch control model is copied directly from
backfill_media_dates_and_timestamps.py.

Usage:
    # Dry-run: scan Redis, validate docs, health-check endpoint, no submissions
    python scripts/backfill_media_manager_faiss.py --dry-run

    # Limited test run (20 docs)
    python scripts/backfill_media_manager_faiss.py --limit 20

    # Full backfill, skip flush (manual finalization)
    python scripts/backfill_media_manager_faiss.py --no-flush

    # Full backfill with poll-before-flush
    python scripts/backfill_media_manager_faiss.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
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

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from etl.media_manager_filter import passes_media_manager_filter  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

PROGRESS_INTERVAL = 100
POLL_INTERVAL_SECONDS = 5.0
POLL_MAX_WAIT_SECONDS = 3600.0


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


async def _poll_until_drained(client: MediaManagerClient) -> None:
    """Poll /insert-docs/status until queue_depth == 0."""
    waited = 0.0
    while waited < POLL_MAX_WAIT_SECONDS:
        status = await client.get_status()
        depth = status["queue_depth"]
        if depth == 0:
            logger.info("Queue drained (total_processed=%d)", status["total_processed"])
            return
        logger.info(
            "Waiting for queue drain: queue_depth=%d, total_processed=%d (%.0fs elapsed)",
            depth,
            status["total_processed"],
            waited,
        )
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        waited += POLL_INTERVAL_SECONDS

    logger.warning(
        "Poll-before-flush timed out after %.0fs — queue may still have items", waited
    )


async def backfill(
    scan_count: int,
    limit: int | None,
    skip: int,
    batch_size: int,
    dry_run: bool,
    no_flush: bool,
) -> dict[str, Any]:
    """Scan Redis media docs and push to Media Manager in batches."""
    stats: dict[str, int] = {
        "scanned": 0,
        "skipped_offset": 0,
        "processed": 0,
        "filtered": 0,
        "submitted": 0,
        "queued": 0,
        "skipped": 0,
        "errors": 0,
        "invalid_docs": 0,
        "movies": 0,
        "tv": 0,
        "last_log_scanned": 0,
    }
    remaining_skip = skip
    all_errors: list[str] = []
    filtered_titles: list[dict[str, str]] = []
    flush_status: str | None = None

    redis = _connect_redis()
    mm_client: MediaManagerClient | None = None

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info("Redis connected")

        mm_client = MediaManagerClient()

        # Health check: always run to validate reachability
        await mm_client.health_check()

        if dry_run:
            logger.info("[DRY RUN] Health check passed — endpoint is reachable")

        logger.info(
            "Backfill started: scan_count=%d, batch_size=%d, limit=%s, skip=%d, dry_run=%s, no_flush=%s",
            scan_count,
            batch_size,
            limit,
            skip,
            dry_run,
            no_flush,
        )

        cursor: int = 0
        pending_batch: list[dict[str, Any]] = []

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)

            if not keys:
                if cursor == 0:
                    break
                continue

            stats["scanned"] += len(keys)

            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key)  # type: ignore[union-attr]
            raw_docs: list[object] = await pipe.execute()

            for key, raw in zip(keys, raw_docs, strict=True):
                doc = raw if isinstance(raw, dict) else None
                if doc is None:
                    stats["invalid_docs"] += 1
                    filtered_titles.append({
                        "key": key,
                        "title": "",
                        "mc_type": "",
                        "reason": "unparseable document",
                    })
                    continue

                mc_type = doc.get("mc_type")
                title = doc.get("title")
                overview = doc.get("overview")

                if not mc_type or not title or not overview:
                    stats["invalid_docs"] += 1
                    missing = []
                    if not mc_type:
                        missing.append("mc_type")
                    if not title:
                        missing.append("title")
                    if not overview:
                        missing.append("overview")
                    filtered_titles.append({
                        "key": key,
                        "title": title or "",
                        "mc_type": mc_type or "",
                        "reason": f"missing {', '.join(missing)}",
                    })
                    continue

                if remaining_skip > 0:
                    remaining_skip -= 1
                    stats["skipped_offset"] += 1
                    continue

                stats["processed"] += 1

                passed, reason = passes_media_manager_filter(doc)
                if not passed:
                    stats["filtered"] += 1
                    filtered_titles.append({
                        "key": key,
                        "title": title or "",
                        "mc_type": mc_type or "",
                        "reason": reason,
                    })
                    continue

                if mc_type == "movie":
                    stats["movies"] += 1
                elif mc_type == "tv":
                    stats["tv"] += 1

                pending_batch.append(doc)

                if len(pending_batch) >= batch_size:
                    if dry_run:
                        stats["submitted"] += len(pending_batch)
                        pending_batch = []
                        continue

                    resp = await mm_client.insert_docs(pending_batch)
                    stats["submitted"] += len(pending_batch)
                    stats["queued"] += resp["queued"]
                    stats["skipped"] += resp["skipped"]
                    if resp["errors"]:
                        stats["errors"] += len(resp["errors"])
                        all_errors.extend(resp["errors"])
                        for err in resp["errors"]:
                            logger.warning("insert-docs error: %s", err)

                    logger.info(
                        "Batch submitted: queued=%d, skipped=%d, queue_depth=%d",
                        resp["queued"],
                        resp["skipped"],
                        resp["queue_depth"],
                    )
                    pending_batch = []

            if stats["scanned"] - stats["last_log_scanned"] >= 1000 or cursor == 0:
                stats["last_log_scanned"] = stats["scanned"]
                logger.info(
                    "  Progress: scanned=%d, submitted=%d, filtered=%d, movies=%d, tv=%d, invalid=%d",
                    stats["scanned"],
                    stats["submitted"],
                    stats["filtered"],
                    stats["movies"],
                    stats["tv"],
                    stats["invalid_docs"],
                )

            if limit is not None and stats["processed"] >= limit:
                logger.info("Reached limit of %d processed documents", limit)
                break

            if cursor == 0:
                break

        # Flush remaining partial batch
        if pending_batch:
            if dry_run:
                stats["submitted"] += len(pending_batch)
            else:
                resp = await mm_client.insert_docs(pending_batch)
                stats["submitted"] += len(pending_batch)
                stats["queued"] += resp["queued"]
                stats["skipped"] += resp["skipped"]
                if resp["errors"]:
                    stats["errors"] += len(resp["errors"])
                    all_errors.extend(resp["errors"])
                logger.info(
                    "Final batch submitted: queued=%d, skipped=%d",
                    resp["queued"],
                    resp["skipped"],
                )

        # Poll-before-flush finalization
        if not dry_run and not no_flush:
            logger.info("Polling until queue is drained before flush...")
            await _poll_until_drained(mm_client)

            logger.info("Calling /insert-docs/flush...")
            try:
                flush_resp = await mm_client.flush()
                logger.info("Flush response: %s", flush_resp)
                flush_status = flush_resp["status"]
            except Exception as flush_err:
                logger.error(
                    "Flush failed (batch submissions already succeeded): %s", flush_err
                )
                flush_status = "flush_error"
        elif dry_run:
            logger.info("[DRY RUN] Skipping insert and flush — scan complete")

        if filtered_titles:
            out_path = Path("data/backfill/filtered_titles.json")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(filtered_titles, indent=2))
            logger.info("Wrote %d filtered titles to %s", len(filtered_titles), out_path)

    finally:
        if mm_client is not None:
            await mm_client.close()
        await redis.aclose()

    return {**stats, "insert_errors": all_errors, "flush_status": flush_status}


def _print_stats(stats: dict[str, Any], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Media Manager FAISS Backfill — Summary")
    print("=" * 60)
    for k, v in stats.items():
        if k in ("last_log_scanned", "insert_errors", "flush_status"):
            continue
        if isinstance(v, (int, float)):
            print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")

    flush_status = stats.get("flush_status")
    if flush_status:
        print(f"  flush_status: {flush_status}")

    insert_errors: list[str] = stats.get("insert_errors", [])
    if insert_errors:
        print(f"\n  Endpoint errors ({len(insert_errors)}):")
        for err in insert_errors[:20]:
            print(f"    - {err}")
        if len(insert_errors) > 20:
            print(f"    ... and {len(insert_errors) - 20} more")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push Redis media documents to Media Manager for FAISS indexing"
    )
    parser.add_argument(
        "--scan-count",
        type=int,
        default=500,
        help="Number of keys to request per SCAN iteration (default 500)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max documents to submit in this run (e.g. 20 for a test)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip the first N valid documents before processing (default 0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Documents per POST batch (max 100, default 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan Redis and validate docs + endpoint health, no submissions",
    )
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Skip calling /insert-docs/flush after submission",
    )

    args = parser.parse_args()

    if args.batch_size > 100:
        parser.error("--batch-size must not exceed 100 (API contract limit)")

    t0 = time.time()
    result = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        skip=args.skip,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        no_flush=args.no_flush,
    )
    elapsed = time.time() - t0

    _print_stats(result, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
