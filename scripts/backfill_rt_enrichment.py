#!/usr/bin/env python3
"""Backfill Rotten Tomatoes enrichment for existing Redis media documents.

Scans ``media:*`` keys, loads the full JSON document, and attempts RT
enrichment using the 3-tier strategy:
  1. Vanity lookup via ``external_ids.rt_id``
  2. Title + year match against local RT content index
  3. (Optional) Live Algolia search fallback (``--algolia-fallback``)

Only documents that gain new RT data are patched (``JSON.SET`` on specific
paths). ``modified_at`` is updated on every write.

With ``--push-to-mm``, enriched documents are batched and sent to
Media Manager via ``/insert-docs`` with ``metadata_only=True`` so only
stored FAISS metadata is updated (no wiki/LLM/embedding cost).

Usage:
    python scripts/backfill_rt_enrichment.py --dry-run
    python scripts/backfill_rt_enrichment.py --limit 500
    python scripts/backfill_rt_enrichment.py --algolia-fallback
    python scripts/backfill_rt_enrichment.py --mc-type movie
    python scripts/backfill_rt_enrichment.py --force
    python scripts/backfill_rt_enrichment.py --push-to-mm
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

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from api.rottentomatoes.local_store import get_store  # noqa: E402
from etl.rt_enrichment import enrich_from_algolia, enrich_from_local  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_LOG_INTERVAL = 5000
PROCESS_LOG_INTERVAL = 500
MM_BATCH_SIZE = 100

RT_PATCH_FIELDS = (
    "rt_audience_score",
    "rt_critics_score",
    "rt_vanity",
    "rt_release_year",
    "rt_runtime",
    "external_ids",
)


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _needs_rt(doc: dict[str, Any], force: bool) -> bool:
    """Return True when the document is missing RT data (or force is on)."""
    if force:
        return True
    return doc.get("rt_audience_score") is None and doc.get("rt_critics_score") is None


async def _flush_mm_buffer(
    mm_client: MediaManagerClient,
    buffer: list[dict[str, Any]],
    stats: dict[str, int],
    dry_run: bool,
) -> None:
    """Send a batch of enriched docs to Media Manager with metadata_only=True."""
    if not buffer:
        return
    try:
        response = await mm_client.insert_docs(
            [dict(doc) for doc in buffer],
            dry_run=dry_run,
            metadata_only=True,
        )
        stats["mm_queued"] += response["queued"]
        stats["mm_skipped"] += response["skipped"]
        mm_errors = response.get("errors", [])
        if mm_errors:
            stats["mm_errors"] += len(mm_errors)
            for err in mm_errors:
                logger.warning("MM batch error: %s", err)
        logger.info(
            "MM batch sent: queued=%d skipped=%d queue_depth=%d",
            response["queued"],
            response["skipped"],
            response["queue_depth"],
        )
    except Exception as exc:
        stats["mm_errors"] += 1
        logger.warning("MM batch failed: %s", exc)


async def backfill(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    mc_type_filter: str | None,
    algolia_fallback: bool,
    force: bool,
    push_to_mm: bool = False,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "candidates": 0,
        "enriched_local": 0,
        "enriched_algolia": 0,
        "updated": 0,
        "already_has_rt": 0,
        "no_match": 0,
        "mm_queued": 0,
        "mm_skipped": 0,
        "mm_errors": 0,
    }

    redis = _connect_redis()
    store = get_store()
    mm_client: MediaManagerClient | None = None
    mm_buffer: list[dict[str, Any]] = []

    if push_to_mm:
        mm_client = MediaManagerClient()

    try:
        await redis.ping()  # type: ignore[misc]

        if mm_client is not None:
            await mm_client.health_check()
            logger.info("Media Manager health check passed")

        logger.info(
            "RT backfill start: scan_count=%d limit=%s algolia=%s dry_run=%s "
            "mc_type=%s force=%s push_to_mm=%s",
            scan_count, limit, algolia_fallback, dry_run,
            mc_type_filter, force, push_to_mm,
        )

        cursor = 0
        candidates: list[tuple[str, dict[str, Any]]] = []

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
                    stats["already_has_rt"],
                )

            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key)
            docs_raw: list[object] = await pipe.execute()

            for key, doc_raw in zip(keys, docs_raw, strict=True):
                if not isinstance(doc_raw, dict):
                    continue

                mc_type_val = doc_raw.get("mc_type")
                if mc_type_filter and mc_type_val != mc_type_filter:
                    continue

                if not _needs_rt(doc_raw, force):
                    stats["already_has_rt"] += 1
                    continue

                candidates.append((str(key), doc_raw))

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

            for key, doc in chunk:
                enriched = False

                local_hit = enrich_from_local(doc, store=store)
                if local_hit:
                    enriched = True
                    stats["enriched_local"] += 1
                elif algolia_fallback:
                    algolia_hit = await enrich_from_algolia(doc, store=store)
                    if algolia_hit:
                        enriched = True
                        stats["enriched_algolia"] += 1

                if not enriched:
                    stats["no_match"] += 1
                    continue

                if dry_run:
                    stats["updated"] += 1
                    continue

                now_ts = int(datetime.now(UTC).timestamp())
                write_pipe = redis.pipeline()
                for field_name in RT_PATCH_FIELDS:
                    value = doc.get(field_name)
                    if value is not None:
                        write_pipe.json().set(key, f"$.{field_name}", value)
                write_pipe.json().set(key, "$.modified_at", now_ts)
                await write_pipe.execute()
                stats["updated"] += 1

                if mm_client is not None:
                    mm_buffer.append(doc)
                    if len(mm_buffer) >= MM_BATCH_SIZE:
                        await _flush_mm_buffer(mm_client, mm_buffer, stats, dry_run)
                        mm_buffer.clear()

            processed = min(offset + len(chunk), len(candidates))
            if (
                processed % max(concurrency, PROCESS_LOG_INTERVAL) < concurrency
                or processed == len(candidates)
            ):
                logger.info(
                    "Progress: handled=%d/%d local=%d algolia=%d updated=%d no_match=%d",
                    processed,
                    len(candidates),
                    stats["enriched_local"],
                    stats["enriched_algolia"],
                    stats["updated"],
                    stats["no_match"],
                )

        if mm_client is not None and mm_buffer:
            await _flush_mm_buffer(mm_client, mm_buffer, stats, dry_run)
            mm_buffer.clear()

        logger.info(
            "RT backfill complete: scanned=%d candidates=%d updated=%d no_match=%d",
            stats["scanned"],
            stats["candidates"],
            stats["updated"],
            stats["no_match"],
        )
        if push_to_mm:
            logger.info(
                "MM propagation: queued=%d skipped=%d errors=%d",
                stats["mm_queued"],
                stats["mm_skipped"],
                stats["mm_errors"],
            )

    finally:
        if mm_client is not None:
            await mm_client.close()
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, int], elapsed: float, dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "WRITE MODE"
    print(f"\n{'=' * 64}")
    print(f"RT Enrichment Backfill ({mode})")
    print("=" * 64)
    for key in (
        "scanned",
        "candidates",
        "already_has_rt",
        "enriched_local",
        "enriched_algolia",
        "no_match",
        "updated",
        "mm_queued",
        "mm_skipped",
        "mm_errors",
    ):
        print(f"  {key}: {stats.get(key, 0):,}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Redis media docs with Rotten Tomatoes enrichment"
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
        default=50,
        help="Batch processing chunk size (default: 50)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover candidates only; do not write Redis updates",
    )
    parser.add_argument(
        "--mc-type",
        choices=["movie", "tv"],
        default=None,
        help="Filter to a single media type (default: both)",
    )
    parser.add_argument(
        "--algolia-fallback",
        action="store_true",
        help="Enable live Algolia search for docs that fail local matching",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-enrich documents that already have RT data",
    )
    parser.add_argument(
        "--push-to-mm",
        action="store_true",
        help="Push enriched docs to Media Manager (metadata_only mode)",
    )

    args = parser.parse_args()

    start = time.time()
    stats = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        mc_type_filter=args.mc_type,
        algolia_fallback=args.algolia_fallback,
        force=args.force,
        push_to_mm=args.push_to_mm,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
