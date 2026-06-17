#!/usr/bin/env python3
"""Backfill Rotten Tomatoes enrichment for existing Redis media documents.

Scans ``media:*`` keys page-by-page, loads full JSON documents, and attempts RT
enrichment using the 3-tier strategy:
  1. Vanity lookup via ``external_ids.rt_id``
  2. Title + year match against local RT content index
  3. (Optional) Live Algolia search fallback (``--algolia-fallback``)

Only documents that gain new RT data are patched (``JSON.SET`` on specific
paths). ``modified_at`` is updated only when a Redis patch is written.

With ``--push-to-mm``, eligible documents are sent to Media Manager via
``/insert-docs`` with ``metadata_only=True`` so only stored FAISS metadata is
updated (no wiki/LLM/embedding cost). When ``--force`` is also supplied,
documents that already have RT enrichment are sent to Media Manager even when
no new Redis patch is needed. When ``--force-all`` is supplied, every scanned
document that passes filtering is sent to Media Manager regardless of RT state.

Usage:
    python scripts/backfill_rt_enrichment.py --dry-run
    python scripts/backfill_rt_enrichment.py --limit 500
    python scripts/backfill_rt_enrichment.py --algolia-fallback
    python scripts/backfill_rt_enrichment.py --mc-type movie
    python scripts/backfill_rt_enrichment.py --push-to-mm --force
    python scripts/backfill_rt_enrichment.py --push-to-mm --force-all
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
from api.rottentomatoes.local_store import RTContentLookupStore, get_store  # noqa: E402
from etl.rt_enrichment import (  # noqa: E402
    enrich_from_algolia,
    enrich_from_local,
    enrich_from_local_title_year,
    rt_record_matches_doc,
)
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

SCAN_LOG_INTERVAL = 5000
PROCESS_LOG_INTERVAL = 500
MM_BATCH_SIZE = 100

RT_METADATA_FIELDS = (
    "rt_audience_score",
    "rt_critics_score",
    "rt_vanity",
    "rt_release_year",
    "rt_runtime",
)


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _has_rt(doc: dict[str, Any]) -> bool:
    """Return True when the document already has RT score data."""
    return doc.get("rt_audience_score") is not None or doc.get("rt_critics_score") is not None


def _needs_rt(doc: dict[str, Any]) -> bool:
    """Return True when the document is missing RT score data."""
    return doc.get("rt_audience_score") is None and doc.get("rt_critics_score") is None


def _rt_vanity(doc: dict[str, Any]) -> str | None:
    external_ids = doc.get("external_ids")
    if isinstance(external_ids, dict):
        rt_id = external_ids.get("rt_id")
        if isinstance(rt_id, str) and rt_id:
            for prefix in ("m/", "tv/"):
                if rt_id.startswith(prefix):
                    return rt_id[len(prefix):]
            return rt_id

    rt_vanity = doc.get("rt_vanity")
    if isinstance(rt_vanity, str) and rt_vanity:
        return rt_vanity
    return None


def _existing_rt_attachment_record(
    doc: dict[str, Any],
    store: RTContentLookupStore,
) -> dict[str, Any] | None:
    vanity = _rt_vanity(doc)
    if vanity is None:
        return None

    return store.lookup_by_vanity(vanity)


def _print_audit_trace(
    doc: dict[str, Any],
    record: dict[str, Any] | None,
    *,
    matched: bool,
) -> None:
    mc_type = str(doc.get("mc_type") or "?")
    source_id = str(doc.get("source_id") or doc.get("id") or "?")
    title = str(doc.get("title") or doc.get("search_title") or "?")
    rt_title = "N/A"
    rt_id = _rt_vanity(doc) or "N/A"
    if record is not None:
        record_title = record.get("title")
        record_vanity = record.get("vanity")
        record_object_id = record.get("objectID")
        rt_title = str(record_title) if record_title is not None else "N/A"
        rt_id = str(record_vanity or record_object_id or rt_id)
    status = "matched" if matched else "rejected"
    print(f"{mc_type} {source_id} {title} ==> {rt_title} : {rt_id} ==> {status}")


def _clear_rt_metadata(doc: dict[str, Any]) -> None:
    for field_name in RT_METADATA_FIELDS:
        doc.pop(field_name, None)

    external_ids = doc.get("external_ids")
    if isinstance(external_ids, dict):
        external_ids.pop("rt_id", None)
        doc["external_ids"] = external_ids


async def _write_rt_patch(redis: Redis, key: str, doc: dict[str, Any], *, clear: bool) -> None:  # type: ignore[type-arg]
    now_ts = int(datetime.now(UTC).timestamp())
    write_pipe = redis.pipeline()

    if clear:
        for field_name in RT_METADATA_FIELDS:
            write_pipe.execute_command("JSON.DEL", key, f"$.{field_name}")
        write_pipe.execute_command("JSON.DEL", key, "$.external_ids.rt_id")
    else:
        for field_name in RT_METADATA_FIELDS:
            value = doc.get(field_name)
            if value is None:
                write_pipe.execute_command("JSON.DEL", key, f"$.{field_name}")
            else:
                write_pipe.json().set(key, f"$.{field_name}", value)
        external_ids = doc.get("external_ids")
        if isinstance(external_ids, dict):
            write_pipe.json().set(key, "$.external_ids", external_ids)

    write_pipe.json().set(key, "$.modified_at", now_ts)
    await write_pipe.execute()


async def _flush_mm_buffer(
    mm_client: MediaManagerClient,
    docs: list[dict[str, Any]],
    stats: dict[str, int],
    dry_run: bool,
) -> None:
    """Fire-and-forget a batch to Media Manager with metadata_only=True."""
    if not docs:
        return
    payload = [dict(doc) for doc in docs]
    stats["mm_submitted"] += len(payload)

    async def _send() -> None:
        try:
            response = await mm_client.insert_docs(
                payload,
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
        except Exception as exc:
            stats["mm_errors"] += 1
            logger.warning("MM batch failed: %s", exc)

    asyncio.create_task(_send())


async def backfill(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    mc_type_filter: str | None,
    algolia_fallback: bool,
    force: bool,
    force_all: bool,
    audit_existing: bool,
    push_to_mm: bool = False,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "scanned": 0,
        "candidates": 0,
        "enriched_local": 0,
        "enriched_algolia": 0,
        "updated": 0,
        "already_has_rt": 0,
        "audit_valid": 0,
        "audit_invalid": 0,
        "audit_repaired": 0,
        "audit_cleared": 0,
        "no_match": 0,
        "mm_submitted": 0,
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
            "mc_type=%s force=%s force_all=%s audit_existing=%s push_to_mm=%s",
            scan_count, limit, algolia_fallback, dry_run,
            mc_type_filter, force, force_all, audit_existing, push_to_mm,
        )

        cursor = 0
        processed = 0

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
                    stats["candidates"],
                    stats["already_has_rt"],
                )

            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key)
            docs_raw: list[object] = await pipe.execute()

            page_candidates: list[tuple[str, dict[str, Any], bool, bool]] = []
            for key, doc_raw in zip(keys, docs_raw, strict=True):
                if not isinstance(doc_raw, dict):
                    continue

                mc_type_val = doc_raw.get("mc_type")
                if mc_type_filter and mc_type_val != mc_type_filter:
                    continue

                had_rt = _has_rt(doc_raw)
                needs_rt = _needs_rt(doc_raw)
                should_force_all_send = force_all and mm_client is not None
                should_force_send = force and mm_client is not None and had_rt

                if audit_existing:
                    if had_rt:
                        page_candidates.append((str(key), doc_raw, had_rt, needs_rt))
                    continue

                if not needs_rt and not should_force_send and not should_force_all_send:
                    stats["already_has_rt"] += 1
                    continue

                page_candidates.append((str(key), doc_raw, had_rt, needs_rt))

            if limit is not None:
                remaining = limit - processed
                if remaining <= 0:
                    break
                page_candidates = page_candidates[:remaining]

            stats["candidates"] += len(page_candidates)

            for offset in range(0, len(page_candidates), concurrency):
                chunk = page_candidates[offset : offset + concurrency]

                for key, doc, had_rt, needs_rt in chunk:
                    enriched = False
                    should_send_to_mm = False
                    clear_rt = False

                    if audit_existing and had_rt:
                        existing_record = _existing_rt_attachment_record(doc, store)
                        existing_valid = (
                            existing_record is not None
                            and rt_record_matches_doc(doc, existing_record)
                        )
                        if dry_run:
                            _print_audit_trace(doc, existing_record, matched=existing_valid)

                        if existing_valid:
                            stats["audit_valid"] += 1
                            if force_all and mm_client is not None:
                                should_send_to_mm = True
                        else:
                            stats["audit_invalid"] += 1
                            _clear_rt_metadata(doc)
                            local_hit = enrich_from_local_title_year(doc, store=store)
                            if local_hit:
                                enriched = True
                                stats["enriched_local"] += 1
                                stats["audit_repaired"] += 1
                            elif algolia_fallback:
                                algolia_hit = await enrich_from_algolia(doc, store=store)
                                if algolia_hit:
                                    enriched = True
                                    stats["enriched_algolia"] += 1
                                    stats["audit_repaired"] += 1

                            if dry_run and enriched:
                                repaired_record = None
                                repaired_vanity = _rt_vanity(doc)
                                if repaired_vanity is not None:
                                    repaired_record = store.lookup_by_vanity(repaired_vanity)
                                _print_audit_trace(doc, repaired_record, matched=True)

                            if not enriched:
                                clear_rt = True
                                stats["audit_cleared"] += 1
                                stats["no_match"] += 1

                    elif needs_rt:
                        local_hit = enrich_from_local(doc, store=store)
                        if local_hit:
                            enriched = True
                            stats["enriched_local"] += 1
                        elif algolia_fallback:
                            algolia_hit = await enrich_from_algolia(doc, store=store)
                            if algolia_hit:
                                enriched = True
                                stats["enriched_algolia"] += 1

                    if enriched or clear_rt:
                        if dry_run:
                            stats["updated"] += 1
                        else:
                            await _write_rt_patch(redis, key, doc, clear=clear_rt)
                            stats["updated"] += 1
                        should_send_to_mm = mm_client is not None
                    elif force_all and mm_client is not None or force and mm_client is not None and had_rt:
                        should_send_to_mm = True
                    elif needs_rt:
                        stats["no_match"] += 1

                    if should_send_to_mm and mm_client is not None:
                        mm_buffer.append(doc)
                        if len(mm_buffer) >= MM_BATCH_SIZE:
                            await _flush_mm_buffer(mm_client, mm_buffer, stats, dry_run)
                            mm_buffer.clear()

                processed += len(chunk)
                if (
                    processed % max(concurrency, PROCESS_LOG_INTERVAL) < len(chunk)
                    or (limit is not None and processed >= limit)
                ):
                    logger.info(
                        "Progress: handled=%d candidates=%d local=%d algolia=%d updated=%d no_match=%d",
                        processed,
                        stats["candidates"],
                        stats["enriched_local"],
                        stats["enriched_algolia"],
                        stats["updated"],
                        stats["no_match"],
                    )

            if limit is not None and processed >= limit:
                break

            if cursor == 0:
                break

        logger.info("Candidate scan complete: candidates=%d", stats["candidates"])

        if stats["candidates"] == 0:
            logger.info("No candidates found, nothing to backfill.")
            await redis.aclose()
            return stats

        if mm_client is not None and mm_buffer:
            await _flush_mm_buffer(mm_client, mm_buffer, stats, dry_run)
            mm_buffer.clear()

        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        if pending:
            logger.info("Waiting for %d MM batches to complete...", len(pending))
            await asyncio.gather(*pending, return_exceptions=True)

        logger.info(
            "RT backfill complete: scanned=%d candidates=%d updated=%d no_match=%d",
            stats["scanned"],
            stats["candidates"],
            stats["updated"],
            stats["no_match"],
        )
        if push_to_mm:
            logger.info(
                "MM propagation: submitted=%d queued=%d skipped=%d errors=%d",
                stats["mm_submitted"],
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
        "audit_valid",
        "audit_invalid",
        "audit_repaired",
        "audit_cleared",
        "enriched_local",
        "enriched_algolia",
        "no_match",
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
        help="With --push-to-mm, also send docs that already have RT data",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="With --push-to-mm, send every filtered doc regardless of RT updates",
    )
    parser.add_argument(
        "--push-to-mm",
        action="store_true",
        help="Push enriched docs to Media Manager (metadata_only mode)",
    )
    parser.add_argument(
        "--audit-existing",
        action="store_true",
        help="Validate existing RT metadata, repair strict matches, and clear invalid unmatched RT data",
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
        force_all=args.force_all,
        audit_existing=args.audit_existing,
        push_to_mm=args.push_to_mm,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
