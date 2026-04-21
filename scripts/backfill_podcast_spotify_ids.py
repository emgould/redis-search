#!/usr/bin/env python3
"""Backfill ``spotify_url`` and ``spotify_id`` on existing ``podcast:*`` Redis docs.

Scans ``podcast:*`` keys, finds documents missing ``spotify_id``, looks up the
matching Spotify show by title, and writes both ``spotify_url`` and
``spotify_id`` back to the JSON document.

Requires the ``spotify_id`` TAG field on ``idx:podcasts`` (run
``scripts/alter_podcast_index_add_spotify_id.py`` first).

Examples:
    # Dry run over the full corpus
    ENV_FILE=config/local.env python scripts/backfill_podcast_spotify_ids.py --dry-run

    # Real run, only after-shows feeds, capped at 500 docs
    ENV_FILE=config/local.env python scripts/backfill_podcast_spotify_ids.py \
        --after-shows-only --limit 500

    # Re-enrich docs that already have a spotify_id
    ENV_FILE=config/local.env python scripts/backfill_podcast_spotify_ids.py --refresh
"""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime

import _bootstrap
from dotenv import load_dotenv
from redis.asyncio import Redis

from src.etl.podcast_parent_resolver import should_resolve_parent_mc_ids
from src.etl.spotify_enrichment import fetch_spotify_ids_for_title

_ = _bootstrap

DEFAULT_SCAN_BATCH = 500
DEFAULT_CONCURRENCY = 10


@dataclass
class BackfillStats:
    scanned: int = 0
    candidates: int = 0
    enriched: int = 0
    skipped: int = 0
    no_match: int = 0
    errors: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


def _is_after_shows(payload: dict[str, object]) -> bool:
    categories = payload.get("categories")
    site_url = payload.get("site")
    normalized_site_url = site_url if isinstance(site_url, str) and site_url else None
    if isinstance(categories, list):
        return should_resolve_parent_mc_ids(
            [c for c in categories if isinstance(c, str)],
            normalized_site_url,
        )
    if isinstance(categories, dict):
        return should_resolve_parent_mc_ids(
            [str(value).strip() for value in categories.values()],
            normalized_site_url,
        )
    return False


def _doc_title(payload: dict[str, object]) -> str | None:
    for field in ("title", "search_title"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


async def _enrich_doc(
    redis: Redis,
    key: str,
    payload: dict[str, object],
    semaphore: asyncio.Semaphore,
    dry_run: bool,
    stats: BackfillStats,
) -> None:
    title = _doc_title(payload)
    if not title:
        stats.skipped += 1
        return

    async with semaphore:
        try:
            spotify_url, spotify_id = await fetch_spotify_ids_for_title(title)
        except Exception as exc:
            stats.errors += 1
            print(f"  error enriching {key} ({title!r}): {exc}")
            return

    if not spotify_id and not spotify_url:
        stats.no_match += 1
        return

    if dry_run:
        stats.enriched += 1
        if stats.enriched <= 5:
            print(f"  [DRY RUN] {key}: {title!r} -> {spotify_id} ({spotify_url})")
        return

    pipe = redis.pipeline()
    pipe.json().set(key, "$.spotify_url", spotify_url)
    pipe.json().set(key, "$.spotify_id", spotify_id)
    try:
        await pipe.execute()
    except Exception as exc:
        stats.errors += 1
        print(f"  write failure for {key}: {exc}")
        return
    stats.enriched += 1


async def _run(args: argparse.Namespace) -> int:
    env_file = os.getenv("ENV_FILE", "config/local.env")
    load_dotenv(env_file)

    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    stats = BackfillStats(started_at=datetime.now())
    semaphore = asyncio.Semaphore(args.concurrency)

    print("=" * 60)
    print("Spotify URL/ID Backfill (podcast:*)")
    print("=" * 60)
    print(f"  Redis: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6380')}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  After-shows only: {args.after_shows_only}")
    print(f"  Refresh existing: {args.refresh}")
    print(f"  Limit: {args.limit if args.limit is not None else 'unlimited'}")
    print(f"  Concurrency: {args.concurrency}")
    print()

    try:
        cursor = 0
        reached_limit = False
        while not reached_limit:
            cursor, keys = await redis.scan(
                cursor=cursor, match="podcast:*", count=args.scan_batch
            )
            if keys:
                stats.scanned += len(keys)
                payloads: list[object] = await redis.json().mget(keys, "$")  # type: ignore[misc]
                pending: list[asyncio.Task[None]] = []
                for key, raw in zip(keys, payloads, strict=True):
                    payload = raw[0] if isinstance(raw, list) and raw else raw
                    if not isinstance(payload, dict):
                        continue
                    if args.after_shows_only and not _is_after_shows(payload):
                        continue
                    if not args.refresh and payload.get("spotify_id"):
                        continue
                    stats.candidates += 1
                    pending.append(
                        asyncio.create_task(
                            _enrich_doc(
                                redis,
                                key,
                                payload,
                                semaphore,
                                args.dry_run,
                                stats,
                            )
                        )
                    )
                    if args.limit is not None and stats.candidates >= args.limit:
                        reached_limit = True
                        break
                if pending:
                    await asyncio.gather(*pending)
                    print(
                        f"  Progress: scanned={stats.scanned:,} "
                        f"candidates={stats.candidates:,} "
                        f"enriched={stats.enriched:,} "
                        f"no_match={stats.no_match:,} "
                        f"errors={stats.errors:,}"
                    )
            if cursor == 0:
                break
    finally:
        await redis.aclose()

    stats.completed_at = datetime.now()

    print()
    print("=" * 60)
    print("Backfill Summary")
    print("=" * 60)
    print(f"  Scanned: {stats.scanned:,}")
    print(f"  Candidates: {stats.candidates:,}")
    print(f"  Enriched: {stats.enriched:,}")
    print(f"  No Spotify match: {stats.no_match:,}")
    print(f"  Skipped (no title): {stats.skipped:,}")
    print(f"  Errors: {stats.errors:,}")
    if stats.duration_seconds:
        print(f"  Duration: {stats.duration_seconds:.1f}s")

    return 0 if stats.errors == 0 else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill spotify_url and spotify_id on podcast:* Redis docs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to Redis; just count candidates and print first matches.",
    )
    parser.add_argument(
        "--after-shows-only",
        action="store_true",
        help="Only enrich after-shows feeds (companion podcasts).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-enrich documents that already have a spotify_id.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of candidate documents to enrich.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Concurrent Spotify lookups (default: {DEFAULT_CONCURRENCY}).",
    )
    parser.add_argument(
        "--scan-batch",
        type=int,
        default=DEFAULT_SCAN_BATCH,
        help=f"Redis SCAN batch size (default: {DEFAULT_SCAN_BATCH}).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
