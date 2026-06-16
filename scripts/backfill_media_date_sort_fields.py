#!/usr/bin/env python3
"""Backfill derived numeric media date fields for RediSearch filtering.

This script does not reload media from TMDB. It scans existing ``media:*`` Redis
JSON documents and derives numeric YYYYMMDD helper fields from stored date
strings:

- ``release_yyyymmdd`` from ``release_date``
- ``first_air_yyyymmdd`` from ``first_air_date``
- ``last_air_yyyymmdd`` from ``last_air_date``

By default this is a dry run. Pass ``--apply`` to write changes.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from core.normalize import date_string_to_yyyymmdd  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

DATE_FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("release_date", "release_yyyymmdd"),
    ("first_air_date", "first_air_yyyymmdd"),
    ("last_air_date", "last_air_yyyymmdd"),
)


@dataclass
class BackfillStats:
    scanned: int = 0
    candidates: int = 0
    would_update: int = 0
    updated: int = 0
    fields_set: int = 0
    fields_cleared: int = 0
    skipped_type: int = 0


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _changed_fields(doc: dict[str, Any]) -> dict[str, int | None]:
    changes: dict[str, int | None] = {}
    for source_field, derived_field in DATE_FIELD_MAP:
        source_value = doc.get(source_field)
        derived_value = (
            date_string_to_yyyymmdd(source_value) if isinstance(source_value, str) else None
        )
        existing_value = doc.get(derived_field)
        if existing_value != derived_value:
            changes[derived_field] = derived_value
    return changes


async def backfill(
    scan_count: int,
    limit: int | None,
    apply: bool,
    mc_type_filter: str | None,
) -> BackfillStats:
    stats = BackfillStats()
    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info(
            "Media date-sort backfill start: scan_count=%d limit=%s apply=%s mc_type=%s",
            scan_count,
            limit,
            apply,
            mc_type_filter,
        )

        cursor = 0
        processed_candidates = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)
            if not keys:
                if cursor == 0:
                    break
                continue

            stats.scanned += len(keys)
            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key)
            docs_raw: list[object] = await pipe.execute()

            write_pipe = redis.pipeline()
            writes_in_batch = 0
            for key, raw_doc in zip(keys, docs_raw, strict=True):
                if not isinstance(raw_doc, dict):
                    continue

                mc_type = raw_doc.get("mc_type")
                if mc_type_filter and mc_type != mc_type_filter:
                    stats.skipped_type += 1
                    continue

                changes = _changed_fields(raw_doc)
                if not changes:
                    continue

                if limit is not None and processed_candidates >= limit:
                    break

                stats.candidates += 1
                processed_candidates += 1
                if apply:
                    for field_name, value in changes.items():
                        write_pipe.json().set(str(key), f"$.{field_name}", value)
                        if value is None:
                            stats.fields_cleared += 1
                        else:
                            stats.fields_set += 1
                    writes_in_batch += 1
                else:
                    stats.would_update += 1

            if apply and writes_in_batch:
                await write_pipe.execute()
                stats.updated += writes_in_batch

            if limit is not None and processed_candidates >= limit:
                break
            if cursor == 0:
                break

        logger.info(
            "Media date-sort backfill complete: scanned=%d candidates=%d updated=%d would_update=%d",
            stats.scanned,
            stats.candidates,
            stats.updated,
            stats.would_update,
        )
        return stats
    finally:
        await redis.aclose()


def _print_stats(stats: BackfillStats, apply: bool) -> None:
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\nMedia Date Sort Field Backfill ({mode})")
    print("=" * 48)
    print(f"  scanned:        {stats.scanned:,}")
    print(f"  candidates:     {stats.candidates:,}")
    print(f"  would_update:   {stats.would_update:,}")
    print(f"  updated:        {stats.updated:,}")
    print(f"  fields_set:     {stats.fields_set:,}")
    print(f"  fields_cleared: {stats.fields_cleared:,}")
    print(f"  skipped_type:   {stats.skipped_type:,}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill derived numeric date fields on Redis media documents"
    )
    parser.add_argument(
        "--scan-count",
        type=int,
        default=500,
        help="Redis SCAN page size (default: 500)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max changed docs to process")
    parser.add_argument(
        "--mc-type",
        choices=["movie", "tv"],
        default=None,
        help="Filter to a single media type",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write derived fields. Omit for dry-run mode.",
    )
    args = parser.parse_args()

    stats = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        apply=args.apply,
        mc_type_filter=args.mc_type,
    )
    _print_stats(stats, apply=args.apply)


if __name__ == "__main__":
    asyncio.run(main())
