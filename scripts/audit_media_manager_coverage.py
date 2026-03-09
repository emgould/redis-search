#!/usr/bin/env python3
"""
Find Redis media documents that are missing from Media Manager.

Scans every `media:*` key in Redis, reads each JSON doc, and checks whether
Media Manager returns metadata for `mc_id` via `POST /api/metadata`.
When missing, the full Redis document is written to a JSON list for
follow-up insert processing.

Run `scripts/push_missing_to_media_manager.py --input <output>` to enqueue
misses to Media Manager for insertion.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypedDict, cast

from redis.asyncio import Redis

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

PROGRESS_INTERVAL = 1000


class RedisMediaDoc(TypedDict, total=False):
    source: str
    source_id: str
    mc_id: str
    mc_type: str
    title: str
    name: str
    _source: str


class MissingMediaManagerItem(TypedDict):
    mc_id: str
    redis_key: str
    source: str
    source_id: str
    mc_type: str
    title: str
    name: str
    redis_doc: RedisMediaDoc


def _connect_redis() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _coerce_string(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int | float | bool):
        return str(value)
    return ""


def _coerce_media_type(document: Mapping[str, object]) -> str | None:
    raw_type = document.get("mc_type")
    if not isinstance(raw_type, str):
        return None
    media_type = raw_type.strip().lower()
    if media_type in {"movie", "tv"}:
        return media_type
    return None


async def _check_media_manager(
    media_manager: MediaManagerClient,
    mc_id: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    async with semaphore:
        metadata = await media_manager.get_metadata(mc_id)
    return metadata is not None


async def audit_media_manager_coverage(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    output_path: Path,
) -> tuple[list[MissingMediaManagerItem], dict[str, int]]:
    stats: dict[str, int] = {
        "keys_scanned": 0,
        "media_items_checked": 0,
        "movies_checked": 0,
        "tv_checked": 0,
        "found_in_media_manager": 0,
        "missing_in_media_manager": 0,
        "errors": 0,
        "invalid_docs": 0,
    }

    redis = _connect_redis()
    media_manager: MediaManagerClient | None = None
    missing_items: list[MissingMediaManagerItem] = []

    try:
        await redis.ping()
        logger.info("Redis connected")

        if not dry_run:
            media_manager = MediaManagerClient()
            await media_manager.health_check()
            logger.info("Media Manager health check passed")

        semaphore = asyncio.Semaphore(concurrency)
        cursor = 0
        last_log_count = 0

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)
            stop_after_batch = False

            if not keys:
                if cursor == 0:
                    break
                continue

            pipe = redis.pipeline()
            for key in keys:
                pipe.json().get(key)
            raw_docs: list[Any] = await pipe.execute()
            stats["keys_scanned"] += len(keys)

            candidates: list[tuple[str, str]] = []
            candidate_docs: dict[str, dict[str, object]] = {}

            for key, raw in zip(keys, raw_docs, strict=False):
                if not isinstance(raw, dict):
                    stats["invalid_docs"] += 1
                    logger.warning("Skipping unparseable doc for redis key %s", key)
                    continue

                document = cast(dict[str, object], raw)
                media_type = _coerce_media_type(document)
                if media_type is None:
                    continue

                stats["media_items_checked"] += 1
                if limit is not None and stats["media_items_checked"] > limit:
                    stats["media_items_checked"] -= 1
                    stop_after_batch = True
                    break

                mc_id = _coerce_string(document.get("mc_id"))
                if not mc_id:
                    mc_id = key.removeprefix("media:")
                if not mc_id:
                    logger.warning("Could not resolve mc_id for redis key %s", key)
                    continue

                media_doc: dict[str, object] = dict(document)
                media_doc["source"] = _coerce_string(document.get("source"))
                media_doc["source_id"] = _coerce_string(document.get("source_id"))
                media_doc["mc_id"] = mc_id
                media_doc["mc_type"] = media_type
                media_doc["title"] = _coerce_string(document.get("title"))
                media_doc["name"] = _coerce_string(document.get("name"))

                candidates.append((key, mc_id))
                candidate_docs[mc_id] = media_doc

                if media_type == "movie":
                    stats["movies_checked"] += 1
                elif media_type == "tv":
                    stats["tv_checked"] += 1

            if candidates:
                if dry_run:
                    logger.info(
                        "[DRY RUN] Skipping /api/metadata checks for %d media docs",
                        len(candidates),
                    )
                else:
                    if media_manager is None:
                        raise RuntimeError("Media manager client is unavailable")
                    checks = await asyncio.gather(
                        *[
                            _check_media_manager(media_manager, media_id, semaphore)
                            for _, media_id in candidates
                        ],
                        return_exceptions=True,
                    )
                    for (redis_key, media_id), in_media_manager in zip(
                        candidates,
                        checks,
                        strict=False,
                    ):
                        media_doc = candidate_docs[media_id]
                        if isinstance(in_media_manager, Exception):
                            stats["errors"] += 1
                            logger.warning(
                                "Metadata lookup failed for %s: %s",
                                media_id,
                                in_media_manager,
                            )
                            continue

                        if in_media_manager:
                            stats["found_in_media_manager"] += 1
                        else:
                            missing_items.append(
                                MissingMediaManagerItem(
                                    mc_id=media_id,
                                    redis_key=redis_key,
                                    source=_coerce_string(media_doc.get("source")),
                                    source_id=_coerce_string(media_doc.get("source_id")),
                                    mc_type=_coerce_string(media_doc.get("mc_type")),
                                    title=_coerce_string(media_doc.get("title")),
                                    name=_coerce_string(media_doc.get("name")),
                                    redis_doc=media_doc,
                                )
                            )
                            stats["missing_in_media_manager"] += 1

                if stats["keys_scanned"] - last_log_count >= PROGRESS_INTERVAL:
                    last_log_count = stats["keys_scanned"]
                    logger.info(
                        "Progress: keys=%d, checked=%d, found=%d, missing=%d",
                        stats["keys_scanned"],
                        stats["media_items_checked"],
                        stats["found_in_media_manager"],
                        stats["missing_in_media_manager"],
                    )

            if stop_after_batch:
                logger.info("Reached limit of %d media items", limit)
                break

            if cursor == 0:
                break
            if limit is not None and stats["media_items_checked"] >= limit:
                logger.info("Reached limit of %d media items", limit)
                break

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(missing_items, indent=2))
        logger.info("Wrote %d missing docs to %s", len(missing_items), output_path)

    finally:
        if media_manager is not None:
            await media_manager.close()
        await redis.aclose()

    return missing_items, stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Redis media documents against Media Manager and write missing items"
    )
    parser.add_argument(
        "--scan-count",
        type=int,
        default=500,
        help="Number of Redis keys to read per SCAN iteration (default 500)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of movie/tv docs to verify (none for all)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Concurrent /api/metadata requests (default 20)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan Redis and validate documents without calling /api/metadata",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/audit/missing_from_media_manager.json"),
        help="Path to write missing media-manager docs JSON file",
    )
    return parser.parse_args()


def _print_stats(stats: dict[str, int], elapsed: float, output_path: Path) -> None:
    print("\n" + "=" * 60)
    print("Media Manager coverage audit summary")
    print("=" * 60)
    for key, value in stats.items():
        print(f"  {key}: {value}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print(f"  output_path: {output_path}")
    print()


async def main() -> None:
    args = _parse_args()
    start = time.time()
    _, stats = await audit_media_manager_coverage(
        scan_count=args.scan_count,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        output_path=args.output,
    )
    _print_stats(stats, time.time() - start, args.output)
    print(
        "Next step: python scripts/push_missing_to_media_manager.py "
        f"--input {args.output} --batch-size 100"
    )


if __name__ == "__main__":
    asyncio.run(main())
