"""Push Redis documents with FlixPatrol metadata to Media Manager.

This command does not fetch FlixPatrol data or modify Redis. It submits the
already-stamped media documents using Media Manager's metadata-only path.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, cast

from redis.asyncio import Redis

from adapters.media_manager_client import MediaManagerClient
from etl.media_manager_filter import passes_media_manager_filter

SCAN_COUNT = 1000
MGET_BATCH_SIZE = 100
MM_BATCH_SIZE = 100


@dataclass
class PushStats:
    """Counts produced by a FlixPatrol Media Manager push."""

    scanned: int = 0
    with_flixpatrol: int = 0
    filtered: int = 0
    submitted: int = 0
    queued: int = 0
    skipped: int = 0
    errors: int = 0


def _unwrap_json_doc(raw: object) -> dict[str, Any] | None:
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return cast(dict[str, Any], raw[0])
    if isinstance(raw, dict):
        return cast(dict[str, Any], raw)
    return None


async def _flush(
    client: MediaManagerClient,
    docs: list[dict[str, Any]],
    stats: PushStats,
    dry_run: bool,
) -> None:
    for offset in range(0, len(docs), MM_BATCH_SIZE):
        batch = docs[offset : offset + MM_BATCH_SIZE]
        stats.submitted += len(batch)
        if dry_run:
            continue
        try:
            response = await client.insert_docs(batch, metadata_only=True)
        except Exception as exc:
            stats.errors += len(batch)
            print(f"Media Manager batch failed: {exc}")
            continue
        stats.queued += response["queued"]
        stats.skipped += response["skipped"]
        errors = response.get("errors", [])
        stats.errors += len(errors)
        for error in errors:
            print(f"Media Manager document error: {error}")


async def push(
    redis: Redis,
    client: MediaManagerClient | None,
    *,
    dry_run: bool,
    limit: int | None,
) -> PushStats:
    """Scan and push all eligible Redis media documents."""
    stats = PushStats()
    pending: list[dict[str, Any]] = []
    batch_keys: list[str] = []

    async def consume(keys: list[str]) -> bool:
        if not keys:
            return False
        raw_docs = await cast(
            Awaitable[list[object]], redis.json().mget(keys, "$")
        )
        for raw_doc in raw_docs:
            stats.scanned += 1
            doc = _unwrap_json_doc(raw_doc)
            if doc is None or not isinstance(doc.get("flixpatrol"), dict):
                continue
            stats.with_flixpatrol += 1
            passed, _reason = passes_media_manager_filter(doc)
            if not passed:
                stats.filtered += 1
                continue
            pending.append(doc)
            if limit is not None and len(pending) >= limit:
                return True
        return False

    async for raw_key in redis.scan_iter(match="media:*", count=SCAN_COUNT):
        batch_keys.append(str(raw_key))
        if len(batch_keys) < MGET_BATCH_SIZE:
            continue
        if await consume(batch_keys):
            break
        batch_keys = []
    else:
        await consume(batch_keys)

    if dry_run:
        stats.submitted = len(pending)
    elif client is not None:
        await _flush(client, pending, stats, dry_run=False)
    return stats


async def finalize(client: MediaManagerClient) -> None:
    """Wait for queued documents, then publish the metadata update."""
    await client.poll_until_drained()
    response = await client.finalize_publish()
    print(
        f"Media Manager finalized: status={response['status']} "
        f"metadata_only_updated={response['metadata_only_updated']}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Push FlixPatrol-enriched Redis media docs to Media Manager"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--no-finalize",
        action="store_true",
        help="Queue metadata updates without waiting and finalizing the publish",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be greater than zero")

    if not args.dry_run and not os.getenv("MEDIA_MANAGER_API_URL"):
        raise SystemExit("MEDIA_MANAGER_API_URL is required unless --dry-run is used")

    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    client: MediaManagerClient | None = None
    try:
        if not args.dry_run:
            client = MediaManagerClient()
            await client.health_check()
        stats = await push(
            redis,
            client,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        if client is not None and stats.submitted > 0 and not args.no_finalize:
            await finalize(client)
        print(
            f"FlixPatrol push complete: scanned={stats.scanned} "
            f"with_flixpatrol={stats.with_flixpatrol} filtered={stats.filtered} "
            f"submitted={stats.submitted} queued={stats.queued} "
            f"skipped={stats.skipped} errors={stats.errors}"
        )
    finally:
        if client is not None:
            await client.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
