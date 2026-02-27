#!/usr/bin/env python3
"""
Send a specific media document to Media Manager by TMDB ID.

Reads the document from Redis and POSTs it to /insert-docs.
Bypasses the intake filter — sends the document as-is.

Usage:
    # Send a movie
    python scripts/send_to_media_manager.py tmdb_238

    # Send a TV show
    python scripts/send_to_media_manager.py tmdb_1396

    # Multiple IDs
    python scripts/send_to_media_manager.py tmdb_238 tmdb_1396

    # Dry run (validates doc against Media Manager but skips FAISS upsert)
    python scripts/send_to_media_manager.py tmdb_238 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from adapters.media_manager_client import MediaManagerClient  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _redis_key(mc_id: str) -> str:
    """Ensure the key has the media: prefix."""
    return mc_id if mc_id.startswith("media:") else f"media:{mc_id}"


async def send_docs(mc_ids: list[str], dry_run: bool) -> None:
    """Fetch documents from Redis and send to Media Manager."""
    redis = _connect_redis()
    mm_client = MediaManagerClient()

    try:
        await redis.ping()  # type: ignore[misc]
        await mm_client.health_check()

        documents: list[dict[str, Any]] = []

        for mc_id in mc_ids:
            key = _redis_key(mc_id)
            doc = await redis.json().get(key)  # type: ignore[union-attr]

            if not isinstance(doc, dict):
                print(f"  NOT FOUND: {key}")
                continue

            title = doc.get("title") or doc.get("name") or "?"
            mc_type = doc.get("mc_type") or "?"
            print(f"  FOUND: {key} — {title} ({mc_type})")
            documents.append(doc)

        if not documents:
            print("\nNo documents to send.")
            return

        resp = await mm_client.insert_docs(documents, dry_run=dry_run)

        prefix = "[DRY RUN] " if dry_run else ""
        print(f"\n{prefix}Response from /insert-docs:")
        print(f"  queued: {resp['queued']}")
        print(f"  skipped: {resp['skipped']}")
        print(f"  queue_depth: {resp['queue_depth']}")
        if resp["errors"]:
            print(f"  errors ({len(resp['errors'])}):")
            for err in resp["errors"]:
                print(f"    - {err}")

    finally:
        await mm_client.close()
        await redis.aclose()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send specific media documents to Media Manager by ID"
    )
    parser.add_argument(
        "ids",
        nargs="+",
        help="One or more mc_ids (e.g. tmdb_238 tmdb_1396)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Send with dry_run=true (validates and embeds but skips FAISS upsert)",
    )

    args = parser.parse_args()
    await send_docs(args.ids, dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
