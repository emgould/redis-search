#!/usr/bin/env python3
"""CLI script to fetch TMDB media details, normalize, and optionally insert into Redis."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from api.tmdb.core import TMDBService
from contracts.models import MCType
from core.normalize import document_to_redis, normalize_document
from utils.genre_mapping import get_genre_mapping_with_fallback

# Load env after imports so E402 is satisfied; path setup is via PYTHONPATH in Makefile
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(str(_PROJECT_ROOT / "config" / "local.env"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch TMDB media details for a single title.",
        usage="%(prog)s <tmdb_id> <tv|movie> [--doc] [--add] [--region REGION] [--indent N]",
    )
    parser.add_argument("tmdb_id", type=int, help="TMDB numeric ID.")
    parser.add_argument("media_type", choices=["tv", "movie"], help="Media type.")
    parser.add_argument(
        "--doc",
        action="store_true",
        help="Normalize through the full ETL pipeline and output the index document.",
    )
    parser.add_argument(
        "--add",
        action="store_true",
        help="Normalize and insert into the Redis media index (implies --doc).",
    )
    parser.add_argument("--region", default="US", help="Region code (default: US).")
    parser.add_argument("--indent", type=int, default=2, help="JSON indent (default: 2).")
    return parser.parse_args()


def _to_serializable(payload: Any) -> dict[str, Any] | None:
    if payload is None:
        return None
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json")  # type: ignore[no-any-return]
    if hasattr(payload, "to_dict"):
        return payload.to_dict()  # type: ignore[no-any-return]
    if isinstance(payload, dict):
        return payload
    return {"value": str(payload)}


def _media_type_to_enum(media_type: str) -> MCType:
    return MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE


async def main() -> None:
    args = _parse_args()

    if not os.getenv("TMDB_READ_TOKEN"):
        print(
            "TMDB_READ_TOKEN is not set. Source config/local.env or export it.",
            file=sys.stderr,
        )
        sys.exit(1)

    service = TMDBService()
    mc_type = _media_type_to_enum(args.media_type)

    details = await service.get_media_details(
        tmdb_id=args.tmdb_id,
        media_type=mc_type,
        region=args.region.upper(),
        no_cache=True,
    )

    if details is None or details.error:
        err = details.error if details else "no result"
        print(f"Error fetching {args.media_type} {args.tmdb_id}: {err}", file=sys.stderr)
        sys.exit(2)

    if not args.doc and not args.add:
        payload = _to_serializable(details)
        print(json.dumps(payload, indent=args.indent, default=str, ensure_ascii=False))
        sys.exit(0)

    item_dict = _to_serializable(details)
    if item_dict is None:
        print("Failed to serialize media details.", file=sys.stderr)
        sys.exit(2)

    item_dict["_media_type"] = args.media_type

    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
    doc = normalize_document(item_dict, genre_mapping=genre_mapping)
    if doc is None:
        print("Normalizer returned None — item did not produce a document.", file=sys.stderr)
        sys.exit(2)

    now_ts = int(datetime.now(UTC).timestamp())
    doc.created_at = now_ts
    doc.modified_at = now_ts
    doc._source = "manual_add"

    redis_doc = document_to_redis(doc)

    if args.add:
        from redis.asyncio import Redis

        redis = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6380")),
            password=os.getenv("REDIS_PASSWORD") or None,
            decode_responses=True,
        )
        key = f"media:{redis_doc['mc_id']}"
        await redis.json().set(key, "$", redis_doc)  # type: ignore[misc]
        await redis.aclose()
        print(f"Inserted: {key}")
        title = redis_doc.get("title", "")
        print(f"  title:   {title}")
        print(f"  mc_id:   {redis_doc['mc_id']}")
        print(f"  mc_type: {redis_doc['mc_type']}")
    else:
        print(json.dumps(redis_doc, indent=args.indent, default=str, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
