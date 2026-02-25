#!/usr/bin/env python3
"""
Enrich Redis Media Documents with Genres and Cast Data.

This script enriches existing Redis documents in-place by:
1. Building a lookup from local JSON files
2. Iterating through all Redis media documents
3. For each doc: lookup enrichment data from JSON, fallback to TMDB API if not found
4. Update Redis document with genre_ids, genres, cast_ids, cast_names, cast

Usage:
    # Dry run (preview changes without writing)
    python scripts/enrich_redis_metadata.py --dry-run

    # Process only first 100 documents (for testing)
    python scripts/enrich_redis_metadata.py --limit 100 --dry-run

    # Run for real (updates Redis)
    python scripts/enrich_redis_metadata.py

    # Skip TMDB API fallback (only use JSON lookup)
    python scripts/enrich_redis_metadata.py --skip-api
"""

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Load environment BEFORE any other imports to ensure TMDB_READ_TOKEN is available
from dotenv import load_dotenv

load_dotenv("config/local.env")

from typing import Any  # noqa: E402

from redis.asyncio import Redis  # noqa: E402

from src.utils.genre_mapping import get_genre_mapping_with_fallback, resolve_genre_ids  # noqa: E402
from src.utils.get_logger import get_logger  # noqa: E402
from src.utils.json_lookup import (  # noqa: E402
    build_lookup_from_json,
    get_enrichment_data,
    get_lookup_stats,
)

logger = get_logger(__name__)


async def fetch_from_tmdb(
    mc_type: str,
    tmdb_id: int,
) -> dict[str, Any] | None:
    """
    Fetch enrichment data from TMDB API.

    Args:
        mc_type: "movie" or "tv"
        tmdb_id: TMDB ID

    Returns:
        Dict with genre_ids and cast, or None on error
    """
    from api.tmdb.core import TMDBService
    from contracts.models import MCType

    try:
        service = TMDBService()

        media_type = MCType.MOVIE if mc_type == "movie" else MCType.TV_SERIES

        # Get detailed info including cast
        detailed = await service.get_media_details(
            tmdb_id=tmdb_id,
            media_type=media_type,
            include_cast=True,
            cast_limit=10,
        )

        if not detailed:
            return None

        # Extract genre_ids
        genre_ids = getattr(detailed, "genre_ids", []) or []

        # If genre_ids is empty but genres list exists, extract IDs
        if not genre_ids:
            genres_list = getattr(detailed, "genres", [])
            if genres_list:
                # genres might be list of dicts with 'id' key or list of strings
                for g in genres_list:
                    if isinstance(g, dict) and "id" in g:
                        genre_ids.append(g["id"])

        # Extract cast
        cast_data = []
        main_cast = getattr(detailed, "main_cast", []) or []

        for actor in main_cast[:10]:
            if isinstance(actor, dict) and actor.get("name"):
                cast_data.append({
                    "id": actor.get("id"),
                    "name": actor.get("name"),
                    "profile_image_url": actor.get("profile_image_url") or actor.get("image_url"),
                })

        return {
            "genre_ids": genre_ids,
            "cast": cast_data,
        }

    except Exception as e:
        logger.error(f"TMDB API error for {mc_type}/{tmdb_id}: {e}")
        return None


async def enrich_redis_documents(
    redis: Redis,
    lookup: dict[str, dict[str, Any]],
    genre_mapping: dict[int, str],
    dry_run: bool = False,
    limit: int | None = None,
    skip_api: bool = False,
) -> dict[str, int]:
    """
    Enrich all Redis media documents with genres and cast data.

    Args:
        redis: Redis async client
        lookup: JSON lookup dict from build_lookup_from_json()
        genre_mapping: Genre ID to name mapping
        dry_run: If True, don't write changes
        limit: Max documents to process (None = all)
        skip_api: If True, skip TMDB API fallback

    Returns:
        Stats dict with counts
    """
    stats = {
        "total_scanned": 0,
        "enriched_from_json": 0,
        "enriched_from_api": 0,
        "already_enriched": 0,
        "not_found": 0,
        "errors": 0,
    }

    # Scan all media documents
    cursor = 0
    batch_size = 100
    documents_processed = 0

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Starting enrichment...")
    print(f"  Lookup size: {len(lookup)} items")
    print(f"  Genre mapping: {len(genre_mapping)} genres")
    print(f"  Limit: {limit or 'None'}")
    print(f"  Skip API: {skip_api}")
    print()

    while True:
        # Scan for media keys
        cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=batch_size)

        for key in keys:
            if limit and documents_processed >= limit:
                break

            stats["total_scanned"] += 1
            documents_processed += 1

            try:
                # Get current document
                doc = await redis.json().get(key)  # type: ignore[misc]

                if not doc:
                    stats["errors"] += 1
                    continue

                # Check if already enriched
                if doc.get("genres") and doc.get("cast_ids"):
                    stats["already_enriched"] += 1
                    continue

                # Extract identifiers
                mc_type = doc.get("mc_type", "")
                source_id = doc.get("source_id", "")

                if not mc_type or not source_id:
                    stats["errors"] += 1
                    continue

                # Try JSON lookup first
                enrichment = get_enrichment_data(mc_type, source_id, lookup)
                source = "json"

                # Fallback to TMDB API if not found and not skipping
                if not enrichment and not skip_api:
                    try:
                        tmdb_id = int(source_id)
                        enrichment = await fetch_from_tmdb(mc_type, tmdb_id)
                        source = "api"
                    except (ValueError, TypeError):
                        pass

                if not enrichment:
                    stats["not_found"] += 1
                    if documents_processed <= 10:
                        logger.debug(f"No enrichment data for {key}")
                    continue

                # Build enrichment fields
                genre_ids = enrichment.get("genre_ids", [])
                genres = resolve_genre_ids(genre_ids, genre_mapping)
                cast = enrichment.get("cast", [])

                # Extract cast_ids and cast_names from cast
                cast_ids = [c.get("id") for c in cast if c.get("id")]
                cast_names = [c.get("name") for c in cast if c.get("name")]

                # Update document
                updates = {
                    "genre_ids": genre_ids,
                    "genres": genres,
                    "cast_ids": cast_ids,
                    "cast_names": cast_names,
                    "cast": cast,
                }

                if not dry_run:
                    # Update each field individually
                    for field, value in updates.items():
                        await redis.json().set(key, f"$.{field}", value)  # type: ignore[misc]
                    now_ts = int(datetime.now(UTC).timestamp())
                    await redis.json().set(key, "$.modified_at", now_ts)  # type: ignore[misc]

                if source == "json":
                    stats["enriched_from_json"] += 1
                else:
                    stats["enriched_from_api"] += 1

                # Progress logging
                if documents_processed % 1000 == 0:
                    print(f"  Processed {documents_processed} documents...")

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error processing {key}: {e}")
                continue

        # Check if we've hit the limit or finished scanning
        if limit and documents_processed >= limit:
            break
        if cursor == 0:
            break

    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Enrich Redis media documents with genres and cast data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Redis",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max documents to process (default: all)",
    )
    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip TMDB API fallback (only use JSON lookup)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/us",
        help="Path to data directory (default: data/us)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Redis Media Metadata Enrichment")
    print("=" * 60)

    # Build JSON lookup
    print("\n📂 Building JSON lookup...")
    data_dir = Path(args.data_dir)
    lookup = build_lookup_from_json(data_dir)
    lookup_stats = get_lookup_stats(lookup)
    print(f"  Movies: {lookup_stats['movie']}")
    print(f"  TV Shows: {lookup_stats['tv']}")
    print(f"  Total: {lookup_stats['total']}")

    # Get genre mapping
    print("\n🎭 Fetching genre mapping...")
    genre_mapping = await get_genre_mapping_with_fallback()
    print(f"  Loaded {len(genre_mapping)} genres")

    # Connect to Redis
    print("\n🔌 Connecting to Redis...")
    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await redis.ping()  # type: ignore[misc]
        print("  Connected!")

        # Run enrichment
        stats = await enrich_redis_documents(
            redis=redis,
            lookup=lookup,
            genre_mapping=genre_mapping,
            dry_run=args.dry_run,
            limit=args.limit,
            skip_api=args.skip_api,
        )

        # Print results
        print("\n" + "=" * 60)
        print("📊 Results")
        print("=" * 60)
        print(f"  Total scanned: {stats['total_scanned']}")
        print(f"  Enriched from JSON: {stats['enriched_from_json']}")
        print(f"  Enriched from API: {stats['enriched_from_api']}")
        print(f"  Already enriched: {stats['already_enriched']}")
        print(f"  Not found: {stats['not_found']}")
        print(f"  Errors: {stats['errors']}")

        if args.dry_run:
            print("\n⚠️  DRY RUN - No changes were made to Redis")
        else:
            print("\n✅ Enrichment complete!")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
