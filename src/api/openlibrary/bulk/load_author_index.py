"""
Load MCAuthorItems into Redis author index.

Reads the JSONL output from bulk_load_openlibrary.py and loads into Redis.
Uses the idx:author index with author: key prefix.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from utils.get_logger import get_logger

logger = get_logger(__name__)

INDEX_NAME = "idx:author"
KEY_PREFIX = "author:"
BATCH_SIZE = 500


def mc_author_to_redis_doc(author: dict[str, Any]) -> dict[str, Any]:
    """
    Convert MCAuthorItem dict to Redis document format.

    The document is stored as JSON and indexed by Redis Search.
    """
    # Extract wikidata metadata for convenience
    wd_meta = author.pop("_wikidata_metadata", {})

    # Build search-optimized document
    doc = {
        # Primary identifiers
        "id": author.get("mc_id") or author.get("key", ""),
        "key": author.get("key", ""),
        "source_id": author.get("source_id", ""),
        # Search fields
        "search_title": author.get("name", ""),
        "name": author.get("name", ""),
        "bio": author.get("bio"),
        # Type tags
        "mc_type": author.get("mc_type", "person"),
        "mc_subtype": author.get("mc_subtype", "author"),
        "source": author.get("source", "openlibrary"),
        # Dates
        "birth_date": author.get("birth_date"),
        "death_date": author.get("death_date"),
        # External IDs
        "remote_ids": author.get("remote_ids", {}),
        "openlibrary_key": author.get("openlibrary_key"),
        "openlibrary_url": author.get("openlibrary_url"),
        # Numeric fields
        "work_count": author.get("work_count", 0),
        "quality_score": wd_meta.get("quality_score", 0),
        # Wikidata enrichment
        "wikidata_id": wd_meta.get("wikidata_id"),
        "wikidata_name": wd_meta.get("wikidata_name"),
        "wikidata_birth_year": wd_meta.get("wikidata_birth_year"),
        # Images - use author_image from extraction, map to 'image' for frontend
        # photo_urls contains both openlibrary and wikidata URLs for fallback
        "image": author.get("author_image"),
        "images": author.get("images", []),
        "photo_urls": author.get("photo_urls", {}),
        # External links
        "author_links": author.get("author_links", []),
    }

    return doc


async def create_author_index(redis: Redis) -> bool:
    """
    Create the author search index if it doesn't exist.

    Returns:
        True if created or already exists
    """
    schema = (
        # Primary search field (name) with high weight
        TextField("$.search_title", as_name="search_title", weight=5.0),
        TextField("$.name", as_name="name", weight=4.0),
        # Bio - searchable but lower weight
        TextField("$.bio", as_name="bio", weight=1.0),
        # Type filters
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.mc_subtype", as_name="mc_subtype"),
        TagField("$.source", as_name="source"),
        # External IDs as tags (exact match)
        TagField("$.wikidata_id", as_name="wikidata_id"),
        TagField("$.openlibrary_key", as_name="openlibrary_key"),
        # Sortable numeric fields
        NumericField("$.work_count", as_name="work_count", sortable=True),
        NumericField("$.quality_score", as_name="quality_score", sortable=True),
        NumericField("$.wikidata_birth_year", as_name="birth_year", sortable=True),
    )

    definition = IndexDefinition(prefix=[KEY_PREFIX], index_type=IndexType.JSON)

    try:
        await redis.ft(INDEX_NAME).create_index(schema, definition=definition)
        logger.info(f"Created index '{INDEX_NAME}'")
        return True
    except Exception as e:
        if "Index already exists" in str(e):
            logger.info(f"Index '{INDEX_NAME}' already exists")
            return True
        else:
            logger.error(f"Failed to create index: {e}")
            return False


async def drop_author_index(redis: Redis) -> bool:
    """Drop the author index."""
    try:
        await redis.ft(INDEX_NAME).dropindex(delete_documents=True)
        logger.info(f"Dropped index '{INDEX_NAME}' and deleted documents")
        return True
    except Exception as e:
        if "Unknown index" in str(e) or "Unknown Index" in str(e):
            logger.info(f"Index '{INDEX_NAME}' does not exist")
            return True
        else:
            logger.error(f"Failed to drop index: {e}")
            return False


async def load_authors_to_redis(
    input_file: str,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    recreate_index: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Load MCAuthorItems from JSONL file to Redis.

    Args:
        input_file: Path to JSONL file
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        recreate_index: Drop and recreate index
        limit: Limit number of authors to load
        dry_run: Don't actually write to Redis

    Returns:
        Stats dict
    """
    stats = {
        "total_read": 0,
        "loaded": 0,
        "errors": 0,
    }

    input_path = Path(input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_file}")
        return stats

    # Connect to Redis
    redis = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

    try:
        # Test connection
        await redis.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")

        # Handle index
        if recreate_index:
            logger.info("Recreating index...")
            await drop_author_index(redis)

        if not await create_author_index(redis):
            logger.error("Failed to create index")
            return stats

        if dry_run:
            logger.info("[DRY RUN] Would load authors but not writing to Redis")

        # Load authors
        start_time = time.time()
        last_update = start_time
        pipeline = redis.pipeline() if not dry_run else None
        batch_count = 0

        with open(input_file, encoding="utf8") as f:
            for line in f:
                if not line.strip():
                    continue

                stats["total_read"] += 1

                if limit and stats["total_read"] > limit:
                    break

                try:
                    author = json.loads(line)
                    redis_doc = mc_author_to_redis_doc(author)

                    # Redis key
                    key = f"{KEY_PREFIX}{redis_doc['id']}"

                    if dry_run:
                        stats["loaded"] += 1
                        if stats["loaded"] <= 3:
                            logger.info(f"  [DRY RUN] {redis_doc['name']} -> {key}")
                    else:
                        pipeline.json().set(key, "$", redis_doc)  # type: ignore
                        batch_count += 1
                        stats["loaded"] += 1

                        if batch_count >= BATCH_SIZE:
                            await pipeline.execute()  # type: ignore
                            pipeline = redis.pipeline()
                            batch_count = 0

                except Exception as e:
                    stats["errors"] += 1
                    if stats["errors"] <= 5:
                        logger.error(f"Error processing line: {e}")

                # Progress
                current_time = time.time()
                if current_time - last_update >= 0.5:
                    elapsed = current_time - start_time
                    rate = stats["loaded"] / elapsed if elapsed > 0 else 0
                    sys.stdout.write(
                        f"\r  Loaded: {stats['loaded']:,} | "
                        f"Errors: {stats['errors']} | "
                        f"{rate:,.0f}/sec"
                    )
                    sys.stdout.flush()
                    last_update = current_time

        # Execute remaining batch
        if not dry_run and batch_count > 0:
            await pipeline.execute()  # type: ignore

        sys.stdout.write("\n")

        elapsed = time.time() - start_time
        logger.info(f"Load complete in {elapsed:.1f}s")
        logger.info(f"  Total read: {stats['total_read']:,}")
        logger.info(f"  Loaded: {stats['loaded']:,}")
        logger.info(f"  Errors: {stats['errors']}")

        # Get index info
        try:
            info = await redis.ft(INDEX_NAME).info()
            logger.info(
                f"  Index '{INDEX_NAME}' now has {info.get('num_docs', 'unknown')} documents"
            )
        except Exception:
            pass

    finally:
        await redis.aclose()

    return stats


async def main(
    input_file: str = "data/openlibrary/mc_authors.jsonl",
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
    recreate_index: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Main entry point."""
    # Use environment variables as defaults
    host = redis_host or os.getenv("REDIS_HOST", "localhost")
    port = redis_port or int(os.getenv("REDIS_PORT", "6380"))
    password = redis_password or os.getenv("REDIS_PASSWORD")

    logger.info("Loading authors to Redis")
    logger.info(f"  Input: {input_file}")
    logger.info(f"  Redis: {host}:{port}")

    return await load_authors_to_redis(
        input_file=input_file,
        redis_host=host,
        redis_port=port,
        redis_password=password,
        recreate_index=recreate_index,
        limit=limit,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load MCAuthorItems to Redis index")
    parser.add_argument(
        "--input",
        default="data/openlibrary/mc_authors.jsonl",
        help="Input JSONL file",
    )
    parser.add_argument(
        "--redis-host",
        default=None,
        help="Redis host (default: REDIS_HOST env or localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis port (default: REDIS_PORT env or 6380)",
    )
    parser.add_argument(
        "--recreate-index",
        action="store_true",
        help="Drop and recreate the index",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of authors to load",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually write to Redis",
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            input_file=args.input,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            recreate_index=args.recreate_index,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    )
