"""
Load MCBookItems into Redis book index.

Reads the JSONL output from bulk_load_books.py and loads into Redis.
Uses the idx:book index with book: key prefix.
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

INDEX_NAME = "idx:book"
KEY_PREFIX = "book:"
BATCH_SIZE = 500


def mc_book_to_redis_doc(book: dict[str, Any]) -> dict[str, Any]:
    """
    Convert MCBookItem dict to Redis document format.

    The document is stored as JSON and indexed by Redis Search.
    """
    # Extract author OLIDs for indexing (enables O(1) author->books lookup)
    author_olids = book.pop("_matching_author_olids", [])

    # Build search-optimized document
    doc = {
        # Primary identifiers
        "id": book.get("mc_id") or book.get("key", ""),
        "key": book.get("key", ""),
        "source_id": book.get("source_id", ""),
        # Search fields
        "search_title": book.get("title", ""),
        "title": book.get("title", ""),
        "description": book.get("description"),
        # Author fields
        "author": book.get("author"),
        "author_name": book.get("author_name", []),
        "author_search": " ".join(book.get("author_name", [])),  # For text search
        "matching_author_olids": author_olids,  # TagField indexed for O(1) relational queries
        # Type tags
        "mc_type": book.get("mc_type", "book"),
        "source": book.get("source", "openlibrary"),
        # OpenLibrary identifiers
        "openlibrary_key": book.get("openlibrary_key"),
        "openlibrary_url": book.get("openlibrary_url"),
        # ISBNs
        "isbn": book.get("isbn", []),
        "primary_isbn13": book.get("primary_isbn13"),
        "primary_isbn10": book.get("primary_isbn10"),
        # Publication info
        "first_publish_year": book.get("first_publish_year"),
        "publisher": book.get("publisher"),
        # Content
        "first_sentence": book.get("first_sentence", []),
        # Subjects
        "subject": book.get("subject", []),
        "subjects": book.get("subjects", []),
        "subjects_search": " ".join(book.get("subjects", [])[:10]),  # For text search
        "language": book.get("language"),
        # Cover images
        "cover_i": book.get("cover_i"),
        "cover_available": str(book.get("cover_available", False)).lower(),  # TagField needs string
        "cover_urls": book.get("cover_urls", {}),
        "book_image": book.get("book_image"),
        "image": book.get("book_image"),  # Standardized image field
        "images": book.get("images", []),
        # Ratings
        "ratings_average": book.get("ratings_average"),
        "ratings_count": book.get("ratings_count"),
        "readinglog_count": book.get("readinglog_count", 0),
        # Physical details
        "number_of_pages": book.get("number_of_pages"),
    }

    return doc


async def create_book_index(redis: Redis) -> bool:
    """
    Create the book search index if it doesn't exist.

    Returns:
        True if created or already exists
    """
    schema = (
        # Primary search field (title) with high weight
        TextField("$.search_title", as_name="search_title", weight=5.0),
        TextField("$.title", as_name="title", weight=4.0),
        # Author search
        TextField("$.author_search", as_name="author_search", weight=3.0),
        TextField("$.author", as_name="author", weight=2.0),
        # Author OLID - O(1) lookup for relational queries (books by author)
        TagField("$.matching_author_olids[*]", as_name="author_olid"),
        # Description - searchable but lower weight
        TextField("$.description", as_name="description", weight=1.0),
        # Subject search
        TextField("$.subjects_search", as_name="subjects_search", weight=1.0),
        # Type filters
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        # External IDs as tags (exact match)
        TagField("$.openlibrary_key", as_name="openlibrary_key"),
        TagField("$.primary_isbn13", as_name="primary_isbn13"),
        TagField("$.primary_isbn10", as_name="primary_isbn10"),
        # Boolean fields
        TagField("$.cover_available", as_name="cover_available"),
        # Sortable numeric fields
        NumericField("$.first_publish_year", as_name="first_publish_year", sortable=True),
        NumericField("$.ratings_average", as_name="ratings_average", sortable=True),
        NumericField("$.ratings_count", as_name="ratings_count", sortable=True),
        NumericField("$.readinglog_count", as_name="readinglog_count", sortable=True),
        NumericField("$.number_of_pages", as_name="number_of_pages", sortable=True),
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


async def drop_book_index(redis: Redis) -> bool:
    """Drop the book index."""
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


async def load_books_to_redis(
    input_file: str,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    recreate_index: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Load MCBookItems from JSONL file to Redis.

    Args:
        input_file: Path to JSONL file
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        recreate_index: Drop and recreate index
        limit: Limit number of books to load
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
            await drop_book_index(redis)

        if not await create_book_index(redis):
            logger.error("Failed to create index")
            return stats

        if dry_run:
            logger.info("[DRY RUN] Would load books but not writing to Redis")

        # Load books
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
                    book = json.loads(line)
                    redis_doc = mc_book_to_redis_doc(book)

                    # Redis key
                    key = f"{KEY_PREFIX}{redis_doc['id']}"

                    if dry_run:
                        stats["loaded"] += 1
                        if stats["loaded"] <= 3:
                            logger.info(f"  [DRY RUN] {redis_doc['title']} -> {key}")
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

        # Get index info and verify indexing
        try:
            # Wait for indexing to complete (check every second, max 60 seconds)
            for _ in range(60):
                info = await redis.ft(INDEX_NAME).info()
                indexing = info.get("indexing", 0)
                if str(indexing) == "0":
                    break
                await asyncio.sleep(1)

            info = await redis.ft(INDEX_NAME).info()
            indexed_count = int(info.get("num_docs", 0))
            logger.info(f"  Index '{INDEX_NAME}' now has {indexed_count:,} documents")

            # Check for discrepancy
            if indexed_count < stats["loaded"]:
                missing = stats["loaded"] - indexed_count
                logger.warning(f"  ⚠️  INDEX DISCREPANCY: {missing:,} documents not indexed!")
                logger.warning(f"     Loaded: {stats['loaded']:,}, Indexed: {indexed_count:,}")

                # Sample unindexed documents for debugging
                logger.info("  Sampling unindexed documents...")
                cursor = 0
                checked = 0
                unindexed_samples = []

                while checked < 1000 and len(unindexed_samples) < 10:
                    cursor, keys = await redis.scan(
                        cursor=cursor, match=f"{KEY_PREFIX}*", count=100
                    )
                    for key in keys:
                        checked += 1
                        # Check if this key is in the index by searching for it
                        doc = await redis.json().get(key, "$.title")
                        title = doc[0] if doc else "Unknown"
                        try:
                            from redis.commands.search.query import Query

                            # Search for exact title match
                            result = await redis.ft(INDEX_NAME).search(
                                Query(f'@title:"{title}"').paging(0, 1)
                            )
                            if result.total == 0:
                                unindexed_samples.append((key, title))
                        except Exception:
                            pass

                        if len(unindexed_samples) >= 10:
                            break

                    if cursor == 0:
                        break

                if unindexed_samples:
                    logger.warning("  Sample unindexed documents:")
                    for key, title in unindexed_samples:
                        doc = await redis.json().get(key)
                        logger.warning(f"    Key: {key}")
                        logger.warning(f"      Title: {title}")
                        logger.warning(
                            f"      Fields: mc_type={doc.get('mc_type')}, "
                            f"source={doc.get('source')}, "
                            f"cover_available={doc.get('cover_available')}"
                        )

                stats["index_discrepancy"] = missing
            else:
                logger.info("  ✓ All documents indexed successfully")

        except Exception as e:
            logger.error(f"  Error checking index: {e}")

    finally:
        await redis.aclose()

    return stats


async def main(
    input_file: str = "data/openlibrary/mc_books.jsonl",
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

    logger.info("Loading books to Redis")
    logger.info(f"  Input: {input_file}")
    logger.info(f"  Redis: {host}:{port}")

    return await load_books_to_redis(
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

    parser = argparse.ArgumentParser(description="Load MCBookItems to Redis index")
    parser.add_argument(
        "--input",
        default="data/openlibrary/mc_books.jsonl",
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
        help="Limit number of books to load",
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
