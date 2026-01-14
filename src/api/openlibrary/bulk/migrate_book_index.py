"""
Migrate book index to add author_olid TagField.

This script:
1. Drops the existing idx:book index WITHOUT deleting documents
2. Creates a new index with the author_olid TagField
3. Redis Search automatically re-indexes all existing book:* documents

This is safe because:
- Documents are stored as JSON and remain intact when index is dropped
- The matching_author_olids field already exists in all documents
- We're just adding it to the searchable schema

Usage:
    # Local Redis
    python -m api.openlibrary.bulk.migrate_book_index --redis-port 6380

    # Public Redis (via IAP tunnel)
    python -m api.openlibrary.bulk.migrate_book_index --redis-port 6381
"""

import argparse
import asyncio
import os
import time

from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from utils.get_logger import get_logger

logger = get_logger(__name__)

INDEX_NAME = "idx:book"
KEY_PREFIX = "book:"


async def migrate_book_index(
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    dry_run: bool = False,
) -> dict[str, int | str]:
    """
    Migrate the book index to include author_olid TagField.

    This drops the index WITHOUT deleting documents, then recreates
    with the new schema. Existing documents are automatically re-indexed.

    Args:
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        dry_run: If True, only show what would happen

    Returns:
        Stats dict with migration results
    """
    stats: dict[str, int | str] = {
        "status": "pending",
        "docs_before": 0,
        "docs_after": 0,
    }

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

        # Get current index info
        try:
            info = await redis.ft(INDEX_NAME).info()
            stats["docs_before"] = int(info.get("num_docs", 0))
            logger.info(f"Current index has {stats['docs_before']:,} documents")
        except Exception as e:
            if "Unknown index" in str(e) or "Unknown Index" in str(e):
                logger.warning(f"Index '{INDEX_NAME}' does not exist - will create new")
                stats["docs_before"] = 0
            else:
                raise

        if dry_run:
            logger.info("[DRY RUN] Would drop and recreate index")
            logger.info("[DRY RUN] Documents would remain intact")
            stats["status"] = "dry_run"
            return stats

        # Step 1: Drop index WITHOUT deleting documents
        # Using dropindex() without delete_documents=True preserves the JSON docs
        logger.info("Step 1: Dropping index (keeping documents)...")
        try:
            await redis.ft(INDEX_NAME).dropindex(delete_documents=False)
            logger.info(f"  ✓ Dropped index '{INDEX_NAME}' (documents preserved)")
        except Exception as e:
            if "Unknown index" in str(e) or "Unknown Index" in str(e):
                logger.info(f"  Index '{INDEX_NAME}' did not exist")
            else:
                raise

        # Step 2: Create new index with author_olid TagField
        logger.info("Step 2: Creating new index with author_olid TagField...")

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

        await redis.ft(INDEX_NAME).create_index(schema, definition=definition)
        logger.info(f"  ✓ Created index '{INDEX_NAME}' with author_olid TagField")

        # Step 3: Wait for indexing to complete
        logger.info("Step 3: Waiting for re-indexing to complete...")
        start_time = time.time()

        while True:
            info = await redis.ft(INDEX_NAME).info()
            indexing = info.get("indexing", 0)
            num_docs = int(info.get("num_docs", 0))

            if str(indexing) == "0":
                break

            elapsed = time.time() - start_time
            logger.info(f"  Indexing in progress: {num_docs:,} docs ({elapsed:.0f}s)")
            await asyncio.sleep(2)

            # Timeout after 10 minutes
            if elapsed > 600:
                logger.warning("  ⚠ Indexing timeout - continuing anyway")
                break

        # Get final stats
        info = await redis.ft(INDEX_NAME).info()
        stats["docs_after"] = int(info.get("num_docs", 0))

        elapsed = time.time() - start_time
        logger.info(f"  ✓ Indexing complete in {elapsed:.1f}s")
        logger.info(f"  Documents indexed: {stats['docs_after']:,}")

        # Verify no data loss
        if stats["docs_before"] > 0:
            if stats["docs_after"] >= stats["docs_before"]:
                logger.info("  ✓ All documents preserved")
            else:
                diff = stats["docs_before"] - int(stats["docs_after"])
                logger.warning(f"  ⚠ Document count changed: {diff:,} difference")

        stats["status"] = "success"

        # Step 4: Test the new author_olid field
        logger.info("Step 4: Testing author_olid field...")
        try:
            # Try a sample query
            from redis.commands.search.query import Query

            result = await redis.ft(INDEX_NAME).search(Query("@author_olid:{OL*}").paging(0, 1))
            if result.total > 0:
                logger.info(
                    f"  ✓ author_olid field working - found {result.total:,} books with author OLIDs"
                )
            else:
                logger.warning("  ⚠ No books found with author_olid - field may be empty")
        except Exception as e:
            logger.warning(f"  ⚠ Test query failed: {e}")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        stats["status"] = f"error: {e}"
        raise

    finally:
        await redis.aclose()

    return stats


async def main():
    parser = argparse.ArgumentParser(description="Migrate book index to add author_olid TagField")
    parser.add_argument(
        "--redis-host",
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port (default: 6380)",
    )
    parser.add_argument(
        "--redis-password",
        default=os.getenv("REDIS_PASSWORD"),
        help="Redis password",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Book Index Migration: Adding author_olid TagField")
    logger.info("=" * 60)
    logger.info(f"  Redis: {args.redis_host}:{args.redis_port}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("")

    stats = await migrate_book_index(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_password=args.redis_password,
        dry_run=args.dry_run,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Migration complete: {stats['status']}")
    logger.info(f"  Docs before: {stats['docs_before']:,}")
    logger.info(f"  Docs after:  {stats['docs_after']:,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
