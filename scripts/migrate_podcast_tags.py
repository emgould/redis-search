"""
Podcast TAG Migration Script

Migrates existing podcast documents to use:
1. Normalized categories array (IPTC-expanded) instead of dict
2. Normalized author field (author_normalized)
3. Normalized language field

This is a Redis-to-Redis transformation - no external API calls needed.
Uses aggressive batching since we're only transforming existing data.
"""

import asyncio
import os
import sys
import time
from collections.abc import Awaitable
from typing import Any, cast

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from core.iptc import expand_keywords, normalize_tag
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Load environment variables
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# Constants - aggressive batching since no API calls
INDEX_NAME = "idx:podcasts"
SCAN_BATCH = 10000
MGET_BATCH = 2000  # Large batches for Redis-to-Redis
WRITE_BATCH = 2000


class MigrationStats:
    """Track migration statistics."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.total_docs = 0
        self.processed_docs = 0
        self.updated_docs = 0
        self.skipped_docs = 0
        self.phase_times: dict[str, float] = {}

    def log_progress(self, phase: str, current: int, total: int) -> None:
        """Log progress at regular intervals."""
        if (current % 5000 == 0) or (current == total):
            elapsed = time.time() - self.start_time
            logger.info(
                f"[{phase}] {current:,}/{total:,} ({current / total:.1%}) "
                f"Elapsed: {elapsed:.1f}s"
            )

    def log_summary(self) -> None:
        """Log final summary."""
        total_elapsed = time.time() - self.start_time
        logger.info("=" * 60)
        logger.info("Migration Summary:")
        logger.info(f"Total documents scanned: {self.total_docs:,}")
        logger.info(f"Documents processed: {self.processed_docs:,}")
        logger.info(f"Documents updated: {self.updated_docs:,}")
        logger.info(f"Documents skipped: {self.skipped_docs:,}")
        logger.info(f"Total elapsed time: {total_elapsed:.2f}s")
        logger.info("Phase timings:")
        for phase, duration in self.phase_times.items():
            logger.info(f"  {phase}: {duration:.2f}s")
        logger.info("=" * 60)


async def get_redis_connection() -> Redis:
    """Get an async Redis connection."""
    r: Redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )
    try:
        await cast(Awaitable[bool], r.ping())
        logger.info("Redis connection OK")
        return r
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise


def build_new_schema() -> tuple:
    """Build the new podcast index schema."""
    return (
        # Primary search field with high weight
        TextField("$.search_title", as_name="search_title", weight=5.0),
        # Author/creator name - searchable (full-text)
        TextField("$.author", as_name="author", weight=3.0),
        # Author normalized for exact TAG matching
        TagField("$.author_normalized", as_name="author_normalized"),
        # SearchDocument standard fields
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        TagField("$.id", as_name="id"),
        # Language filter (normalized)
        TagField("$.language", as_name="language"),
        # Categories array (normalized, IPTC-expanded)
        TagField("$.categories[*]", as_name="categories"),
        # Sortable numeric fields
        NumericField("$.popularity", as_name="popularity", sortable=True),
        NumericField("$.episode_count", as_name="episode_count", sortable=True),
    )


def transform_categories(categories: dict[str, str] | list[str] | None) -> list[str]:
    """
    Transform categories from dict format to normalized array with IPTC expansion.

    Input: {"1": "news", "2": "politics", "3": "technology"} or already a list
    Output: ["news", "politics", "technology", ...expanded IPTC aliases...]
    """
    if categories is None:
        return []

    # Handle already transformed (list) format
    if isinstance(categories, list):
        return categories

    # Extract values from dict
    raw_categories: list[str] = []
    for key in sorted(categories.keys(), key=lambda k: int(k) if k.isdigit() else 0):
        value = categories[key]
        if value and str(value).strip():
            raw_categories.append(str(value).strip())

    if not raw_categories:
        return []

    # Convert to IPTC keyword format for expansion
    keyword_dicts = [{"name": cat} for cat in raw_categories]

    # expand_keywords normalizes and expands with IPTC aliases
    return expand_keywords(keyword_dicts)


def transform_document(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a podcast document to the new format.

    - Converts categories dict to normalized/expanded array
    - Adds author_normalized field
    - Normalizes language field
    """
    # Transform categories
    doc["categories"] = transform_categories(doc.get("categories"))

    # Add normalized author
    raw_author = doc.get("author") or ""
    doc["author_normalized"] = normalize_tag(raw_author) if raw_author else None

    # Normalize language
    raw_language = doc.get("language") or ""
    doc["language"] = normalize_tag(raw_language) if raw_language else None

    return doc


async def migrate_podcast_tags(dry_run: bool = False) -> None:
    """
    Migrate existing podcast documents to include normalized TAG fields.
    """
    logger.info("=" * 60)
    logger.info("Podcast TAG Migration")
    logger.info("=" * 60)
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Dry run: {dry_run}")

    redis = await get_redis_connection()
    stats = MigrationStats()

    try:
        # Phase 1: Collect all podcast document keys
        phase_start = time.time()
        logger.info("[SCAN] Collecting all podcast:* keys...")
        all_keys: list[str] = []
        async for key in redis.scan_iter(match="podcast:*", count=SCAN_BATCH):
            all_keys.append(key)
        stats.total_docs = len(all_keys)
        stats.phase_times["scan"] = time.time() - phase_start
        logger.info(
            f"[SCAN] Found {len(all_keys):,} documents in {stats.phase_times['scan']:.2f}s"
        )

        if not all_keys:
            logger.info("No podcast documents found to migrate.")
            return

        # Phase 2: Read existing documents in batches
        phase_start = time.time()
        logger.info("[READ] Reading existing documents...")
        existing_docs: dict[str, dict] = {}
        for i in range(0, len(all_keys), MGET_BATCH):
            batch_keys = all_keys[i : i + MGET_BATCH]
            raw_docs = await redis.json().mget(batch_keys, "$")
            for key, doc_list in zip(batch_keys, raw_docs, strict=True):
                if doc_list and doc_list[0]:
                    existing_docs[key] = doc_list[0]
            stats.log_progress("READ", min(i + MGET_BATCH, len(all_keys)), len(all_keys))
        stats.phase_times["read"] = time.time() - phase_start
        logger.info(
            f"[READ] Read {len(existing_docs):,} documents in {stats.phase_times['read']:.2f}s"
        )

        # Phase 3: Transform documents
        phase_start = time.time()
        logger.info("[TRANSFORM] Transforming documents...")
        transformed_docs: list[tuple[str, dict]] = []

        for key, doc in existing_docs.items():
            stats.processed_docs += 1
            try:
                transformed = transform_document(doc)
                transformed_docs.append((key, transformed))
                stats.updated_docs += 1
            except Exception as e:
                logger.warning(f"Error transforming {key}: {e}")
                stats.skipped_docs += 1

            if stats.processed_docs % 10000 == 0:
                stats.log_progress("TRANSFORM", stats.processed_docs, len(existing_docs))

        stats.phase_times["transform"] = time.time() - phase_start
        logger.info(
            f"[TRANSFORM] Transformed {len(transformed_docs):,} documents "
            f"in {stats.phase_times['transform']:.2f}s"
        )

        # Phase 4: Write transformed documents
        if not dry_run:
            phase_start = time.time()
            logger.info("[WRITE] Writing transformed documents...")
            for i in range(0, len(transformed_docs), WRITE_BATCH):
                pipeline = redis.pipeline()
                batch = transformed_docs[i : i + WRITE_BATCH]
                for key, doc in batch:
                    pipeline.json().set(key, "$", doc)
                await pipeline.execute()
                stats.log_progress(
                    "WRITE", min(i + WRITE_BATCH, len(transformed_docs)), len(transformed_docs)
                )
            stats.phase_times["write"] = time.time() - phase_start
            logger.info(
                f"[WRITE] Wrote {len(transformed_docs):,} documents "
                f"in {stats.phase_times['write']:.2f}s"
            )
        else:
            logger.info(f"[DRY RUN] Would update {len(transformed_docs):,} documents.")
            # Show sample transformations
            for i, (key, doc) in enumerate(transformed_docs[:3]):
                logger.info(
                    f"  Sample {i + 1}: {key}\n"
                    f"    categories: {doc.get('categories', [])[:5]}...\n"
                    f"    author_normalized: {doc.get('author_normalized')}\n"
                    f"    language: {doc.get('language')}"
                )

        # Phase 5: Recreate index with new schema
        phase_start = time.time()
        logger.info("[INDEX] Recreating index with new schema...")

        # Drop existing index
        try:
            await redis.ft(INDEX_NAME).dropindex(delete_documents=False)
            logger.info(f"[INDEX] Dropped {INDEX_NAME}")
        except Exception as e:
            logger.warning(f"[INDEX] Could not drop index (may not exist): {e}")

        # Create new index
        schema = build_new_schema()
        definition = IndexDefinition(prefix=["podcast:"], index_type=IndexType.JSON)

        try:
            await redis.ft(INDEX_NAME).create_index(schema, definition=definition)
            logger.info(f"[INDEX] Created {INDEX_NAME} with new schema")
        except Exception as e:
            logger.error(f"[INDEX] Failed to create index: {e}")
            raise

        stats.phase_times["index"] = time.time() - phase_start
        logger.info(f"[INDEX] Index recreated in {stats.phase_times['index']:.2f}s")

    finally:
        await redis.aclose()
        stats.log_summary()
        logger.info("Migration complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate podcast index with normalized TAGs and IPTC expansion"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without making any changes to Redis.",
    )
    args = parser.parse_args()

    asyncio.run(migrate_podcast_tags(dry_run=args.dry_run))
