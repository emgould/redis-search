"""
Book TAG Migration Script

Migrates existing book documents to use:
1. Normalized subjects array (IPTC-expanded) - subjects_normalized
2. Normalized author field - author_normalized
3. Popularity score computed from:
   - Edition count (fetched from OpenLibrary API for top authors)
   - Author quality score (fallback for other books)

For ~782K books, this uses a hybrid approach:
- Top 20% authors by quality score: fetch edition counts from API (~2-3 hours)
- Other books: use author quality score as popularity proxy (instant)
"""

import asyncio
import math
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

# Constants
INDEX_NAME = "idx:book"
SCAN_BATCH = 10000
MGET_BATCH = 2000
WRITE_BATCH = 2000


class MigrationStats:
    """Track migration statistics."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.total_docs = 0
        self.processed_docs = 0
        self.updated_docs = 0
        self.skipped_docs = 0
        self.api_calls = 0
        self.cache_hits = 0
        self.phase_times: dict[str, float] = {}

    def log_progress(self, phase: str, current: int, total: int) -> None:
        """Log progress at regular intervals."""
        if (current % 10000 == 0) or (current == total):
            elapsed = time.time() - self.start_time
            rate = current / elapsed if elapsed > 0 else 0
            logger.info(
                f"[{phase}] {current:,}/{total:,} ({current / total:.1%}) "
                f"Rate: {rate:.1f}/s Elapsed: {elapsed:.1f}s"
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
        logger.info(f"API calls made: {self.api_calls:,}")
        logger.info(f"Cache hits: {self.cache_hits:,}")
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
    """Build the new book index schema."""
    return (
        # Primary search fields
        TextField("$.search_title", as_name="search_title", weight=5.0),
        TextField("$.title", as_name="title", weight=4.0),
        TextField("$.author_search", as_name="author_search", weight=3.0),
        TextField("$.author", as_name="author", weight=2.0),
        TextField("$.description", as_name="description", weight=1.0),
        TextField("$.subjects_search", as_name="subjects_search", weight=1.0),
        # TAG fields for exact matching
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.source", as_name="source"),
        TagField("$.openlibrary_key", as_name="openlibrary_key"),
        TagField("$.primary_isbn13", as_name="primary_isbn13"),
        TagField("$.primary_isbn10", as_name="primary_isbn10"),
        TagField("$.cover_available", as_name="cover_available"),
        TagField("$.author_olids[*]", as_name="author_olid"),
        # NEW: Normalized fields for search
        TagField("$.author_normalized", as_name="author_normalized"),
        TagField("$.subjects_normalized[*]", as_name="subjects"),
        # Sortable numeric fields
        NumericField("$.popularity_score", as_name="popularity_score", sortable=True),
        NumericField("$.edition_count", as_name="edition_count", sortable=True),
    )


def compute_book_popularity(author_quality_score: float | None) -> float:
    """
    Compute book popularity score using author quality as proxy.

    OpenLibrary rate limits are too strict for fetching edition counts
    at scale (~100 req/5min), so we use author quality as a proxy.

    Args:
        author_quality_score: Author's quality score from idx:author

    Returns:
        Popularity score (higher = more popular)
    """
    if author_quality_score is not None and author_quality_score > 0:
        # Scale author quality with log for diminishing returns
        # Top author (42.28) → ~3.8
        # Good author (20) → ~3.0
        # Average author (10) → ~2.4
        # Low author (2) → ~1.1
        return math.log1p(author_quality_score)

    return 0.0


async def load_author_quality_scores(redis: Redis) -> dict[str, float]:
    """
    Load author quality scores from idx:author into a dict mapping bare OLID -> score.

    Author documents store the OpenLibrary key as `openlibrary_key` in path format
    (e.g., "/authors/OL3121210A"). Book documents reference authors by bare OLID
    (e.g., "OL3121210A") in their `author_olids` array. This function strips the
    "/authors/" prefix to produce bare OLIDs for matching.
    """
    logger.info("[AUTHORS] Loading author quality scores...")
    author_scores: dict[str, float] = {}

    # Scan all author documents
    async for key in redis.scan_iter(match="author:*", count=10000):
        try:
            doc = await redis.json().get(key, "$.openlibrary_key", "$.quality_score")
            if doc:
                ol_key = doc.get("$.openlibrary_key", [None])[0]
                score = doc.get("$.quality_score", [0])[0]
                if ol_key and score:
                    # Strip "/authors/" prefix to get bare OLID matching book author_olids
                    bare_olid = ol_key.replace("/authors/", "")
                    author_scores[bare_olid] = float(score)
        except Exception as e:
            logger.debug(f"Error loading author {key}: {e}")
            continue

    logger.info(f"[AUTHORS] Loaded {len(author_scores):,} author quality scores")
    return author_scores


def transform_subjects(subjects: list[str] | None) -> list[str]:
    """
    Transform subjects to normalized array with IPTC expansion.

    Input: ["Fiction", "Mystery", "Fiction, mystery & detective, general"]
    Output: ["fiction", "mystery", "detective_fiction", ...]
    """
    if not subjects:
        return []

    # Convert to IPTC keyword format for expansion
    keyword_dicts = [{"name": subj} for subj in subjects if subj]

    # expand_keywords normalizes and expands with IPTC aliases
    return expand_keywords(keyword_dicts)


def transform_document(
    doc: dict[str, Any],
    author_scores: dict[str, float],
) -> dict[str, Any]:
    """
    Transform a book document to the new format.

    - Adds subjects_normalized (IPTC-expanded)
    - Adds author_normalized
    - Adds popularity_score (based on author quality)
    - Adds author_quality_score (for transparency)
    """
    # Get author quality score from any matching author OLID
    author_olids = doc.get("author_olids") or doc.get("matching_author_olids") or []
    author_quality = 0.0
    for olid in author_olids:
        if olid in author_scores:
            author_quality = max(author_quality, author_scores[olid])
            break

    # Transform subjects
    raw_subjects = doc.get("subjects") or doc.get("subject") or []
    doc["subjects_normalized"] = transform_subjects(raw_subjects)

    # Normalize author
    raw_author = doc.get("author") or ""
    doc["author_normalized"] = normalize_tag(raw_author) if raw_author else None

    # Add popularity fields (author quality as proxy - OpenLibrary rate limits prevent edition count fetching)
    doc["author_quality_score"] = author_quality
    doc["edition_count"] = None  # Not fetched due to API rate limits
    doc["popularity_score"] = compute_book_popularity(author_quality)

    return doc


async def migrate_book_tags(
    dry_run: bool = False,
    limit: int | None = None,
) -> None:
    """
    Migrate existing book documents to include normalized TAG fields.

    Uses author quality score as popularity proxy (OpenLibrary API rate limits
    are too strict for fetching edition counts at scale).

    Args:
        dry_run: If True, don't write changes to Redis
        limit: If set, only process this many documents (for testing)
    """
    logger.info("=" * 60)
    logger.info("Book TAG Migration")
    logger.info("=" * 60)
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Document limit: {limit or 'None'}")
    logger.info("Popularity: Using author quality score (no API calls)")

    redis = await get_redis_connection()
    stats = MigrationStats()

    try:
        # Phase 1: Load author quality scores
        phase_start = time.time()
        author_scores = await load_author_quality_scores(redis)
        stats.phase_times["load_authors"] = time.time() - phase_start

        # Phase 2: Collect all book document keys
        phase_start = time.time()
        logger.info("[SCAN] Collecting all book:* keys...")
        all_keys: list[str] = []
        async for key in redis.scan_iter(match="book:*", count=SCAN_BATCH):
            all_keys.append(key)
            if limit and len(all_keys) >= limit:
                break
        stats.total_docs = len(all_keys)
        stats.phase_times["scan"] = time.time() - phase_start
        logger.info(
            f"[SCAN] Found {len(all_keys):,} documents in {stats.phase_times['scan']:.2f}s"
        )

        if not all_keys:
            logger.info("No book documents found to migrate.")
            return

        # Phase 3: Read existing documents in batches
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

        # Phase 4: Transform documents
        phase_start = time.time()
        logger.info("[TRANSFORM] Transforming documents...")
        transformed_docs: list[tuple[str, dict]] = []

        for key, doc in existing_docs.items():
            stats.processed_docs += 1
            try:
                transformed = transform_document(doc, author_scores)
                transformed_docs.append((key, transformed))
                stats.updated_docs += 1
            except Exception as e:
                logger.warning(f"Error transforming {key}: {e}")
                stats.skipped_docs += 1

            if stats.processed_docs % 50000 == 0:
                stats.log_progress("TRANSFORM", stats.processed_docs, len(existing_docs))

        stats.phase_times["transform"] = time.time() - phase_start
        logger.info(
            f"[TRANSFORM] Transformed {len(transformed_docs):,} documents "
            f"in {stats.phase_times['transform']:.2f}s"
        )

        # Phase 5: Write transformed documents
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
            for i, (key, doc) in enumerate(transformed_docs[:5]):
                aq_score = doc.get("author_quality_score") or 0
                pop_score = doc.get("popularity_score") or 0
                logger.info(
                    f"  Sample {i + 1}: {key}\n"
                    f"    title: {doc.get('title', '')[:50]}\n"
                    f"    author_normalized: {doc.get('author_normalized')}\n"
                    f"    subjects_normalized: {doc.get('subjects_normalized', [])[:5]}...\n"
                    f"    author_quality_score: {aq_score:.2f}\n"
                    f"    popularity_score: {pop_score:.2f}"
                )

        # Phase 8: Recreate index with new schema
        if not dry_run:
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
            definition = IndexDefinition(prefix=["book:"], index_type=IndexType.JSON)

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
        description="Migrate book index with normalized TAGs and popularity scores"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without making any changes to Redis.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of documents to process (for testing).",
    )
    args = parser.parse_args()

    asyncio.run(migrate_book_tags(dry_run=args.dry_run, limit=args.limit))
