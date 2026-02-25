"""
Migrate media index documents to add normalized TAGs and new fields.

This script:
1. Scans all media:* documents in Redis
2. For each document, fetches enriched data from cache-backed TMDB API
3. Normalizes all TAG fields (lowercase, special chars -> underscore)
4. Adds new fields: keywords (IPTC expanded), director_id, director_name, origin_country
5. Writes updated documents back to Redis
6. Recreates the index with the new schema

Performance: Since TMDB data is cached in Redis, this should complete in seconds
for tens of thousands of documents.
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Any

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.tmdb.core import TMDBService
from contracts.models import MCType
from core.iptc import IPTCKeywordExpander, normalize_tag
from utils.get_logger import get_logger

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

logger = get_logger(__name__)

# Constants
INDEX_NAME = "idx:media"
SCAN_BATCH = 10000
MGET_BATCH = 500
# Lower concurrency to respect TMDB rate limit (40 req/sec, we use 25)
# TMDBService has internal rate limiter at 25 req/sec
# Push concurrency high - rate limiter will throttle as needed
ENRICH_CONCURRENCY = 50
WRITE_BATCH = 500


class MigrationStats:
    """Track migration statistics."""

    def __init__(self) -> None:
        self.total_docs = 0
        self.enriched = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.errors = 0
        self.error_messages: list[str] = []
        self.start_time: float = 0
        self.phase_times: dict[str, float] = {}

    def log_summary(self) -> None:
        """Log migration summary."""
        elapsed = time.time() - self.start_time
        logger.info("=" * 60)
        logger.info("Migration Summary")
        logger.info("=" * 60)
        logger.info(f"Total documents: {self.total_docs:,}")
        logger.info(f"Enriched: {self.enriched:,}")
        logger.info(f"Errors: {self.errors:,}")
        logger.info(f"Total time: {elapsed:.2f}s")
        for phase, duration in self.phase_times.items():
            logger.info(f"  {phase}: {duration:.2f}s")
        if self.error_messages:
            logger.info("First 5 errors:")
            for msg in self.error_messages[:5]:
                logger.info(f"  - {msg}")


def build_new_schema() -> tuple:
    """Build the new index schema with additional TAG fields."""
    from web.app import INDEX_CONFIGS
    return INDEX_CONFIGS["media"]["schema"]


def normalize_existing_tags(doc: dict[str, Any]) -> dict[str, Any]:
    """Normalize existing TAG fields in a document."""
    # Normalize genres
    if "genres" in doc and doc["genres"]:
        doc["genres"] = [normalize_tag(g) for g in doc["genres"] if normalize_tag(g)]

    # Normalize cast_names (keep cast for display)
    if "cast_names" in doc and doc["cast_names"]:
        doc["cast_names"] = [normalize_tag(n) for n in doc["cast_names"] if normalize_tag(n)]

    return doc


def extract_new_fields(
    doc: dict[str, Any],
    details: Any,
    iptc_expander: IPTCKeywordExpander,
) -> dict[str, Any]:
    """Extract new fields from enriched details and add to document."""
    # Director
    director = getattr(details, "director", None) or {}
    if not director:
        tmdb_cast = getattr(details, "tmdb_cast", None) or {}
        director = tmdb_cast.get("director", {}) if isinstance(tmdb_cast, dict) else {}

    if isinstance(director, dict) and director.get("id"):
        doc["director_id"] = str(director["id"])
        doc["director_name"] = normalize_tag(director.get("name", "")) or None
    else:
        doc["director_id"] = None
        doc["director_name"] = None

    # Keywords (IPTC expanded)
    keywords = getattr(details, "keywords", None) or []
    if keywords:
        doc["keywords"] = iptc_expander.expand(keywords)
    else:
        doc["keywords"] = []

    # Origin country
    origin_country = getattr(details, "origin_country", None) or []
    if not origin_country:
        # Try production_countries for movies
        prod_countries = getattr(details, "production_countries", None) or []
        if prod_countries:
            origin_country = [
                c.get("iso_3166_1", "") for c in prod_countries if isinstance(c, dict)
            ]
    doc["origin_country"] = [normalize_tag(c) for c in origin_country if c and normalize_tag(c)]

    # Additional fields from recent schema updates
    if hasattr(details, "original_title") and details.original_title:
        doc["original_title"] = details.original_title
    if hasattr(details, "tagline") and details.tagline:
        doc["tagline"] = details.tagline
    
    # Language fields
    if hasattr(details, "original_language") and details.original_language:
        doc["original_language"] = details.original_language
    
    # spoken_languages is now stored as an array of strings
    spoken_langs = getattr(details, "spoken_languages", [])
    if spoken_langs:
        # Assuming spoken_languages on the API model is already a list of strings
        doc["spoken_languages"] = spoken_langs
    
    # Networks
    networks = getattr(details, "networks", [])
    if networks:
        doc["networks"] = networks
        
    # Numeric stats
    if hasattr(details, "vote_count"):
        doc["vote_count"] = details.vote_count
    
    if hasattr(details, "number_of_seasons"):
        doc["number_of_seasons"] = details.number_of_seasons
        
    if hasattr(details, "revenue"):
        doc["revenue"] = details.revenue

    return doc


async def migrate_documents(
    redis: Redis,
    tmdb_service: TMDBService,
    iptc_expander: IPTCKeywordExpander,
    stats: MigrationStats,
    dry_run: bool = False,
) -> None:
    """Migrate all media documents with new TAG fields."""
    # Phase 1: Scan all keys
    phase_start = time.time()
    logger.info("[SCAN] Collecting all media:* keys...")

    keys: list[str] = []
    async for key in redis.scan_iter(match="media:*", count=SCAN_BATCH):
        keys.append(key if isinstance(key, str) else key.decode())

    stats.total_docs = len(keys)
    stats.phase_times["scan"] = time.time() - phase_start
    logger.info(f"[SCAN] Found {stats.total_docs:,} documents in {stats.phase_times['scan']:.2f}s")

    if not keys:
        logger.warning("No documents found!")
        return

    # Phase 2: Read existing documents in batches
    phase_start = time.time()
    logger.info("[READ] Reading existing documents...")

    all_docs: list[tuple[str, dict[str, Any] | None]] = []
    for i in range(0, len(keys), MGET_BATCH):
        batch_keys = keys[i : i + MGET_BATCH]
        # Use JSON.MGET for JSON documents
        try:
            docs = await redis.json().mget(batch_keys, "$")
            for key, doc_list in zip(batch_keys, docs, strict=True):
                if doc_list and len(doc_list) > 0:
                    all_docs.append((key, doc_list[0]))
                else:
                    all_docs.append((key, None))
        except Exception as e:
            logger.error(f"Error reading batch at {i}: {e}")
            # Fall back to individual reads
            for key in batch_keys:
                try:
                    doc = await redis.json().get(key)
                    all_docs.append((key, doc))
                except Exception:
                    all_docs.append((key, None))

        if (i + MGET_BATCH) % 5000 == 0:
            logger.info(f"[READ] {min(i + MGET_BATCH, len(keys)):,}/{len(keys):,}")

    stats.phase_times["read"] = time.time() - phase_start
    logger.info(f"[READ] Read {len(all_docs):,} documents in {stats.phase_times['read']:.2f}s")

    # Phase 3: Enrich documents in parallel batches
    phase_start = time.time()
    logger.info("[ENRICH] Enriching documents with TMDB cache...")

    async def enrich_one(key: str, doc: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        """Enrich a single document."""
        try:
            # Get source_id and mc_type
            source_id = doc.get("source_id")
            mc_type_str = doc.get("mc_type", "movie")

            if not source_id:
                stats.errors += 1
                return key, None

            tmdb_id = int(source_id)
            mc_type = MCType.TV_SERIES if mc_type_str == "tv" else MCType.MOVIE

            # Get enriched details (cache hit should be ~1ms)
            details = await tmdb_service.get_media_details(
                tmdb_id,
                mc_type,
                include_cast=True,
                include_videos=False,  # Don't need videos
                include_watch_providers=False,  # Don't need providers
                include_keywords=True,
                include_release_dates=False,
            )

            # Normalize existing tags
            doc = normalize_existing_tags(doc)

            # Extract new fields
            doc = extract_new_fields(doc, details, iptc_expander)

            stats.enriched += 1
            return key, doc

        except Exception as e:
            stats.errors += 1
            if len(stats.error_messages) < 10:
                stats.error_messages.append(f"{key}: {e}")
            return key, None

    # Process in concurrent batches
    enriched_docs: list[tuple[str, dict[str, Any]]] = []
    valid_docs = [(k, d) for k, d in all_docs if d is not None]

    for i in range(0, len(valid_docs), ENRICH_CONCURRENCY):
        batch = valid_docs[i : i + ENRICH_CONCURRENCY]
        tasks = [enrich_one(k, d) for k, d in batch]
        results = await asyncio.gather(*tasks)

        for key, doc in results:
            if doc is not None:
                enriched_docs.append((key, doc))

        progress = min(i + ENRICH_CONCURRENCY, len(valid_docs))
        pct = (progress / len(valid_docs)) * 100
        logger.info(f"[ENRICH] {progress:,}/{len(valid_docs):,} ({pct:.0f}%)")

    stats.phase_times["enrich"] = time.time() - phase_start
    logger.info(f"[ENRICH] Enriched {len(enriched_docs):,} documents in {stats.phase_times['enrich']:.2f}s")

    if dry_run:
        logger.info("[DRY RUN] Skipping write phase")
        # Show sample
        if enriched_docs:
            sample_key, sample_doc = enriched_docs[0]
            logger.info(f"Sample document ({sample_key}):")
            logger.info(f"  genres: {sample_doc.get('genres', [])[:3]}")
            logger.info(f"  cast_names: {sample_doc.get('cast_names', [])[:3]}")
            logger.info(f"  director_id: {sample_doc.get('director_id')}")
            logger.info(f"  director_name: {sample_doc.get('director_name')}")
            logger.info(f"  keywords: {sample_doc.get('keywords', [])[:5]}")
            logger.info(f"  origin_country: {sample_doc.get('origin_country', [])}")
        return

    # Phase 4: Write updated documents
    phase_start = time.time()
    logger.info("[WRITE] Writing updated documents...")

    for i in range(0, len(enriched_docs), WRITE_BATCH):
        batch = enriched_docs[i : i + WRITE_BATCH]
        pipe = redis.pipeline()

        for key, doc in batch:
            pipe.json().set(key, "$", doc)

        await pipe.execute()

        progress = min(i + WRITE_BATCH, len(enriched_docs))
        logger.info(f"[WRITE] {progress:,}/{len(enriched_docs):,}")

    stats.phase_times["write"] = time.time() - phase_start
    logger.info(f"[WRITE] Wrote {len(enriched_docs):,} documents in {stats.phase_times['write']:.2f}s")

    # Phase 5: Recreate index
    phase_start = time.time()
    logger.info("[INDEX] Recreating index with new schema...")

    try:
        await redis.ft(INDEX_NAME).dropindex(delete_documents=False)
        logger.info(f"[INDEX] Dropped {INDEX_NAME}")
    except Exception as e:
        logger.warning(f"[INDEX] Could not drop index (may not exist): {e}")

    schema = build_new_schema()
    definition = IndexDefinition(prefix=["media:"], index_type=IndexType.JSON)

    try:
        await redis.ft(INDEX_NAME).create_index(schema, definition=definition)
        logger.info(f"[INDEX] Created {INDEX_NAME} with new schema")
    except Exception as e:
        logger.error(f"[INDEX] Failed to create index: {e}")
        raise

    stats.phase_times["index"] = time.time() - phase_start
    logger.info(f"[INDEX] Index recreated in {stats.phase_times['index']:.2f}s")


async def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Migrate media index with normalized TAGs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write changes, just show what would be done",
    )
    parser.add_argument(
        "--redis-host",
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port",
    )
    args = parser.parse_args()

    redis_password = os.getenv("REDIS_PASSWORD") or None

    logger.info("=" * 60)
    logger.info("Media TAG Migration")
    logger.info("=" * 60)
    logger.info(f"Redis: {args.redis_host}:{args.redis_port}")
    logger.info(f"Dry run: {args.dry_run}")

    # Initialize
    stats = MigrationStats()
    stats.start_time = time.time()

    redis = Redis(
        host=args.redis_host,
        port=args.redis_port,
        password=redis_password,
        decode_responses=True,
    )

    try:
        # Test connection
        await redis.ping()
        logger.info("Redis connection OK")

        # Initialize services
        tmdb_service = TMDBService()
        iptc_expander = IPTCKeywordExpander()
        logger.info(f"IPTC expander loaded with {len(iptc_expander._alias_map):,} aliases")

        # Run migration
        await migrate_documents(
            redis,
            tmdb_service,
            iptc_expander,
            stats,
            dry_run=args.dry_run,
        )

        # Log summary
        stats.log_summary()

        # Log IPTC stats
        iptc_stats = iptc_expander.stats
        logger.info(f"IPTC stats: {iptc_stats}")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
