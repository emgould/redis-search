"""
Migrate media index documents with local-only normalization.

This script:
1. Scans all media:* documents in Redis.
2. Normalizes TAG and provider-id fields already stored in documents.
3. Writes updated documents back to Redis.
4. Recreates the index with the media schema from web.app.
"""

import argparse
import asyncio
import os
import logging
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Add src to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))


def _normalize_tag(value: str) -> str | None:
    """Import-normalize tag values lazily after path setup."""
    from core.iptc import normalize_tag

    return normalize_tag(value)


def _get_logger() -> logging.Logger:
    """Import logger lazily after path setup."""
    from utils.get_logger import get_logger

    return get_logger(__name__)

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

logger = _get_logger()

# Constants
INDEX_NAME = "idx:media"
SCAN_BATCH = 10000
MGET_BATCH = 500
WRITE_BATCH = 500


class MigrationStats:
    """Track migration statistics."""

    def __init__(self) -> None:
        self.total_docs = 0
        self.enriched = 0
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
        doc["genres"] = [_normalize_tag(g) for g in doc["genres"] if _normalize_tag(g)]

    # Normalize cast_names (keep cast for display)
    if "cast_names" in doc and doc["cast_names"]:
        doc["cast_names"] = [_normalize_tag(n) for n in doc["cast_names"] if _normalize_tag(n)]

    watch_providers = doc.get("watch_providers")
    if isinstance(watch_providers, dict):
        normalized_watch_providers = dict(watch_providers)
        if "streaming_platform_ids" in watch_providers:
            normalized_watch_providers["streaming_platform_ids"] = _normalize_id_array(
                watch_providers.get("streaming_platform_ids")
            )
        if "on_demand_platform_ids" in watch_providers:
            normalized_watch_providers["on_demand_platform_ids"] = _normalize_id_array(
                watch_providers.get("on_demand_platform_ids")
            )
        doc["watch_providers"] = normalized_watch_providers

    return doc


def _normalize_id_array(values: Any) -> list[str]:
    """Normalize ID arrays to strings for TagField compatibility."""
    if values is None or isinstance(values, bool):
        return []
    if isinstance(values, int):
        return [str(values)]
    if isinstance(values, str):
        normalized = values.strip()
        return [normalized] if normalized else []
    if not isinstance(values, list):
        return []

    output: list[str] = []
    for value in values:
        output.extend(_normalize_id_array(value))
    return output


async def migrate_documents(
    redis: Redis,
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

    # Phase 3: Normalize documents in batches
    phase_start = time.time()
    logger.info("[NORMALIZE] Normalizing existing documents...")

    def normalize_one(key: str, doc: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        """Normalize a single document."""
        try:
            # Normalize existing tags
            doc = normalize_existing_tags(doc)

            stats.enriched += 1
            return key, doc

        except Exception as e:
            stats.errors += 1
            if len(stats.error_messages) < 10:
                stats.error_messages.append(f"{key}: {e}")
            return key, None

    enriched_docs: list[tuple[str, dict[str, Any]]] = []
    valid_docs = [(k, d) for k, d in all_docs if d is not None]
    if not valid_docs:
        logger.warning("No readable media documents found!")
        return

    for i in range(0, len(valid_docs), MGET_BATCH):
        batch = valid_docs[i : i + MGET_BATCH]
        for key, doc in batch:
            key, normalized = normalize_one(key, doc)
            if normalized is not None:
                enriched_docs.append((key, normalized))

        progress = min(i + MGET_BATCH, len(valid_docs))
        if progress % (MGET_BATCH * 10) == 0 or progress == len(valid_docs):
            pct = (progress / len(valid_docs)) * 100
            logger.info(f"[NORMALIZE] {progress:,}/{len(valid_docs):,} ({pct:.0f}%)")

    if enriched_docs:
        sample_key, sample_doc = enriched_docs[0]
        sample_watch_providers = sample_doc.get("watch_providers", {})
        logger.info(f"Sample document ({sample_key}):")
        logger.info(f"  genres: {sample_doc.get('genres', [])[:3]}")
        logger.info(f"  cast_names: {sample_doc.get('cast_names', [])[:3]}")
        logger.info(
            f"  streaming_platform_ids: {sample_watch_providers.get('streaming_platform_ids', [])}"
        )
        logger.info(
            f"  on_demand_platform_ids: {sample_watch_providers.get('on_demand_platform_ids', [])}"
        )

    stats.phase_times["enrich"] = time.time() - phase_start
    logger.info(f"[NORMALIZE] Enriched {len(enriched_docs):,} documents in {stats.phase_times['enrich']:.2f}s")

    if dry_run:
        logger.info("[DRY RUN] Skipping write phase")
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

        # Run migration
        await migrate_documents(
            redis,
            stats,
            dry_run=args.dry_run,
        )

        # Log summary
        stats.log_summary()

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
