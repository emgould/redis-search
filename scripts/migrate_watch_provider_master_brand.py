"""
Migrate watch_providers to add is_master_brand flag and filter platform IDs.

This script:
1. Scans all media:* documents in Redis
2. For each watch_providers entry, adds is_master_brand to provider objects
3. Filters streaming_platform_ids and on_demand_platform_ids to master brands only
4. Writes updated documents back to Redis

No TMDB API calls - uses local provider_map.json for master brand lookup.
"""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from redis.asyncio import Redis

# Add src to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

from api.tmdb.utils.provider_utils import (  # noqa: E402
    get_full_provider_map,
    get_provider_display_map,
)
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

# Constants
SCAN_BATCH = 10000
MGET_BATCH = 500
WRITE_BATCH = 500


class MigrationStats:
    """Track migration statistics."""

    def __init__(self) -> None:
        self.total_docs = 0
        self.docs_with_watch_providers = 0
        self.docs_updated = 0
        self.docs_skipped = 0
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
        logger.info(f"Total documents scanned: {self.total_docs:,}")
        logger.info(f"Documents with watch_providers: {self.docs_with_watch_providers:,}")
        logger.info(f"Documents updated: {self.docs_updated:,}")
        logger.info(f"Documents skipped (already migrated): {self.docs_skipped:,}")
        logger.info(f"Errors: {self.errors:,}")
        logger.info(f"Total time: {elapsed:.2f}s")
        for phase, duration in self.phase_times.items():
            logger.info(f"  {phase}: {duration:.2f}s")
        if self.error_messages:
            logger.info("First 5 errors:")
            for msg in self.error_messages[:5]:
                logger.info(f"  - {msg}")


def build_master_brand_lookup() -> set[str]:
    """Build a set of provider IDs that are master brands."""
    provider_display_map = get_provider_display_map()
    provider_map = get_full_provider_map(provider_display_map)
    # provider_map keys are the provider IDs (as strings) that map to master brand entries
    return set(provider_map.keys())


def add_is_master_brand_to_provider(
    provider: dict[str, Any], master_brand_ids: set[str]
) -> dict[str, Any]:
    """Add is_master_brand flag to a provider dict."""
    provider_id = provider.get("provider_id")
    is_master = str(provider_id) in master_brand_ids if provider_id is not None else False
    return {**provider, "is_master_brand": is_master}


def migrate_watch_providers(
    watch_providers: dict[str, Any], master_brand_ids: set[str]
) -> tuple[dict[str, Any], bool]:
    """
    Migrate watch_providers to include is_master_brand and filter IDs.

    Returns:
        Tuple of (migrated_watch_providers, was_changed)
    """
    # Check if already migrated (primary_provider has is_master_brand)
    primary = watch_providers.get("primary_provider")
    if isinstance(primary, dict) and "is_master_brand" in primary:
        return watch_providers, False

    migrated = dict(watch_providers)

    # Migrate streaming_platforms
    streaming_platforms = watch_providers.get("streaming_platforms")
    if isinstance(streaming_platforms, list):
        migrated_streaming = [
            add_is_master_brand_to_provider(p, master_brand_ids)
            for p in streaming_platforms
            if isinstance(p, dict)
        ]
        migrated["streaming_platforms"] = migrated_streaming
        # Filter streaming_platform_ids to master brands only
        migrated["streaming_platform_ids"] = [
            str(p.get("provider_id"))
            for p in migrated_streaming
            if p.get("is_master_brand") and p.get("provider_id") is not None
        ]

    # Migrate on_demand_platforms
    on_demand_platforms = watch_providers.get("on_demand_platforms")
    if isinstance(on_demand_platforms, list):
        migrated_on_demand = [
            add_is_master_brand_to_provider(p, master_brand_ids)
            for p in on_demand_platforms
            if isinstance(p, dict)
        ]
        migrated["on_demand_platforms"] = migrated_on_demand
        # Filter on_demand_platform_ids to master brands only
        migrated["on_demand_platform_ids"] = [
            str(p.get("provider_id"))
            for p in migrated_on_demand
            if p.get("is_master_brand") and p.get("provider_id") is not None
        ]

    # Migrate primary_provider
    if isinstance(primary, dict):
        migrated["primary_provider"] = add_is_master_brand_to_provider(
            primary, master_brand_ids
        )

    return migrated, True


async def migrate_documents(
    redis: Redis,
    stats: MigrationStats,
    master_brand_ids: set[str],
    dry_run: bool = False,
) -> None:
    """Migrate all media documents with is_master_brand flag."""
    # Phase 1: Scan all keys
    phase_start = time.time()
    logger.info("[SCAN] Collecting all media:* keys...")

    keys: list[str] = []
    async for key in redis.scan_iter(match="media:*", count=SCAN_BATCH):
        keys.append(key if isinstance(key, str) else key.decode())

    stats.total_docs = len(keys)
    stats.phase_times["scan"] = time.time() - phase_start
    logger.info(f"[SCAN] Found {stats.total_docs:,} documents in {stats.phase_times['scan']:.2f}s")

    # Phase 2: Process in batches
    phase_start = time.time()
    logger.info("[MIGRATE] Processing documents...")

    for batch_start in range(0, len(keys), MGET_BATCH):
        batch_keys = keys[batch_start : batch_start + MGET_BATCH]

        # Read batch
        pipe = redis.pipeline()
        for key in batch_keys:
            pipe.json().get(key)
        docs = await pipe.execute()

        # Process batch
        updates: list[tuple[str, dict[str, Any]]] = []
        for key, doc in zip(batch_keys, docs, strict=True):
            if not isinstance(doc, dict):
                continue

            watch_providers = doc.get("watch_providers")
            if not isinstance(watch_providers, dict):
                continue

            stats.docs_with_watch_providers += 1

            try:
                migrated_wp, was_changed = migrate_watch_providers(
                    watch_providers, master_brand_ids
                )
                if was_changed:
                    updates.append((key, migrated_wp))
                    stats.docs_updated += 1
                else:
                    stats.docs_skipped += 1
            except Exception as e:
                stats.errors += 1
                if len(stats.error_messages) < 10:
                    stats.error_messages.append(f"{key}: {e}")

        # Write batch
        if updates and not dry_run:
            write_pipe = redis.pipeline()
            for key, migrated_wp in updates:
                write_pipe.json().set(key, "$.watch_providers", migrated_wp)
            await write_pipe.execute()

        # Progress
        processed = min(batch_start + MGET_BATCH, len(keys))
        if processed % 10000 == 0 or processed == len(keys):
            logger.info(
                f"[MIGRATE] Processed {processed:,}/{stats.total_docs:,} "
                f"({100 * processed / stats.total_docs:.1f}%) - "
                f"updated: {stats.docs_updated:,}, skipped: {stats.docs_skipped:,}"
            )

    stats.phase_times["migrate"] = time.time() - phase_start
    logger.info(f"[MIGRATE] Completed in {stats.phase_times['migrate']:.2f}s")


async def main(dry_run: bool = False) -> None:
    """Main migration entry point."""
    stats = MigrationStats()
    stats.start_time = time.time()

    logger.info("=" * 60)
    logger.info("Watch Provider Master Brand Migration")
    logger.info("=" * 60)
    if dry_run:
        logger.info("DRY RUN - no changes will be written")

    # Build master brand lookup
    logger.info("[INIT] Building master brand lookup from provider_map.json...")
    master_brand_ids = build_master_brand_lookup()
    logger.info(f"[INIT] Found {len(master_brand_ids)} master brand provider IDs")

    # Connect to Redis
    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )

    try:
        await migrate_documents(redis, stats, master_brand_ids, dry_run)
    finally:
        await redis.aclose()

    stats.log_summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate watch_providers to add is_master_brand flag"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Redis",
    )
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run))
