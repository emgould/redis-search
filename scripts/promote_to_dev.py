"""
Promote Local Redis to Dev.

This script copies documents from the local Redis index to the public/dev
Redis instance, effectively "syncing" local development work to the dev environment.

Usage:
    # Dry run - show what would be copied
    python scripts/promote_to_dev.py --dry-run

    # Copy all media:* documents from local to dev
    python scripts/promote_to_dev.py

    # Copy and create index if it doesn't exist
    python scripts/promote_to_dev.py --create-index

    # Clear target index before copying
    python scripts/promote_to_dev.py --clear-target
"""

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

# Index name constant
INDEX_NAME = "idx:media"
MEDIA_PREFIX = "media:*"


async def get_redis_connection(
    host: str,
    port: int,
    password: str | None,
    name: str,
) -> Redis:
    """Create and test a Redis connection."""
    redis = Redis(
        host=host,
        port=port,
        password=password,
        decode_responses=True,
    )
    await redis.ping()  # type: ignore[misc]
    return redis


async def get_document_count(redis: Redis) -> int:
    """Count media:* documents in a Redis instance."""
    count = 0
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=MEDIA_PREFIX, count=1000)
        count += len(keys)
        if cursor == 0:
            break
    return count


async def scan_all_keys(redis: Redis, pattern: str = MEDIA_PREFIX) -> list[str]:
    """Scan all keys matching a pattern."""
    keys = []
    cursor = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


async def create_index_if_needed(redis: Redis) -> bool:
    """Create the search index if it doesn't exist. Returns True if created."""
    schema = [
        TextField("$.search_title", as_name="search_title", weight=5.0),
        TagField("$.mc_type", as_name="mc_type"),
        TagField("$.mc_subtype", as_name="mc_subtype"),
        TagField("$.source", as_name="source"),
        NumericField("$.popularity", as_name="popularity", sortable=True),
        NumericField("$.rating", as_name="rating", sortable=True),
        NumericField("$.year", as_name="year", sortable=True),
    ]
    definition = IndexDefinition(prefix=["media:"], index_type=IndexType.JSON)

    try:
        await redis.ft(INDEX_NAME).create_index(schema, definition=definition)
        return True
    except Exception as e:
        if "Index already exists" in str(e):
            return False
        raise


async def clear_media_documents(redis: Redis) -> int:
    """Delete all media:* keys from Redis. Returns count deleted."""
    keys = await scan_all_keys(redis)
    if not keys:
        return 0

    # Delete in batches
    deleted = 0
    batch_size = 100
    for i in range(0, len(keys), batch_size):
        batch = keys[i : i + batch_size]
        await redis.delete(*batch)
        deleted += len(batch)

    return deleted


async def copy_documents(
    source: Redis,
    target: Redis,
    dry_run: bool = False,
    batch_size: int = 100,
) -> tuple[int, int, list[str]]:
    """
    Copy all media:* documents from source to target.

    Returns:
        (copied_count, error_count, error_messages)
    """
    keys = await scan_all_keys(source)

    if not keys:
        return 0, 0, []

    copied = 0
    errors = 0
    error_messages: list[str] = []

    # Process in batches
    for i in range(0, len(keys), batch_size):
        batch_keys = keys[i : i + batch_size]

        if dry_run:
            copied += len(batch_keys)
            continue

        # Get documents from source
        pipeline = source.pipeline()
        for key in batch_keys:
            pipeline.json().get(key)

        try:
            docs = await pipeline.execute()

            # Write to target
            target_pipeline = target.pipeline()
            for key, doc in zip(batch_keys, docs, strict=True):
                if doc is not None:
                    target_pipeline.json().set(key, "$", doc)

            await target_pipeline.execute()
            copied += len([d for d in docs if d is not None])
        except Exception as e:
            errors += len(batch_keys)
            error_messages.append(f"Batch error at {i}: {e}")

    return copied, errors, error_messages


async def main(
    dry_run: bool = False,
    create_index: bool = False,
    clear_target: bool = False,
) -> int:
    """
    Main promote function.

    Returns exit code (0 = success, 1 = error).
    """
    print("=" * 60)
    print("🚀 Promote Local Redis to Dev")
    print("=" * 60)

    # Get configuration
    local_host = os.getenv("REDIS_HOST", "localhost")
    local_port = int(os.getenv("REDIS_PORT", "6380"))
    local_password = os.getenv("REDIS_PASSWORD") or None

    public_host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
    public_port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
    public_password = os.getenv("PUBLIC_REDIS_PASSWORD") or None

    print()
    print("📍 Source (Local):")
    print(f"   Host: {local_host}:{local_port}")
    print(f"   Password: {'***' if local_password else 'None'}")
    print()
    print("🎯 Target (Dev/Public):")
    print(f"   Host: {public_host}:{public_port}")
    print(f"   Password: {'***' if public_password else 'None'}")
    print()

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print()

    # Connect to local Redis
    print("🔌 Connecting to Local Redis...")
    try:
        local_redis = await get_redis_connection(
            local_host, local_port, local_password, "local"
        )
        local_count = await get_document_count(local_redis)
        print(f"   ✅ Connected - {local_count} documents")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return 1

    # Connect to public Redis
    print("🔌 Connecting to Public Redis...")
    try:
        public_redis = await get_redis_connection(
            public_host, public_port, public_password, "public"
        )
        public_count = await get_document_count(public_redis)
        print(f"   ✅ Connected - {public_count} documents")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        print()
        print("💡 Tip: Make sure the IAP tunnel is running:")
        print("   make tunnel")
        await local_redis.aclose()
        return 1

    print()

    # Create index if requested
    if create_index and not dry_run:
        print("📑 Creating search index on target...")
        created = await create_index_if_needed(public_redis)
        if created:
            print("   ✅ Index created")
        else:
            print("   ℹ️  Index already exists")
        print()

    # Clear target if requested
    if clear_target:
        print("🗑️  Clearing target documents...")
        if dry_run:
            print(f"   Would delete {public_count} documents")
        else:
            deleted = await clear_media_documents(public_redis)
            print(f"   ✅ Deleted {deleted} documents")
        print()

    # Copy documents
    print("📦 Copying documents...")
    copied, errors, error_messages = await copy_documents(
        local_redis, public_redis, dry_run=dry_run
    )

    if dry_run:
        print(f"   Would copy {copied} documents")
    else:
        print(f"   ✅ Copied {copied} documents")
        if errors > 0:
            print(f"   ⚠️  {errors} errors")
            for msg in error_messages[:5]:
                print(f"      - {msg}")

    print()

    # Final status
    print("=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"   Source documents: {local_count}")
    if not dry_run:
        final_count = await get_document_count(public_redis)
        print(f"   Target documents: {final_count}")
    print(f"   Documents {'would be ' if dry_run else ''}copied: {copied}")
    if clear_target:
        print(f"   Target {'would be ' if dry_run else ''}cleared: Yes")
    print()

    # Cleanup
    await local_redis.aclose()
    await public_redis.aclose()

    if errors > 0:
        print("❌ Completed with errors")
        return 1

    print("🎉 Promote complete!")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Promote local Redis documents to dev/public Redis"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without making changes",
    )
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="Create the search index on target if it doesn't exist",
    )
    parser.add_argument(
        "--clear-target",
        action="store_true",
        help="Clear all media:* documents from target before copying",
    )

    args = parser.parse_args()

    exit_code = asyncio.run(
        main(
            dry_run=args.dry_run,
            create_index=args.create_index,
            clear_target=args.clear_target,
        )
    )
    sys.exit(exit_code)

