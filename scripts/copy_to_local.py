"""
Copy Public Redis to Local.

This script copies indices and documents from the public/dev Redis instance to the
local Redis instance. It's the reverse of promote_to_dev.py.

Usage:
    # Dry run - show what would be copied
    python scripts/copy_to_local.py --dry-run

    # List available indices
    python scripts/copy_to_local.py --list

    # Copy all indices (complete replacement)
    python scripts/copy_to_local.py

    # Copy specific indices
    python scripts/copy_to_local.py --indices media people

    # Custom batch size (default 1000, overrides COPY_TO_LOCAL_BATCH_SIZE env)
    python scripts/copy_to_local.py --batch-size 500

    # JSON output for API integration
    python scripts/copy_to_local.py --list --json
"""

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import Field, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Load environment
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))

from etl.etl_metadata import ETLMetadataStore, ETLStateConfig  # noqa: E402

INDEX_TO_ETL_JOBS: dict[str, list[str]] = {
    "media": ["tmdb_movie_changes_movie", "tmdb_tv_changes_tv"],
    "people": ["tmdb_person_changes_person"],
    "podcast": ["podcastindex_changes_podcast"],
    "author": ["bestseller_authors_book"],
}


@dataclass
class IndexInfo:
    """Information about a Redis search index."""

    name: str
    redis_name: str
    prefix: str
    num_docs: int
    index_memory_bytes: int
    schema_fields: list[dict]


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


async def list_indices(redis: Redis) -> list[str]:
    """List all search indices in Redis."""
    try:
        indices = await redis.execute_command("FT._LIST")
        return list(indices) if indices else []
    except Exception:
        return []


async def get_index_info(redis: Redis, index_name: str) -> IndexInfo | None:
    """Get detailed information about an index."""
    try:
        info = await redis.ft(index_name).info()

        # Extract prefix from index definition
        prefix = ""
        if "index_definition" in info:
            idx_def = info["index_definition"]
            for j in range(0, len(idx_def), 2):
                if idx_def[j] == "prefixes":
                    prefixes = idx_def[j + 1]
                    if prefixes:
                        prefix = prefixes[0]
                    break

        # Parse schema fields from attributes
        schema_fields = []
        if "attributes" in info:
            attrs = info["attributes"]
            for attr in attrs:
                field_info = {}
                for k in range(0, len(attr), 2):
                    field_info[attr[k]] = attr[k + 1]
                schema_fields.append(field_info)

        friendly_name = index_name
        if index_name.startswith("idx:"):
            friendly_name = index_name[4:]

        return IndexInfo(
            name=friendly_name,
            redis_name=index_name,
            prefix=prefix,
            num_docs=int(info.get("num_docs", 0)),
            index_memory_bytes=int(
                float(info.get("inverted_sz_mb", 0)) * 1024 * 1024
                + float(info.get("vector_index_sz_mb", 0)) * 1024 * 1024
                + float(info.get("offset_vectors_sz_mb", 0)) * 1024 * 1024
            ),
            schema_fields=schema_fields,
        )
    except Exception as e:
        print(f"   ⚠️  Could not get info for index '{index_name}': {e}", file=sys.stderr)
        return None


async def scan_keys_by_prefix(redis: Redis, prefix: str) -> list[str]:
    """Scan all keys matching a prefix pattern."""
    pattern = f"{prefix}*"
    keys = []
    cursor = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


async def count_keys_by_prefix(redis: Redis, prefix: str) -> int:
    """Count all keys matching a prefix pattern."""
    pattern = f"{prefix}*"
    count = 0
    cursor = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        count += len(batch)
        if cursor == 0:
            break
    return count


def build_schema_from_fields(fields: list[dict]) -> list[Field]:
    """Reconstruct Redis schema from field definitions."""
    schema: list[Field] = []
    for field in fields:
        field_type = field.get("type", "").upper()
        identifier = field.get("identifier", "")
        attribute = field.get("attribute", identifier)

        sortable = "SORTABLE" in field.get("flags", []) if "flags" in field else False
        weight = float(field.get("weight", 1.0)) if "weight" in field else 1.0

        if field_type == "TEXT":
            schema.append(
                TextField(identifier, as_name=attribute, weight=weight)
            )
        elif field_type == "TAG":
            schema.append(TagField(identifier, as_name=attribute))
        elif field_type == "NUMERIC":
            schema.append(NumericField(identifier, as_name=attribute, sortable=sortable))

    return schema


async def drop_index_safe(redis: Redis, index_name: str, delete_documents: bool = True) -> bool:
    """Drop an index if it exists. Returns True if dropped.

    Args:
        redis: Redis connection
        index_name: Name of the index to drop
        delete_documents: If True, also delete all documents with the index prefix (DD flag)
    """
    try:
        await redis.ft(index_name).dropindex(delete_documents=delete_documents)
        return True
    except Exception as e:
        if "Unknown index name" in str(e) or "Unknown Index name" in str(e):
            return False
        raise


async def create_index_from_schema(
    redis: Redis,
    index_name: str,
    prefix: str,
    schema_fields: list[dict],
) -> None:
    """Create an index with the given schema."""
    schema = build_schema_from_fields(schema_fields)
    definition = IndexDefinition(prefix=[prefix], index_type=IndexType.JSON)
    await redis.ft(index_name).create_index(schema, definition=definition)


async def copy_documents(
    source: Redis,
    target: Redis,
    prefix: str,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> tuple[int, int, list[str]]:
    """
    Copy all documents with given prefix from source to target.

    Returns:
        (copied_count, error_count, error_messages)
    """
    keys = await scan_keys_by_prefix(source, prefix)

    if not keys:
        return 0, 0, []

    copied = 0
    errors = 0
    error_messages: list[str] = []

    for i in range(0, len(keys), batch_size):
        batch_keys = keys[i : i + batch_size]

        if dry_run:
            copied += len(batch_keys)
            continue

        pipeline = source.pipeline()
        for key in batch_keys:
            pipeline.json().get(key)

        try:
            docs = await pipeline.execute()

            target_pipeline = target.pipeline()
            for key, doc in zip(batch_keys, docs, strict=True):
                if doc is not None:
                    target_pipeline.json().set(key, "$", doc)

            await target_pipeline.execute()
            copied += len([d for d in docs if d is not None])
        except Exception as e:
            errors += len(batch_keys)
            error_messages.append(f"Batch error at {i}: {e}")

        if (i // batch_size) % 10 == 0 and i > 0:
            print(f"      Progress: {copied:,} documents copied...")

    return copied, errors, error_messages


async def copy_index(
    source: Redis,
    target: Redis,
    index_info: IndexInfo,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> dict:
    """
    Copy a single index from source (public) to target (local).

    This performs a complete replacement:
    1. Drop the index on target with DD flag (deletes index and all documents)
    2. Recreate the index on target with same schema
    3. Copy all documents from source to target

    Returns dict with results.
    """
    result = {
        "index": index_info.name,
        "redis_name": index_info.redis_name,
        "prefix": index_info.prefix,
        "success": False,
        "source_docs": 0,
        "docs_copied": 0,
        "errors": 0,
        "error_messages": [],
    }

    prefix = index_info.prefix

    try:
        source_count = await count_keys_by_prefix(source, prefix)
        result["source_docs"] = source_count
        print(f"      Source documents: {source_count:,}")

        if dry_run:
            target_count = await count_keys_by_prefix(target, prefix)
            print(f"      Would drop index and delete {target_count:,} documents from target")
            print(f"      Would recreate index '{index_info.redis_name}'")
            print(f"      Would copy {source_count:,} documents")
            result["docs_copied"] = source_count
            result["success"] = True
            return result

        print("      Dropping target index (with documents)...")
        dropped = await drop_index_safe(target, index_info.redis_name, delete_documents=True)
        if dropped:
            print("      Index and documents dropped")
        else:
            print("      Index did not exist")

        print("      Creating index with schema...")
        await create_index_from_schema(
            target,
            index_info.redis_name,
            prefix,
            index_info.schema_fields,
        )
        print("      Index created")

        print("      Copying documents...")
        copied, errors, error_msgs = await copy_documents(
            source, target, prefix, dry_run=False, batch_size=batch_size
        )
        result["docs_copied"] = copied
        result["errors"] = errors
        result["error_messages"] = error_msgs
        print(f"      Copied {copied:,} documents")

        if errors > 0:
            print(f"      ⚠️  {errors} errors during copy")

        result["success"] = True

    except Exception as e:
        result["error_messages"].append(str(e))
        print(f"      ❌ Error: {e}")

    return result


async def list_available_indices(
    public_host: str,
    public_port: int,
    public_password: str | None,
    output_json: bool = False,
) -> list[IndexInfo]:
    """List all available indices from public Redis."""
    redis = await get_redis_connection(public_host, public_port, public_password, "public")

    indices = await list_indices(redis)
    index_infos = []

    for idx_name in indices:
        info = await get_index_info(redis, idx_name)
        if info:
            index_infos.append(info)

    await redis.aclose()

    if output_json:
        print(json.dumps([asdict(i) for i in index_infos], indent=2))
    else:
        print("\n📋 Available Indices (from Public Redis):")
        print("-" * 60)
        for info in index_infos:
            print(f"  • {info.name}")
            print(f"    Redis name: {info.redis_name}")
            print(f"    Prefix: {info.prefix}")
            print(f"    Documents: {info.num_docs:,}")
            print(f"    Fields: {len(info.schema_fields)}")
            print()

    return index_infos


def sync_etl_metadata(copied_indices: list[str], dry_run: bool = False) -> None:
    """Sync ETL job states from source (dev/prod) to local for copied indices."""
    jobs_to_sync: set[str] = set()
    for idx_name in copied_indices:
        friendly = idx_name[4:] if idx_name.startswith("idx:") else idx_name
        if friendly in INDEX_TO_ETL_JOBS:
            jobs_to_sync.update(INDEX_TO_ETL_JOBS[friendly])

    if not jobs_to_sync:
        return

    gcs_bucket = os.getenv("GCS_BUCKET")
    if not gcs_bucket:
        print("   ⚠️  GCS_BUCKET not set, skipping ETL metadata sync")
        return

    source_prefix = os.getenv("PUBLIC_GCS_ETL_PREFIX", "redis-search/etl/dev")
    local_prefix = os.getenv("GCS_ETL_PREFIX", "redis-search/etl/local")

    if source_prefix == local_prefix:
        print("   ⚠️  Source and local GCS prefixes are identical, skipping metadata sync")
        return

    print(f"   Syncing ETL metadata: {source_prefix} → {local_prefix}")

    source_store = ETLMetadataStore(
        config=ETLStateConfig(gcs_bucket=gcs_bucket, gcs_prefix=source_prefix)
    )
    local_store = ETLMetadataStore(
        config=ETLStateConfig(gcs_bucket=gcs_bucket, gcs_prefix=local_prefix)
    )

    source_states = source_store.get_all_job_states()
    local_states = local_store.get_all_job_states()

    synced: list[str] = []
    for job_name in sorted(jobs_to_sync):
        if job_name in source_states:
            state = source_states[job_name]
            if dry_run:
                print(f"      Would sync {job_name}: {state.last_run_date} ({state.last_status})")
            else:
                local_states[job_name] = state
                synced.append(job_name)
                print(f"      Synced {job_name}: {state.last_run_date} ({state.last_status})")
        else:
            print(f"      ⚠️  No source state for {job_name}")

    if synced:
        local_store.save_job_states(local_states)
        print(f"   ✅ Saved {len(synced)} job state(s) to local metadata")


async def main(
    dry_run: bool = False,
    indices_to_copy: list[str] | None = None,
    list_only: bool = False,
    output_json: bool = False,
    batch_size: int = 1000,
) -> int:
    """
    Main copy function.

    Args:
        dry_run: Show what would be done without making changes
        indices_to_copy: List of index names to copy (None = all)
        list_only: Just list available indices
        output_json: Output JSON format (for API integration)
        batch_size: Documents per pipeline batch (env: COPY_TO_LOCAL_BATCH_SIZE)

    Returns exit code (0 = success, 1 = error).
    """
    # Get configuration - NOTE: source is PUBLIC, target is LOCAL
    local_host = os.getenv("REDIS_HOST", "localhost")
    local_port = int(os.getenv("REDIS_PORT", "6380"))
    local_password = os.getenv("REDIS_PASSWORD") or None

    public_host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
    public_port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
    public_password = os.getenv("PUBLIC_REDIS_PASSWORD") or None

    # List only mode
    if list_only:
        await list_available_indices(
            public_host, public_port, public_password, output_json
        )
        return 0

    print("=" * 60)
    print("📥 Copy Public Redis to Local")
    print("=" * 60)

    print()
    print("📍 Source (Public/Dev):")
    print(f"   Host: {public_host}:{public_port}")
    print(f"   Password: {'***' if public_password else 'None'}")
    print()
    print("🎯 Target (Local):")
    print(f"   Host: {local_host}:{local_port}")
    print(f"   Password: {'***' if local_password else 'None'}")
    print()

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print()

    print(f"   Batch size: {batch_size:,} documents per pipeline")
    print()

    # Connect to public Redis (SOURCE)
    print("🔌 Connecting to Public Redis...")
    try:
        public_redis = await get_redis_connection(
            public_host, public_port, public_password, "public"
        )
        print("   ✅ Connected")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        print()
        print("💡 Tip: Make sure the IAP tunnel is running:")
        print("   make tunnel")
        return 1

    # Connect to local Redis (TARGET)
    print("🔌 Connecting to Local Redis...")
    try:
        local_redis = await get_redis_connection(
            local_host, local_port, local_password, "local"
        )
        print("   ✅ Connected")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        await public_redis.aclose()
        return 1

    print()

    # Discover available indices from PUBLIC
    print("📋 Discovering indices from public Redis...")
    indices = await list_indices(public_redis)
    print(f"   Found {len(indices)} indices: {', '.join(indices)}")
    print()

    # Get info for each index
    available_indices: list[IndexInfo] = []
    for idx_name in indices:
        info = await get_index_info(public_redis, idx_name)
        if info:
            available_indices.append(info)

    # Filter to requested indices if specified
    if indices_to_copy:
        filtered = []
        for info in available_indices:
            if info.name in indices_to_copy or info.redis_name in indices_to_copy:
                filtered.append(info)

        known_names = {i.name for i in available_indices} | {
            i.redis_name for i in available_indices
        }
        unknown = [i for i in indices_to_copy if i not in known_names]
        if unknown:
            print(f"⚠️  Unknown indices (will be skipped): {', '.join(unknown)}")
            print()

        available_indices = filtered

    if not available_indices:
        print("❌ No indices to copy")
        await public_redis.aclose()
        await local_redis.aclose()
        return 1

    # Copy each index
    print(f"📥 Copying {len(available_indices)} indices...")
    print()

    results = []
    total_copied = 0
    total_errors = 0

    for info in available_indices:
        print(f"   📦 Index: {info.name} ({info.redis_name})")
        # NOTE: source is public, target is local
        result = await copy_index(
            public_redis, local_redis, info, dry_run=dry_run, batch_size=batch_size
        )
        results.append(result)
        total_copied += result["docs_copied"]
        total_errors += result["errors"]
        if result["success"]:
            print("      ✅ Success")
        else:
            print("      ❌ Failed")
        print()

    # Sync ETL metadata for successfully copied indices
    successfully_copied = [r["index"] for r in results if r["success"]]
    if successfully_copied:
        print("📋 Syncing ETL metadata...")
        sync_etl_metadata(successfully_copied, dry_run=dry_run)
        print()

    # Final summary
    print("=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"   Indices copied: {len([r for r in results if r['success']])}/{len(results)}")
    print(f"   Total documents {'would be ' if dry_run else ''}copied: {total_copied:,}")
    if total_errors > 0:
        print(f"   Total errors: {total_errors}")
    print()

    # Cleanup
    await public_redis.aclose()
    await local_redis.aclose()

    if total_errors > 0 or any(not r["success"] for r in results):
        print("❌ Completed with errors")
        return 1

    print("🎉 Copy complete!")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy public Redis indices and documents to local Redis"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available indices and exit",
    )
    parser.add_argument(
        "--indices",
        nargs="+",
        help="Specific indices to copy (by name, e.g., 'media people')",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON format (for API integration)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("COPY_TO_LOCAL_BATCH_SIZE", "1000")),
        help="Documents per pipeline batch (default: 1000, env: COPY_TO_LOCAL_BATCH_SIZE)",
    )

    args = parser.parse_args()

    exit_code = asyncio.run(
        main(
            dry_run=args.dry_run,
            indices_to_copy=args.indices,
            list_only=args.list,
            output_json=args.json,
            batch_size=args.batch_size,
        )
    )
    sys.exit(exit_code)

