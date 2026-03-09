"""
Bulk Clone Prefix - Tunnel-compatible prefix transfer using DUMP/RESTORE.

Transfers selected key prefixes from public Redis to local/scratch Redis
using Redis-native DUMP/RESTORE serialization. Significantly faster than
document-level JSON.GET/JSON.SET rehydration because it avoids Python-level
JSON deserialization/reserialization and transfers raw binary payloads.

Usage:
    # Clone media prefix to scratch (safe default)
    python scripts/bulk_clone_prefix.py --prefixes media:

    # Clone multiple prefixes
    python scripts/bulk_clone_prefix.py --prefixes media: person:

    # Clone all known prefixes to scratch
    python scripts/bulk_clone_prefix.py --prefixes all

    # Clone to primary local (requires confirmation)
    python scripts/bulk_clone_prefix.py --prefixes media: --target local --confirm-replace

    # Dry run - count keys only
    python scripts/bulk_clone_prefix.py --prefixes media: --dry-run

    # Validate only - compare without transfer
    python scripts/bulk_clone_prefix.py --prefixes media: --validate-only

    # Custom batch size (default 500)
    python scripts/bulk_clone_prefix.py --prefixes media: --batch-size 500
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis
from redis.commands.search.field import Field, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))

from etl.etl_metadata import ETLMetadataStore, ETLStateConfig  # noqa: E402

PREFIX_TO_INDEX: dict[str, str] = {
    "media:": "idx:media",
    "person:": "idx:people",
    "podcast:": "idx:podcasts",
    "book:": "idx:book",
    "author:": "idx:author",
}

INDEX_TO_ETL_JOBS: dict[str, list[str]] = {
    "media": ["tmdb_movie_changes_movie", "tmdb_tv_changes_tv"],
    "people": ["tmdb_person_changes_person"],
    "podcast": ["podcastindex_changes_podcast"],
    "author": ["bestseller_authors_book"],
}

TARGETS: dict[str, dict[str, object]] = {
    "scratch": {
        "host": "localhost",
        "port": 6382,
        "password": None,
        "label": "Scratch Redis (localhost:6382)",
    },
    "local": {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", "6380")),
        "password": os.getenv("REDIS_PASSWORD") or None,
        "label": f"Local Redis ({os.getenv('REDIS_HOST', 'localhost')}:"
        f"{os.getenv('REDIS_PORT', '6380')})",
    },
}


@dataclass
class TransferResult:
    prefix: str
    keys_found: int
    keys_transferred: int
    errors: int
    error_messages: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return self.errors == 0 and self.keys_transferred == self.keys_found


@dataclass
class IndexSchema:
    name: str
    prefix: str
    fields: list[dict[str, str]]


async def get_meta_connection(
    host: str, port: int, password: str | None, label: str
) -> Redis:
    """Create a decoded connection for metadata operations."""
    redis: Redis = Redis(
        host=host, port=port, password=password, decode_responses=True
    )
    await redis.ping()  # type: ignore[misc]
    print(f"   Connected to {label}")
    return redis


async def get_raw_connection(
    host: str, port: int, password: str | None
) -> Redis:
    """Create a raw (bytes) connection for DUMP/RESTORE operations."""
    redis: Redis = Redis(
        host=host, port=port, password=password, decode_responses=False
    )
    await redis.ping()  # type: ignore[misc]
    return redis


async def scan_keys(redis: Redis, prefix: str) -> list[str]:
    """Scan all keys matching a prefix."""
    pattern = f"{prefix}*"
    keys: list[str] = []
    cursor: int = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


async def count_keys(redis: Redis, prefix: str) -> int:
    """Count keys matching a prefix."""
    pattern = f"{prefix}*"
    count = 0
    cursor: int = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        count += len(batch)
        if cursor == 0:
            break
    return count


async def get_index_schema(redis: Redis, index_name: str) -> IndexSchema | None:
    """Introspect an FT index schema from Redis."""
    try:
        info = await redis.ft(index_name).info()
    except Exception:
        return None

    prefix = ""
    if "index_definition" in info:
        idx_def = info["index_definition"]
        for j in range(0, len(idx_def), 2):
            if idx_def[j] == "prefixes":
                prefixes = idx_def[j + 1]
                if prefixes:
                    prefix = prefixes[0]
                break

    schema_fields: list[dict[str, str]] = []
    if "attributes" in info:
        for attr in info["attributes"]:
            field_info: dict[str, str] = {}
            for k in range(0, len(attr), 2):
                field_info[attr[k]] = attr[k + 1]
            schema_fields.append(field_info)

    return IndexSchema(name=index_name, prefix=prefix, fields=schema_fields)


def build_schema_from_fields(fields: list[dict[str, str]]) -> list[Field]:
    """Reconstruct a Redis search schema from introspected field definitions."""
    schema: list[Field] = []
    for f in fields:
        field_type = f.get("type", "").upper()
        identifier = f.get("identifier", "")
        attribute = f.get("attribute", identifier)
        sortable = "SORTABLE" in f.get("flags", "") if "flags" in f else False
        weight = float(f.get("weight", 1.0)) if "weight" in f else 1.0

        if field_type == "TEXT":
            schema.append(TextField(identifier, as_name=attribute, weight=weight))
        elif field_type == "TAG":
            schema.append(TagField(identifier, as_name=attribute))
        elif field_type == "NUMERIC":
            schema.append(
                NumericField(identifier, as_name=attribute, sortable=sortable)
            )

    return schema


async def drop_index_safe(
    redis: Redis, index_name: str, delete_documents: bool = False
) -> bool:
    """Drop an FT index if it exists. Does NOT delete documents by default."""
    try:
        await redis.ft(index_name).dropindex(delete_documents=delete_documents)
        return True
    except Exception as e:
        if "Unknown index name" in str(e) or "Unknown Index name" in str(e):
            return False
        raise


async def recreate_index(
    target_meta: Redis, index_name: str, schema: IndexSchema
) -> None:
    """Drop and recreate an FT index on the target using introspected schema."""
    dropped = await drop_index_safe(target_meta, index_name, delete_documents=False)
    if dropped:
        print(f"      Dropped existing index '{index_name}' (documents preserved)")

    ft_schema = build_schema_from_fields(schema.fields)
    definition = IndexDefinition(prefix=[schema.prefix], index_type=IndexType.JSON)
    await target_meta.ft(index_name).create_index(ft_schema, definition=definition)
    print(f"      Created index '{index_name}' (prefix: {schema.prefix})")


async def delete_keys_by_prefix(redis: Redis, prefix: str) -> int:
    """Delete all keys matching a prefix on the target before transfer."""
    keys = await scan_keys(redis, prefix)
    if not keys:
        return 0
    batch_size = 1000
    deleted = 0
    for i in range(0, len(keys), batch_size):
        batch = keys[i : i + batch_size]
        deleted += await redis.delete(*batch)
    return deleted


async def transfer_prefix(
    source_meta: Redis,
    source_raw: Redis,
    target_raw: Redis,
    prefix: str,
    batch_size: int = 500,
    dry_run: bool = False,
) -> TransferResult:
    """Transfer all keys with a given prefix using DUMP/RESTORE."""
    start = time.monotonic()

    print(f"   Scanning keys with prefix '{prefix}'...")
    keys = await scan_keys(source_meta, prefix)

    if not keys:
        print("   No keys found")
        return TransferResult(prefix=prefix, keys_found=0, keys_transferred=0, errors=0)

    print(f"   Found {len(keys):,} keys")

    if dry_run:
        elapsed = time.monotonic() - start
        return TransferResult(
            prefix=prefix,
            keys_found=len(keys),
            keys_transferred=len(keys),
            errors=0,
            elapsed_seconds=elapsed,
        )

    transferred = 0
    errors = 0
    error_messages: list[str] = []

    for i in range(0, len(keys), batch_size):
        batch = keys[i : i + batch_size]

        pipe = source_raw.pipeline()
        for key in batch:
            pipe.dump(key)
            pipe.pttl(key)

        try:
            results = await pipe.execute()
        except Exception as e:
            errors += len(batch)
            error_messages.append(f"Source DUMP batch at offset {i}: {e}")
            continue

        target_pipe = target_raw.pipeline()
        valid_count = 0
        for j, key in enumerate(batch):
            dump_val = results[j * 2]
            pttl_val = results[j * 2 + 1]
            if dump_val is not None:
                ttl = max(pttl_val, 0)
                target_pipe.restore(key, ttl, dump_val, replace=True)
                valid_count += 1

        if valid_count > 0:
            try:
                await target_pipe.execute()
                transferred += valid_count
            except Exception as e:
                errors += valid_count
                error_messages.append(f"Target RESTORE batch at offset {i}: {e}")

        if (i // batch_size) % 10 == 0 and i > 0:
            elapsed = time.monotonic() - start
            rate = transferred / elapsed if elapsed > 0 else 0
            print(f"      Progress: {transferred:,}/{len(keys):,} ({rate:.0f} keys/sec)")

    elapsed = time.monotonic() - start
    return TransferResult(
        prefix=prefix,
        keys_found=len(keys),
        keys_transferred=transferred,
        errors=errors,
        error_messages=error_messages,
        elapsed_seconds=elapsed,
    )


def sync_etl_metadata(
    transferred_prefixes: list[str], dry_run: bool = False
) -> None:
    """Sync ETL job states from public to local for transferred prefixes."""
    jobs_to_sync: set[str] = set()
    for prefix in transferred_prefixes:
        friendly = PREFIX_TO_INDEX.get(prefix, "")
        if friendly.startswith("idx:"):
            friendly = friendly[4:]
        if friendly in INDEX_TO_ETL_JOBS:
            jobs_to_sync.update(INDEX_TO_ETL_JOBS[friendly])

    if not jobs_to_sync:
        return

    gcs_bucket = os.getenv("GCS_BUCKET")
    if not gcs_bucket:
        print("   GCS_BUCKET not set, skipping ETL metadata sync")
        return

    source_prefix = os.getenv("PUBLIC_GCS_ETL_PREFIX", "redis-search/etl/dev")
    local_prefix = os.getenv("GCS_ETL_PREFIX", "redis-search/etl/local")

    if source_prefix == local_prefix:
        print("   Source and local GCS prefixes are identical, skipping metadata sync")
        return

    print(f"   Syncing ETL metadata: {source_prefix} -> {local_prefix}")

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
                print(
                    f"      Would sync {job_name}: "
                    f"{state.last_run_date} ({state.last_status})"
                )
            else:
                local_states[job_name] = state
                synced.append(job_name)
                print(
                    f"      Synced {job_name}: "
                    f"{state.last_run_date} ({state.last_status})"
                )
        else:
            print(f"      No source state for {job_name}")

    if synced:
        local_store.save_job_states(local_states)
        print(f"   Saved {len(synced)} job state(s) to local metadata")


def _check_sibling_redis(target_name: str) -> tuple[bool, str]:
    """Check if a sibling Redis instance is running that could cause OOM.

    Returns (is_running, description) for the *other* Redis instance.
    """
    sibling = "local" if target_name == "scratch" else "scratch"
    sibling_port = 6380 if sibling == "local" else 6382

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t")
            name = parts[0].lower()
            if sibling == "local" and "redis-search-redis-1" in name:
                return True, f"{sibling} Redis on port {sibling_port} ({parts[0]})"
            if sibling == "scratch" and "scratch" in name and "redis" in name:
                return True, f"{sibling} Redis on port {sibling_port} ({parts[0]})"
    except Exception:
        pass
    return False, ""


def _prompt_sibling_warning(sibling_desc: str) -> bool:
    """Warn about a running sibling Redis and prompt to continue or abort."""
    print(f"   WARNING: {sibling_desc} is also running.")
    print(
        "   Running two large Redis instances simultaneously can cause OOM kills."
    )
    print(
        "   Recommendation: shut down the other instance first to avoid "
        "memory pressure."
    )
    if not sys.stdin.isatty():
        print("   Non-interactive mode: proceeding (use --skip-sibling-check to suppress).")
        return True
    try:
        answer = input("   Continue anyway? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ("y", "yes")


async def preflight_checks(
    source_meta: Redis,
    target_meta: Redis,
    target_name: str,
    confirm_replace: bool,
    skip_sibling_check: bool = False,
) -> bool:
    """Run safety checks before transfer."""
    if not skip_sibling_check:
        sibling_running, sibling_desc = _check_sibling_redis(target_name)
        if sibling_running and not _prompt_sibling_warning(sibling_desc):
            print("   Aborted by user.")
            return False

    source_version = (await source_meta.info("server")).get("redis_version", "unknown")
    target_version = (await target_meta.info("server")).get("redis_version", "unknown")

    print(f"   Source Redis version: {source_version}")
    print(f"   Target Redis version: {target_version}")

    if source_version.split(".")[:2] != target_version.split(".")[:2]:
        print(
            f"   WARNING: Major/minor version mismatch "
            f"({source_version} vs {target_version})"
        )

    source_modules = {
        m["name"] for m in await source_meta.module_list()  # type: ignore[union-attr]
    }
    target_modules = {
        m["name"] for m in await target_meta.module_list()  # type: ignore[union-attr]
    }
    missing = source_modules - target_modules
    if missing:
        print(f"   ERROR: Target is missing modules: {missing}")
        return False

    if target_name == "local" and not confirm_replace:
        target_dbsize = await target_meta.dbsize()
        if target_dbsize > 0:
            print(
                f"   ERROR: Target has {target_dbsize:,} keys. "
                f"Use --confirm-replace to overwrite."
            )
            return False

    return True


async def main(
    prefixes: list[str],
    target_name: str = "scratch",
    dry_run: bool = False,
    validate_only: bool = False,
    confirm_replace: bool = False,
    batch_size: int = 500,
    output_json: bool = False,
    skip_sibling_check: bool = False,
) -> int:
    """Run the bulk prefix clone."""
    if prefixes == ["all"]:
        prefixes = list(PREFIX_TO_INDEX.keys())

    target_cfg = TARGETS[target_name]
    target_host = str(target_cfg["host"])
    target_port = int(target_cfg["port"])  # type: ignore[arg-type]
    target_password = target_cfg["password"]

    source_host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
    source_port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
    source_password = os.getenv("PUBLIC_REDIS_PASSWORD") or None

    print("=" * 60)
    print("Bulk Clone Prefix (DUMP/RESTORE)")
    print("=" * 60)
    print()
    print(f"   Source: {source_host}:{source_port} (public)")
    print(f"   Target: {target_cfg['label']} ({target_name})")
    print(f"   Prefixes: {', '.join(prefixes)}")
    print(f"   Batch size: {batch_size:,}")
    if dry_run:
        print("   Mode: DRY RUN")
    elif validate_only:
        print("   Mode: VALIDATE ONLY")
    print()

    # --- Connect to source ---
    print("Connecting to source (public)...")
    try:
        source_meta = await get_meta_connection(
            source_host, source_port, source_password, "public"
        )
        source_raw = await get_raw_connection(source_host, source_port, source_password)
    except Exception as e:
        print(f"   FAILED: {e}")
        print("   Tip: Make sure the IAP tunnel is running (make tunnel)")
        return 1

    # --- Connect to target ---
    print(f"Connecting to target ({target_name})...")
    try:
        target_meta = await get_meta_connection(
            target_host,
            target_port,
            str(target_password) if target_password else None,
            str(target_cfg["label"]),
        )
        target_raw = await get_raw_connection(
            target_host,
            target_port,
            str(target_password) if target_password else None,
        )
    except Exception as e:
        print(f"   FAILED: {e}")
        if target_name == "scratch":
            print("   Tip: Start scratch Redis with: make scratch-redis-up")
        return 1

    print()

    # --- Preflight ---
    print("Running preflight checks...")
    ok = await preflight_checks(
        source_meta, target_meta, target_name, confirm_replace, skip_sibling_check
    )
    if not ok:
        await source_meta.aclose()
        await source_raw.aclose()
        await target_meta.aclose()
        await target_raw.aclose()
        return 1
    print()

    if validate_only:
        print("Validate-only mode: running validation script instead.")
        print(
            "   Use: python scripts/validate_clone.py "
            f"--source public --target {target_name}"
        )
        await source_meta.aclose()
        await source_raw.aclose()
        await target_meta.aclose()
        await target_raw.aclose()
        return 0

    # --- Transfer each prefix ---
    print(f"Transferring {len(prefixes)} prefix(es)...")
    print()

    all_results: list[TransferResult] = []
    total_start = time.monotonic()

    for prefix in prefixes:
        print(f"   --- Prefix: {prefix} ---")

        if not dry_run:
            existing = await count_keys(target_meta, prefix)
            if existing > 0:
                print(f"      Clearing {existing:,} existing keys on target...")
                await delete_keys_by_prefix(target_meta, prefix)

        result = await transfer_prefix(
            source_meta, source_raw, target_raw, prefix,
            batch_size=batch_size, dry_run=dry_run,
        )
        all_results.append(result)

        rate = (
            result.keys_transferred / result.elapsed_seconds
            if result.elapsed_seconds > 0
            else 0
        )
        status = "OK" if result.success else "ERRORS"
        print(
            f"      {status}: {result.keys_transferred:,}/{result.keys_found:,} "
            f"keys in {result.elapsed_seconds:.1f}s ({rate:.0f} keys/sec)"
        )
        if result.error_messages:
            for msg in result.error_messages[:5]:
                print(f"      ERROR: {msg}")
        print()

    # --- Recreate FT indexes ---
    if not dry_run:
        print("Recreating FT indexes on target...")
        for prefix in prefixes:
            index_name = PREFIX_TO_INDEX.get(prefix)
            if not index_name:
                continue
            schema = await get_index_schema(source_meta, index_name)
            if schema:
                await recreate_index(target_meta, index_name, schema)
            else:
                print(f"      No schema found for '{index_name}' on source, skipping")
        print()

        # Wait briefly for background indexing to start
        await asyncio.sleep(2)

    # --- ETL metadata sync ---
    if not dry_run and target_name == "local":
        print("Syncing ETL metadata...")
        transferred_prefixes = [
            r.prefix for r in all_results if r.success
        ]
        sync_etl_metadata(transferred_prefixes, dry_run=dry_run)
        print()

    # --- Summary ---
    total_elapsed = time.monotonic() - total_start
    total_keys = sum(r.keys_found for r in all_results)
    total_transferred = sum(r.keys_transferred for r in all_results)
    total_errors = sum(r.errors for r in all_results)

    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"   Prefixes: {len(all_results)}")
    print(
        f"   Keys {'would be ' if dry_run else ''}transferred: "
        f"{total_transferred:,}/{total_keys:,}"
    )
    print(f"   Total time: {total_elapsed:.1f}s")
    if total_errors > 0:
        print(f"   Errors: {total_errors}")
    print()

    if output_json:
        report = {
            "target": target_name,
            "dry_run": dry_run,
            "total_keys": total_keys,
            "total_transferred": total_transferred,
            "total_errors": total_errors,
            "elapsed_seconds": round(total_elapsed, 1),
            "prefixes": [asdict(r) for r in all_results],
        }
        print(json.dumps(report, indent=2))

    # --- Cleanup ---
    await source_meta.aclose()
    await source_raw.aclose()
    await target_meta.aclose()
    await target_raw.aclose()

    if total_errors > 0:
        print("Completed with errors")
        return 1

    if not dry_run:
        print(
            "Transfer complete. Run validation:\n"
            f"   python scripts/validate_clone.py "
            f"--source public --target {target_name}"
        )
    else:
        print("Dry run complete. No changes made.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bulk clone Redis prefixes from public to local/scratch "
        "using DUMP/RESTORE"
    )
    parser.add_argument(
        "--prefixes",
        nargs="+",
        required=True,
        help="Key prefixes to transfer (e.g., media: person:) or 'all'",
    )
    parser.add_argument(
        "--target",
        choices=["scratch", "local"],
        default="scratch",
        help="Target Redis instance (default: scratch)",
    )
    parser.add_argument(
        "--confirm-replace",
        action="store_true",
        help="Required when --target local to confirm overwriting data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count keys only, do not transfer",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip transfer, print validation instructions",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Keys per pipeline batch (default: 500)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON summary",
    )
    parser.add_argument(
        "--skip-sibling-check",
        action="store_true",
        help="Skip the sibling Redis memory-safety check",
    )

    args = parser.parse_args()

    exit_code = asyncio.run(
        main(
            prefixes=args.prefixes,
            target_name=args.target,
            dry_run=args.dry_run,
            validate_only=args.validate_only,
            confirm_replace=args.confirm_replace,
            batch_size=args.batch_size,
            output_json=args.json,
            skip_sibling_check=args.skip_sibling_check,
        )
    )
    sys.exit(exit_code)
