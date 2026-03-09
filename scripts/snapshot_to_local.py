"""
Snapshot Public Redis to Local/Scratch.

Clones the entire public Redis dataset by triggering a BGSAVE on the remote,
downloading the RDB file via gcloud compute scp (IAP-tunneled), and restoring
it into the target Docker-based Redis instance.

This is the fastest path for "make local look like public" but it is a full
replacement -- all existing data in the target is destroyed.

Usage:
    # Snapshot to scratch (safe default)
    python scripts/snapshot_to_local.py --target scratch

    # Snapshot to primary local (requires confirmation)
    python scripts/snapshot_to_local.py --target local --confirm-replace

    # Dry run - show what would happen
    python scripts/snapshot_to_local.py --dry-run

    # Validate only - skip transfer, just compare
    python scripts/snapshot_to_local.py --validate-only
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis

env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

GCE_VM_NAME = os.getenv("REDIS_VM_NAME", "redis-stack-vm")
GCE_ZONE = os.getenv("REDIS_VM_ZONE", "us-central1-a")
GCE_PROJECT = os.getenv("GCP_PROJECT_ID", "media-circle")

TARGETS: dict[str, dict[str, object]] = {
    "scratch": {
        "host": "localhost",
        "port": 6382,
        "password": None,
        "label": "Scratch Redis (localhost:6382)",
        "compose_file": "docker/docker-compose.scratch.yml",
        "service_name": "redis-scratch",
        "volume_name": "redis_scratch_data",
    },
    "local": {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", "6380")),
        "password": os.getenv("REDIS_PASSWORD") or None,
        "label": f"Local Redis ({os.getenv('REDIS_HOST', 'localhost')}:"
        f"{os.getenv('REDIS_PORT', '6380')})",
        "compose_file": "docker/docker-compose.yml",
        "service_name": "redis",
        "volume_name": "docker_redis_data",
    },
}


async def get_connection(
    host: str, port: int, password: str | None, label: str
) -> Redis:
    """Create and test a Redis connection."""
    redis: Redis = Redis(
        host=host, port=port, password=password, decode_responses=True
    )
    await redis.ping()  # type: ignore[misc]
    print(f"   Connected to {label}")
    return redis


async def trigger_bgsave(redis: Redis) -> bool:
    """Trigger BGSAVE on source Redis and wait for completion."""
    initial_lastsave: int = await redis.lastsave()  # type: ignore[assignment]
    print(f"   Last save timestamp: {initial_lastsave}")

    print("   Triggering BGSAVE...")
    await redis.bgsave()

    max_wait = 120
    poll_interval = 2
    waited = 0

    while waited < max_wait:
        await asyncio.sleep(poll_interval)
        waited += poll_interval
        current_lastsave: int = await redis.lastsave()  # type: ignore[assignment]
        if current_lastsave != initial_lastsave:
            print(f"   BGSAVE complete (waited {waited}s)")
            return True
        if waited % 10 == 0:
            print(f"   Waiting for BGSAVE... ({waited}s)")

    print(f"   ERROR: BGSAVE did not complete within {max_wait}s")
    return False


async def get_rdb_path(redis: Redis) -> str:
    """Get the RDB file path from Redis CONFIG."""
    config_dir = await redis.config_get("dir")
    config_file = await redis.config_get("dbfilename")
    rdb_dir = config_dir.get("dir", "/data")
    rdb_file = config_file.get("dbfilename", "dump.rdb")
    return f"{rdb_dir}/{rdb_file}"


def check_gcloud() -> bool:
    """Verify gcloud CLI is available."""
    return shutil.which("gcloud") is not None


def download_rdb(remote_path: str, local_path: Path) -> bool:
    """Download the RDB file from the GCE VM via IAP-tunneled SCP."""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "gcloud", "compute", "scp",
        f"{GCE_VM_NAME}:{remote_path}",
        str(local_path),
        f"--zone={GCE_ZONE}",
        f"--project={GCE_PROJECT}",
        "--tunnel-through-iap",
    ]

    print(f"   Downloading: {GCE_VM_NAME}:{remote_path}")
    print(f"   To: {local_path}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"   ERROR: gcloud scp failed (exit {result.returncode})")
        if result.stderr:
            for line in result.stderr.strip().split("\n")[:10]:
                print(f"      {line}")
        return False

    if not local_path.exists():
        print("   ERROR: Downloaded file not found")
        return False

    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"   Downloaded {size_mb:.1f} MB")
    return True


def stop_redis_container(compose_file: str, service_name: str) -> bool:
    """Stop a Redis Docker container."""
    compose_path = Path(compose_file)
    cmd = [
        "docker", "compose",
        "-f", str(compose_path),
        "stop", service_name,
    ]
    print(f"   Stopping {service_name}...")
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(compose_path.parent.parent),
    )
    if result.returncode != 0:
        print(f"   WARNING: stop returned exit {result.returncode}")
        if result.stderr:
            print(f"      {result.stderr.strip()}")
    return True


def start_redis_container(compose_file: str, service_name: str) -> bool:
    """Start a Redis Docker container."""
    compose_path = Path(compose_file)
    cmd = [
        "docker", "compose",
        "-f", str(compose_path),
        "start", service_name,
    ]
    print(f"   Starting {service_name}...")
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(compose_path.parent.parent),
    )
    if result.returncode != 0:
        print(f"   ERROR: start returned exit {result.returncode}")
        if result.stderr:
            print(f"      {result.stderr.strip()}")
        return False
    return True


def replace_rdb_in_volume(volume_name: str, rdb_path: Path) -> bool:
    """Replace the RDB file inside a Docker volume using a temporary container."""
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{volume_name}:/data",
        "-v", f"{rdb_path.parent}:/backup:ro",
        "alpine",
        "sh", "-c",
        "rm -f /data/dump.rdb /data/appendonly.aof && "
        f"cp /backup/{rdb_path.name} /data/dump.rdb && "
        "chmod 644 /data/dump.rdb",
    ]

    print(f"   Replacing RDB in volume '{volume_name}'...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"   ERROR: Volume replacement failed (exit {result.returncode})")
        if result.stderr:
            print(f"      {result.stderr.strip()}")
        return False

    print("   RDB replaced successfully")
    return True


async def wait_for_redis(
    host: str, port: int, password: str | None, max_wait: int = 60
) -> bool:
    """Wait for Redis to be ready after restart."""
    poll_interval = 2
    waited = 0

    while waited < max_wait:
        try:
            redis: Redis = Redis(
                host=host, port=port, password=password, decode_responses=True
            )
            await redis.ping()  # type: ignore[misc]
            info = await redis.info("persistence")
            loading = info.get("loading", 0)
            await redis.aclose()

            if not loading:
                print(f"   Redis ready (waited {waited}s)")
                return True
            if waited % 10 == 0:
                print(f"   Redis is loading data... ({waited}s)")
        except Exception:
            pass

        await asyncio.sleep(poll_interval)
        waited += poll_interval

    print(f"   ERROR: Redis not ready after {max_wait}s")
    return False


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
    source: Redis,
    target_name: str,
    confirm_replace: bool,
    skip_sibling_check: bool = False,
) -> bool:
    """Run preflight checks before snapshot."""
    if not skip_sibling_check:
        sibling_running, sibling_desc = _check_sibling_redis(target_name)
        if sibling_running and not _prompt_sibling_warning(sibling_desc):
            print("   Aborted by user.")
            return False

    if not check_gcloud():
        print("   ERROR: gcloud CLI not found. Install the Google Cloud SDK.")
        return False

    source_version = (await source.info("server")).get("redis_version", "unknown")
    print(f"   Source Redis version: {source_version}")

    source_dbsize = await source.dbsize()
    print(f"   Source DBSIZE: {source_dbsize:,}")

    source_info = await source.info("memory")
    used_mb = source_info.get("used_memory", 0) / (1024 * 1024)
    print(f"   Source memory usage: {used_mb:.0f} MB")

    if target_name == "local" and not confirm_replace:
        print(
            "   ERROR: --target local requires --confirm-replace. "
            "This will destroy ALL local Redis data."
        )
        return False

    return True


async def main(
    target_name: str = "scratch",
    dry_run: bool = False,
    validate_only: bool = False,
    confirm_replace: bool = False,
    skip_sibling_check: bool = False,
) -> int:
    """Run the snapshot/restore workflow."""
    target_cfg = TARGETS[target_name]
    target_host = str(target_cfg["host"])
    target_port = int(target_cfg["port"])  # type: ignore[arg-type]
    target_password = target_cfg["password"]
    compose_file = str(target_cfg["compose_file"])
    service_name = str(target_cfg["service_name"])
    volume_name = str(target_cfg["volume_name"])

    source_host = os.getenv("PUBLIC_REDIS_HOST", "localhost")
    source_port = int(os.getenv("PUBLIC_REDIS_PORT", "6381"))
    source_password = os.getenv("PUBLIC_REDIS_PASSWORD") or None

    print("=" * 60)
    print("Snapshot Public Redis to Local")
    print("=" * 60)
    print()
    print(f"   Source: {source_host}:{source_port} (public)")
    print(f"   Target: {target_cfg['label']} ({target_name})")
    print(f"   GCE VM: {GCE_VM_NAME} ({GCE_ZONE})")
    if dry_run:
        print("   Mode: DRY RUN")
    elif validate_only:
        print("   Mode: VALIDATE ONLY")
    print()

    if target_name == "local":
        print("   *** WARNING: This will DESTROY all data in local Redis ***")
        print()

    # --- Connect to source ---
    print("Connecting to source (public)...")
    try:
        source = await get_connection(
            source_host, source_port, source_password, "public"
        )
    except Exception as e:
        print(f"   FAILED: {e}")
        print("   Tip: Make sure the IAP tunnel is running (make tunnel)")
        return 1

    # --- Preflight ---
    print()
    print("Running preflight checks...")
    ok = await preflight_checks(source, target_name, confirm_replace, skip_sibling_check)
    if not ok:
        await source.aclose()
        return 1
    print()

    if validate_only:
        print("Validate-only mode: running validation script instead.")
        print(
            "   Use: python scripts/validate_clone.py "
            f"--source public --target {target_name}"
        )
        await source.aclose()
        return 0

    # --- Trigger BGSAVE ---
    print("Triggering remote snapshot...")
    rdb_remote_path = await get_rdb_path(source)
    print(f"   Remote RDB path: {rdb_remote_path}")

    if dry_run:
        print("   DRY RUN: Would trigger BGSAVE")
        print(f"   DRY RUN: Would download {GCE_VM_NAME}:{rdb_remote_path}")
        print(f"   DRY RUN: Would stop {service_name}")
        print(f"   DRY RUN: Would replace RDB in volume '{volume_name}'")
        print(f"   DRY RUN: Would start {service_name}")
        await source.aclose()
        print()
        print("Dry run complete. No changes made.")
        return 0

    bgsave_ok = await trigger_bgsave(source)
    if not bgsave_ok:
        await source.aclose()
        return 1

    await source.aclose()
    print()

    # --- Download RDB ---
    print("Downloading RDB snapshot...")
    start = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="redis-snapshot-") as tmpdir:
        local_rdb = Path(tmpdir) / "dump.rdb"

        if not download_rdb(rdb_remote_path, local_rdb):
            return 1
        print()

        download_elapsed = time.monotonic() - start

        # --- Stop target ---
        print("Preparing target Redis...")
        stop_redis_container(compose_file, service_name)

        # --- Replace RDB ---
        if not replace_rdb_in_volume(volume_name, local_rdb):
            print("   Attempting to restart target despite failure...")
            start_redis_container(compose_file, service_name)
            return 1

    print()

    # --- Start target ---
    print("Starting target Redis...")
    if not start_redis_container(compose_file, service_name):
        return 1
    print()

    # --- Wait for ready ---
    print("Waiting for Redis to load data...")
    ready = await wait_for_redis(
        target_host,
        target_port,
        str(target_password) if target_password else None,
    )
    if not ready:
        return 1
    print()

    # --- Post-restore stats ---
    print("Post-restore status...")
    try:
        target = await get_connection(
            target_host,
            target_port,
            str(target_password) if target_password else None,
            str(target_cfg["label"]),
        )
        dbsize = await target.dbsize()
        print(f"   Target DBSIZE: {dbsize:,}")

        try:
            indices = await target.execute_command("FT._LIST")
            print(f"   FT indexes: {', '.join(indices) if indices else '(none)'}")
        except Exception:
            print("   FT indexes: (unable to list)")

        await target.aclose()
    except Exception as e:
        print(f"   WARNING: Could not read post-restore stats: {e}")
    print()

    total_elapsed = time.monotonic() - start
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"   Download time: {download_elapsed:.1f}s")
    print(f"   Total time: {total_elapsed:.1f}s")
    print()
    print(
        "Snapshot restore complete. Run validation:\n"
        f"   python scripts/validate_clone.py "
        f"--source public --target {target_name}"
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Snapshot public Redis and restore to local/scratch"
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
        help="Required when --target local to confirm destroying all local data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip transfer, print validation instructions",
    )
    parser.add_argument(
        "--skip-sibling-check",
        action="store_true",
        help="Skip the sibling Redis memory-safety check",
    )

    args = parser.parse_args()

    exit_code = asyncio.run(
        main(
            target_name=args.target,
            dry_run=args.dry_run,
            validate_only=args.validate_only,
            confirm_replace=args.confirm_replace,
            skip_sibling_check=args.skip_sibling_check,
        )
    )
    sys.exit(exit_code)
