#!/usr/bin/env python3
"""Backfill full overviews and keyword metadata from cached TMDB data into Redis.

Scans data/us/movie and data/us/tv JSON files and patches:
  - $.overview  (full text, was truncated to 200 chars)
  - $.keywords  (re-normalized with accent transliteration)

Usage:
    python scripts/backfill_overviews.py              # dry-run
    python scripts/backfill_overviews.py --apply       # patch Redis
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from redis.asyncio import Redis  # noqa: E402

from adapters.config import load_env  # noqa: E402
from core.iptc import normalize_tag  # noqa: E402

load_env()


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
        socket_timeout=10.0,
        socket_connect_timeout=5.0,
    )


def _extract_source_id(item: dict[str, Any]) -> str | None:
    """Get the numeric TMDB ID from a cached item."""
    raw_id = item.get("tmdb_id") or item.get("id")
    if raw_id is None:
        return None
    sid = str(raw_id)
    for prefix in ("tmdb_movie_", "tmdb_tv_", "tmdb_person_", "tmdb_"):
        if sid.startswith(prefix):
            sid = sid[len(prefix):]
            break
    return sid if sid.isdigit() else None


async def backfill(apply: bool, data_dir: Path) -> dict[str, int]:
    stats: dict[str, int] = {
        "files_scanned": 0,
        "items_scanned": 0,
        "keys_found": 0,
        "overview_updated": 0,
        "overview_already_full": 0,
        "keywords_renormalized": 0,
        "no_overview_in_source": 0,
        "key_missing": 0,
    }

    redis = _connect_redis()
    await redis.ping()  # type: ignore[misc]
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6380")
    print(f"Connected to Redis at {host}:{port}")

    media_dirs = [
        data_dir / "movie",
        data_dir / "tv",
    ]

    batch: list[tuple[str, dict[str, Any]]] = []
    batch_size = 200

    async def flush_batch() -> None:
        if not batch:
            return
        pipe = redis.pipeline()
        for key, patches in batch:
            for json_path, value in patches.items():
                pipe.json().set(key, json_path, value)  # type: ignore[union-attr]
        await pipe.execute()
        batch.clear()

    for media_dir in media_dirs:
        if not media_dir.exists():
            continue

        files = sorted(media_dir.glob("tmdb_*.json"))
        for fpath in files:
            stats["files_scanned"] += 1

            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"  Error reading {fpath.name}: {e}")
                continue

            items: list[dict[str, Any]] = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("results", data.get("items", []))
                if not items and "id" in data:
                    items = [data]

            for item in items:
                stats["items_scanned"] += 1
                source_id = _extract_source_id(item)
                if not source_id:
                    continue

                key = f"media:tmdb_{source_id}"
                exists = await redis.exists(key)
                if not exists:
                    stats["key_missing"] += 1
                    continue

                stats["keys_found"] += 1
                patches: dict[str, Any] = {}

                overview = item.get("overview") or ""
                if overview:
                    current: Any = await redis.json().get(key, "$.overview")  # type: ignore[misc]
                    current_text = ""
                    if isinstance(current, list) and current:
                        current_text = current[0] or ""
                    if current_text != overview:
                        patches["$.overview"] = overview
                        stats["overview_updated"] += 1
                    else:
                        stats["overview_already_full"] += 1
                else:
                    stats["no_overview_in_source"] += 1

                existing_kw: Any = await redis.json().get(key, "$.keywords")  # type: ignore[misc]
                if isinstance(existing_kw, list) and existing_kw and isinstance(existing_kw[0], list):
                    old_tags: list[str] = existing_kw[0]
                    new_tags = [normalize_tag(t) for t in old_tags if normalize_tag(t)]
                    if old_tags != new_tags:
                        patches["$.keywords"] = new_tags
                        stats["keywords_renormalized"] += 1

                if patches and apply:
                    batch.append((key, patches))
                    if len(batch) >= batch_size:
                        await flush_batch()

            if stats["files_scanned"] % 50 == 0:
                print(
                    f"  Progress: files={stats['files_scanned']}, "
                    f"items={stats['items_scanned']}, "
                    f"overview={stats['overview_updated']}, "
                    f"keywords_renorm={stats['keywords_renormalized']}"
                )

    if apply:
        await flush_batch()

    await redis.aclose()  # type: ignore[misc]
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill overviews and keywords from cached TMDB data")
    parser.add_argument("--apply", action="store_true", help="Actually update Redis (default is dry-run)")
    parser.add_argument("--data-dir", type=str, default="data/us", help="Path to cached data directory")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    data_dir = Path(args.data_dir)
    print(f"Mode: {mode}")
    print(f"Data dir: {data_dir}\n")

    t0 = time.time()
    result = asyncio.run(backfill(apply=args.apply, data_dir=data_dir))
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print(f"[{mode}] Backfill Summary")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
