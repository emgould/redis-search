#!/usr/bin/env python3
"""
Backfill media index documents by re-fetching from the TMDB API.

Uses a SCAN cursor loop to safely iterate over all media documents.
Checkpointing is handled per-chunk by saving the cursor to Redis.
The `_source: backfill` field acts as the canonical resume marker,
allowing interrupted runs to skip already-processed documents.

The TMDB rate limiter (35 req/s) is built into TMDBService and handles
throttling automatically. The --concurrency flag controls how many API
calls are in-flight simultaneously.

Usage:
    # Dry-run: count and test skip logic, no writes
    python scripts/backfill_media_dates_and_timestamps.py --dry-run --scan-count 100

    # Full backfill of entire index
    python scripts/backfill_media_dates_and_timestamps.py --concurrency 20

    # Force re-run over already-processed documents
    python scripts/backfill_media_dates_and_timestamps.py --force
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
from contracts.models import MCType  # noqa: E402
from core.normalize import (  # noqa: E402
    document_to_redis,
    normalize_document,
    resolve_timestamps,
)
from utils.genre_mapping import get_genre_mapping_with_fallback  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

PROGRESS_INTERVAL = 100
CURSOR_KEY = "media:reindex:cursor"


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


async def backfill(
    scan_count: int,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
    force: bool,
    output_file: str | None,
) -> dict[str, Any]:
    """Re-fetch every matching doc from TMDB API, normalize, and full-replace."""
    stats: dict[str, int] = {
        "scanned": 0,
        "processed_this_run": 0,
        "fetched": 0,
        "normalized": 0,
        "written": 0,
        "fetch_failed": 0,
        "normalize_failed": 0,
        "skipped_source": 0,
        "skipped_invalid": 0,
        "deleted": 0,
    }
    deleted_movies: list[dict[str, Any]] = []
    deleted_tv: list[dict[str, Any]] = []

    redis = _connect_redis()

    try:
        await redis.ping()  # type: ignore[misc]
        service = TMDBService()

        genre_mapping: dict[int, str] = {}
        try:
            genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
            logger.info("Loaded %d genre mappings", len(genre_mapping))
        except Exception as e:
            logger.warning("Failed to load genre mapping: %s. Genres will be empty.", e)

        saved = await redis.get(CURSOR_KEY)
        cursor = int(saved) if saved else 0

        logger.info(
            "Backfill started: cursor=%d, scan_count=%d, force=%s, dry_run=%s, output=%s",
            cursor,
            scan_count,
            force,
            dry_run,
            output_file,
        )

        all_docs_for_output = []

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="media:*", count=scan_count)

            if not keys:
                if cursor == 0:
                    break
                continue

            # Accumulate keys from the SCAN
            stats["scanned"] += len(keys)

            if not force:
                source_marks = await redis.execute_command("JSON.MGET", *keys, "$._source")
                if not isinstance(source_marks, list):
                    source_marks = []

                import json

                todo_keys = []
                for k, m in zip(keys, source_marks, strict=True):
                    if m is None or m == [None]:
                        todo_keys.append(k)
                        continue
                    try:
                        parsed = json.loads(m[0])
                        if parsed != "backfill":
                            todo_keys.append(k)
                    except (json.JSONDecodeError, TypeError, IndexError):
                        todo_keys.append(k)

                stats["skipped_source"] += len(keys) - len(todo_keys)
            else:
                todo_keys = keys

            if todo_keys:
                # 2. Load minimal fields needed for fetching
                source_ids = await redis.execute_command("JSON.MGET", *todo_keys, "$.source_id")
                mc_types = await redis.execute_command("JSON.MGET", *todo_keys, "$.mc_type")

                # Try to get titles for better 404 logging, fallback safely
                titles = await redis.execute_command("JSON.MGET", *todo_keys, "$.title")

                # execute_command("JSON.MGET") returns a list (or None if totally empty, though unlikely with keys)
                if not isinstance(source_ids, list):
                    source_ids = []
                if not isinstance(mc_types, list):
                    mc_types = []
                if not isinstance(titles, list):
                    titles = []

                batch = []
                import json

                for k, sid, mtype, t in zip(todo_keys, source_ids, mc_types, titles, strict=True):
                    # Validate fields are present
                    if not sid or sid == [None] or not mtype or mtype == [None]:
                        stats["skipped_invalid"] += 1
                        continue

                    # JSON.MGET returns a list of matching values. Since we query root-level, it's a 1-item list containing a JSON string.
                    # Example: sid is '["1991"]' when querying `$.source_id`
                    try:
                        import json

                        # If key was deleted between SCAN and MGET, sid/mtype will be None
                        if not sid or not mtype:
                            stats["skipped_invalid"] += 1
                            continue

                        # Parse outer JSON array to get inner value
                        source_id_parsed = json.loads(sid)
                        mc_type_parsed = json.loads(mtype)

                        if not source_id_parsed or not mc_type_parsed:
                            stats["skipped_invalid"] += 1
                            continue

                        source_id_val = int(source_id_parsed[0])
                        mc_type_str = str(mc_type_parsed[0])

                    except (json.JSONDecodeError, ValueError, TypeError, IndexError):
                        stats["skipped_invalid"] += 1
                        continue

                    if mc_type_str not in ("movie", "tv"):
                        stats["skipped_invalid"] += 1
                        continue

                    try:
                        t_parsed = json.loads(t) if t else []
                        title_val = str(t_parsed[0]) if t_parsed else "?"
                    except (json.JSONDecodeError, ValueError, TypeError, IndexError):
                        title_val = "?"

                    batch.append((k, source_id_val, mc_type_str, title_val))

                stats["processed_this_run"] += len(batch)

                # 3. Process the batch in concurrent chunks
                for batch_start in range(0, len(batch), concurrency):
                    chunk = batch[batch_start : batch_start + concurrency]

                    if dry_run and not output_file:
                        continue

                    fetch_tasks = []
                    for _key, tmdb_id, mc_type_str, _title in chunk:
                        mc_type_enum = MCType.MOVIE if mc_type_str == "movie" else MCType.TV_SERIES

                        fetch_tasks.append(
                            service.get_media_details(
                                tmdb_id,
                                mc_type_enum,
                                include_cast=True,
                                include_videos=True,
                                include_watch_providers=True,
                                include_keywords=True,
                            )
                        )

                    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

                    prepared: list[tuple[str, dict[str, Any]]] = []

                    for (key, tmdb_id, mc_type_str, title), result in zip(
                        chunk, results, strict=True
                    ):
                        if isinstance(result, BaseException):
                            stats["fetch_failed"] += 1
                            logger.warning("Fetch failed tmdb_id=%s: %s", tmdb_id, result)
                            continue

                        if result is None:
                            stats["fetch_failed"] += 1
                            logger.warning("Null result tmdb_id=%s", tmdb_id)
                            continue

                        if hasattr(result, "model_dump"):
                            item_dict: dict[str, Any] = result.model_dump(mode="json")
                        elif isinstance(result, dict):
                            item_dict = result
                        else:
                            stats["fetch_failed"] += 1
                            logger.warning("Unexpected type for %s: %s", tmdb_id, type(result))
                            continue

                        if not item_dict or item_dict.get("status_code") == 404:
                            stats["fetch_failed"] += 1
                            logger.warning(
                                "404 for tmdb_id=%s — removing stale key %s", tmdb_id, key
                            )
                            if not output_file:
                                await redis.delete(key)
                            stats["deleted"] += 1
                            entry = {"tmdb_id": tmdb_id, "title": title, "key": key}
                            if mc_type_str == "movie":
                                deleted_movies.append(entry)
                            else:
                                deleted_tv.append(entry)
                            continue

                        if item_dict.get("error"):
                            stats["fetch_failed"] += 1
                            logger.warning("API error tmdb_id=%s: %s", tmdb_id, item_dict["error"])
                            continue

                        stats["fetched"] += 1

                        doc = normalize_document(item_dict, genre_mapping=genre_mapping)
                        if doc is None:
                            stats["normalize_failed"] += 1
                            logger.warning("Normalize returned None for tmdb_id=%s", tmdb_id)
                            continue

                        doc._source = "backfill"
                        redis_doc = document_to_redis(doc)
                        prepared.append((key, redis_doc))
                        stats["normalized"] += 1

                    if prepared:
                        now_ts = int(datetime.now(UTC).timestamp())

                        if output_file:
                            for pkey, redis_doc in prepared:
                                redis_doc["created_at"] = now_ts
                                redis_doc["modified_at"] = now_ts
                                redis_doc["_source"] = "backfill"
                                redis_doc["_key"] = pkey
                                all_docs_for_output.append(redis_doc)
                            stats["written"] += len(prepared)
                        else:
                            read_pipe = redis.pipeline()
                            for pkey, _ in prepared:
                                read_pipe.json().get(pkey)
                            existing_docs: list[object] = await read_pipe.execute()

                            write_pipe = redis.pipeline()
                            for (pkey, redis_doc), existing in zip(
                                prepared, existing_docs, strict=True
                            ):
                                existing_dict = existing if isinstance(existing, dict) else None
                                ca, ma, src = resolve_timestamps(
                                    existing_dict, now_ts, source_tag="backfill"
                                )
                                redis_doc["created_at"] = ca
                                redis_doc["modified_at"] = ma
                                redis_doc["_source"] = src
                                write_pipe.json().set(pkey, "$", redis_doc)
                            await write_pipe.execute()
                            stats["written"] += len(prepared)

            # 4. Checkpoint cursor AFTER successfully processing the full chunk
            if not dry_run and not output_file:
                await redis.set(CURSOR_KEY, cursor)

            if stats["scanned"] - stats.get("last_log_scanned", 0) >= 1000 or cursor == 0:
                stats["last_log_scanned"] = stats["scanned"]
                logger.info(
                    "  Progress: scanned=%d, processed=%d, skipped_source=%d, skipped_invalid=%d, fetched=%d, written=%d",
                    stats["scanned"],
                    stats["processed_this_run"],
                    stats["skipped_source"],
                    stats["skipped_invalid"],
                    stats["fetched"],
                    stats["written"],
                )

            if limit is not None and stats["processed_this_run"] >= limit:
                logger.info("Reached limit of %d processed items", limit)
                break

            if cursor == 0:
                break

        # 5. Cleanup cursor on complete run
        if cursor == 0 and not dry_run and limit is None and not output_file:
            logger.info("Backfill complete, deleting cursor key")
            await redis.delete(CURSOR_KEY)

        if output_file and all_docs_for_output:
            import json

            out_path = Path(output_file)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w") as f:
                json.dump(all_docs_for_output, f, indent=2)
            logger.info("Wrote %d normalized docs to %s", len(all_docs_for_output), output_file)

    finally:
        await redis.aclose()

    return {**stats, "deleted_movies": deleted_movies, "deleted_tv": deleted_tv}


def _print_stats(stats: dict[str, Any], elapsed: float, dry_run: bool) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{'=' * 60}")
    print(f"{prefix}Backfill — Summary")
    print("=" * 60)
    for k, v in stats.items():
        if isinstance(v, (int, float)):
            print(f"  {k}: {v:,}")
    print(f"  elapsed: {elapsed:.2f}s")
    print()


def _print_deleted(stats: dict[str, Any]) -> None:
    deleted_movies: list[dict[str, Any]] = stats.get("deleted_movies", [])
    deleted_tv: list[dict[str, Any]] = stats.get("deleted_tv", [])
    if not deleted_movies and not deleted_tv:
        return

    print("=" * 60)
    print("  Stale entries removed (TMDB returned 404)")
    print("=" * 60)
    if deleted_movies:
        print(f"\n  Deleted Movies ({len(deleted_movies)}):")
        for entry in deleted_movies:
            print(f"    tmdb_id={entry['tmdb_id']}  {entry['title']!r}  key={entry['key']}")
    if deleted_tv:
        print(f"\n  Deleted TV ({len(deleted_tv)}):")
        for entry in deleted_tv:
            print(f"    tmdb_id={entry['tmdb_id']}  {entry['title']!r}  key={entry['key']}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill media index by re-fetching from TMDB API via SCAN"
    )
    parser.add_argument(
        "--scan-count",
        type=int,
        default=500,
        help="Number of keys to request per SCAN iteration (default 500)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max documents to process in this run",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Number of concurrent API calls (default 15)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and evaluate skip logic without writing to Redis or fetching from TMDB",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore _source checkpoint and reprocess all documents",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Write the normalized JSON documents to this file instead of writing to Redis",
    )

    args = parser.parse_args()

    t0 = time.time()
    result = await backfill(
        scan_count=args.scan_count,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        force=args.force,
        output_file=args.output,
    )
    elapsed = time.time() - t0

    _print_stats(result, elapsed, args.dry_run)
    _print_deleted(result)


if __name__ == "__main__":
    asyncio.run(main())
