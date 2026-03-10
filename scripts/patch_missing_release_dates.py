#!/usr/bin/env python3
"""
Lightweight patch for media missing date fields in Redis.

Uses FT.SEARCH on idx:media to query by mc_type tag, then checks for
null/empty date fields and patches from TMDB.

Movies:  release_date (+ year) via movie endpoint with append_to_response=release_dates.
         US-specific release dates preferred over top-level.
TV:      first_air_date and last_air_date (+ year) via tv endpoint.

Usage:
    python scripts/patch_missing_release_dates.py --type movie --dry-run
    python scripts/patch_missing_release_dates.py --type tv
    python scripts/patch_missing_release_dates.py --type movie --concurrency 20
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

from adapters.config import load_env  # noqa: E402

load_env()

from redis.asyncio import Redis  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
from utils.get_logger import get_logger  # noqa: E402

logger = get_logger(__name__)

INDEX_NAME = "idx:media"
PAGE_SIZE = 200
PROCESS_LOG_INTERVAL = 200


def _connect_redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    return isinstance(value, str) and value.strip() == ""


def _extract_us_release_date(release_dates_payload: dict[str, Any]) -> str | None:
    """Extract the best US release date from TMDB release_dates response.

    Prefers theatrical (type 3), then digital (4), then any other US entry.
    """
    results = release_dates_payload.get("results")
    if not isinstance(results, list):
        return None

    us_entries: list[dict[str, Any]] = []
    for country_block in results:
        if not isinstance(country_block, dict):
            continue
        if str(country_block.get("iso_3166_1", "")).upper() != "US":
            continue
        dates = country_block.get("release_dates")
        if isinstance(dates, list):
            us_entries.extend(d for d in dates if isinstance(d, dict))

    if not us_entries:
        return None

    type_priority = {3: 0, 4: 1, 2: 2, 5: 3, 6: 4, 1: 5}

    def _sort_key(entry: dict[str, Any]) -> tuple[int, str]:
        rtype = entry.get("type", 99)
        priority = type_priority.get(rtype, 99) if isinstance(rtype, int) else 99
        rd = entry.get("release_date", "")
        date_str = rd[:10] if isinstance(rd, str) else ""
        return (priority, date_str)

    us_entries.sort(key=_sort_key)

    for entry in us_entries:
        rd = entry.get("release_date")
        if isinstance(rd, str) and rd.strip():
            return rd[:10]

    return None


def _year_from_date(date_str: str) -> int | None:
    if len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except ValueError:
            return None
    return None


async def _fetch_movie_dates(
    service: TMDBService, tmdb_id: int
) -> dict[str, str | None]:
    data = await service._make_request(
        f"movie/{tmdb_id}",
        params={"language": "en-US", "append_to_response": "release_dates"},
    )
    if not data or not isinstance(data, dict):
        return {"release_date": None}

    us_date = _extract_us_release_date(data.get("release_dates", {}))
    if us_date:
        return {"release_date": us_date}

    top_level = data.get("release_date")
    if isinstance(top_level, str) and top_level.strip():
        return {"release_date": top_level.strip()[:10]}

    return {"release_date": None}


async def _fetch_tv_dates(
    service: TMDBService, tmdb_id: int
) -> dict[str, str | None]:
    data = await service._make_request(
        f"tv/{tmdb_id}",
        params={"language": "en-US"},
    )
    if not data or not isinstance(data, dict):
        return {"first_air_date": None, "last_air_date": None}

    first = data.get("first_air_date")
    last = data.get("last_air_date")
    return {
        "first_air_date": first.strip()[:10] if isinstance(first, str) and first.strip() else None,
        "last_air_date": last.strip()[:10] if isinstance(last, str) and last.strip() else None,
    }


async def _find_candidates_via_index(
    redis: Redis,  # type: ignore[type-arg]
    mc_type: str,
    limit: int | None,
) -> list[tuple[str, int, dict[str, object]]]:
    """Use FT.AGGREGATE on idx:media with cursor to find candidates with missing dates."""
    candidates: list[tuple[str, int, dict[str, object]]] = []
    total_scanned = 0
    already_has_dates = 0

    date_fields = (
        ["release_date"]
        if mc_type == "movie"
        else ["first_air_date", "last_air_date"]
    )
    json_fields = ["source_id"] + date_fields
    load_args: list[str] = ["@__key", "AS", "__key"]
    for f in json_fields:
        load_args.extend([f"$.{f}", "AS", f])
    nargs = len(load_args)

    result = await redis.execute_command(
        "FT.AGGREGATE",
        INDEX_NAME,
        f"@mc_type:{{{mc_type}}}",
        "LOAD",
        str(nargs),
        *load_args,
        "WITHCURSOR",
        "COUNT",
        str(PAGE_SIZE),
    )

    while True:
        if not isinstance(result, list) or len(result) < 2:
            break

        rows = result[0]
        cursor_id = result[1]

        if isinstance(rows, list) and len(rows) > 1:
            for row in rows[1:]:
                if not isinstance(row, list):
                    continue
                total_scanned += 1
                fields: dict[str, object] = {}
                for j in range(0, len(row), 2):
                    if j + 1 < len(row):
                        fields[row[j]] = row[j + 1]

                any_missing = any(_is_blank(fields.get(df)) for df in date_fields)
                if not any_missing:
                    already_has_dates += 1
                    continue

                key = fields.get("__key")
                sid = fields.get("source_id")
                if key is None or sid is None:
                    continue
                try:
                    tmdb_id = int(str(sid))
                except ValueError:
                    continue

                candidates.append((str(key), tmdb_id, fields))

                if limit is not None and len(candidates) >= limit:
                    break

        if limit is not None and len(candidates) >= limit:
            candidates = candidates[:limit]
            if cursor_id:
                await redis.execute_command("FT.CURSOR", "DEL", INDEX_NAME, str(cursor_id))
            break

        if not cursor_id:
            break

        result = await redis.execute_command("FT.CURSOR", "READ", INDEX_NAME, str(cursor_id), "COUNT", str(PAGE_SIZE))

    logger.info(
        "Index query complete: type=%s scanned=%d already_ok=%d candidates=%d",
        mc_type,
        total_scanned,
        already_has_dates,
        len(candidates),
    )
    return candidates


async def patch(
    mc_type: str,
    limit: int | None,
    concurrency: int,
    dry_run: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "candidates": 0,
        "fetched": 0,
        "fetch_failed": 0,
        "tmdb_has_date": 0,
        "tmdb_no_date": 0,
        "patched": 0,
    }

    redis = _connect_redis()
    service = TMDBService()

    try:
        await redis.ping()  # type: ignore[misc]
        logger.info("Patch start: type=%s limit=%s concurrency=%d dry_run=%s", mc_type, limit, concurrency, dry_run)

        candidates = await _find_candidates_via_index(redis, mc_type, limit)
        stats["candidates"] = len(candidates)

        if not candidates:
            logger.info("No candidates found.")
            return stats

        fetch_fn = _fetch_movie_dates if mc_type == "movie" else _fetch_tv_dates

        for offset in range(0, len(candidates), concurrency):
            chunk = candidates[offset : offset + concurrency]
            fetch_tasks = [fetch_fn(service, tmdb_id) for _, tmdb_id, _ in chunk]
            results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            pipe = redis.pipeline() if not dry_run else None
            pipe_count = 0

            for (key, tmdb_id, existing_fields), result in zip(chunk, results, strict=True):
                if isinstance(result, BaseException):
                    stats["fetch_failed"] += 1
                    logger.warning("Fetch failed tmdb_id=%d: %s", tmdb_id, result)
                    continue

                stats["fetched"] += 1
                wrote_any = False

                if mc_type == "movie":
                    rd = result.get("release_date")
                    if isinstance(rd, str) and rd.strip():
                        wrote_any = True
                        year = _year_from_date(rd)
                        if not dry_run:
                            assert pipe is not None
                            pipe.execute_command("JSON.SET", key, "$.release_date", json.dumps(rd))
                            if year is not None:
                                pipe.execute_command("JSON.SET", key, "$.year", json.dumps(year))
                else:
                    fad = result.get("first_air_date")
                    lad = result.get("last_air_date")
                    if not dry_run:
                        assert pipe is not None
                    if isinstance(fad, str) and fad.strip() and _is_blank(existing_fields.get("first_air_date")):
                        wrote_any = True
                        year = _year_from_date(fad)
                        if not dry_run:
                            assert pipe is not None
                            pipe.execute_command("JSON.SET", key, "$.first_air_date", json.dumps(fad))
                            if year is not None:
                                pipe.execute_command("JSON.SET", key, "$.year", json.dumps(year))
                    if isinstance(lad, str) and lad.strip() and _is_blank(existing_fields.get("last_air_date")):
                        wrote_any = True
                        if not dry_run:
                            assert pipe is not None
                            pipe.execute_command("JSON.SET", key, "$.last_air_date", json.dumps(lad))

                if wrote_any:
                    stats["tmdb_has_date"] += 1
                    if dry_run:
                        stats["patched"] += 1
                    else:
                        pipe_count += 1
                else:
                    stats["tmdb_no_date"] += 1

            if pipe is not None and pipe_count > 0:
                await pipe.execute()
                stats["patched"] += pipe_count

            processed = min(offset + len(chunk), len(candidates))
            if processed % PROCESS_LOG_INTERVAL < concurrency or processed == len(candidates):
                logger.info(
                    "Progress: %d/%d fetched=%d has_date=%d no_date=%d patched=%d",
                    processed,
                    len(candidates),
                    stats["fetched"],
                    stats["tmdb_has_date"],
                    stats["tmdb_no_date"],
                    stats["patched"],
                )

        logger.info(
            "Complete: candidates=%d patched=%d tmdb_no_date=%d",
            stats["candidates"],
            stats["patched"],
            stats["tmdb_no_date"],
        )
    finally:
        await redis.aclose()

    return stats


def _print_stats(stats: dict[str, int], elapsed: float, mc_type: str, dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "WRITE"
    print(f"\n{'=' * 56}")
    print(f"Patch Missing Dates — {mc_type} ({mode})")
    print("=" * 56)
    for key in (
        "candidates",
        "fetched",
        "fetch_failed",
        "tmdb_has_date",
        "tmdb_no_date",
        "patched",
    ):
        print(f"  {key}: {stats.get(key, 0):,}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Patch media with missing dates from TMDB (lightweight, no full normalize)"
    )
    parser.add_argument("--type", choices=["movie", "tv"], required=True, help="Media type to patch")
    parser.add_argument("--limit", type=int, default=None, help="Max candidates to process")
    parser.add_argument("--concurrency", type=int, default=15, help="Concurrent TMDB calls (default: 15)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch from TMDB but skip Redis writes")

    args = parser.parse_args()
    start = time.time()
    stats = await patch(
        mc_type=args.type,
        limit=args.limit,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
    )
    elapsed = time.time() - start
    _print_stats(stats, elapsed, args.type, args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
