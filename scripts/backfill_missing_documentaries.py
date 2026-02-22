#!/usr/bin/env python3
"""Backfill only missing documentary titles from cached TMDB files."""

import argparse
import asyncio
import json
import os
import re
from collections.abc import Awaitable
from datetime import date
from pathlib import Path
from typing import Any, cast

from redis.asyncio import Redis

from src.adapters.config import load_env
from src.contracts.models import MCType
from src.core.normalize import document_to_redis, normalize_document
from src.etl.documentary_filter import is_eligible_documentary
from src.utils.genre_mapping import get_genre_mapping_with_fallback
from src.utils.get_logger import get_logger

logger = get_logger(__name__)
SCRIPT_TMDB_FILE_RE = re.compile(r"^tmdb_(movie|tv)_(\d{4})_(\d{2})\.json$")


def _parse_month_file(file_path: Path) -> tuple[int, int] | None:
    match = SCRIPT_TMDB_FILE_RE.match(file_path.name)
    if not match:
        return None
    return int(match.group(2)), int(match.group(3))


def _inside_window(file_year: int, file_month: int, years_back: int) -> bool:
    today = date.today()
    cutoff = date(today.year - years_back, today.month, 1)
    file_date = date(file_year, file_month, 1)
    current_month = date(today.year, today.month, 1)
    return cutoff <= file_date <= current_month


def _iter_cache_records(cache_path: Path) -> list[dict[str, Any]]:
    try:
        parsed = cache_path.read_text(encoding="utf-8")
        payload = json.loads(parsed)
    except (OSError, json.JSONDecodeError):
        logger.warning(f"Skipping unreadable cache file: {cache_path}")
        return []
    if not isinstance(payload, dict):
        return []

    raw_records = payload.get("results")
    if not isinstance(raw_records, list):
        return []

    return [cast(dict[str, Any], record) for record in raw_records if isinstance(record, dict)]


async def _record_missing_from_index(redis: Redis, redis_key: str) -> bool:
    exists = await redis.exists(redis_key)
    return int(cast(int, exists)) == 0


def _media_type_to_enum(media_type: str) -> MCType:
    return MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE


async def run_backfill_missing_documentaries(
    data_root: Path,
    years_back: int,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    apply_updates: bool,
) -> None:
    """Run missing documentary backfill from cached movie/TV files."""
    load_env()

    if years_back < 1:
        raise ValueError("years_back must be at least 1")

    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)

    redis = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )
    await cast(Awaitable[bool], redis.ping())

    total_candidates = 0
    missing_count = 0
    already_indexed = 0
    loaded_count = 0
    skipped_count = 0
    seen_keys: set[str] = set()

    try:
        movie_files = sorted([p for p in (data_root / "movie").glob("tmdb_movie_*.json") if _parse_month_file(p)])
        tv_files = sorted([p for p in (data_root / "tv").glob("tmdb_tv_*.json") if _parse_month_file(p)])

        media_files_by_type: dict[str, list[Path]] = {"movie": movie_files, "tv": tv_files}

        for media_type, files in media_files_by_type.items():
            logger.info(f"Processing cached {media_type} files")
            for cache_path in files:
                parsed_date = _parse_month_file(cache_path)
                if parsed_date is None:
                    continue
                year, month = parsed_date
                if not _inside_window(year, month, years_back):
                    continue

                records = _iter_cache_records(cache_path)
                if not records:
                    continue

                logger.info(
                    f"Scanning {cache_path.name}: {len(records)} records (window {years_back} years back from today)"
                )

                for record in records:
                    if not is_eligible_documentary(
                        record,
                        years_back=years_back,
                        as_of=date.today(),
                        require_major_provider=True,
                    ):
                        continue

                    total_candidates += 1
                    normalized = normalize_document(
                        dict(record),
                        source=None,
                        mc_type=_media_type_to_enum(media_type),
                        genre_mapping=genre_mapping,
                    )
                    if normalized is None:
                        skipped_count += 1
                        continue

                    redis_key = f"media:{normalized.id}"
                    if redis_key in seen_keys:
                        continue

                    is_missing = await _record_missing_from_index(redis, redis_key)
                    if is_missing:
                        missing_count += 1
                        if apply_updates:
                            redis_doc = document_to_redis(normalized)
                            await cast(Awaitable[bool | None], redis.json().set(redis_key, "$", redis_doc))
                            loaded_count += 1
                        seen_keys.add(redis_key)
                    else:
                        already_indexed += 1
                        seen_keys.add(redis_key)

    finally:
        await redis.aclose()

    logger.info("Backfill summary")
    logger.info(f"  Total documentary candidates: {total_candidates}")
    logger.info(f"  Missing from index: {missing_count}")
    logger.info(f"  Already indexed: {already_indexed}")
    logger.info(f"  Loaded: {loaded_count}")
    logger.info(f"  Skipped (normalization): {skipped_count}")
    logger.info(f"  Dry-run mode: {not apply_updates}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing documentaries from cached TMDB data")
    parser.add_argument(
        "--data-root",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data"),
        help="Path to cache root (default: data/)",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=10,
        help="Number of years to include when selecting cache files",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default=os.getenv("REDIS_HOST", "localhost"),
        help="Redis host",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.getenv("REDIS_PORT", "6380")),
        help="Redis port",
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        default=os.getenv("REDIS_PASSWORD"),
        help="Redis password",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write missing documentaries to Redis (default: dry run)",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root)
    if not data_root.exists():
        raise FileNotFoundError(f"Data root not found: {data_root}")

    asyncio.run(
        run_backfill_missing_documentaries(
            data_root=data_root,
            years_back=args.years_back,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            apply_updates=args.apply,
        )
    )


if __name__ == "__main__":
    main()
