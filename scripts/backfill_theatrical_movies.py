#!/usr/bin/env python3
"""
Backfill script for theatrical movies that were missed due to the release_dates bug.

This script:
1. Uses the discover endpoint to find movies released in a date range
    2. Enriches them with full details (bypassing the broken nightly ETL filter)
    3. Loads directly into Redis

Usage:
    python scripts/backfill_theatrical_movies.py --start-date 2025-12 --months 3
"""

import argparse
import asyncio
import os
from calendar import monthrange
from collections.abc import Awaitable
from datetime import date
from typing import Any, cast

from redis.asyncio import Redis

from src.adapters.config import load_env
from src.api.tmdb.core import TMDBService
from src.api.tmdb.models import MCMovieItem
from src.contracts.models import MCType
from src.core.normalize import document_to_redis, normalize_document
from src.etl.documentary_filter import is_documentary, is_eligible_documentary
from src.utils.genre_mapping import get_genre_mapping_with_fallback
from src.utils.get_logger import get_logger

logger = get_logger(__name__)
BATCH_SIZE = 15
DOCUMENTARY_LOOKBACK_YEARS = 10


class TheatricalBackfillETL(TMDBService):
    """ETL for backfilling theatrical movies that were missed."""

    async def discover_movies_by_date_range(
        self,
        release_date_gte: str,
        release_date_lte: str,
        include_adult: bool = False,
        language: str = "en-US",
        region: str = "US",
        max_pages: int = 500,
    ) -> list[int]:
        """
        Discover movies released within a date range.
        Uses release_type filter at API level to catch theatrical releases.
        """
        endpoint = "discover/movie"
        all_ids: list[int] = []
        seen_ids: set[int] = set()

        # Key: with_release_type includes theatrical (2, 3)
        params = {
            "release_date.gte": release_date_gte,
            "release_date.lte": release_date_lte,
            "include_adult": str(include_adult).lower(),
            "include_video": "false",
            "language": language,
            "region": region,
            "sort_by": "popularity.desc",
            "vote_average.gte": "1",
            "with_runtime.gte": "50",
            "with_release_type": "2|3|4|5|6",  # Theatrical + Digital + Physical + TV
            "page": 1,
        }

        first_page = await self._make_request(endpoint, params)
        if not first_page:
            logger.warning(f"No results for date range {release_date_gte} to {release_date_lte}")
            return []

        total_pages = min(first_page.get("total_pages", 1), max_pages)
        total_results = first_page.get("total_results", 0)
        logger.info(
            f"Discover: Found {total_results} results across {total_pages} pages "
            f"for {release_date_gte} to {release_date_lte}"
        )

        # Process first page
        for item in first_page.get("results", []):
            tmdb_id = item.get("id")
            if tmdb_id and tmdb_id not in seen_ids:
                seen_ids.add(tmdb_id)
                all_ids.append(tmdb_id)

        # Fetch remaining pages
        if total_pages > 1:
            for page_num in range(2, total_pages + 1):
                page_params = {**params, "page": page_num}
                result = await self._make_request(endpoint, page_params)
                if result and isinstance(result, dict):
                    for item in result.get("results", []):
                        tmdb_id = item.get("id")
                        if tmdb_id and tmdb_id not in seen_ids:
                            seen_ids.add(tmdb_id)
                            all_ids.append(tmdb_id)

                if page_num % 10 == 0:
                    logger.info(f"  Processed page {page_num}/{total_pages}")
                    await asyncio.sleep(0.2)

        logger.info(f"Discovered {len(all_ids)} unique movies")
        return all_ids

    async def enrich_movies(
        self,
        tmdb_ids: list[int],
        batch_size: int = BATCH_SIZE,
    ) -> list[dict[str, Any]]:
        """Enrich movies with full details."""
        enriched_items: list[dict[str, Any]] = []
        total = len(tmdb_ids)
        logger.info(f"Enriching {total} movies...")

        for i in range(0, total, batch_size):
            batch_ids = tmdb_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            logger.info(f"Processing batch {batch_num}/{total_batches}")

            tasks = [
                self.get_media_details(
                    tmdb_id,
                    MCType.MOVIE,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    cast_limit=10,
                )
                for tmdb_id in batch_ids
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(results):
                tmdb_id = batch_ids[idx]
                if isinstance(result, Exception):
                    logger.warning(f"Error enriching movie {tmdb_id}: {result}")
                    continue
                if result is not None and isinstance(result, MCMovieItem):
                    payload = result.model_dump(mode="json")
                    if is_eligible_documentary(
                        payload,
                        years_back=DOCUMENTARY_LOOKBACK_YEARS,
                        as_of=date.today(),
                        require_major_provider=True,
                    ):
                        enriched_items.append(payload)
                        continue
                    if is_documentary(payload):
                        logger.debug(f"Skipping documentary {tmdb_id}: no streaming+poster+10y rule match")
                        continue
                    if result.poster_path:
                        # Keep original behavior for non-documentaries
                        enriched_items.append(result.model_dump(mode="json"))
                    else:
                        logger.debug(f"Skipping movie {tmdb_id}: no poster")

            if i + batch_size < total:
                await asyncio.sleep(0.3)

        logger.info(f"Enriched {len(enriched_items)} of {total} movies")
        return enriched_items


def get_month_date_range(year: int, month: int) -> tuple[str, str]:
    """Get the first and last day of a month."""
    first_day = f"{year:04d}-{month:02d}-01"
    _, last_day_num = monthrange(year, month)
    last_day = f"{year:04d}-{month:02d}-{last_day_num:02d}"
    return first_day, last_day


def parse_start_date(date_str: str) -> tuple[int, int]:
    """Parse a YYYY-MM date string."""
    parts = date_str.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM")
    return int(parts[0]), int(parts[1])


def get_next_month(year: int, month: int) -> tuple[int, int]:
    """Get the next month's year and month."""
    if month == 12:
        return year + 1, 1
    return year, month + 1


async def run_backfill(
    start_date: str,
    months: int,
    redis_host: str,
    redis_port: int,
    redis_password: str | None,
    dry_run: bool = False,
) -> None:
    """Run the backfill process."""
    load_env()

    year, month = parse_start_date(start_date)
    etl = TheatricalBackfillETL()

    # Load genre mapping
    logger.info("Loading genre mapping...")
    genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
    logger.info(f"Loaded {len(genre_mapping)} genres")

    # Connect to Redis (unless dry run)
    redis: Redis | None = None
    if not dry_run:
        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        await cast(Awaitable[bool], redis.ping())
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")

    total_loaded = 0
    total_skipped = 0

    try:
        current_year, current_month = year, month
        for i in range(months):
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing month {i + 1}/{months}: {current_year}-{current_month:02d}")
            logger.info(f"{'=' * 60}")

            # Get date range
            release_date_gte, release_date_lte = get_month_date_range(current_year, current_month)

            # Discover movies
            tmdb_ids = await etl.discover_movies_by_date_range(
                release_date_gte=release_date_gte,
                release_date_lte=release_date_lte,
            )

            if not tmdb_ids:
                logger.warning(f"No movies found for {current_year}-{current_month:02d}")
                current_year, current_month = get_next_month(current_year, current_month)
                continue

            if dry_run:
                logger.info(f"[DRY RUN] Would process {len(tmdb_ids)} discovered movies for this month")
                if tmdb_ids:
                    logger.info(f"  Sample IDs: {', '.join(map(str, tmdb_ids[:10]))}")
                continue

            # Enrich movies
            enriched_movies = await etl.enrich_movies(tmdb_ids)

            # Load to Redis
            loaded = 0
            skipped = 0

            if redis:
                pipe = redis.pipeline()
                batch_count = 0

                for movie in enriched_movies:
                    doc = normalize_document(movie, genre_mapping=genre_mapping)
                    if doc is None:
                        skipped += 1
                        continue

                    key = f"media:{doc.id}"
                    redis_doc = document_to_redis(doc)
                    pipe.json().set(key, "$", redis_doc)
                    loaded += 1
                    batch_count += 1

                    # Execute in batches
                    if batch_count >= 100:
                        await pipe.execute()
                        pipe = redis.pipeline()
                        batch_count = 0

                # Execute remaining
                if batch_count > 0:
                    await pipe.execute()

            logger.info(f"Loaded {loaded} movies, skipped {skipped}")
            total_loaded += loaded
            total_skipped += skipped

            # Move to next month
            current_year, current_month = get_next_month(current_year, current_month)

    finally:
        if redis:
            await redis.aclose()

    logger.info(f"\n{'=' * 60}")
    logger.info("Backfill Complete!")
    logger.info(f"{'=' * 60}")
    logger.info(f"Total loaded: {total_loaded}")
    logger.info(f"Total skipped: {total_skipped}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill theatrical movies that were missed due to release_dates bug",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date in YYYY-MM format (e.g., 2025-12)",
    )

    parser.add_argument(
        "--months",
        type=int,
        required=True,
        help="Number of months to process (going forward from start date)",
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
        "--dry-run",
        action="store_true",
        help="Don't actually load to Redis, just show what would be loaded",
    )

    args = parser.parse_args()

    # Validate start date
    try:
        parse_start_date(args.start_date)
    except ValueError as e:
        parser.error(str(e))

    asyncio.run(
        run_backfill(
            start_date=args.start_date,
            months=args.months,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
