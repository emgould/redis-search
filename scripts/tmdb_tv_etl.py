#!/usr/bin/env python3
"""
TMDB TV ETL Script - Extract TV shows by monthly air date ranges

This script uses the TMDB discover endpoint to find TV shows that aired
within specific monthly date ranges, then enriches each show with full
details from get_media_details.

Usage:
    python scripts/tmdb_tv_etl.py --start-date 2025-11 --months-back 1
    python scripts/tmdb_tv_etl.py --start-date 2025-11 --months-back 12
"""

import argparse
import asyncio
import json
import sys
from calendar import monthrange
from datetime import datetime
from pathlib import Path
from typing import Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from api.tmdb.core import TMDBService
from api.tmdb.models import MCTvItem
from contracts.models import MCType
from utils.get_logger import get_logger

logger = get_logger(__name__)
BATCH_SIZE = 15


class TMDBTvETL(TMDBService):
    """ETL service for extracting TV show data from TMDB discover endpoint."""

    async def discover_tv_by_date_range(
        self,
        air_date_gte: str,
        air_date_lte: str,
        include_adult: bool = False,
        language: str = "en-US",
        max_pages: int = 500,
    ) -> list[int]:
        """
        Discover TV shows that PREMIERED within a date range.

        Uses first_air_date filter to get only shows that debuted in the range,
        not shows with any episode airing (which would include ongoing series).

        Args:
            air_date_gte: Start date (YYYY-MM-DD format) - filters first_air_date >= this
            air_date_lte: End date (YYYY-MM-DD format) - filters first_air_date <= this
            include_adult: Include adult content (default: False)
            language: Language code (default: en-US)
            max_pages: Maximum number of pages to fetch (default: 500, TMDB API limit)

        Returns:
            List of TMDB IDs for discovered TV shows that premiered in this range
        """
        endpoint = "discover/tv"
        all_ids: list[int] = []
        seen_ids: set[int] = set()

        # Fetch first page to get total pages
        # Use first_air_date to get only shows that PREMIERED in this date range
        # (not shows with any episode airing - that would include reruns/ongoing shows)
        # Filter to US watch region with streaming/purchase availability
        params = {
            "first_air_date.gte": air_date_gte,
            "first_air_date.lte": air_date_lte,
            "include_adult": str(include_adult).lower(),
            "include_null_first_air_dates": "false",
            "language": language,
            "sort_by": "first_air_date.asc",
            "watch_region": "US",
            "with_watch_monetization_types": "flatrate|free|ads|rent|buy",
            "page": 1,
        }

        first_page = await self._make_request(endpoint, params)
        if not first_page:
            logger.warning(f"No results for date range {air_date_gte} to {air_date_lte}")
            return []

        total_pages = min(first_page.get("total_pages", 1), max_pages)
        total_results = first_page.get("total_results", 0)
        logger.info(
            f"Discover TV: Found {total_results} results across {total_pages} pages "
            f"for {air_date_gte} to {air_date_lte}"
        )

        # Process first page
        for item in first_page.get("results", []):
            tmdb_id = item.get("id")
            if tmdb_id and tmdb_id not in seen_ids:
                seen_ids.add(tmdb_id)
                all_ids.append(tmdb_id)

        # Fetch remaining pages concurrently in batches
        if total_pages > 1:
            page_numbers = list(range(2, total_pages + 1))
            batch_size = BATCH_SIZE

            for i in range(0, len(page_numbers), batch_size):
                batch = page_numbers[i : i + batch_size]
                tasks = []
                for page_num in batch:
                    page_params = {**params, "page": page_num}
                    tasks.append(self._make_request(endpoint, page_params))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        logger.warning(f"Error fetching discover page: {result}")
                        continue
                    if result and isinstance(result, dict):
                        for item in result.get("results", []):
                            tmdb_id = item.get("id")
                            if tmdb_id and tmdb_id not in seen_ids:
                                seen_ids.add(tmdb_id)
                                all_ids.append(tmdb_id)

                # Small delay between batches to respect rate limits
                if i + batch_size < len(page_numbers):
                    await asyncio.sleep(0.2)

        logger.info(f"Discovered {len(all_ids)} unique TV shows for date range")
        return all_ids

    async def enrich_tv_shows(
        self,
        tmdb_ids: list[int],
        batch_size: int = BATCH_SIZE,
    ) -> list[dict[str, Any]]:
        """
        Enrich TV shows with full details using get_media_details.

        Args:
            tmdb_ids: List of TMDB IDs to enrich
            batch_size: Number of concurrent requests per batch

        Returns:
            List of enriched TV show data as JSON-serializable dicts
        """
        enriched_items: list[dict[str, Any]] = []
        total = len(tmdb_ids)
        logger.info(f"Starting enrichment of {total} TV shows...")

        for i in range(0, total, batch_size):
            batch_ids = tmdb_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} shows)")

            tasks = [
                self.get_media_details(
                    tmdb_id,
                    MCType.TV_SERIES,
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
                    logger.warning(f"Error enriching TV show {tmdb_id}: {result}")
                    continue
                if result is not None and isinstance(result, MCTvItem):
                    # Validate the item has required data
                    if result.name and result.overview and result.poster_path:
                        enriched_items.append(result.model_dump(mode="json"))
                    else:
                        logger.debug(f"Skipping TV show {tmdb_id}: missing name/overview/poster")

            # Delay between batches to respect rate limits
            if i + batch_size < total:
                await asyncio.sleep(0.3)

        logger.info(f"Successfully enriched {len(enriched_items)} of {total} TV shows")
        return enriched_items


def get_month_date_range(year: int, month: int) -> tuple[str, str]:
    """
    Get the first and last day of a month in YYYY-MM-DD format.

    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)

    Returns:
        Tuple of (first_day, last_day) as YYYY-MM-DD strings
    """
    first_day = f"{year:04d}-{month:02d}-01"
    _, last_day_num = monthrange(year, month)
    last_day = f"{year:04d}-{month:02d}-{last_day_num:02d}"
    return first_day, last_day


def parse_start_date(date_str: str) -> tuple[int, int]:
    """
    Parse a YYYY-MM date string into year and month.

    Args:
        date_str: Date string in YYYY-MM format

    Returns:
        Tuple of (year, month)

    Raises:
        ValueError: If date string is invalid
    """
    try:
        parts = date_str.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM")
        year = int(parts[0])
        month = int(parts[1])
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month: {month}. Must be 1-12")
        return year, month
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM. Error: {e}") from e


def get_previous_month(year: int, month: int) -> tuple[int, int]:
    """
    Get the previous month's year and month.

    Args:
        year: Current year
        month: Current month (1-12)

    Returns:
        Tuple of (year, month) for the previous month
    """
    if month == 1:
        return year - 1, 12
    return year, month - 1


async def run_etl(start_date: str, months_back: int, output_dir: str) -> None:
    """
    Run the TMDB TV ETL process.

    Args:
        start_date: Start date in YYYY-MM format
        months_back: Number of months to process (going backwards)
        output_dir: Directory to save output JSON files
    """
    # Parse start date
    year, month = parse_start_date(start_date)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Initialize ETL service
    etl = TMDBTvETL()

    # Process each month
    current_year, current_month = year, month
    for i in range(months_back):
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing month {i + 1}/{months_back}: {current_year}-{current_month:02d}")
        logger.info(f"{'=' * 60}")

        # Get date range for this month
        air_date_gte, air_date_lte = get_month_date_range(current_year, current_month)
        logger.info(f"Date range: {air_date_gte} to {air_date_lte}")

        # Discover TV shows
        tmdb_ids = await etl.discover_tv_by_date_range(
            air_date_gte=air_date_gte,
            air_date_lte=air_date_lte,
            include_adult=False,
            language="en-US",
        )

        if not tmdb_ids:
            logger.warning(f"No TV shows found for {current_year}-{current_month:02d}")
            current_year, current_month = get_previous_month(current_year, current_month)
            continue

        # Enrich TV shows
        enriched_shows = await etl.enrich_tv_shows(tmdb_ids)

        # Save to JSON file
        output_file = output_path / f"tmdb_tv_{current_year}_{current_month:02d}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "metadata": {
                        "year": current_year,
                        "month": current_month,
                        "air_date_gte": air_date_gte,
                        "air_date_lte": air_date_lte,
                        "total_discovered": len(tmdb_ids),
                        "total_enriched": len(enriched_shows),
                        "generated_at": datetime.now().isoformat(),
                    },
                    "results": enriched_shows,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        logger.info(f"Saved {len(enriched_shows)} TV shows to {output_file}")

        # Move to previous month
        current_year, current_month = get_previous_month(current_year, current_month)

    logger.info(f"\n{'=' * 60}")
    logger.info("ETL process complete!")
    logger.info(f"{'=' * 60}")


def main() -> None:
    """Main entry point for the TMDB TV ETL script."""
    parser = argparse.ArgumentParser(
        description="TMDB TV ETL - Extract and enrich TV shows by monthly air date",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Extract TV shows for November 2025 only
    python scripts/tmdb_tv_etl.py --start-date 2025-11 --months-back 1

    # Extract TV shows for the last 12 months starting from November 2025
    python scripts/tmdb_tv_etl.py --start-date 2025-11 --months-back 12

    # Custom output directory
    python scripts/tmdb_tv_etl.py --start-date 2025-11 --months-back 1 --output-dir data/custom/
        """,
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date in YYYY-MM format (e.g., 2025-11)",
    )

    parser.add_argument(
        "--months-back",
        type=int,
        required=True,
        help="Number of months to process, going backwards from start date",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/us/tv",
        help="Output directory for JSON files (default: data/us/tv/)",
    )

    args = parser.parse_args()

    # Validate arguments
    try:
        parse_start_date(args.start_date)
    except ValueError as e:
        parser.error(str(e))

    if args.months_back < 1:
        parser.error("--months-back must be at least 1")

    # Run the ETL
    asyncio.run(run_etl(args.start_date, args.months_back, args.output_dir))


if __name__ == "__main__":
    main()
