"""
Bestseller Author ETL - Fetch new authors from NYTimes bestseller lists.

This ETL:
1. Fetches current fiction and nonfiction bestsellers from the mediacircle Firebase function API
2. Extracts unique author names from the books
3. Checks which authors are missing from the Redis author index
4. Searches OpenLibrary for missing authors
5. Loads new authors into the Redis idx:author index

Usage:
    # Full ETL (from redis-search root)
    python -m etl.bestseller_author_etl

    # With custom API URL (for local emulator testing)
    python -m etl.bestseller_author_etl --api-url http://localhost:5001/media-circle/us-central1
"""

import asyncio
import os
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from redis.asyncio import Redis

from adapters.config import load_env
from api.openlibrary.bulk.load_author_index import mc_author_to_redis_doc
from api.openlibrary.models import MCAuthorItem
from api.openlibrary.wrappers import openlibrary_wrapper
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Constants
INDEX_NAME = "idx:author"
KEY_PREFIX = "author:"
BATCH_SIZE = 10

# Default mediacircle Firebase Functions URLs
MEDIACIRCLE_API_URL_PROD = "https://us-central1-media-circle.cloudfunctions.net"
MEDIACIRCLE_API_URL_LOCAL = "http://localhost:5001/media-circle/us-central1"


@dataclass
class BestsellerETLPhaseStats:
    """Stats for a single ETL phase."""

    phase: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    items_processed: int = 0
    items_success: int = 0
    items_failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    @property
    def items_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.items_processed / self.duration_seconds
        return 0.0


@dataclass
class BestsellerETLStats:
    """Statistics from a bestseller author ETL run."""

    # Phase stats
    fetch_phase: BestsellerETLPhaseStats = field(
        default_factory=lambda: BestsellerETLPhaseStats("fetch_bestsellers")
    )
    search_phase: BestsellerETLPhaseStats = field(
        default_factory=lambda: BestsellerETLPhaseStats("search_authors")
    )
    load_phase: BestsellerETLPhaseStats = field(
        default_factory=lambda: BestsellerETLPhaseStats("load_to_redis")
    )

    # Discovery stats
    total_books_found: int = 0
    fiction_books: int = 0
    nonfiction_books: int = 0
    unique_authors: int = 0
    existing_authors: int = 0
    new_authors: int = 0
    authors_not_found: int = 0

    # Compatibility with ETL runner (matches ChangesETLStats interface)
    total_changes_found: int = 0
    non_adult_changes: int = 0
    passed_filter: int = 0
    failed_filter: int = 0
    staging_file: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_books_found": self.total_books_found,
            "fiction_books": self.fiction_books,
            "nonfiction_books": self.nonfiction_books,
            "unique_authors": self.unique_authors,
            "existing_authors": self.existing_authors,
            "new_authors": self.new_authors,
            "authors_not_found": self.authors_not_found,
            "fetch_phase": {
                "duration_seconds": self.fetch_phase.duration_seconds,
                "items_processed": self.fetch_phase.items_processed,
                "items_success": self.fetch_phase.items_success,
                "items_failed": self.fetch_phase.items_failed,
                "items_per_second": self.fetch_phase.items_per_second,
                "errors": self.fetch_phase.errors[:10],
            },
            "search_phase": {
                "duration_seconds": self.search_phase.duration_seconds,
                "items_processed": self.search_phase.items_processed,
                "items_success": self.search_phase.items_success,
                "items_failed": self.search_phase.items_failed,
                "items_per_second": self.search_phase.items_per_second,
                "errors": self.search_phase.errors[:10],
            },
            "load_phase": {
                "duration_seconds": self.load_phase.duration_seconds,
                "items_processed": self.load_phase.items_processed,
                "items_success": self.load_phase.items_success,
                "items_failed": self.load_phase.items_failed,
                "items_per_second": self.load_phase.items_per_second,
                "errors": self.load_phase.errors[:10],
            },
        }


class BestsellerAuthorETL(BaseAPIClient):
    """ETL for fetching new authors from NYTimes bestseller lists."""

    def __init__(
        self,
        api_url: str | None = None,
        verbose: bool = False,
    ):
        """
        Initialize the Bestseller Author ETL.

        Args:
            api_url: Base URL for mediacircle Firebase Functions API
                     Defaults to production URL if not specified
            verbose: Enable verbose logging
        """
        super().__init__()
        self.api_url = api_url or os.getenv("MEDIACIRCLE_API_URL", MEDIACIRCLE_API_URL_PROD)
        self.verbose = verbose

    async def _fetch_bestsellers(
        self, list_type: str, stats: BestsellerETLStats, date: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetch bestsellers from mediacircle Firebase function API.

        Args:
            list_type: Either "fiction" or "nonfiction"
            stats: Stats object to update
            date: Optional date in YYYY-MM-DD format (defaults to current)

        Returns:
            List of book dictionaries
        """
        if list_type == "fiction":
            endpoint = f"{self.api_url}/get_fiction_bestsellers"
        else:
            endpoint = f"{self.api_url}/get_nonfiction_bestsellers"

        # Build query params
        params: dict[str, str] = {}
        if date:
            params["date"] = date

        logger.info(f"Fetching {list_type} bestsellers from {endpoint} (date={date or 'current'})")

        try:
            result = await self._core_async_request(
                url=endpoint,
                params=params if params else None,
                timeout=60,
                max_retries=3,
                rate_limit_max=5,
                rate_limit_period=1.0,
                return_exceptions=True,
            )

            if not result:
                stats.fetch_phase.errors.append(f"Failed to fetch {list_type} bestsellers")
                return []

            # Handle both direct response and nested data structure
            if isinstance(result, dict):
                # Check for nested data structure from Firebase handler
                if "data" in result:
                    result = result["data"]

                # Extract books from results
                books: list[dict[str, Any]] = result.get("results", [])
                if not books and "list_results" in result:
                    books = result["list_results"].get("books", [])

                return books

            return []

        except Exception as e:
            error_msg = f"Error fetching {list_type} bestsellers: {e}"
            logger.error(error_msg)
            stats.fetch_phase.errors.append(error_msg)
            return []

    def _extract_authors(self, books: list[dict[str, Any]]) -> set[str]:
        """
        Extract unique author names from books.

        Handles co-authors by splitting on " and " to search each individually.

        Args:
            books: List of book dictionaries

        Returns:
            Set of unique author names
        """
        authors: set[str] = set()

        for book in books:
            author_field = book.get("author", "")
            if author_field:
                # Clean up author name (remove "by " prefix if present)
                author_field = author_field.strip()
                if author_field.lower().startswith("by "):
                    author_field = author_field[3:].strip()

                # Split co-authors (e.g., "John Grisham and Jim McCloskey")
                # Handle both " and " and ", " as separators
                if " and " in author_field:
                    individual_authors = author_field.split(" and ")
                elif ", " in author_field and author_field.count(", ") <= 2:
                    # Only split on comma if there are 1-2 commas (likely co-authors)
                    # More commas might be "Last, First" format
                    individual_authors = author_field.split(", ")
                else:
                    individual_authors = [author_field]

                for author in individual_authors:
                    author = author.strip()
                    if author:
                        authors.add(author)

        return authors

    async def _check_existing_authors(
        self,
        authors: set[str],
        redis: Redis,
        stats: BestsellerETLStats,
    ) -> set[str]:
        """
        Check which authors already exist in Redis index.

        Args:
            authors: Set of author names to check
            redis: Redis connection
            stats: Stats object to update

        Returns:
            Set of author names NOT found in Redis (new authors)
        """
        missing_authors: set[str] = set()

        for author in authors:
            try:
                # Search for author by name in the idx:author index
                # Use FT.SEARCH to find exact or close matches
                query = f"@search_title:{author}"
                result = await redis.ft(INDEX_NAME).search(query)

                if result.total == 0:
                    missing_authors.add(author)
                else:
                    stats.existing_authors += 1
                    if self.verbose:
                        logger.info(f"Author already exists: {author}")

            except Exception as e:
                # If search fails (e.g., special characters), try exact match
                logger.debug(f"FT.SEARCH failed for '{author}': {e}, trying SCAN")
                # Fall back to checking if author might exist - add to missing to be safe
                missing_authors.add(author)

        return missing_authors

    async def _search_author_openlibrary(
        self,
        author_name: str,
        stats: BestsellerETLStats,
    ) -> MCAuthorItem | None:
        """
        Search for an author in OpenLibrary.

        Args:
            author_name: Name of the author to search
            stats: Stats object to update

        Returns:
            MCAuthorItem if found, None otherwise
        """
        try:
            # Search for the author using OpenLibrary wrapper
            response = await openlibrary_wrapper.search_authors(
                query=author_name,
                limit=1,  # We only need the first/best match
            )

            if response.status_code == 200 and response.results:
                author = response.results[0]
                if self.verbose:
                    logger.info(f"Found author in OpenLibrary: {author.name} ({author.key})")
                return author
            else:
                if self.verbose:
                    logger.info(f"Author not found in OpenLibrary: {author_name}")
                return None

        except Exception as e:
            error_msg = f"Error searching OpenLibrary for '{author_name}': {e}"
            logger.error(error_msg)
            stats.search_phase.errors.append(error_msg)
            return None

    async def run(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_password: str | None = None,
        max_authors: int = 0,  # 0 = no limit
        date: str | None = None,  # Optional date for bestseller list (YYYY-MM-DD)
    ) -> BestsellerETLStats:
        """
        Run the bestseller author ETL.

        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_password: Redis password
            max_authors: Maximum number of new authors to process (0 = unlimited)
            date: Optional date for bestseller list (YYYY-MM-DD format)

        Returns:
            BestsellerETLStats with run results
        """
        stats = BestsellerETLStats()

        print("=" * 60)
        print("🚀 Bestseller Author ETL")
        print("=" * 60)
        print(f"  API URL: {self.api_url}")
        print(f"  Redis: {redis_host}:{redis_port}")
        print(f"  Date: {date or 'current'}")
        print()

        # ========================================
        # Phase 1: Fetch Bestsellers
        # ========================================
        stats.fetch_phase.started_at = datetime.now()
        logger.info("Phase 1: Fetching bestsellers from mediacircle API")

        # Fetch fiction and nonfiction bestsellers
        fiction_books = await self._fetch_bestsellers("fiction", stats, date=date)
        nonfiction_books = await self._fetch_bestsellers("nonfiction", stats, date=date)

        stats.fiction_books = len(fiction_books)
        stats.nonfiction_books = len(nonfiction_books)
        stats.total_books_found = stats.fiction_books + stats.nonfiction_books

        all_books = fiction_books + nonfiction_books

        # Extract unique authors
        unique_authors = self._extract_authors(all_books)
        stats.unique_authors = len(unique_authors)

        stats.fetch_phase.items_processed = stats.total_books_found
        stats.fetch_phase.items_success = stats.total_books_found
        stats.fetch_phase.completed_at = datetime.now()

        print("📚 Phase 1 Results:")
        print(f"   Fiction books: {stats.fiction_books}")
        print(f"   Non-fiction books: {stats.nonfiction_books}")
        print(f"   Unique authors: {stats.unique_authors}")
        print(f"   Duration: {stats.fetch_phase.duration_seconds:.1f}s")
        print()

        if not unique_authors:
            logger.warning("No authors found in bestsellers")
            return stats

        # ========================================
        # Phase 2: Check Existing & Search OpenLibrary
        # ========================================
        stats.search_phase.started_at = datetime.now()
        logger.info("Phase 2: Checking existing authors and searching OpenLibrary")

        # Connect to Redis
        redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )

        try:
            ping_result = await cast(Awaitable[bool], redis.ping())
            if not ping_result:
                raise ConnectionError("Redis ping failed")
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")

            # Check which authors already exist
            missing_authors = await self._check_existing_authors(unique_authors, redis, stats)
            stats.new_authors = len(missing_authors)

            print("🔍 Phase 2 - Checking authors:")
            print(f"   Already in Redis: {stats.existing_authors}")
            print(f"   New authors to add: {stats.new_authors}")

            if not missing_authors:
                logger.info("All authors already exist in Redis")
                stats.search_phase.completed_at = datetime.now()
                await redis.aclose()
                return stats

            # Apply limit if specified
            if max_authors > 0:
                missing_authors_list = list(missing_authors)[:max_authors]
                missing_authors = set(missing_authors_list)
                logger.info(f"Limited to {max_authors} new authors for testing")

            # Search OpenLibrary for missing authors
            found_authors: list[MCAuthorItem] = []
            not_found_authors: list[str] = []
            search_start = time.time()

            for i, author_name in enumerate(missing_authors, 1):
                stats.search_phase.items_processed += 1

                if self.verbose or i % 10 == 0:
                    logger.info(f"Searching OpenLibrary [{i}/{len(missing_authors)}]: {author_name}")

                author = await self._search_author_openlibrary(author_name, stats)

                if author:
                    found_authors.append(author)
                    stats.search_phase.items_success += 1
                else:
                    stats.authors_not_found += 1
                    stats.search_phase.items_failed += 1
                    not_found_authors.append(author_name)
                    logger.warning(f"Author not found in OpenLibrary: {author_name}")

                # Small delay to be respectful to OpenLibrary API
                await asyncio.sleep(0.1)

            stats.search_phase.completed_at = datetime.now()
            search_duration = time.time() - search_start

            print("🔍 Phase 2 Results:")
            print(f"   Found in OpenLibrary: {len(found_authors)}")
            print(f"   Not found: {stats.authors_not_found}")
            if not_found_authors:
                print(f"   Not found names: {', '.join(not_found_authors)}")
            print(f"   Duration: {search_duration:.1f}s")
            print()

            # ========================================
            # Phase 3: Load to Redis
            # ========================================
            if found_authors:
                stats.load_phase.started_at = datetime.now()
                logger.info(f"Phase 3: Loading {len(found_authors)} new authors to Redis")

                load_start = time.time()
                pipeline = redis.pipeline()
                batch_count = 0

                for author in found_authors:
                    stats.load_phase.items_processed += 1

                    try:
                        # Convert MCAuthorItem to Redis document format
                        author_dict = author.model_dump(mode="json")
                        redis_doc = mc_author_to_redis_doc(author_dict)

                        # Generate key
                        key = f"{KEY_PREFIX}{redis_doc['id']}"

                        pipeline.json().set(key, "$", redis_doc)
                        batch_count += 1
                        stats.load_phase.items_success += 1

                        if batch_count >= BATCH_SIZE:
                            await pipeline.execute()
                            pipeline = redis.pipeline()
                            batch_count = 0

                    except Exception as e:
                        error_msg = f"Error loading author {author.name}: {e}"
                        logger.error(error_msg)
                        stats.load_phase.errors.append(error_msg)
                        stats.load_phase.items_failed += 1

                # Execute remaining batch
                if batch_count > 0:
                    await pipeline.execute()

                stats.load_phase.completed_at = datetime.now()
                load_duration = time.time() - load_start

                print("💾 Phase 3 Results:")
                print(f"   Authors loaded: {stats.load_phase.items_success}")
                print(f"   Errors: {stats.load_phase.items_failed}")
                print(f"   Duration: {load_duration:.1f}s")
                print()

        finally:
            await redis.aclose()

        # Update compatibility fields for ETL runner
        stats.total_changes_found = stats.unique_authors
        stats.passed_filter = stats.load_phase.items_success

        # Summary
        total_duration = (
            stats.fetch_phase.duration_seconds
            + stats.search_phase.duration_seconds
            + stats.load_phase.duration_seconds
        )

        print("=" * 60)
        print("📊 ETL Summary")
        print("=" * 60)
        print(f"  Total duration: {total_duration:.1f}s")
        print(f"  Bestseller books: {stats.total_books_found}")
        print(f"  Unique authors: {stats.unique_authors}")
        print(f"  Already in Redis: {stats.existing_authors}")
        print(f"  New authors added: {stats.load_phase.items_success}")
        print(f"  Not found in OpenLibrary: {stats.authors_not_found}")
        print()
        print("🎉 Bestseller Author ETL Complete!")

        return stats


async def run_bestseller_author_etl(
    media_type: str = "book",  # Ignored, for compatibility with ETL runner
    start_date: str | None = None,  # Used as date for bestseller list
    end_date: str | None = None,  # Ignored
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_password: str | None = None,
    verbose: bool = False,
    max_batches: int = 0,  # Used as max_authors for testing
    api_url: str | None = None,
    date: str | None = None,  # Explicit date parameter (takes precedence over start_date)
) -> BestsellerETLStats:
    """
    Run the bestseller author ETL.

    This function signature is compatible with the ETL runner.

    Args:
        media_type: Ignored (for compatibility)
        start_date: Used as date for bestseller list if date not specified
        end_date: Ignored (for compatibility)
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        verbose: Enable verbose logging
        max_batches: Maximum new authors to process (0 = unlimited)
        api_url: Optional mediacircle API URL override
        date: Optional date for bestseller list (YYYY-MM-DD format)

    Returns:
        BestsellerETLStats with run results
    """
    load_env()

    etl = BestsellerAuthorETL(api_url=api_url, verbose=verbose)

    # Use explicit date, or fall back to start_date from ETL runner
    bestseller_date = date or start_date

    return await etl.run(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        max_authors=max_batches,
        date=bestseller_date,
    )


if __name__ == "__main__":
    import argparse

    # Load env file BEFORE parsing args so defaults pick up config/local.env
    load_env()

    parser = argparse.ArgumentParser(description="Bestseller Author ETL")
    parser.add_argument(
        "--api-url",
        default=None,
        help=f"Mediacircle API URL (default: {MEDIACIRCLE_API_URL_PROD})",
    )
    parser.add_argument(
        "--redis-host",
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
        default=os.getenv("REDIS_PASSWORD"),
        help="Redis password",
    )
    parser.add_argument(
        "--max-authors",
        type=int,
        default=0,
        help="Maximum new authors to process (0 = unlimited)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help=f"Use local emulator URL ({MEDIACIRCLE_API_URL_LOCAL})",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date for bestseller list (YYYY-MM-DD format, defaults to current)",
    )

    args = parser.parse_args()

    # Use local emulator URL if --local flag is set
    api_url = args.api_url
    if args.local:
        api_url = MEDIACIRCLE_API_URL_LOCAL

    asyncio.run(
        run_bestseller_author_etl(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            verbose=args.verbose,
            max_batches=args.max_authors,
            api_url=api_url,
            date=args.date,
        )
    )

