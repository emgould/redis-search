"""
TMDB Changes ETL - Process daily changes from TMDB API.

This module fetches changed entities (TV, Movie, Person) from TMDB's
changes endpoints and upserts them into Redis Search.

The changes endpoints return IDs of entities that have been modified
since a given start_date. We then fetch full details for each changed
entity and apply the same filtering rules as the main ETL.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Literal

from redis.asyncio import Redis

from adapters.config import load_env
from api.tmdb.core import TMDBService
from api.tmdb.models import MCBaseMediaItem
from api.tmdb.person import TMDBPersonService
from contracts.models import MCSources, MCType
from core.normalize import document_to_redis, normalize_document
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Batch size for concurrent API calls
BATCH_SIZE = 15

# Retry configuration for transient failures
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 10.0  # seconds
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}  # Transient HTTP errors

# Minimum popularity score for persons
MIN_PERSON_POPULARITY = 1.0

# TMDB image base URL
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"

# Major streaming platforms for filtering
MAJOR_STREAMING_PROVIDERS = {
    "Netflix",
    "Amazon Prime Video",
    "Amazon Video",
    "Hulu",
    "Max",
    "HBO Max",
    "Disney Plus",
    "Peacock",
    "Peacock Premium",
    "Apple TV Plus",
    "Apple TV",
    "Paramount Plus",
    "Paramount+",
    "Fubo",
    "FuboTV",
}


@dataclass
class ChangesETLStats:
    """Statistics from a changes ETL run."""

    media_type: str = ""
    start_date: str = ""
    end_date: str = ""

    # Discovery stats
    total_changes_found: int = 0
    non_adult_changes: int = 0
    pages_processed: int = 0

    # Enrichment stats
    enriched_count: int = 0
    enrichment_errors: int = 0

    # Filtering stats
    passed_filter: int = 0
    failed_filter: int = 0

    # Load stats
    documents_upserted: int = 0
    documents_skipped: int = 0
    load_errors: int = 0

    # Progress tracking
    current_batch: int = 0
    total_batches: int = 0
    current_phase: str = ""  # "fetching", "enriching", "loading"

    # Timing
    started_at: datetime | None = None
    completed_at: datetime | None = None

    # Error tracking
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary for JSON serialization."""
        return {
            "media_type": self.media_type,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_changes_found": self.total_changes_found,
            "non_adult_changes": self.non_adult_changes,
            "pages_processed": self.pages_processed,
            "enriched_count": self.enriched_count,
            "enrichment_errors": self.enrichment_errors,
            "passed_filter": self.passed_filter,
            "failed_filter": self.failed_filter,
            "documents_upserted": self.documents_upserted,
            "documents_skipped": self.documents_skipped,
            "load_errors": self.load_errors,
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
            "current_phase": self.current_phase,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors[:50] if self.errors else [],  # Limit errors in output
            "total_errors": len(self.errors),
        }


class TMDBChangesETL(TMDBService):
    """ETL service for processing TMDB daily changes."""

    def __init__(self):
        super().__init__()
        self.person_service = TMDBPersonService()

    async def _get_media_details_with_retry(
        self,
        tmdb_id: int,
        mc_type: MCType,
        max_retries: int = MAX_RETRIES,
    ) -> MCBaseMediaItem | None:
        """
        Get media details with exponential backoff retry for transient failures.

        Args:
            tmdb_id: TMDB ID to fetch
            mc_type: Media type (MOVIE or TV_SERIES)
            max_retries: Maximum retry attempts

        Returns:
            MCBaseMediaItem or None on permanent failure
        """
        delay = INITIAL_RETRY_DELAY

        for attempt in range(max_retries + 1):
            try:
                result = await self.get_media_details(
                    tmdb_id,
                    mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    cast_limit=10,
                )

                # Check for retryable status codes
                if (
                    isinstance(result, MCBaseMediaItem)
                    and result.status_code in RETRY_STATUS_CODES
                    and attempt < max_retries
                ):
                    logger.warning(
                        f"Retryable error {result.status_code} for {mc_type.value} {tmdb_id}, "
                        f"attempt {attempt + 1}/{max_retries + 1}, retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, MAX_RETRY_DELAY)
                    continue

                return result

            except Exception as e:
                if attempt < max_retries:
                    logger.warning(
                        f"Exception fetching {mc_type.value} {tmdb_id}: {e}, "
                        f"attempt {attempt + 1}/{max_retries + 1}, retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, MAX_RETRY_DELAY)
                else:
                    logger.error(
                        f"Failed to fetch {mc_type.value} {tmdb_id} after {max_retries + 1} attempts: {e}"
                    )
                    return None

        return None

    async def get_changes(
        self,
        media_type: Literal["tv", "movie", "person"],
        start_date: str,
        end_date: str | None = None,
    ) -> list[int]:
        """
        Fetch all changed entity IDs from TMDB changes endpoint.

        Args:
            media_type: Type of media ('tv', 'movie', or 'person')
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)

        Returns:
            List of TMDB IDs that have changed (adult=false only)
        """
        endpoint = f"{media_type}/changes"
        all_ids: list[int] = []
        seen_ids: set[int] = set()

        params: dict[str, Any] = {
            "start_date": start_date,
            "page": 1,
        }
        if end_date:
            params["end_date"] = end_date

        # Fetch first page
        first_page = await self._make_request(endpoint, params)
        if not first_page:
            logger.warning(f"No changes found for {media_type} since {start_date}")
            return []

        total_pages = first_page.get("total_pages", 1)
        total_results = first_page.get("total_results", 0)
        logger.info(f"Changes for {media_type}: {total_results} results across {total_pages} pages")

        # Process first page - filter out adult content
        for item in first_page.get("results", []):
            if item.get("adult", False):
                continue
            tmdb_id = item.get("id")
            if tmdb_id and tmdb_id not in seen_ids:
                seen_ids.add(tmdb_id)
                all_ids.append(tmdb_id)

        # Fetch remaining pages
        if total_pages > 1:
            page_numbers = list(range(2, total_pages + 1))

            for i in range(0, len(page_numbers), BATCH_SIZE):
                batch = page_numbers[i : i + BATCH_SIZE]
                tasks = []
                for page_num in batch:
                    page_params = {**params, "page": page_num}
                    tasks.append(self._make_request(endpoint, page_params))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        logger.warning(f"Error fetching changes page: {result}")
                        continue
                    if result and isinstance(result, dict):
                        for item in result.get("results", []):
                            if item.get("adult", False):
                                continue
                            tmdb_id = item.get("id")
                            if tmdb_id and tmdb_id not in seen_ids:
                                seen_ids.add(tmdb_id)
                                all_ids.append(tmdb_id)

                # Small delay between batches
                if i + BATCH_SIZE < len(page_numbers):
                    await asyncio.sleep(0.2)

        logger.info(f"Found {len(all_ids)} non-adult changes for {media_type}")
        return all_ids

    def _passes_media_filter(self, item: dict, media_type: Literal["tv", "movie"]) -> bool:
        """
        Apply the same filtering rules as the main ETL.

        Returns True if the item passes all filters.
        """
        # 1. Poster must exist
        if not item.get("poster_path"):
            return False

        # 2. Popularity must be >= 1
        metrics = item.get("metrics", {})
        popularity = metrics.get("popularity") or item.get("popularity") or 0
        if popularity < 1:
            return False

        # 3. Vote count must be > 1
        vote_count = metrics.get("vote_count") or item.get("vote_count") or 0
        if vote_count <= 1:
            return False

        # 4. Runtime < 50 only allowed if vote_count >= 10 (movies only)
        if media_type == "movie":
            runtime = item.get("runtime") or 0
            if runtime < 50 and vote_count < 10:
                return False

        # 5. Either released in last 10 years OR on major streaming platform
        ten_years_ago = datetime.now() - timedelta(days=365 * 10)
        cutoff_year = ten_years_ago.year

        release_date = item.get("release_date") or item.get("first_air_date") or ""
        release_year = int(release_date[:4]) if len(release_date) >= 4 else 0
        is_recent = release_year >= cutoff_year

        is_on_major_platform = False
        watch_providers = item.get("watch_providers", {})
        for provider_type in ["flatrate", "buy", "rent"]:
            providers = watch_providers.get(provider_type, [])
            for provider in providers:
                if provider.get("provider_name") in MAJOR_STREAMING_PROVIDERS:
                    is_on_major_platform = True
                    break
            if is_on_major_platform:
                break

        return is_recent or is_on_major_platform

    def _passes_person_filter(self, person: dict) -> bool:
        """
        Apply filtering rules for person data.

        Returns True if the person passes all filters.
        """
        # Must have name and profile image
        if not person.get("name"):
            return False
        if not person.get("profile_path") and not person.get("has_image"):
            return False

        # Must have minimum popularity
        popularity: float = person.get("popularity", 0)
        return bool(popularity >= MIN_PERSON_POPULARITY)

    async def enrich_and_filter_media(
        self,
        tmdb_ids: list[int],
        media_type: Literal["tv", "movie"],
        stats: ChangesETLStats,
    ) -> list[dict[str, Any]]:
        """
        Enrich media items with full details and apply filters.

        Args:
            tmdb_ids: List of TMDB IDs to process
            media_type: 'tv' or 'movie'
            stats: Stats object to update

        Returns:
            List of enriched items that pass the filter
        """
        enriched_items: list[dict[str, Any]] = []
        mc_type = MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE

        total = len(tmdb_ids)
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        stats.total_batches = total_batches
        stats.current_phase = "enriching"
        logger.info(f"Enriching {total} {media_type} items in {total_batches} batches...")

        for i in range(0, total, BATCH_SIZE):
            batch_ids = tmdb_ids[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            stats.current_batch = batch_num

            logger.info(f"Processing batch {batch_num}/{total_batches}")

            # Use retry helper for each enrichment call
            tasks = [self._get_media_details_with_retry(tmdb_id, mc_type) for tmdb_id in batch_ids]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(results):
                tmdb_id = batch_ids[idx]
                if isinstance(result, Exception):
                    stats.enrichment_errors += 1
                    stats.errors.append(f"Error enriching {media_type} {tmdb_id}: {result}")
                    continue

                if result is None:
                    stats.enrichment_errors += 1
                    continue

                # Convert Pydantic model to dict
                if isinstance(result, MCBaseMediaItem):
                    # Check for error response (404 not found or 500 validation error)
                    if result.error or result.status_code in (404, 500):
                        stats.enrichment_errors += 1
                        error_msg = result.error or f"Error {result.status_code}"
                        stats.errors.append(f"{media_type} {tmdb_id}: {error_msg}")
                        continue
                    item_dict = result.model_dump(mode="json")
                elif isinstance(result, dict):
                    item_dict = result
                else:
                    stats.enrichment_errors += 1
                    stats.errors.append(
                        f"Unexpected result type for {media_type} {tmdb_id}: {type(result)}"
                    )
                    continue

                stats.enriched_count += 1

                # Apply filter
                if self._passes_media_filter(item_dict, media_type):
                    stats.passed_filter += 1

                    # Clean up data
                    if item_dict.get("main_cast") and item_dict.get("tmdb_cast"):
                        del item_dict["tmdb_cast"]

                    # Filter cast members without profile images
                    if item_dict.get("main_cast"):
                        item_dict["main_cast"] = [
                            cm
                            for cm in item_dict["main_cast"]
                            if cm.get("profile_path") or cm.get("has_image")
                        ]

                    enriched_items.append(item_dict)
                else:
                    stats.failed_filter += 1

            # Delay between batches
            if i + BATCH_SIZE < total:
                await asyncio.sleep(0.3)

        logger.info(f"Enriched {stats.enriched_count}, passed filter: {stats.passed_filter}")
        return enriched_items

    async def enrich_and_filter_persons(
        self,
        tmdb_ids: list[int],
        stats: ChangesETLStats,
    ) -> list[dict[str, Any]]:
        """
        Enrich person items with full details and apply filters.

        Args:
            tmdb_ids: List of TMDB person IDs to process
            stats: Stats object to update

        Returns:
            List of enriched persons that pass the filter
        """
        enriched_items: list[dict[str, Any]] = []
        total = len(tmdb_ids)
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        stats.total_batches = total_batches
        stats.current_phase = "enriching"
        logger.info(f"Enriching {total} person items in {total_batches} batches...")

        semaphore = asyncio.Semaphore(5)  # Limit concurrent person API calls

        async def enrich_person(person_id: int) -> dict[str, Any] | None:
            async with semaphore:
                try:
                    details = await self.person_service.get_person_details(person_id)
                    if details:
                        result: dict[str, Any] = details.to_dict()
                        return result
                    return None
                except Exception as e:
                    stats.errors.append(f"Error enriching person {person_id}: {e}")
                    return None

        for i in range(0, total, BATCH_SIZE):
            batch_ids = tmdb_ids[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            stats.current_batch = batch_num

            logger.info(f"Processing person batch {batch_num}/{total_batches}")

            tasks = [enrich_person(pid) for pid in batch_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, BaseException):
                    stats.enrichment_errors += 1
                    continue

                if result is None:
                    stats.enrichment_errors += 1
                    continue

                stats.enriched_count += 1

                if self._passes_person_filter(result):
                    stats.passed_filter += 1
                    enriched_items.append(result)
                else:
                    stats.failed_filter += 1

            if i + BATCH_SIZE < total:
                await asyncio.sleep(0.3)

        logger.info(
            f"Enriched {stats.enriched_count} persons, passed filter: {stats.passed_filter}"
        )
        return enriched_items


async def run_changes_etl(
    media_type: Literal["tv", "movie", "person"],
    start_date: str | None = None,
    end_date: str | None = None,
    redis_host: str = "localhost",
    redis_port: int = 6380,
    redis_password: str | None = None,
    verbose: bool = False,
    stats: ChangesETLStats | None = None,
) -> ChangesETLStats:
    """
    Run the TMDB changes ETL for a specific media type.

    Args:
        media_type: Type of media to process ('tv', 'movie', or 'person')
        start_date: Start date in YYYY-MM-DD format (defaults to yesterday)
        end_date: End date in YYYY-MM-DD format (defaults to today)
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        verbose: Enable verbose logging
        stats: Optional pre-created stats object (for progress tracking from caller)

    Returns:
        ChangesETLStats with run results
    """
    load_env()

    if stats is None:
        stats = ChangesETLStats()

    stats.media_type = media_type
    stats.started_at = datetime.now()

    # Default dates: yesterday to today
    today = date.today()
    yesterday = today - timedelta(days=1)

    if not start_date:
        start_date = yesterday.isoformat()
    if not end_date:
        end_date = today.isoformat()

    stats.start_date = start_date
    stats.end_date = end_date

    print("=" * 60)
    print(f"🚀 TMDB Changes ETL - {media_type.upper()}")
    print("=" * 60)
    print(f"  Start date: {start_date}")
    print(f"  End date: {end_date}")
    print(f"  Redis: {redis_host}:{redis_port}")
    print()

    # Initialize ETL service
    etl = TMDBChangesETL()

    # Step 1: Get changed IDs
    print("📋 Fetching changes from TMDB...")
    changed_ids = await etl.get_changes(media_type, start_date, end_date)
    stats.total_changes_found = len(changed_ids)
    stats.non_adult_changes = len(changed_ids)

    if not changed_ids:
        print("  ℹ️  No changes found")
        stats.completed_at = datetime.now()
        return stats

    print(f"  Found {len(changed_ids)} changed items")
    print()

    # Step 2: Enrich and filter
    print("🔄 Enriching and filtering...")
    if media_type == "person":
        enriched_items = await etl.enrich_and_filter_persons(changed_ids, stats)
    else:
        enriched_items = await etl.enrich_and_filter_media(
            changed_ids,
            media_type,
            stats,  # type: ignore[arg-type]
        )

    print(f"  Enriched: {stats.enriched_count}")
    print(f"  Passed filter: {stats.passed_filter}")
    print(f"  Failed filter: {stats.failed_filter}")
    print()

    if not enriched_items:
        print("  ℹ️  No items passed filtering")
        stats.completed_at = datetime.now()
        return stats

    # Step 3: Upsert to Redis
    print("📥 Upserting to Redis...")

    redis = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

    try:
        # Verify connection
        ping_result = redis.ping()
        if asyncio.iscoroutine(ping_result):
            await ping_result
        print("  ✅ Connected to Redis")

        # Determine the key prefix and mc_type based on media type
        if media_type == "person":
            key_prefix = "person"
            mc_type = MCType.PERSON
        else:
            key_prefix = "media"
            mc_type = MCType.TV_SERIES if media_type == "tv" else MCType.MOVIE

        # Process items
        pipeline = redis.pipeline()
        batch_count = 0
        batch_size = 100

        for item in enriched_items:
            # Normalize the document for search index
            search_doc = normalize_document(item, source=MCSources.TMDB, mc_type=mc_type)

            if search_doc is None:
                stats.documents_skipped += 1
                continue

            # Convert to Redis format
            key = f"{key_prefix}:{search_doc.id}"
            redis_doc = document_to_redis(search_doc)

            # Add additional fields for persons
            if media_type == "person":
                also_known_as = item.get("also_known_as", [])
                redis_doc["also_known_as"] = " | ".join(also_known_as[:10]) if also_known_as else ""
                redis_doc["known_for_department"] = item.get("known_for_department") or ""
                redis_doc["birthday"] = item.get("birthday")
                redis_doc["deathday"] = item.get("deathday")
                redis_doc["place_of_birth"] = item.get("place_of_birth")

                # Calculate age
                birthday = item.get("birthday")
                deathday = item.get("deathday")
                if birthday:
                    try:
                        birth_date = date.fromisoformat(birthday)
                        end_date_obj = date.fromisoformat(deathday) if deathday else date.today()
                        age = (
                            end_date_obj.year
                            - birth_date.year
                            - (
                                (end_date_obj.month, end_date_obj.day)
                                < (birth_date.month, birth_date.day)
                            )
                        )
                        redis_doc["age"] = age
                        redis_doc["is_deceased"] = deathday is not None
                    except (ValueError, TypeError):
                        redis_doc["age"] = None
                        redis_doc["is_deceased"] = False
                else:
                    redis_doc["age"] = None
                    redis_doc["is_deceased"] = False

                # Add known_for titles
                known_for = item.get("known_for", [])
                if known_for:
                    known_for_titles = []
                    for kf_item in known_for[:3]:
                        title = kf_item.get("title") or kf_item.get("name")
                        if title:
                            known_for_titles.append(title)
                    redis_doc["known_for_titles"] = known_for_titles
                else:
                    redis_doc["known_for_titles"] = []

            # Upsert to Redis (JSON.SET replaces existing or creates new)
            pipeline.json().set(key, "$", redis_doc)
            batch_count += 1
            stats.documents_upserted += 1

            # Execute batch
            if batch_count >= batch_size:
                await pipeline.execute()
                pipeline = redis.pipeline()
                batch_count = 0

        # Execute remaining items
        if batch_count > 0:
            await pipeline.execute()

    except Exception as e:
        stats.load_errors += 1
        stats.errors.append(f"Redis error: {e}")
        print(f"  ❌ Redis error: {e}")
    finally:
        await redis.aclose()

    stats.completed_at = datetime.now()

    print()
    print("=" * 60)
    print("📊 ETL Summary")
    print("=" * 60)
    print(f"  Media type: {media_type}")
    print(f"  Date range: {stats.start_date} to {stats.end_date}")
    print(f"  Changes found: {stats.total_changes_found}")
    print(f"  Enriched: {stats.enriched_count}")
    print(f"  Passed filter: {stats.passed_filter}")
    print(f"  Documents upserted: {stats.documents_upserted}")
    print(f"  Documents skipped: {stats.documents_skipped}")
    if stats.duration_seconds:
        print(f"  Duration: {stats.duration_seconds:.1f}s")
    if stats.errors:
        print(f"  Errors: {len(stats.errors)}")
    print()
    print("🎉 ETL Complete!")

    return stats
