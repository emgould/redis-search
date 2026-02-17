"""
TMDB Changes ETL - Two-phase ETL with file staging.

Phase 1: Fetch all changes from TMDB API → save to local JSON file
Phase 2: Load from local file → upsert to Redis

This separation allows:
- Clear performance diagnostics (API vs Redis)
- Re-running Redis load without re-fetching
- File-based recovery if Redis load fails
- Ephemeral files cleaned up after 1 day

Usage:
    # Full ETL (both phases)
    python -m etl.tmdb_nightly_etl_v2 --media-type tv

    # Phase 1 only (fetch and save)
    python -m etl.tmdb_nightly_etl_v2 --media-type tv --fetch-only

    # Phase 2 only (load from existing file)
    python -m etl.tmdb_nightly_etl_v2 --media-type tv --load-only --file /path/to/file.json
"""

import asyncio
import gzip
import json
import os
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from redis.asyncio import Redis

from adapters.config import load_env
from api.tmdb.core import TMDBService
from api.tmdb.person import TMDBPersonService
from contracts.models import MCType
from core.normalize import document_to_redis, normalize_document
from core.streaming_providers import MAJOR_STREAMING_PROVIDERS, TV_SHOW_CUTOFF_DATE
from utils.get_logger import get_logger
from utils.redis_cache import disable_cache

logger = get_logger(__name__)

# Batch size for concurrent API calls
# Keep small to avoid overwhelming TMDB's rate limit (40 req/sec)
BATCH_SIZE = 10

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 10.0
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Filtering thresholds
# Lowered from 1.0 to 0.5 to capture more celebrities (e.g., Cher, Beyoncé, Adele, Prince)
# whose TMDB popularity can temporarily dip below 1.0
MIN_PERSON_POPULARITY = 0.5
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"


@dataclass
class ETLPhaseStats:
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
class ChangesETLStats:
    """Statistics from a v2 changes ETL run."""

    media_type: str = ""
    start_date: str = ""
    end_date: str = ""

    # Phase stats
    fetch_phase: ETLPhaseStats = field(default_factory=lambda: ETLPhaseStats("fetch"))
    load_phase: ETLPhaseStats = field(default_factory=lambda: ETLPhaseStats("load"))

    # Discovery
    total_changes_found: int = 0
    non_adult_changes: int = 0

    # Filtering
    passed_filter: int = 0
    failed_filter: int = 0

    # Output
    staging_file: str = ""

    # Live progress tracking (used by web UI status polling)
    current_batch: int = 0
    total_batches: int = 0
    current_phase: str = ""
    enriched_count: int = 0
    enrichment_errors: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "media_type": self.media_type,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_changes_found": self.total_changes_found,
            "non_adult_changes": self.non_adult_changes,
            "passed_filter": self.passed_filter,
            "failed_filter": self.failed_filter,
            "staging_file": self.staging_file,
            "fetch_phase": {
                "duration_seconds": self.fetch_phase.duration_seconds,
                "items_processed": self.fetch_phase.items_processed,
                "items_success": self.fetch_phase.items_success,
                "items_failed": self.fetch_phase.items_failed,
                "items_per_second": self.fetch_phase.items_per_second,
                "errors": self.fetch_phase.errors[:10],
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


class TMDBChangesETL(TMDBService):
    """Two-phase ETL: Fetch to file, then load to Redis."""

    def __init__(self, staging_dir: str = "/tmp/etl-staging", verbose: bool = False):
        super().__init__()
        self.person_service = TMDBPersonService()
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.verbose = verbose

    def _get_staging_path(self, media_type: str, run_date: str) -> Path:
        """Get path for staging file."""
        return self.staging_dir / f"tmdb_changes_{media_type}_{run_date}.json.gz"

    def _get_errors_path(self, media_type: str, run_date: str) -> Path:
        """Get path for errors file."""
        return self.staging_dir / f"tmdb_changes_{media_type}_{run_date}_errors.json.gz"

    async def _fetch_changes_page(
        self, media_type: Literal["tv", "movie", "person"], start_date: str, end_date: str, page: int
    ) -> dict:
        """Fetch a single page of changes."""
        endpoint = f"/{media_type}/changes"
        params = {"start_date": start_date, "end_date": end_date, "page": page}
        result = await self._make_request(endpoint, params=params)
        return result or {}

    async def _get_all_change_ids(
        self, media_type: Literal["tv", "movie", "person"], start_date: str, end_date: str
    ) -> list[int]:
        """Fetch all changed IDs from TMDB changes endpoint."""
        ids = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            data = await self._fetch_changes_page(media_type, start_date, end_date, page)
            results = data.get("results", [])
            total_pages = data.get("total_pages", 1)

            # Filter out adult content
            for item in results:
                if not item.get("adult", False):
                    ids.append(item["id"])

            page += 1

        return list(set(ids))  # Dedupe

    def _passes_media_filter(self, item: dict) -> bool:
        """Check if media item passes quality filters."""
        item_name = item.get("title") or item.get("name") or item.get("id")
        log_fn = logger.info if self.verbose else logger.debug
        media_type = str(item.get("_media_type") or item.get("media_type") or "").lower()

        # Must have poster
        if not item.get("poster_path"):
            log_fn(f"Filter reject {item_name}: no poster_path")
            return False

        # Check for major streaming platforms.
        # Handle multiple formats:
        # 1. Raw TMDB API: {"watch/providers": {"results": {"US": {"flatrate": [...]}}}}
        # 2. Pydantic model_dump: {"watch_providers": {"flatrate": [...], "region": "US"}}
        watch_providers = item.get("watch/providers", {}).get("results", {}).get("US", {})
        if not watch_providers:
            watch_providers = item.get("watch_providers", {})
        flatrate = watch_providers.get("flatrate", [])

        if not flatrate:
            log_fn(
                f"Filter: {item_name} no flatrate. wp_keys={list(watch_providers.keys()) if watch_providers else 'empty'}"
            )
        provider_names = {p.get("provider_name") for p in flatrate if p.get("provider_name")}
        has_streaming = bool(provider_names & MAJOR_STREAMING_PROVIDERS)

        if flatrate and not has_streaming:
            log_fn(f"Filter: {item_name} providers={provider_names} not in MAJOR")

        if media_type == "tv":
            status = item.get("status") or ""
            is_returning_series = status == "Returning Series"
            last_air_date = item.get("last_air_date") or ""
            has_recent_activity = bool(last_air_date and last_air_date >= TV_SHOW_CUTOFF_DATE)

            passed_tv_filter = is_returning_series or has_recent_activity or has_streaming
            if not passed_tv_filter:
                log_fn(
                    f"Filter reject {item_name}: status={status}, last_air_date={last_air_date}, has_streaming={has_streaming}"
                )
            return passed_tv_filter

        # Popularity threshold - check both direct field and metrics dict
        popularity = item.get("popularity", 0) or item.get("metrics", {}).get("popularity", 0)
        if popularity < 1.0:
            log_fn(f"Filter reject {item_name}: popularity={popularity}")
            return False

        # Vote count threshold - check both direct field and metrics dict
        vote_count = item.get("vote_count", 0) or item.get("metrics", {}).get("vote_count", 0)
        if vote_count < 5:
            log_fn(f"Filter reject {item_name}: vote_count={vote_count}")
            return False

        # For movies: must have runtime
        if media_type == "movie":
            runtime = item.get("runtime") or 0
            if runtime < 40:
                return False

        # Check release info
        release_dates = item.get("release_dates", {}).get("results", [])
        has_theatrical = False
        for country in release_dates:
            if country.get("iso_3166_1") == "US":
                for release in country.get("release_dates", []):
                    if release.get("type") in [2, 3]:  # Theatrical
                        has_theatrical = True
                        break

        if not has_streaming and not has_theatrical:
            log_fn(f"Filter reject {item_name}: no streaming or theatrical")
        return has_streaming or has_theatrical

    def _passes_person_filter(self, person: dict) -> bool:
        """Check if person passes quality filters."""
        if not person.get("profile_path"):
            return False
        popularity: float = float(person.get("popularity", 0) or 0)
        return popularity >= MIN_PERSON_POPULARITY

    async def fetch_and_stage(
        self,
        media_type: Literal["tv", "movie", "person"],
        start_date: str,
        end_date: str,
        stats: ChangesETLStats,
        max_batches: int = 0,  # ETL should always bypass cache
    ) -> Path:
        """Phase 1: Fetch all data from TMDB and save to staging file."""
        stats.fetch_phase.started_at = datetime.now()
        stats.fetch_phase.phase = f"fetch_{media_type}"

        logger.info(f"Phase 1: Fetching {media_type} changes from {start_date} to {end_date}")

        # Get all changed IDs
        change_ids = await self._get_all_change_ids(media_type, start_date, end_date)
        stats.total_changes_found = len(change_ids)
        stats.non_adult_changes = len(change_ids)

        logger.info(f"Found {len(change_ids)} changed {media_type} items")

        if not change_ids:
            stats.fetch_phase.completed_at = datetime.now()
            return self._get_staging_path(media_type, start_date)

        # Fetch details in batches
        enriched_items = []

        batches = [change_ids[i : i + BATCH_SIZE] for i in range(0, len(change_ids), BATCH_SIZE)]
        total_batches = len(batches)
        stats.total_batches = total_batches
        stats.current_phase = "fetch"

        # Apply batch limit for testing
        if max_batches > 0:
            batches = batches[:max_batches]
            logger.info(f"⚠️  TESTING MODE: Limited to {max_batches} batches")

        for batch_idx, batch in enumerate(batches, 1):
            stats.current_batch = batch_idx
            batch_start = time.time()
            logger.info(f"Fetching batch {batch_idx}/{total_batches} ({len(batch)} items)")

            if media_type == "person":
                tasks = [self.person_service.get_person_details(pid) for pid in batch]
            else:
                # Use get_media_details with no_cache to get fresh data
                mc_type = MCType.MOVIE if media_type == "movie" else MCType.TV_SERIES
                tasks = [
                    self.get_media_details(
                        mid,
                        mc_type,
                        include_cast=True,
                        include_videos=True,
                        include_watch_providers=True,
                        include_keywords=True,
                    )
                    for mid in batch
                ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_time = time.time() - batch_start

            for tmdb_id, result in zip(batch, results, strict=True):
                stats.fetch_phase.items_processed += 1

                if isinstance(result, BaseException):
                    stats.fetch_phase.items_failed += 1
                    stats.fetch_phase.errors.append(f"{tmdb_id}: {result}")
                    stats.enrichment_errors += 1
                    logger.error(f"Exception for {tmdb_id}: {result}")
                    continue

                if result is None:
                    stats.fetch_phase.items_failed += 1
                    stats.enrichment_errors += 1
                    logger.warning(f"Null result for {tmdb_id}")
                    continue

                # Raw API returns dict directly, person service returns model
                if hasattr(result, "model_dump"):
                    item_dict = result.model_dump(mode="json")
                elif isinstance(result, dict):
                    item_dict = result
                else:
                    stats.fetch_phase.items_failed += 1
                    logger.warning(f"Unexpected type for {tmdb_id}: {type(result)}")
                    continue

                if not item_dict or item_dict.get("status_code") == 404:
                    stats.fetch_phase.items_failed += 1
                    logger.warning(f"Empty or 404 for {tmdb_id}")
                    continue

                # Check for enrichment errors (e.g. partial failures in get_media_details)
                item_error = item_dict.get("error")
                if item_error:
                    stats.fetch_phase.items_failed += 1
                    stats.fetch_phase.errors.append(f"{tmdb_id}: {item_error}")
                    logger.warning(f"Enrichment error for {tmdb_id}: {item_error}")
                    continue

                item_dict["_media_type"] = media_type
                item_dict["_tmdb_id"] = tmdb_id

                # Debug: log first item's key fields
                if stats.fetch_phase.items_processed == 1:
                    logger.info(f"DEBUG first item keys: {list(item_dict.keys())[:15]}")
                    logger.info(
                        f"DEBUG first item popularity={item_dict.get('popularity')} vote_count={item_dict.get('vote_count')}"
                    )

                # Apply filter
                stats.enriched_count += 1
                if media_type == "person":
                    if self._passes_person_filter(item_dict):
                        enriched_items.append(item_dict)
                        stats.passed_filter += 1
                    else:
                        stats.failed_filter += 1
                else:
                    if self._passes_media_filter(item_dict):
                        enriched_items.append(item_dict)
                        stats.passed_filter += 1
                    else:
                        stats.failed_filter += 1

                stats.fetch_phase.items_success += 1

            # Log batch performance
            items_per_sec = len(batch) / batch_time if batch_time > 0 else 0
            logger.info(f"  Batch {batch_idx}: {batch_time:.1f}s ({items_per_sec:.1f} items/sec)")

        # Save to staging file
        staging_path = self._get_staging_path(media_type, start_date)
        with gzip.open(staging_path, "wt", encoding="utf-8") as f:
            json.dump(
                {
                    "media_type": media_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "fetched_at": datetime.now().isoformat(),
                    "total_items": len(enriched_items),
                    "items": enriched_items,
                },
                f,
            )

        stats.staging_file = str(staging_path)
        stats.fetch_phase.completed_at = datetime.now()

        # Save errors to separate file if any
        if stats.fetch_phase.errors:
            errors_path = self._get_errors_path(media_type, start_date)
            with gzip.open(errors_path, "wt", encoding="utf-8") as f:
                json.dump(
                    {
                        "media_type": media_type,
                        "start_date": start_date,
                        "end_date": end_date,
                        "fetched_at": datetime.now().isoformat(),
                        "total_errors": len(stats.fetch_phase.errors),
                        "errors": stats.fetch_phase.errors,
                    },
                    f,
                    indent=2,
                )
            logger.info(f"Errors saved to {errors_path}")

        logger.info(f"Phase 1 complete: {len(enriched_items)} items saved to {staging_path}")
        logger.info(f"  Duration: {stats.fetch_phase.duration_seconds:.1f}s")
        logger.info(f"  Rate: {stats.fetch_phase.items_per_second:.1f} items/sec")

        return staging_path

    async def load_from_staging(
        self,
        staging_path: Path,
        redis_host: str,
        redis_port: int,
        redis_password: str | None,
        stats: ChangesETLStats,
    ) -> None:
        """Phase 2: Load from staging file to Redis."""
        stats.load_phase.started_at = datetime.now()
        stats.load_phase.phase = "load_to_redis"
        stats.current_phase = "load"

        logger.info(f"Phase 2: Loading from {staging_path} to Redis")

        # Read staging file
        with gzip.open(staging_path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("items", [])
        media_type = data.get("media_type", "")

        if not items:
            logger.info("No items to load")
            stats.load_phase.completed_at = datetime.now()
            return

        # Load genre mapping for media types
        genre_mapping: dict[int, str] = {}
        if media_type in ("movie", "tv"):
            logger.info("Loading genre mapping...")
            try:
                from utils.genre_mapping import get_genre_mapping_with_fallback

                genre_mapping = await get_genre_mapping_with_fallback(allow_fallback=True)
                logger.info(f"Loaded {len(genre_mapping)} genres")
            except Exception as e:
                logger.warning(f"Failed to load genre mapping: {e}. Continuing without it.")

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

            # Determine index prefix
            if media_type == "person":
                prefix = "person:"
            else:
                prefix = "media:"

            # Load items
            load_start = time.time()
            batch_size = 100

            for i in range(0, len(items), batch_size):
                batch = items[i : i + batch_size]
                batch_start = time.time()

                pipe = redis.pipeline()

                for item in batch:
                    stats.load_phase.items_processed += 1

                    try:
                        # Normalize and prepare for Redis
                        if media_type == "person":
                            doc_dict = self._normalize_person(item)
                            # For person, doc_dict is already in Redis format
                            key = f"{prefix}{doc_dict['id']}"
                            redis_doc = doc_dict
                        else:
                            doc = normalize_document(item, genre_mapping=genre_mapping)
                            if doc is None:
                                stats.load_phase.items_failed += 1
                                stats.load_phase.errors.append(
                                    f"{item.get('id')}: normalize_document returned None"
                                )
                                continue
                            # Use doc.id (mc_id like "tmdb_tv_12345") for key consistency
                            # This matches etl_service.py which uses search_doc.id
                            key = f"{prefix}{doc.id}"
                            redis_doc = document_to_redis(doc)

                        pipe.json().set(key, "$", redis_doc)
                        stats.load_phase.items_success += 1

                    except Exception as e:
                        stats.load_phase.items_failed += 1
                        stats.load_phase.errors.append(f"{item.get('id')}: {e}")

                await pipe.execute()

                batch_time = time.time() - batch_start
                items_per_sec = len(batch) / batch_time if batch_time > 0 else 0

                logger.info(
                    f"  Loaded {i + len(batch)}/{len(items)} "
                    f"({batch_time:.2f}s, {items_per_sec:.0f} items/sec)"
                )

            total_time = time.time() - load_start
            logger.info(f"Phase 2 complete: {stats.load_phase.items_success} items loaded")
            logger.info(f"  Duration: {total_time:.1f}s")
            logger.info(f"  Rate: {stats.load_phase.items_success / total_time:.1f} items/sec")

        finally:
            await redis.aclose()

        stats.load_phase.completed_at = datetime.now()

        # Save load errors to separate file if any
        if stats.load_phase.errors:
            # Extract media_type and date from staging path for errors filename
            errors_path = staging_path.parent / staging_path.name.replace(
                ".json.gz", "_load_errors.json.gz"
            )
            with gzip.open(errors_path, "wt", encoding="utf-8") as f:
                json.dump(
                    {
                        "staging_file": str(staging_path),
                        "loaded_at": datetime.now().isoformat(),
                        "total_errors": len(stats.load_phase.errors),
                        "errors": stats.load_phase.errors,
                    },
                    f,
                    indent=2,
                )
            logger.info(f"Load errors saved to {errors_path}")

    def _normalize_person(self, person: dict) -> dict:
        """Normalize person data for Redis."""
        profile_path = person.get("profile_path", "")
        return {
            "id": str(person.get("id", "")),
            "name": person.get("name", ""),
            "type": "person",
            "image": f"{IMAGE_BASE_URL}w185{profile_path}" if profile_path else "",
            "popularity": person.get("popularity", 0),
            "known_for_department": person.get("known_for_department", ""),
            "biography": (person.get("biography", "") or "")[:500],
        }


async def run_nightly_etl(
    media_type: Literal["tv", "movie", "person"],
    start_date: str | None = None,
    end_date: str | None = None,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_password: str | None = None,
    staging_dir: str = "/tmp/etl-staging",
    fetch_only: bool = False,
    load_only: bool = False,
    staging_file: str | None = None,
    max_batches: int = 0,
    verbose: bool = False,
    stats: ChangesETLStats | None = None,
) -> ChangesETLStats:
    """
    Run the two-phase changes ETL.

    Args:
        media_type: Type of media to process
        start_date: Start date (YYYY-MM-DD), defaults to yesterday
        end_date: End date (YYYY-MM-DD), defaults to today
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        staging_dir: Directory for staging files
        fetch_only: Only run phase 1 (fetch)
        load_only: Only run phase 2 (load)
        staging_file: Path to existing staging file (for load_only)
        max_batches: Max batches to process (0 = unlimited)
        verbose: Enable verbose logging (shows filter rejection reasons)
        stats: Optional pre-created stats object for live progress tracking
    """
    load_env()

    # Default dates
    if not end_date:
        end_date = date.today().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    if stats is None:
        stats = ChangesETLStats(
            media_type=media_type,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        stats.media_type = media_type
        stats.start_date = start_date
        stats.end_date = end_date

    etl = TMDBChangesETL(staging_dir=staging_dir, verbose=verbose)

    print("=" * 60)
    print(f"🚀 TMDB Changes ETL v2 - {media_type.upper()}")
    print("=" * 60)
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Redis: {redis_host}:{redis_port}")
    print(f"  Staging dir: {staging_dir}")
    print(f"  Mode: {'fetch only' if fetch_only else 'load only' if load_only else 'full'}")
    print()

    staging_path = None

    # Phase 1: Fetch
    if not load_only:
        staging_path = await etl.fetch_and_stage(
            media_type, start_date, end_date, stats, max_batches
        )

        print("📊 Phase 1 (Fetch) Results:")
        print(f"  Items found: {stats.total_changes_found}")
        print(f"  Items fetched: {stats.fetch_phase.items_success}")
        print(f"  Passed filter: {stats.passed_filter}")
        print(f"  Failed filter: {stats.failed_filter}")
        print(f"  Errors: {stats.fetch_phase.items_failed}")
        print(f"  Duration: {stats.fetch_phase.duration_seconds:.1f}s")
        print(f"  Rate: {stats.fetch_phase.items_per_second:.1f} items/sec")
        print(f"  Staging file: {staging_path}")
        print()

    # Phase 2: Load
    if not fetch_only:
        if load_only and staging_file:
            staging_path = Path(staging_file)

        if staging_path and staging_path.exists():
            logger.info(f"Loading from {staging_path}")
            await etl.load_from_staging(staging_path, redis_host, redis_port, redis_password, stats)

            print()
            print("📊 Phase 2 (Load) Results:")
            print(f"  Items loaded: {stats.load_phase.items_success}")
            print(f"  Errors: {stats.load_phase.items_failed}")
            print(f"  Duration: {stats.load_phase.duration_seconds:.1f}s")
            print(f"  Rate: {stats.load_phase.items_per_second:.1f} items/sec")
            print()

    # Summary
    total_duration = stats.fetch_phase.duration_seconds + stats.load_phase.duration_seconds

    print("=" * 60)
    print("📊 ETL Summary")
    print("=" * 60)
    print(f"  Total duration: {total_duration:.1f}s")
    print(
        f"  Fetch phase: {stats.fetch_phase.duration_seconds:.1f}s ({stats.fetch_phase.items_per_second:.1f}/sec)"
    )
    print(
        f"  Load phase: {stats.load_phase.duration_seconds:.1f}s ({stats.load_phase.items_per_second:.1f}/sec)"
    )
    print()

    return stats


def cleanup_old_staging_files(staging_dir: str = "/tmp/etl-staging", max_age_days: int = 1):
    """Remove staging files older than max_age_days."""
    staging_path = Path(staging_dir)
    if not staging_path.exists():
        return

    cutoff = datetime.now() - timedelta(days=max_age_days)

    for file in staging_path.glob("*.json.gz"):
        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        if mtime < cutoff:
            logger.info(f"Removing old staging file: {file}")
            file.unlink()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TMDB Changes ETL v2")
    parser.add_argument("--media-type", "-m", required=True, choices=["tv", "movie", "person"])
    parser.add_argument("--start-date", "-s", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", "-e", help="End date (YYYY-MM-DD)")
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD"))
    parser.add_argument("--staging-dir", default="/tmp/etl-staging")
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch, don't load")
    parser.add_argument("--load-only", action="store_true", help="Only load from existing file")
    parser.add_argument("--file", help="Staging file path (for --load-only)")
    parser.add_argument(
        "--max-batches", type=int, default=0, help="Limit batches for testing (0=no limit)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable Redis cache (default: cache enabled)",
    )

    args = parser.parse_args()

    # Cleanup old files first
    cleanup_old_staging_files(args.staging_dir)

    if args.no_cache:
        logger.info("⚠️  Cache disabled: Fetching fresh data from TMDB API")
        disable_cache()
    else:
        logger.info("✅ Cache enabled: Using Redis cache (7-day TTL)")

    asyncio.run(
        run_nightly_etl(
            media_type=args.media_type,
            start_date=args.start_date,
            end_date=args.end_date,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_password=args.redis_password,
            staging_dir=args.staging_dir,
            fetch_only=args.fetch_only,
            load_only=args.load_only,
            staging_file=args.file,
            max_batches=args.max_batches,  # Default is no_cache=True
        )
    )
