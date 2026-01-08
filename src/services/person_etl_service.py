"""
Person ETL Service for downloading, enriching, and loading person data into Redis Search.

This module handles a 3-step process:
1. Bulk Download: Download daily person ID exports from TMDB
2. Extract (Enrich): Load person IDs, filter by popularity, call get_person_details,
   and save enriched MCPersonItem data
3. Load: Load enriched data into Redis Search index

The process is different from movies/TV because:
- Person data comes from a daily TMDB export (JSON lines format)
- We need to pre-filter to reduce API calls
- Each person needs individual API enrichment
"""

import asyncio
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from adapters.config import load_env
from api.tmdb.person import TMDBPersonService
from contracts.models import MCSources, MCType
from core.normalize import SearchDocument, document_to_redis
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Minimum popularity score to include in the index
MIN_POPULARITY = 1.0

# TMDB image base URL
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"


@dataclass
class PersonETLStats:
    """Statistics from a person ETL run."""

    # Download stats
    downloaded_count: int = 0
    download_file: str = ""

    # Extract stats
    pre_filter_count: int = 0
    enriched_count: int = 0
    post_filter_count: int = 0
    extract_errors: list[str] = field(default_factory=list)

    # Load stats
    files_processed: int = 0
    documents_loaded: int = 0
    documents_skipped: int = 0
    load_errors: list[str] = field(default_factory=list)

    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class PersonETLConfig:
    """Configuration for Person ETL runs."""

    data_dir: Path
    redis_host: str
    redis_port: int
    redis_password: str | None
    batch_size: int = 50  # Smaller batches for API calls
    api_concurrency: int = 5  # Concurrent API calls

    @classmethod
    def from_env(cls) -> "PersonETLConfig":
        """Create config from environment variables."""
        load_env()
        return cls(
            data_dir=Path(os.getenv("ETL_DATA_DIR", "data/person")),
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", "6380")),
            redis_password=os.getenv("REDIS_PASSWORD") or None,
            batch_size=int(os.getenv("PERSON_ETL_BATCH_SIZE", "50")),
            api_concurrency=int(os.getenv("PERSON_API_CONCURRENCY", "5")),
        )


class PersonETLService:
    """Service for downloading, enriching, and loading person data into Redis."""

    def __init__(self, config: PersonETLConfig):
        self.config = config
        self.redis: Redis | None = None
        self.tmdb_service = TMDBPersonService()

    async def connect(self) -> None:
        """Connect to Redis."""
        self.redis = Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            decode_responses=True,
        )
        ping_result = self.redis.ping()
        if asyncio.iscoroutine(ping_result):
            await ping_result

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis:
            await self.redis.aclose()

    # =========================================================================
    # Step 1: Bulk Download
    # =========================================================================

    def discover_person_id_files(self) -> list[Path]:
        """
        Discover person ID export files in the data directory.

        Returns:
            List of paths to person_ids_*.json files
        """
        files = list(self.config.data_dir.glob("person_ids_*.json"))
        return sorted(files, reverse=True)  # Most recent first

    def get_latest_person_id_file(self) -> Path | None:
        """Get the most recent person ID file."""
        files = self.discover_person_id_files()
        return files[0] if files else None

    # =========================================================================
    # Step 2: Extract (Enrich)
    # =========================================================================

    def discover_enriched_files(self) -> list[Path]:
        """
        Discover enriched person files in the data directory.

        Returns:
            List of paths to enriched_person_*.json files
        """
        files = list(self.config.data_dir.glob("enriched_person_*.json"))
        return sorted(files, reverse=True)  # Most recent first

    async def extract_and_enrich(
        self,
        source_file: Path,
        limit: int | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> PersonETLStats:
        """
        Load person IDs, filter by popularity, enrich with TMDB details, and save.

        Args:
            source_file: Path to person_ids_*.json file
            limit: Optional limit on number of persons to process (for testing)
            progress_callback: Optional callback for progress updates

        Returns:
            PersonETLStats with extraction results
        """
        stats = PersonETLStats(started_at=datetime.now())

        print("=" * 60)
        print("🚀 Person Extract & Enrich")
        print("=" * 60)
        print(f"  Source file: {source_file}")
        print(f"  Minimum popularity: {MIN_POPULARITY}")
        print()

        # Extract date from filename: person_ids_YYYY_MM_DD.json
        date_str = source_file.stem.replace("person_ids_", "")

        # Load and pre-filter by popularity
        print("📂 Loading and pre-filtering by popularity...")
        persons_to_enrich = []
        total_lines = 0

        with open(source_file) as f:
            for line in f:
                total_lines += 1
                if not line.strip():
                    continue

                try:
                    person = json.loads(line)
                    popularity = person.get("popularity", 0)

                    if popularity > MIN_POPULARITY:
                        persons_to_enrich.append(person)

                        # Apply limit if specified
                        if limit and len(persons_to_enrich) >= limit:
                            break

                except json.JSONDecodeError:
                    continue

        stats.pre_filter_count = len(persons_to_enrich)
        print(f"  Total records: {total_lines:,}")
        print(f"  After pre-filter (popularity > {MIN_POPULARITY}): {stats.pre_filter_count:,}")
        print()

        # Enrich each person with TMDB details
        print("🔍 Enriching with TMDB details...")
        enriched_persons: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(self.config.api_concurrency)

        async def enrich_person(person: dict[str, Any]) -> dict[str, Any] | None:
            """Enrich a single person with TMDB details."""
            async with semaphore:
                try:
                    person_id = person.get("id")
                    if not person_id:
                        return None

                    details = await self.tmdb_service.get_person_details(person_id)
                    if details:
                        # Convert MCPersonItem to dict for storage
                        result: dict[str, Any] = details.to_dict()
                        return result
                    return None
                except Exception as e:
                    stats.extract_errors.append(f"Error enriching person {person.get('id')}: {e}")
                    return None

        # Process in batches
        batch_size = self.config.batch_size
        total_batches = (len(persons_to_enrich) + batch_size - 1) // batch_size

        for batch_idx in range(0, len(persons_to_enrich), batch_size):
            batch = persons_to_enrich[batch_idx : batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1

            if progress_callback is not None:
                progress_callback(
                    batch_current=batch_num,
                    batch_total=total_batches,
                    status_message=f"Processing batch {batch_num}/{total_batches}",
                )

            print(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} items)...")

            # Enrich batch concurrently
            tasks = [enrich_person(p) for p in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, BaseException):
                    stats.extract_errors.append(str(result))
                elif result is not None:
                    enriched_persons.append(result)

        stats.enriched_count = len(enriched_persons)
        print()
        print(f"  Enriched: {stats.enriched_count:,}")

        # Post-filter: ensure we have required fields
        print("🔄 Post-filtering enriched data...")
        filtered_persons = []
        for person in enriched_persons:
            # Must have name and profile image for display
            if not person.get("name"):
                continue
            if not person.get("profile_path") and not person.get("has_image"):
                continue

            # Additional quality checks can be added here
            filtered_persons.append(person)

        stats.post_filter_count = len(filtered_persons)
        print(f"  After post-filter: {stats.post_filter_count:,}")
        print()

        # Save enriched data
        output_file = self.config.data_dir / f"enriched_person_{date_str}.json"
        print(f"💾 Saving to {output_file}...")

        output_data = {
            "metadata": {
                "source_file": str(source_file),
                "created_at": datetime.now().isoformat(),
                "pre_filter_count": stats.pre_filter_count,
                "enriched_count": stats.enriched_count,
                "post_filter_count": stats.post_filter_count,
                "errors": len(stats.extract_errors),
            },
            "results": filtered_persons,
        }

        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2, default=str)

        stats.completed_at = datetime.now()

        print()
        print("=" * 60)
        print("📊 Extract Summary")
        print("=" * 60)
        print(f"  Pre-filtered: {stats.pre_filter_count:,}")
        print(f"  Enriched: {stats.enriched_count:,}")
        print(f"  Post-filtered: {stats.post_filter_count:,}")
        print(f"  Errors: {len(stats.extract_errors)}")
        if stats.duration_seconds:
            print(f"  Duration: {stats.duration_seconds:.1f}s")
        print(f"  Output: {output_file}")
        print()

        return stats

    # =========================================================================
    # Step 3: Load
    # =========================================================================

    async def load_enriched_file(self, file_path: Path) -> PersonETLStats:
        """
        Load enriched person data into Redis.

        Args:
            file_path: Path to enriched_person_*.json file

        Returns:
            PersonETLStats with load results
        """
        stats = PersonETLStats(started_at=datetime.now())

        print("=" * 60)
        print("🚀 Person Load")
        print("=" * 60)
        print(f"  File: {file_path}")
        print(f"  Redis: {self.config.redis_host}:{self.config.redis_port}")
        print()

        # Connect to Redis
        print("🔌 Connecting to Redis...")
        try:
            await self.connect()
            print("  ✅ Connected")
        except Exception as e:
            stats.load_errors.append(f"Redis connection failed: {e}")
            print(f"  ❌ Connection failed: {e}")
            return stats

        # Load the enriched file
        print(f"📂 Loading: {file_path.name}")
        try:
            with open(file_path) as f:
                data = json.load(f)
        except Exception as e:
            stats.load_errors.append(f"Failed to read {file_path}: {e}")
            return stats

        results = data.get("results", [])
        if not results:
            print("  ⚠️ No results found")
            return stats

        stats.files_processed = 1
        print(f"  Found {len(results):,} persons")
        print()

        # Process and load into Redis
        print("📥 Loading into Redis...")

        if not self.redis:
            raise RuntimeError("Not connected to Redis")

        pipeline = self.redis.pipeline()
        batch_count = 0

        for person in results:
            # Normalize the document for search index
            search_doc = self._normalize_person_for_search(person)

            if search_doc is None:
                stats.documents_skipped += 1
                continue

            # Convert to Redis format
            key = f"person:{search_doc.id}"
            redis_doc = document_to_redis(search_doc)

            # Add also_known_as to the document for search
            also_known_as = person.get("also_known_as", [])
            if also_known_as:
                redis_doc["also_known_as"] = " | ".join(also_known_as[:10])  # Limit to 10
            else:
                redis_doc["also_known_as"] = ""

            # Add display fields not in SearchDocument
            redis_doc["known_for_department"] = person.get("known_for_department") or ""
            redis_doc["birthday"] = person.get("birthday")
            redis_doc["deathday"] = person.get("deathday")
            redis_doc["place_of_birth"] = person.get("place_of_birth")

            # Calculate age
            birthday = person.get("birthday")
            deathday = person.get("deathday")
            if birthday:
                try:
                    birth_date = date.fromisoformat(birthday)
                    end_date = date.fromisoformat(deathday) if deathday else date.today()
                    age = (
                        end_date.year
                        - birth_date.year
                        - ((end_date.month, end_date.day) < (birth_date.month, birth_date.day))
                    )
                    redis_doc["age"] = age
                    redis_doc["is_deceased"] = deathday is not None
                except (ValueError, TypeError):
                    redis_doc["age"] = None
                    redis_doc["is_deceased"] = False
            else:
                redis_doc["age"] = None
                redis_doc["is_deceased"] = False

            # Add known_for titles if available
            known_for = person.get("known_for", [])
            if known_for:
                known_for_titles = []
                for item in known_for[:3]:  # Top 3
                    title = item.get("title") or item.get("name")
                    if title:
                        known_for_titles.append(title)
                redis_doc["known_for_titles"] = known_for_titles
            else:
                redis_doc["known_for_titles"] = []

            # Add to pipeline
            pipeline.json().set(key, "$", redis_doc)
            batch_count += 1
            stats.documents_loaded += 1

            # Execute batch
            if batch_count >= self.config.batch_size:
                await pipeline.execute()
                pipeline = self.redis.pipeline()
                batch_count = 0

        # Execute remaining items
        if batch_count > 0:
            await pipeline.execute()

        await self.disconnect()

        stats.completed_at = datetime.now()

        print()
        print("=" * 60)
        print("📊 Load Summary")
        print("=" * 60)
        print(f"  Documents loaded: {stats.documents_loaded:,}")
        print(f"  Documents skipped: {stats.documents_skipped:,}")
        if stats.duration_seconds:
            print(f"  Duration: {stats.duration_seconds:.1f}s")
        print()
        print("🎉 Load Complete!")

        return stats

    def _normalize_person_for_search(self, person: dict) -> SearchDocument | None:
        """
        Normalize a person dict into a SearchDocument for the index.

        Args:
            person: Enriched person data dict (from MCPersonItem.to_dict())

        Returns:
            SearchDocument or None if cannot normalize
        """
        # Extract ID - source_id is the numeric TMDB ID
        person_id = person.get("id")
        if not person_id:
            return None

        source_id = str(person_id)
        doc_id = f"tmdb_person_{person_id}"

        # Extract name
        name = person.get("name")
        if not name:
            return None

        # Extract profile image (medium size)
        image = None
        profile_images = person.get("profile_images", {})
        if profile_images:
            image = profile_images.get("medium")
        elif person.get("profile_path"):
            image = f"{IMAGE_BASE_URL}w185{person['profile_path']}"

        # Detect subtype from known_for_department
        from src.contracts.models import MCSubType

        mc_subtype = MCSubType.PERSON
        department = (person.get("known_for_department") or "").lower()
        if department == "acting":
            mc_subtype = MCSubType.ACTOR
        elif department == "directing":
            mc_subtype = MCSubType.DIRECTOR
        elif department == "writing":
            mc_subtype = MCSubType.WRITER
        elif department in ("production", "producing"):
            mc_subtype = MCSubType.PRODUCER

        # Compute popularity score
        popularity = person.get("popularity", 0)
        # Normalize to 0-100 scale (TMDB popularity can be very high for famous people)
        normalized_popularity = min(popularity, 100)

        # Extract biography (truncated)
        biography = person.get("biography") or ""
        if len(biography) > 200:
            biography = biography[:200].rsplit(" ", 1)[0] + "..."

        return SearchDocument(
            id=doc_id,
            search_title=name,
            mc_type=MCType.PERSON,
            mc_subtype=mc_subtype,
            source=MCSources.TMDB,
            source_id=source_id,
            year=None,  # Persons don't have a year
            popularity=round(normalized_popularity, 2),
            rating=0.0,  # Persons don't have ratings
            image=image,
            cast=[],  # Persons don't have cast
            overview=biography if biography else None,
        )


async def run_person_extract(
    source_file: str | None = None,
    limit: int | None = None,
    progress_callback: Callable[..., Any] | None = None,
) -> PersonETLStats:
    """
    Run person extraction and enrichment.

    Args:
        source_file: Optional specific source file path
        limit: Optional limit on number of persons
        progress_callback: Optional callback for progress updates

    Returns:
        PersonETLStats with results
    """
    config = PersonETLConfig.from_env()
    service = PersonETLService(config)

    # Find source file
    path: Path
    if source_file:
        path = Path(source_file)
    else:
        found_path = service.get_latest_person_id_file()
        if not found_path:
            raise FileNotFoundError("No person_ids_*.json files found")
        path = found_path

    return await service.extract_and_enrich(path, limit=limit, progress_callback=progress_callback)


async def run_person_load(
    source_file: str | None = None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
) -> PersonETLStats:
    """
    Run person load into Redis.

    Args:
        source_file: Optional specific enriched file path
        redis_host: Optional Redis host override
        redis_port: Optional Redis port override
        redis_password: Optional Redis password override

    Returns:
        PersonETLStats with results
    """
    config = PersonETLConfig.from_env()

    # Override config if specified
    if redis_host:
        config.redis_host = redis_host
    if redis_port:
        config.redis_port = redis_port
    if redis_password is not None:
        config.redis_password = redis_password

    service = PersonETLService(config)

    # Find source file
    if source_file:
        path = Path(source_file)
    else:
        files = service.discover_enriched_files()
        if not files:
            raise FileNotFoundError("No enriched_person_*.json files found")
        path = files[0]  # Most recent

    return await service.load_enriched_file(path)
