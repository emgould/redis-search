"""
PodcastIndex Nightly ETL - Download database dump and load recent updates.

Downloads the daily PodcastIndex database dump, queries for recently updated
podcasts, and upserts them to Redis. Cleans up the downloaded file afterwards.

Flow:
1. Download podcastindex_feeds.db.tgz (~500MB compressed, ~2GB uncompressed)
2. Extract to temp directory
3. Query for records with lastUpdate >= since_timestamp
4. Apply same filters as bulk loader (popularity >= 3, English, etc.)
5. Upsert to Redis
6. Clean up downloaded files

Usage:
    # Full ETL (download, load updates from last 48 hours, cleanup)
    python -m src.etl.pi_nightly_etl

    # Custom time window
    python -m src.etl.pi_nightly_etl --since-hours 24

    # Keep downloaded file (don't cleanup)
    python -m src.etl.pi_nightly_etl --keep-db

    # Dry run (don't write to Redis)
    python -m src.etl.pi_nightly_etl --dry-run

    # Limit records for testing
    python -m src.etl.pi_nightly_etl --limit 100
"""

import asyncio
import math
import os
import shutil
import sqlite3
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from redis.asyncio import Redis

from adapters.config import load_env
from contracts.models import MCSources, MCType
from core.iptc import expand_keywords, normalize_tag
from core.normalize import SearchDocument, document_to_redis, resolve_timestamps
from utils.get_logger import get_logger

logger = get_logger(__name__)

# PodcastIndex database dump URL
DB_DOWNLOAD_URL = "https://public.podcastindex.org/podcastindex_feeds.db.tgz"
DB_FILENAME = "podcastindex_feeds.db"

# User-Agent header required by Cloudflare (blocks default Python urllib User-Agent)
USER_AGENT = "Mozilla/5.0 (compatible; MediaCircle-ETL/1.0; +https://mediacircle.io)"

# Redis batch size
REDIS_BATCH_SIZE = 100

# Filtering thresholds (consistent with bulk loader)
MIN_POPULARITY_SCORE = 3
MIN_EPISODE_COUNT = 1


@dataclass
class PIPhaseStats:
    """Stats for a single ETL phase (compatibility with TMDB ETL)."""

    phase: str = ""
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


@dataclass
class PIETLStats:
    """Statistics from a PodcastIndex nightly ETL run.

    Compatible with ChangesETLStats interface for ETL runner integration.
    """

    started_at: datetime | None = None
    completed_at: datetime | None = None

    # Download
    download_started: datetime | None = None
    download_completed: datetime | None = None
    download_size_mb: float = 0.0

    # Query
    since_timestamp: int = 0
    total_updated: int = 0
    after_filters: int = 0

    # Load
    documents_loaded: int = 0
    documents_skipped: int = 0
    errors: int = 0
    error_messages: list[str] = field(default_factory=list)

    # Compatibility with ChangesETLStats interface
    media_type: str = "podcast"
    total_changes_found: int = 0
    passed_filter: int = 0
    failed_filter: int = 0
    fetch_phase: PIPhaseStats = field(default_factory=lambda: PIPhaseStats(phase="fetch"))
    load_phase: PIPhaseStats = field(default_factory=lambda: PIPhaseStats(phase="load"))

    @property
    def duration_seconds(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    def finalize(self) -> None:
        """Update compatibility fields from internal stats."""
        self.total_changes_found = self.after_filters
        self.passed_filter = self.documents_loaded
        self.failed_filter = self.documents_skipped
        self.fetch_phase.items_success = self.after_filters
        self.fetch_phase.items_processed = self.after_filters
        self.load_phase.items_success = self.documents_loaded
        self.load_phase.items_failed = self.errors
        self.load_phase.errors = self.error_messages

    def to_dict(self) -> dict[str, Any]:
        return {
            "duration_seconds": self.duration_seconds,
            "download_size_mb": self.download_size_mb,
            "since_timestamp": self.since_timestamp,
            "total_updated": self.total_updated,
            "after_filters": self.after_filters,
            "documents_loaded": self.documents_loaded,
            "documents_skipped": self.documents_skipped,
            "errors": self.errors,
        }


class PodcastIndexNightlyETL:
    """PodcastIndex Nightly ETL using database dump."""

    def __init__(self, temp_dir: str | None = None):
        self.temp_dir = Path(temp_dir) if temp_dir else Path(tempfile.mkdtemp(prefix="pi_etl_"))
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path: Path | None = None
        self.redis: Redis | None = None

    async def _connect_redis(self) -> None:
        """Connect to Redis."""
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", "6380"))
        redis_password = os.getenv("REDIS_PASSWORD") or None

        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )
        await self.redis.ping()  # type: ignore[misc]
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")

    async def _disconnect_redis(self) -> None:
        """Disconnect from Redis."""
        if self.redis:
            await self.redis.aclose()
            self.redis = None

    def _download_database(self, stats: PIETLStats) -> Path:
        """Download and extract the PodcastIndex database dump."""
        stats.download_started = datetime.now()

        tgz_path = self.temp_dir / "podcastindex_feeds.db.tgz"
        db_path = self.temp_dir / DB_FILENAME

        logger.info(f"Downloading database from {DB_DOWNLOAD_URL}")
        logger.info(f"  Target: {tgz_path}")

        # Download with progress using custom User-Agent (required by Cloudflare)
        request = urllib.request.Request(DB_DOWNLOAD_URL, headers={"User-Agent": USER_AGENT})
        response = urllib.request.urlopen(request)
        total_size = int(response.headers.get("Content-Length", 0))

        downloaded = 0
        block_size = 8192
        last_log_mb = 0

        with open(tgz_path, "wb") as f:
            while True:
                block = response.read(block_size)
                if not block:
                    break
                f.write(block)
                downloaded += len(block)

                # Log progress every ~50MB
                current_mb = downloaded // (1024 * 1024)
                if total_size > 0 and current_mb >= last_log_mb + 50:
                    percent = downloaded * 100 / total_size
                    mb_total = total_size / (1024 * 1024)
                    logger.info(
                        f"  Download progress: {percent:.1f}% ({current_mb}/{mb_total:.0f} MB)"
                    )
                    last_log_mb = current_mb

        # Record download size
        stats.download_size_mb = tgz_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Download complete: {stats.download_size_mb:.1f} MB")

        # Extract
        logger.info("Extracting database...")
        with tarfile.open(tgz_path, "r:gz") as tar:
            tar.extractall(path=str(self.temp_dir))

        # Remove tgz to save space
        tgz_path.unlink()

        if not db_path.exists():
            # Try to find the extracted file
            extracted_files = list(self.temp_dir.glob("*.db"))
            if extracted_files:
                db_path = extracted_files[0]
            else:
                raise FileNotFoundError("Database file not found after extraction")

        db_size_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Extracted: {db_path.name} ({db_size_mb:.1f} MB)")

        stats.download_completed = datetime.now()
        self.db_path = db_path
        return db_path

    def _compute_popularity(self, popularity_score: int, episode_count: int) -> float:
        """
        Compute a normalized popularity score (0-100).
        Same logic as bulk loader.
        """
        # PodcastIndex popularityScore is 0-29
        # Normalize to 0-100 scale
        pop_normalized = min(popularity_score * 3.5, 100)

        # Episode count: log scale contribution
        episode_score = min(math.log10(episode_count + 1) * 20, 50) if episode_count > 0 else 0

        # Combined: 70% popularity, 30% episode count
        combined = (pop_normalized * 0.7) + (episode_score * 0.3)

        return round(combined, 2)

    def _build_categories_array(self, row: sqlite3.Row) -> list[str]:
        """
        Build normalized and IPTC-expanded categories array from category1-10 fields.

        Extracts all non-empty categories, normalizes them, and expands using IPTC aliases.
        """
        raw_categories: list[str] = []
        row_keys = row.keys()
        for i in range(1, 11):
            cat_key = f"category{i}"
            cat_value = row[cat_key] if cat_key in row_keys else ""
            if cat_value and str(cat_value).strip():
                raw_categories.append(str(cat_value).strip())

        if not raw_categories:
            return []

        # Convert to IPTC keyword format for expansion
        # IPTC expand_keywords expects [{"name": "category"}, ...]
        keyword_dicts: list[dict[str, str]] = [{"name": cat} for cat in raw_categories]

        # expand_keywords normalizes and expands with IPTC aliases
        expanded: list[str] = expand_keywords(keyword_dicts)
        return expanded

    def _row_to_search_document(self, row: sqlite3.Row) -> SearchDocument:
        """Convert SQLite row to SearchDocument."""
        feed_id = row["id"]
        mc_id = f"podcastindex_podcast_{feed_id}"

        # Compute popularity
        popularity_score = row["popularityScore"] or 0
        episode_count = row["episodeCount"] or 0
        popularity = self._compute_popularity(popularity_score, episode_count)

        # Get image
        image = row["imageUrl"]

        # Get description (truncate if needed)
        description = row["description"] or ""
        if description and len(description) > 500:
            description = description[:497] + "..."

        return SearchDocument(
            id=mc_id,
            search_title=row["title"] or "",
            mc_type=MCType.PODCAST,
            mc_subtype=None,
            source=MCSources.PODCASTINDEX,
            source_id=str(feed_id),
            year=None,
            popularity=popularity,
            rating=0.0,
            image=image,
            cast=[],
            overview=description,
            genre_ids=[],
            genres=[],
            cast_ids=[],
            cast_names=[],
        )

    def _add_display_fields(self, redis_doc: dict[str, Any], row: sqlite3.Row) -> dict[str, Any]:
        """Add podcast-specific display fields to Redis document."""
        redis_doc["title"] = row["title"]
        redis_doc["url"] = row["url"]
        redis_doc["site"] = row["link"]
        redis_doc["author"] = row["itunesAuthor"] or None
        redis_doc["owner_name"] = row["itunesOwnerName"] or None
        # Normalize language for consistent filtering
        raw_language = row["language"] or ""
        redis_doc["language"] = normalize_tag(raw_language) if raw_language else None
        # Normalized and IPTC-expanded categories array
        redis_doc["categories"] = self._build_categories_array(row)
        # Normalized author for exact TAG matching
        raw_author = row["itunesAuthor"] or ""
        redis_doc["author_normalized"] = normalize_tag(raw_author) if raw_author else None
        redis_doc["episode_count"] = row["episodeCount"] or 0
        redis_doc["itunes_id"] = row["itunesId"] or None
        redis_doc["podcast_guid"] = row["podcastGuid"] or None
        redis_doc["popularity_score"] = row["popularityScore"] or 0

        # Timestamp
        row_keys = row.keys()
        last_update = row["lastUpdate"] if "lastUpdate" in row_keys else None
        if last_update:
            redis_doc["last_update_time"] = datetime.fromtimestamp(last_update).isoformat()

        # Fields not available in DB dump
        redis_doc["artwork"] = None
        redis_doc["trend_score"] = None
        redis_doc["spotify_url"] = None
        redis_doc["relevancy_score"] = None

        return redis_doc

    async def run(
        self,
        since_hours: int = 48,
        limit: int | None = None,
        dry_run: bool = False,
        keep_db: bool = False,
    ) -> PIETLStats:
        """
        Run the nightly ETL.

        Args:
            since_hours: Load podcasts updated in the last N hours (default: 48)
            limit: Maximum records to process (for testing)
            dry_run: If True, don't write to Redis
            keep_db: If True, don't delete downloaded database

        Returns:
            ETL statistics
        """
        stats = PIETLStats(started_at=datetime.now())

        try:
            # Step 1: Download database
            logger.info("=" * 60)
            logger.info("🎙️  PodcastIndex Nightly ETL")
            logger.info("=" * 60)

            db_path = self._download_database(stats)

            # Step 2: Connect to Redis
            if not dry_run:
                await self._connect_redis()

            # Step 3: Query database
            logger.info("Querying database for recent updates...")

            since_time = datetime.now() - timedelta(hours=since_hours)
            since_timestamp = int(since_time.timestamp())
            stats.since_timestamp = since_timestamp

            logger.info(f"  Since: {since_time} (ts: {since_timestamp})")

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Build query with filters (same as bulk loader + lastUpdate filter)
            query = """
                SELECT
                    id, title, url, link, description,
                    itunesAuthor, itunesOwnerName, imageUrl, language,
                    category1, category2, category3, category4, category5,
                    category6, category7, category8, category9, category10,
                    episodeCount, popularityScore, itunesId, podcastGuid, lastUpdate
                FROM podcasts
                WHERE
                    lastUpdate >= ?
                    AND popularityScore >= ?
                    AND language LIKE 'en%'
                    AND episodeCount > 0
                    AND imageUrl != ''
                    AND imageUrl IS NOT NULL
                    AND dead = 0
                ORDER BY popularityScore DESC, episodeCount DESC
            """
            params: list[Any] = [since_timestamp, MIN_POPULARITY_SCORE]

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            stats.after_filters = len(rows)
            logger.info(f"  Found {len(rows):,} podcasts matching criteria")

            if not rows:
                logger.info("No podcasts to update")
                conn.close()
                return stats

            # Step 4: Load into Redis
            logger.info("Loading into Redis...")

            prepared: list[tuple[str, dict[str, Any]]] = []

            for row in rows:
                try:
                    search_doc = self._row_to_search_document(row)
                    redis_doc = document_to_redis(search_doc)
                    redis_doc = self._add_display_fields(redis_doc, row)
                    key = f"podcast:{search_doc.id}"

                    if dry_run:
                        stats.documents_loaded += 1
                        if stats.documents_loaded <= 5:
                            logger.info(
                                f"  [DRY RUN] Would load: {search_doc.search_title[:50]} -> {key}"
                            )
                    else:
                        prepared.append((key, redis_doc))
                        stats.documents_loaded += 1

                except Exception as e:
                    stats.errors += 1
                    if stats.errors <= 5:
                        row_keys = row.keys()
                        podcast_id = row["id"] if "id" in row_keys else "unknown"
                        stats.error_messages.append(f"Feed {podcast_id}: {e}")
                        logger.error(f"Error processing podcast {podcast_id}: {e}")

            if not dry_run:
                for i in range(0, len(prepared), REDIS_BATCH_SIZE):
                    batch = prepared[i : i + REDIS_BATCH_SIZE]
                    now_ts = int(datetime.now(UTC).timestamp())

                    read_pipe = self.redis.pipeline()  # type: ignore[union-attr]
                    for key, _ in batch:
                        read_pipe.json().get(key)
                    existing_docs: list[object] = await read_pipe.execute()

                    write_pipe = self.redis.pipeline()  # type: ignore[union-attr]
                    for (key, redis_doc), existing in zip(
                        batch, existing_docs, strict=True
                    ):
                        existing_dict = existing if isinstance(existing, dict) else None
                        ca, ma, _ = resolve_timestamps(existing_dict, now_ts)
                        redis_doc["created_at"] = ca
                        redis_doc["modified_at"] = ma
                        write_pipe.json().set(key, "$", redis_doc)
                    await write_pipe.execute()

                    logger.info(
                        f"  Loaded {min(i + REDIS_BATCH_SIZE, len(prepared)):,}"
                        f"/{len(prepared):,} podcasts..."
                    )

            conn.close()

        finally:
            # Step 5: Cleanup
            if not dry_run:
                await self._disconnect_redis()

            if not keep_db and self.temp_dir.exists():
                logger.info("Cleaning up downloaded files...")
                shutil.rmtree(self.temp_dir)
                logger.info("  Cleanup complete")

        stats.completed_at = datetime.now()
        stats.finalize()  # Update compatibility fields
        return stats


async def run_pi_nightly_etl(
    media_type: str = "podcast",  # Ignored, for compatibility with ETL runner
    start_date: str | None = None,  # Ignored, we use since_hours
    end_date: str | None = None,  # Ignored
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_password: str | None = None,
    staging_dir: str = "/tmp/etl-staging",
    fetch_only: bool = False,  # Not used
    load_only: bool = False,  # Not used
    staging_file: str | None = None,  # Not used
    max_batches: int = 0,  # Used as limit
    verbose: bool = False,  # Not used
) -> PIETLStats:
    """
    Run the PodcastIndex nightly ETL.

    This wrapper provides compatibility with the ETL runner interface.
    The media_type, start_date, end_date parameters are ignored as PI ETL
    uses a different approach (since_hours based on database lastUpdate).

    Args:
        media_type: Ignored (always "podcast")
        start_date: Ignored
        end_date: Ignored
        redis_host: Redis host
        redis_port: Redis port
        redis_password: Redis password
        staging_dir: Directory for temp files
        fetch_only: Not used
        load_only: Not used
        staging_file: Not used
        max_batches: Used as limit for testing (0 = no limit)
        verbose: Not used

    Returns:
        PIETLStats with run statistics
    """
    # Set Redis environment variables for the ETL
    os.environ["REDIS_HOST"] = redis_host
    os.environ["REDIS_PORT"] = str(redis_port)
    if redis_password:
        os.environ["REDIS_PASSWORD"] = redis_password

    etl = PodcastIndexNightlyETL(temp_dir=staging_dir)

    # Use max_batches as limit if set
    limit = max_batches if max_batches > 0 else None

    stats = await etl.run(
        since_hours=48,  # Always check last 48 hours for safety
        limit=limit,
        dry_run=False,
        keep_db=False,
    )

    return stats


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="PodcastIndex Nightly ETL")
    parser.add_argument(
        "--since-hours",
        type=int,
        default=48,
        help="Load podcasts updated in the last N hours (default: 48)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum records to process (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to Redis",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Don't delete downloaded database after ETL",
    )

    args = parser.parse_args()

    # Load environment
    load_env()

    etl = PodcastIndexNightlyETL()

    try:
        stats = await etl.run(
            since_hours=args.since_hours,
            limit=args.limit,
            dry_run=args.dry_run,
            keep_db=args.keep_db,
        )

        print()
        print("=" * 60)
        print("📊 ETL Summary")
        print("=" * 60)
        print(f"  Duration: {stats.duration_seconds:.1f}s")
        print(f"  Download size: {stats.download_size_mb:.1f} MB")
        print(f"  Records matching filters: {stats.after_filters:,}")
        print(f"  Documents loaded: {stats.documents_loaded:,}")
        print(f"  Errors: {stats.errors}")
        print()

        if args.dry_run:
            print("🔍 DRY RUN - No data was written to Redis")
        else:
            print("🎉 ETL Complete!")

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
