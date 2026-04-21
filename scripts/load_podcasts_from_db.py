"""
Bulk loader for importing podcasts from SQLite database into Redis.

Loads podcasts from the PodcastIndex SQLite database dump into Redis
for autocomplete search functionality.

Follows the normalization paradigm used by other ETL services:
- Uses SearchDocument for index format
- Uses document_to_redis() for Redis format conversion
- Adds display fields for MCPodcastItem compatibility

Usage:
    python scripts/load_podcasts_from_db.py
    python scripts/load_podcasts_from_db.py --dry-run
    python scripts/load_podcasts_from_db.py --limit 1000
"""

import argparse
import asyncio
import math
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import _bootstrap
from dotenv import load_dotenv
from redis.asyncio import Redis

from src.adapters.redis_repository import RedisRepository
from src.contracts.models import MCSources, MCType
from src.core.iptc import normalize_tag
from src.core.normalize import SearchDocument, document_to_redis
from src.etl.podcast_parent_resolver import resolve_parent_mc_ids, should_resolve_parent_mc_ids
from src.etl.podcastindex_shared import (
    build_after_shows_query,
    build_categories_array,
    build_default_query,
    merge_rows_by_feed_id,
)
from src.etl.spotify_enrichment import fetch_spotify_ids_for_title

_ = _bootstrap

# Load env file (defaults to local.env for local development)
env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

# Default database path
DEFAULT_DB_PATH = "data/podcastindex/podcastindex_feeds.db"
PARENT_RESOLUTION_BATCH_SIZE = 20


@dataclass
class PodcastBulkLoaderStats:
    """Statistics for bulk loading operation."""

    total_queried: int = 0
    documents_loaded: int = 0
    documents_skipped: int = 0
    errors: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class PodcastBulkLoader:
    """Bulk loader for podcasts from SQLite database.

    Follows the normalization paradigm:
    - Normalizes raw data to SearchDocument format
    - Uses document_to_redis() for Redis conversion
    - Adds extra display fields for MCPodcastItem compatibility
    """

    def __init__(
        self,
        db_path: str,
        redis_host: str,
        redis_port: int,
        redis_password: str | None = None,
        batch_size: int = 100,
    ):
        self.db_path = db_path
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.batch_size = batch_size
        self.redis: Redis | None = None
        self.search_repo: RedisRepository | None = None

    async def connect(self) -> None:
        """Connect to Redis."""
        self.redis = Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password or None,
            decode_responses=True,
        )
        await self.redis.ping()
        self.search_repo = RedisRepository()

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis:
            await self.redis.aclose()
        self.search_repo = None

    def _convert_timestamp(self, ts: int | None) -> str | None:
        """Convert Unix timestamp to ISO format string."""
        if ts is None:
            return None
        try:
            dt = datetime.fromtimestamp(ts, tz=UTC)
            return dt.isoformat()
        except (ValueError, OSError):
            return None

    def _build_categories_array(self, row: sqlite3.Row) -> list[str]:
        """Build the normalized category array used by idx:podcasts."""
        return build_categories_array(row)

    def _compute_popularity(self, popularity_score: int, episode_count: int) -> float:
        """
        Compute a normalized popularity score (0-100).

        Combines popularity score and episode count similar to TMDB normalizer.
        """
        # PodcastIndex popularityScore is 0-29
        # Normalize to 0-100 scale (multiply by ~3.3)
        pop_normalized = min(popularity_score * 3.5, 100)

        # Episode count: log scale contribution
        episode_score = min(math.log10(episode_count + 1) * 20, 50) if episode_count > 0 else 0

        # Combined: 70% popularity, 30% episode count
        combined = (pop_normalized * 0.7) + (episode_score * 0.3)

        return round(combined, 2)

    def _extract_overview(self, description: str | None, max_length: int = 200) -> str | None:
        """Extract and truncate description for overview field."""
        if not description:
            return None
        if len(description) <= max_length:
            return description
        return description[:max_length].rsplit(" ", 1)[0] + "..."

    def _normalize_podcast_for_search(self, row: sqlite3.Row) -> SearchDocument | None:
        """
        Normalize a podcast row into a SearchDocument for the index.

        Follows the same pattern as TMDBPersonNormalizer and PersonETLService.

        Args:
            row: SQLite row with podcast data

        Returns:
            SearchDocument or None if cannot normalize
        """
        # Extract and validate ID
        podcast_id = row["id"]
        if not podcast_id:
            return None

        source_id = str(podcast_id)
        # mc_id format: {source}_{type}_{id} e.g., "podcastindex_podcast_12345"
        mc_id = f"podcastindex_podcast_{podcast_id}"

        # Extract and validate title
        title = row["title"]
        if not title or not str(title).strip():
            return None
        title = str(title).strip()

        # Extract popularity and episode count for scoring
        popularity_score = row["popularityScore"] or 0
        episode_count = row["episodeCount"] or 0

        # Get image URL
        image_url = row["imageUrl"]
        if image_url:
            image_url = str(image_url).strip() if image_url else None

        # Create SearchDocument (follows normalization paradigm)
        return SearchDocument(
            id=mc_id,
            search_title=title,
            mc_type=MCType.PODCAST,
            mc_subtype=None,  # Podcasts don't have subtypes
            source=MCSources.PODCASTINDEX,
            source_id=source_id,
            year=None,  # Podcasts don't have a release year
            popularity=self._compute_popularity(popularity_score, episode_count),
            rating=0.0,  # Podcasts don't have ratings in the index
            image=image_url,
            cast=[],  # Podcasts don't have cast (use author in display fields)
            overview=self._extract_overview(row["description"]),
            genre_ids=[],
            genres=[],
            cast_ids=[],
            cast_names=[],
        )

    def _add_podcast_display_fields(
        self,
        redis_doc: dict[str, object],
        row: sqlite3.Row,
        parent_mc_ids: list[str] | None = None,
        spotify_url: str | None = None,
        spotify_id: str | None = None,
    ) -> dict[str, object]:
        """
        Add podcast-specific display fields to the Redis document.

        These fields are stored but not indexed - they're for display
        and to maintain MCPodcastItem compatibility.

        Args:
            redis_doc: Base Redis document from document_to_redis()
            row: SQLite row with podcast data

        Returns:
            Redis document with additional podcast fields
        """
        # MCPodcastItem fields not in SearchDocument
        redis_doc["title"] = row["title"] or ""
        redis_doc["url"] = row["url"] or ""
        redis_doc["site"] = row["link"] or None
        redis_doc["author"] = row["itunesAuthor"] or None
        redis_doc["owner_name"] = row["itunesOwnerName"] or None
        raw_language = row["language"] or ""
        redis_doc["language"] = normalize_tag(raw_language) if raw_language else None
        redis_doc["categories"] = self._build_categories_array(row)
        if parent_mc_ids is not None:
            redis_doc["parent_mc_ids"] = parent_mc_ids
        raw_author = row["itunesAuthor"] or ""
        redis_doc["author_normalized"] = normalize_tag(raw_author) if raw_author else None
        redis_doc["episode_count"] = row["episodeCount"] or 0
        redis_doc["itunes_id"] = row["itunesId"] or None
        redis_doc["podcast_guid"] = row["podcastGuid"] or None
        redis_doc["popularity_score"] = row["popularityScore"] or 0
        row_keys = row.keys()
        redis_doc["last_update_time"] = self._convert_timestamp(row["lastUpdate"] if "lastUpdate" in row_keys else None)

        # Fields that would require API enrichment (set to None)
        redis_doc["artwork"] = None
        redis_doc["trend_score"] = None
        redis_doc["relevancy_score"] = None
        # Spotify linkage (populated via spotify_enrichment helper when available)
        redis_doc["spotify_url"] = spotify_url
        redis_doc["spotify_id"] = spotify_id

        return redis_doc

    async def _prepare_row_document(self, row: sqlite3.Row) -> tuple[str, dict[str, object]] | None:
        """Normalize a SQLite row and attach optional parent mc_ids."""
        search_doc = self._normalize_podcast_for_search(row)
        if search_doc is None:
            return None

        redis_doc = document_to_redis(search_doc)
        categories = self._build_categories_array(row)
        parent_mc_ids: list[str] | None = None
        row_keys = row.keys()
        site_url = row["link"] if "link" in row_keys else None
        normalized_site_url = str(site_url) if isinstance(site_url, str) and site_url else None
        if should_resolve_parent_mc_ids(categories, normalized_site_url):
            if self.search_repo is None:
                raise RuntimeError("Search repository is not initialized")
            title = row["title"] or ""
            parent_mc_ids = await resolve_parent_mc_ids(
                self.search_repo,
                str(title),
                site_url=normalized_site_url,
            )
        spotify_url, spotify_id = await fetch_spotify_ids_for_title(row["title"])
        redis_doc = self._add_podcast_display_fields(
            redis_doc,
            row,
            parent_mc_ids=parent_mc_ids,
            spotify_url=spotify_url,
            spotify_id=spotify_id,
        )
        key = f"podcast:{search_doc.id}"
        return key, redis_doc

    async def load_podcasts(
        self,
        min_popularity: int = 3,
        limit: int | None = None,
        dry_run: bool = False,
        after_shows_only: bool = False,
    ) -> PodcastBulkLoaderStats:
        """
        Load podcasts from SQLite database into Redis.

        Args:
            min_popularity: Minimum popularity score (default: 3)
            limit: Maximum number of records to load (None = all)
            dry_run: If True, don't actually write to Redis

        Returns:
            PodcastBulkLoaderStats with load statistics
        """
        stats = PodcastBulkLoaderStats(started_at=datetime.now())

        if not Path(self.db_path).exists():
            print(f"❌ Database file not found: {self.db_path}")
            return stats

        print("=" * 60)
        print("🚀 Podcast Bulk Loader")
        print("=" * 60)
        print(f"  Database: {self.db_path}")
        print(f"  Redis: {self.redis_host}:{self.redis_port}")
        print(f"  Min Popularity: {min_popularity}")
        print(f"  Limit: {limit or 'unlimited'}")
        print(f"  Batch Size: {self.batch_size}")
        print(f"  Dry Run: {dry_run}")
        print(f"  After-Shows Only: {after_shows_only}")
        print()

        # Connect to Redis
        if not dry_run:
            print("🔌 Connecting to Redis...")
            try:
                await self.connect()
                print("  ✅ Connected")
            except Exception as e:
                print(f"  ❌ Connection failed: {e}")
                return stats
        elif self.search_repo is None:
            self.search_repo = RedisRepository()

        # Connect to SQLite
        print("📂 Connecting to SQLite database...")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("📊 Querying database...")
        default_rows: list[sqlite3.Row] = []
        after_shows_rows: list[sqlite3.Row] = []
        if after_shows_only:
            after_shows_query, after_shows_params = build_after_shows_query(
                since_timestamp=None,
                limit=limit,
            )
            cursor.execute(after_shows_query, after_shows_params)
            after_shows_rows = cursor.fetchall()
            rows = after_shows_rows
        else:
            default_query, default_params = build_default_query(
                since_timestamp=None,
                min_popularity=min_popularity,
                limit=limit,
            )
            after_shows_query, after_shows_params = build_after_shows_query(
                since_timestamp=None,
                limit=limit,
            )
            cursor.execute(default_query, default_params)
            default_rows = cursor.fetchall()
            cursor.execute(after_shows_query, after_shows_params)
            after_shows_rows = cursor.fetchall()
            rows = merge_rows_by_feed_id(default_rows, after_shows_rows)
        if limit is not None:
            rows = rows[:limit]
        stats.total_queried = len(rows)
        if after_shows_only:
            print(f"  Found {len(rows):,} after-shows podcasts")
        else:
            print(
                "  Found "
                f"{len(rows):,} podcasts matching criteria "
                f"({len(default_rows):,} default + {len(after_shows_rows):,} after-shows override)"
            )
        print()

        if not rows:
            conn.close()
            if not dry_run:
                await self.disconnect()
            return stats

        # Process in batches
        print("📥 Loading into Redis...")

        if not dry_run and not self.redis:
            raise RuntimeError("Not connected to Redis")

        pipeline = None if dry_run else self.redis.pipeline()
        batch_count = 0

        for batch_start in range(0, len(rows), PARENT_RESOLUTION_BATCH_SIZE):
            row_batch = rows[batch_start : batch_start + PARENT_RESOLUTION_BATCH_SIZE]
            prepared_batch = await asyncio.gather(
                *(self._prepare_row_document(row) for row in row_batch),
                return_exceptions=True,
            )
            for row, prepared_result in zip(row_batch, prepared_batch, strict=True):
                if isinstance(prepared_result, BaseException):
                    stats.errors += 1
                    if stats.errors <= 5:
                        row_keys = row.keys()
                        podcast_id = row["id"] if "id" in row_keys else "unknown"
                        print(f"  ⚠️  Error processing podcast {podcast_id}: {prepared_result}")
                    continue

                if prepared_result is None:
                    stats.documents_skipped += 1
                    continue

                key, redis_doc = prepared_result
                search_title = redis_doc.get("search_title")

                if dry_run:
                    stats.documents_loaded += 1
                    if stats.documents_loaded <= 5 and isinstance(search_title, str):
                        print(f"  [DRY RUN] Would load: {search_title[:50]} -> {key}")
                    continue

                pipeline.json().set(key, "$", redis_doc)
                batch_count += 1
                stats.documents_loaded += 1

                if batch_count >= self.batch_size:
                    await pipeline.execute()
                    pipeline = self.redis.pipeline()
                    batch_count = 0
                    print(f"  ✅ Loaded {stats.documents_loaded:,} podcasts...")

        # Execute remaining items
        if not dry_run and batch_count > 0:
            await pipeline.execute()
            print(f"  ✅ Loaded final batch ({batch_count} podcasts)")

        conn.close()
        if not dry_run:
            await self.disconnect()

        stats.completed_at = datetime.now()

        print()
        print("=" * 60)
        print("📊 Load Summary")
        print("=" * 60)
        print(f"  Total queried: {stats.total_queried:,}")
        print(f"  Documents loaded: {stats.documents_loaded:,}")
        print(f"  Documents skipped: {stats.documents_skipped:,}")
        print(f"  Errors: {stats.errors:,}")
        if stats.duration_seconds:
            print(f"  Duration: {stats.duration_seconds:.1f}s")
        print()

        if dry_run:
            print("🔍 DRY RUN - No data was written to Redis")
        else:
            print("🎉 Load Complete!")

        return stats

    async def relink_existing_after_shows(self, dry_run: bool = False) -> PodcastBulkLoaderStats:
        """Recompute parent_mc_ids for existing after-shows podcast docs in Redis."""
        stats = PodcastBulkLoaderStats(started_at=datetime.now())

        if not dry_run:
            print("🔌 Connecting to Redis...")
            try:
                await self.connect()
                print("  ✅ Connected")
            except Exception as e:
                print(f"  ❌ Connection failed: {e}")
                return stats
        elif self.search_repo is None:
            self.search_repo = RedisRepository()

        if self.redis is None and not dry_run:
            raise RuntimeError("Not connected to Redis")
        if self.search_repo is None:
            raise RuntimeError("Search repository is not initialized")

        print("🔁 Relinking existing After-Shows podcasts...")
        cursor = 0
        pending_updates: list[tuple[str, list[str]]] = []

        while True:
            if self.redis is None:
                break
            cursor, keys = await self.redis.scan(cursor=cursor, match="podcast:*", count=self.batch_size)
            stats.total_queried += len(keys)
            if not keys:
                if cursor == 0:
                    break
                continue

            docs = await self.redis.json().mget(keys, "$")
            batch_candidates: list[tuple[str, str]] = []
            for key, doc_value in zip(keys, docs, strict=True):
                payload = doc_value[0] if isinstance(doc_value, list) and doc_value else doc_value
                if not isinstance(payload, dict):
                    continue
                categories = payload.get("categories")
                site_url = payload.get("site")
                normalized_site_url = site_url if isinstance(site_url, str) and site_url else None
                has_after_shows = False
                if isinstance(categories, list):
                    has_after_shows = should_resolve_parent_mc_ids(
                        [category for category in categories if isinstance(category, str)],
                        normalized_site_url,
                    )
                elif isinstance(categories, dict):
                    has_after_shows = should_resolve_parent_mc_ids(
                        [str(value).strip() for value in categories.values()],
                        normalized_site_url,
                    )
                if not has_after_shows:
                    continue

                title = payload.get("title") or payload.get("search_title")
                if isinstance(title, str) and title:
                    batch_candidates.append((key, title, normalized_site_url))

            for batch_start in range(0, len(batch_candidates), PARENT_RESOLUTION_BATCH_SIZE):
                relink_batch = batch_candidates[batch_start : batch_start + PARENT_RESOLUTION_BATCH_SIZE]
                resolved_batch = await asyncio.gather(
                    *(
                        resolve_parent_mc_ids(self.search_repo, title, site_url=site_url)
                        for _, title, site_url in relink_batch
                    ),
                    return_exceptions=True,
                )
                for (key, _, _), resolved in zip(relink_batch, resolved_batch, strict=True):
                    if isinstance(resolved, BaseException):
                        stats.errors += 1
                        if stats.errors <= 5:
                            print(f"  ⚠️  Error relinking {key}: {resolved}")
                        continue
                    pending_updates.append((key, resolved))
                    stats.documents_loaded += 1

            if not dry_run and pending_updates and self.redis is not None:
                pipeline = self.redis.pipeline()
                for key, parent_mc_ids in pending_updates:
                    pipeline.json().set(key, "$.parent_mc_ids", parent_mc_ids)
                await pipeline.execute()
                pending_updates.clear()

            if cursor == 0:
                break

        if dry_run:
            print(f"  [DRY RUN] Would relink {stats.documents_loaded:,} existing after-shows podcasts")

        if not dry_run:
            await self.disconnect()

        stats.completed_at = datetime.now()
        return stats


async def main():
    parser = argparse.ArgumentParser(description="Load podcasts from SQLite database into Redis")
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--min-popularity",
        type=int,
        default=3,
        help="Minimum popularity score (default: 3)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to load (default: unlimited)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - don't write to Redis",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for Redis pipeline (default: 100)",
    )
    parser.add_argument(
        "--after-shows-only",
        action="store_true",
        help="Backfill only PodcastIndex feeds tagged as After-Shows",
    )
    parser.add_argument(
        "--relink-existing-after-shows",
        action="store_true",
        help="Recompute parent_mc_ids for existing after-shows Redis docs",
    )

    args = parser.parse_args()

    # Get Redis connection info from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6380"))
    redis_password = os.getenv("REDIS_PASSWORD") or None

    loader = PodcastBulkLoader(
        db_path=args.db_path,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        batch_size=args.batch_size,
    )

    if args.relink_existing_after_shows:
        stats = await loader.relink_existing_after_shows(dry_run=args.dry_run)
    else:
        stats = await loader.load_podcasts(
            min_popularity=args.min_popularity,
            limit=args.limit,
            dry_run=args.dry_run,
            after_shows_only=args.after_shows_only,
        )

    raise SystemExit(0 if stats.errors == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
