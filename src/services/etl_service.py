"""
ETL Service for loading TMDB data into Redis Search index.

This module handles:
1. Reading JSON files from the local data directory
2. Normalizing documents using the normalization layer
3. Loading documents into the Redis Search index
4. Non-blocking upload to GCS for backup
"""

import asyncio
import gzip
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import storage  # type: ignore[attr-defined]
from redis.asyncio import Redis

from adapters.config import load_env
from contracts.models import MCSources, MCType
from core.normalize import document_to_redis, normalize_document

# Major streaming platforms for filtering
MAJOR_STREAMING_PROVIDERS = {
    "Netflix",
    "Amazon Prime Video",
    "Amazon Video",
    "Hulu",
    "Max",  # HBO Max rebranded
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
    "fuboTV",
}


@dataclass
class ETLStats:
    """Statistics from an ETL run."""

    files_processed: int = 0
    documents_loaded: int = 0
    documents_skipped: int = 0
    errors: list[str] | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    def add_error(self, error: str) -> None:
        """Add an error message to the errors list."""
        if self.errors is None:
            self.errors = []
        self.errors.append(error)

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class ETLConfig:
    """Configuration for ETL runs."""

    data_dir: Path
    gcs_bucket: str | None
    gcs_prefix: str
    redis_host: str
    redis_port: int
    redis_password: str | None
    batch_size: int = 100

    @classmethod
    def from_env(cls) -> "ETLConfig":
        """Create config from environment variables."""
        load_env()
        return cls(
            data_dir=Path(os.getenv("ETL_DATA_DIR", "data/us")),
            gcs_bucket=os.getenv("GCS_BUCKET"),
            gcs_prefix=os.getenv("GCS_ETL_PREFIX", "redis-search/etl"),
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", "6380")),
            redis_password=os.getenv("REDIS_PASSWORD") or None,
            batch_size=int(os.getenv("ETL_BATCH_SIZE", "100")),
        )


class GCSUploader:
    """Non-blocking GCS upload handler."""

    def __init__(self, bucket_name: str | None, prefix: str):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._pending_uploads: list[asyncio.Future] = []

    def _upload_file_sync(self, local_path: Path, gcs_path: str) -> str:
        """Synchronous upload with gzip compression (runs in thread pool)."""
        if not self.bucket_name:
            return f"Skipped (no bucket configured): {gcs_path}"

        try:
            # Read and compress the file
            with open(local_path, "rb") as f:
                original_data = f.read()
            compressed_data = gzip.compress(original_data)

            # Calculate compression ratio for logging
            original_size = len(original_data)
            compressed_size = len(compressed_data)
            ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

            # Upload compressed data with .gz extension
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            gcs_path_gz = f"{gcs_path}.gz"
            blob = bucket.blob(gcs_path_gz)
            blob.upload_from_string(
                compressed_data,
                content_type="application/gzip",
            )
            return f"Uploaded: gs://{self.bucket_name}/{gcs_path_gz} ({ratio:.1f}% smaller)"
        except Exception as e:
            return f"Upload failed for {gcs_path}: {e}"

    async def upload_file(self, local_path: Path, gcs_path: str) -> None:
        """Queue a non-blocking file upload."""
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(self._executor, self._upload_file_sync, local_path, gcs_path)
        self._pending_uploads.append(future)

    async def upload_json(self, data: dict, gcs_path: str) -> None:
        """Queue a non-blocking JSON upload with gzip compression."""
        if not self.bucket_name:
            print(f"  ⏭️  GCS upload skipped (no bucket): {gcs_path}")
            return

        loop = asyncio.get_event_loop()

        def upload_sync() -> str:
            try:
                # Compress JSON data
                json_bytes = json.dumps(data, default=str).encode("utf-8")
                compressed_data = gzip.compress(json_bytes)

                # Upload with .gz extension
                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                gcs_path_gz = f"{gcs_path}.gz"
                blob = bucket.blob(gcs_path_gz)
                blob.upload_from_string(
                    compressed_data,
                    content_type="application/gzip",
                )
                return f"Uploaded: gs://{self.bucket_name}/{gcs_path_gz}"
            except Exception as e:
                return f"Upload failed for {gcs_path}: {e}"

        future = loop.run_in_executor(self._executor, upload_sync)
        self._pending_uploads.append(future)

    async def wait_for_uploads(self) -> list[str]:
        """Wait for all pending uploads to complete."""
        if not self._pending_uploads:
            return []

        results = await asyncio.gather(*self._pending_uploads, return_exceptions=True)
        self._pending_uploads.clear()
        return [str(r) for r in results]

    def shutdown(self):
        """Shutdown the executor."""
        self._executor.shutdown(wait=False)


class TMDBETLService:
    """Service for loading TMDB data into Redis."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.redis: Redis | None = None
        self.gcs_uploader = GCSUploader(config.gcs_bucket, config.gcs_prefix)

    async def connect(self) -> None:
        """Connect to Redis."""
        self.redis = Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            password=self.config.redis_password,
            decode_responses=True,
        )
        # Verify connection - handle both sync and async return types
        ping_result = self.redis.ping()
        if asyncio.iscoroutine(ping_result):
            await ping_result

    async def disconnect(self) -> None:
        """Disconnect from Redis and cleanup."""
        if self.redis:
            await self.redis.aclose()
        self.gcs_uploader.shutdown()

    def discover_json_files(self, media_type: str | None = None) -> list[Path]:
        """
        Discover JSON files in the data directory.

        Args:
            media_type: Optional filter ('movie' or 'tv'). None for all.

        Returns:
            List of paths to JSON files.
        """
        files = []

        if media_type:
            subdirs = [self.config.data_dir / media_type]
        else:
            subdirs = [
                self.config.data_dir / "movie",
                self.config.data_dir / "tv",
            ]

        for subdir in subdirs:
            if subdir.exists():
                files.extend(sorted(subdir.glob("*.json")))

        return files

    def resolve_file_names(
        self, file_names: list[str], media_type: str | None = None
    ) -> list[Path]:
        """
        Resolve file names to full paths.

        Args:
            file_names: List of file names (e.g., ['tmdb_movie_2025_10.json'])
            media_type: Optional media type hint for directory

        Returns:
            List of resolved file paths
        """
        resolved = []
        all_files = self.discover_json_files(media_type)

        # Create a lookup by file name
        file_lookup = {f.name: f for f in all_files}

        for name in file_names:
            if name in file_lookup:
                resolved.append(file_lookup[name])
            else:
                # Try to find in both directories
                for mtype in ["movie", "tv"]:
                    path = self.config.data_dir / mtype / name
                    if path.exists():
                        resolved.append(path)
                        break
                else:
                    print(f"  ⚠️  File not found: {name}")

        return resolved

    def filter_files_by_date(
        self,
        files: list[Path],
        year: int | None = None,
        month: int | None = None,
        year_lte: int | None = None,
        year_gte: int | None = None,
    ) -> list[Path]:
        """
        Filter files by year and/or month based on filename pattern.

        Expects filenames like: tmdb_movie_2025_10.json or tmdb_tv_2025_11.json

        Args:
            files: List of file paths
            year: Optional exact year filter (e.g., 2025)
            month: Optional month filter (e.g., 10)
            year_lte: Optional year <= filter (e.g., 2020 means 2020 and earlier)
            year_gte: Optional year >= filter (e.g., 2023 means 2023 and later)

        Returns:
            Filtered list of file paths
        """
        if year is None and month is None and year_lte is None and year_gte is None:
            return files

        filtered = []
        for f in files:
            # Parse year/month from filename: tmdb_TYPE_YYYY_MM.json
            parts = f.stem.split("_")
            if len(parts) >= 4:
                try:
                    file_year = int(parts[2])
                    file_month = int(parts[3])

                    # Exact year filter
                    if year is not None and file_year != year:
                        continue

                    # Year range filters
                    if year_lte is not None and file_year > year_lte:
                        continue
                    if year_gte is not None and file_year < year_gte:
                        continue

                    # Month filter
                    if month is not None and file_month != month:
                        continue

                    filtered.append(f)
                except ValueError:
                    # Filename doesn't match expected pattern
                    continue

        return filtered

    async def run_etl_for_files(
        self,
        files: list[Path],
        upload_to_gcs: bool = True,
    ) -> ETLStats:
        """
        Run ETL for a specific list of files.

        Args:
            files: List of file paths to load
            upload_to_gcs: Whether to upload to GCS after loading

        Returns:
            ETLStats with results
        """
        stats = ETLStats(started_at=datetime.now())

        print("=" * 60)
        print("🚀 TMDB ETL Service")
        print("=" * 60)
        print(f"  Redis: {self.config.redis_host}:{self.config.redis_port}")
        print(f"  GCS bucket: {self.config.gcs_bucket or 'Not configured'}")
        print(f"  Files to load: {len(files)}")
        print()

        # Connect to Redis
        print("🔌 Connecting to Redis...")
        try:
            await self.connect()
            print("  ✅ Connected")
        except Exception as e:
            stats.add_error(f"Redis connection failed: {e}")
            print(f"  ❌ Connection failed: {e}")
            return stats

        # Process each file
        print()
        print("📥 Loading data into Redis...")
        for file_path in files:
            await self.load_file(file_path, stats)

            # Queue GCS upload (non-blocking)
            if upload_to_gcs and self.config.gcs_bucket:
                gcs_path = f"{self.config.gcs_prefix}/{file_path.parent.name}/{file_path.name}"
                await self.gcs_uploader.upload_file(file_path, gcs_path)

        # Wait for GCS uploads to complete
        if upload_to_gcs and self.gcs_uploader._pending_uploads:
            print()
            print("☁️  Waiting for GCS uploads...")
            upload_results = await self.gcs_uploader.wait_for_uploads()
            for result in upload_results:
                print(f"  {result}")

        # Upload ETL manifest to GCS
        if upload_to_gcs and self.config.gcs_bucket:
            stats.completed_at = datetime.now()
            manifest = {
                "etl_run": {
                    "started_at": stats.started_at.isoformat() if stats.started_at else None,
                    "completed_at": stats.completed_at.isoformat() if stats.completed_at else None,
                    "duration_seconds": stats.duration_seconds,
                    "files_processed": stats.files_processed,
                    "documents_loaded": stats.documents_loaded,
                    "documents_skipped": stats.documents_skipped,
                    "errors": stats.errors,
                },
                "files": [str(f) for f in files],
            }
            manifest_path = f"{self.config.gcs_prefix}/manifests/etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            await self.gcs_uploader.upload_json(manifest, manifest_path)
            await self.gcs_uploader.wait_for_uploads()

        stats.completed_at = datetime.now()

        # Summary
        print()
        print("=" * 60)
        print("📊 ETL Summary")
        print("=" * 60)
        print(f"  Files processed: {stats.files_processed}")
        print(f"  Documents loaded: {stats.documents_loaded}")
        print(f"  Documents skipped: {stats.documents_skipped}")
        if stats.duration_seconds:
            print(f"  Duration: {stats.duration_seconds:.2f}s")
        if stats.errors:
            print(f"  Errors: {len(stats.errors)}")
            for err in stats.errors[:5]:
                print(f"    - {err}")

        await self.disconnect()
        print()
        print("🎉 ETL Complete!")

        return stats

    async def load_file(
        self,
        file_path: Path,
        stats: ETLStats,
    ) -> None:
        """
        Load a single JSON file into Redis.

        Args:
            file_path: Path to the JSON file
            stats: Stats object to update
        """
        if not self.redis:
            raise RuntimeError("Not connected to Redis")

        # Determine media type from file path
        if "movie" in str(file_path):
            mc_type = MCType.MOVIE
        elif "tv" in str(file_path):
            mc_type = MCType.TV_SERIES
        else:
            mc_type = None

        print(f"  📂 Loading: {file_path.name}")

        try:
            with open(file_path) as f:
                data = json.load(f)
        except Exception as e:
            stats.add_error(f"Failed to read {file_path}: {e}")
            return

        # Extract results from the file
        results = data.get("results", [])
        if not results:
            print(f"    ⚠️  No results found in {file_path.name}")
            return

        stats.files_processed += 1
        loaded_in_file = 0
        skipped_in_file = 0

        # Calculate cutoff date for "last 10 years" filter
        ten_years_ago = datetime.now() - timedelta(days=365 * 10)
        cutoff_year = ten_years_ago.year

        # Process each item
        pipeline = self.redis.pipeline()
        batch_count = 0

        for item in results:
            # === FILTERING LOGIC ===

            # 1. Poster must exist
            if not item.get("poster_path"):
                skipped_in_file += 1
                continue

            # 2. Popularity must be >= 1
            metrics = item.get("metrics", {})
            popularity = metrics.get("popularity") or item.get("popularity") or 0
            if popularity < 1:
                skipped_in_file += 1
                continue

            # 3. Vote count must be > 1
            vote_count = metrics.get("vote_count") or item.get("vote_count") or 0
            if vote_count <= 1:
                skipped_in_file += 1
                continue

            # 4. Runtime < 50 only allowed if vote_count >= 10
            runtime = item.get("runtime") or 0
            if runtime < 50 and vote_count < 10:
                skipped_in_file += 1
                continue

            # 5. Either released in last 10 years OR on major streaming platform
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

            if not is_recent and not is_on_major_platform:
                skipped_in_file += 1
                continue

            # === DATA CLEANUP ===

            # 6. Remove tmdb_cast if main_cast exists (they're the same)
            if item.get("main_cast") and item.get("tmdb_cast"):
                del item["tmdb_cast"]

            # 7. Filter cast members without profile images
            if item.get("main_cast"):
                item["main_cast"] = [
                    cast_member
                    for cast_member in item["main_cast"]
                    if cast_member.get("profile_path") or cast_member.get("has_image")
                ]

            # Normalize the document
            search_doc = normalize_document(item, source=MCSources.TMDB, mc_type=mc_type)

            if search_doc is None:
                skipped_in_file += 1
                continue

            # Convert to Redis format
            key = f"media:{search_doc.id}"
            redis_doc = document_to_redis(search_doc)

            # Add to pipeline
            pipeline.json().set(key, "$", redis_doc)
            batch_count += 1
            loaded_in_file += 1

            # Execute batch
            if batch_count >= self.config.batch_size:
                await pipeline.execute()
                pipeline = self.redis.pipeline()
                batch_count = 0

        # Execute remaining items
        if batch_count > 0:
            await pipeline.execute()

        stats.documents_loaded += loaded_in_file
        stats.documents_skipped += skipped_in_file

        print(f"    ✅ Loaded {loaded_in_file}, skipped {skipped_in_file}")
