"""
Load TMDB metadata from Google Cloud Storage into Redis.

This script downloads movie and/or TV metadata from GCS and seeds it into Redis
using the existing normalization layer.

Usage:
    python scripts/load_gcs_metadata.py --type movie
    python scripts/load_gcs_metadata.py --type tv
    python scripts/load_gcs_metadata.py --type all
"""

import argparse
import asyncio
import json
import os
import sys

from google.cloud import storage
from redis.asyncio import Redis

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.adapters.config import load_env
from src.core.normalize import document_to_redis, normalize_document

# GCS configuration
DEFAULT_BUCKET = "mc-media-manager"
DEFAULT_PREFIX = "faiss-indexes"
METADATA_FILES = {
    "movie": "movie-index.metadata.json",
    "tv": "tv-index.metadata.json",
}


def download_metadata_from_gcs(bucket_name: str, blob_path: str) -> dict:
    """
    Download and parse JSON metadata from GCS.

    Args:
        bucket_name: GCS bucket name
        blob_path: Path to the blob within the bucket

    Returns:
        Parsed JSON data as a dictionary
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    print(f"📥 Downloading gs://{bucket_name}/{blob_path}...")
    content = blob.download_as_text()

    print("📄 Parsing JSON...")
    return json.loads(content)


def extract_items_from_metadata(data: dict) -> list[dict]:
    """
    Extract individual items from the GCS metadata format.

    The GCS files have structure:
    {
        "metadata": {
            "0": {"id": "...", "metadata": {...}},
            "1": {"id": "...", "metadata": {...}},
            ...
        }
    }

    Returns:
        List of metadata dictionaries ready for normalization
    """
    metadata_dict = data.get("metadata", {})
    items = []

    for _key, entry in metadata_dict.items():
        # The actual metadata is nested under "metadata" key
        item_metadata = entry.get("metadata", {})
        if item_metadata:
            items.append(item_metadata)

    return items


async def load_metadata_to_redis(
    items: list[dict],
    media_type: str,
    redis_client: Redis,
    batch_size: int = 100,
) -> tuple[int, int]:
    """
    Load metadata items into Redis using the normalization layer.

    Args:
        items: List of metadata dictionaries
        media_type: Type of media ('movie' or 'tv')
        redis_client: Redis async client
        batch_size: Number of items to process before printing progress

    Returns:
        Tuple of (seeded_count, skipped_count)
    """
    seeded_count = 0
    skipped_count = 0

    for _i, item in enumerate(items):
        # Normalize the document using the existing abstraction layer
        search_doc = normalize_document(item, source="tmdb")

        if search_doc is None:
            skipped_count += 1
            title = item.get("title") or item.get("name") or "unknown"
            if skipped_count <= 5:  # Only log first 5 skips
                print(f"  ⚠️  Skipping unnormalizable item: {title}")
            continue

        # Convert to Redis format and store
        key = f"media:{search_doc.id}"
        redis_doc = document_to_redis(search_doc)

        await redis_client.json().set(key, "$", redis_doc)
        seeded_count += 1

        if seeded_count % batch_size == 0:
            print(f"  📦 Loaded {seeded_count} {media_type} items...")

    return seeded_count, skipped_count


async def load_from_gcs(media_types: list[str]) -> None:
    """
    Main function to load metadata from GCS into Redis.

    Args:
        media_types: List of media types to load ('movie', 'tv', or both)
    """
    # Load environment configuration
    load_env()

    # Get GCS bucket from environment or use default
    bucket_name = os.getenv("GCS_BUCKET", DEFAULT_BUCKET)
    prefix = os.getenv("GCS_METADATA_PREFIX", DEFAULT_PREFIX)

    print("🔧 Configuration:")
    print(f"   GCS Bucket: {bucket_name}")
    print(f"   Prefix: {prefix}")
    print(f"   Media types: {', '.join(media_types)}")
    print()

    # Connect to Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6380"))
    redis_password = os.getenv("REDIS_PASSWORD") or None

    print(f"🔌 Connecting to Redis at {redis_host}:{redis_port}...")
    redis_client = Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

    # Test connection
    try:
        await redis_client.ping()
        print("✅ Redis connection successful")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return

    print()

    total_seeded = 0
    total_skipped = 0

    for media_type in media_types:
        if media_type not in METADATA_FILES:
            print(f"⚠️  Unknown media type: {media_type}, skipping")
            continue

        filename = METADATA_FILES[media_type]
        blob_path = f"{prefix}/{filename}"

        print(f"{'='*60}")
        print(f"📽️  Loading {media_type.upper()} metadata")
        print(f"{'='*60}")

        try:
            # Download from GCS
            data = download_metadata_from_gcs(bucket_name, blob_path)

            # Extract items
            items = extract_items_from_metadata(data)
            print(f"📊 Found {len(items)} {media_type} items")

            # Load to Redis
            seeded, skipped = await load_metadata_to_redis(
                items, media_type, redis_client
            )

            total_seeded += seeded
            total_skipped += skipped

            print(f"✅ {media_type.upper()}: Loaded {seeded} items, skipped {skipped}")
            print()

        except Exception as e:
            print(f"❌ Error loading {media_type} metadata: {e}")
            print()
            continue

    # Summary
    print(f"{'='*60}")
    print("📈 SUMMARY")
    print(f"{'='*60}")
    print(f"   Total items loaded: {total_seeded}")
    print(f"   Total items skipped: {total_skipped}")

    # Show sample
    if total_seeded > 0:
        sample_keys = await redis_client.keys("media:tmdb_*")
        if sample_keys:
            sample = await redis_client.json().get(sample_keys[0])
            print()
            print(f"📝 Sample document ({sample_keys[0]}):")
            print(f"   Title: {sample.get('search_title')}")
            print(f"   Type: {sample.get('type')}")
            print(f"   Year: {sample.get('year')}")
            print(f"   Popularity: {sample.get('popularity')}")
            print(f"   Rating: {sample.get('rating')}")

    await redis_client.aclose()
    print()
    print("🎉 Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Load TMDB metadata from GCS into Redis"
    )
    parser.add_argument(
        "--type",
        choices=["movie", "tv", "all"],
        required=True,
        help="Type of metadata to load: movie, tv, or all",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help=f"GCS bucket name (default: {DEFAULT_BUCKET} or GCS_BUCKET env var)",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help=f"GCS path prefix (default: {DEFAULT_PREFIX} or GCS_METADATA_PREFIX env var)",
    )

    args = parser.parse_args()

    # Override environment if CLI args provided
    if args.bucket:
        os.environ["GCS_BUCKET"] = args.bucket
    if args.prefix:
        os.environ["GCS_METADATA_PREFIX"] = args.prefix

    # Determine media types to load
    if args.type == "all":
        media_types = ["movie", "tv"]
    else:
        media_types = [args.type]

    # Run async loader
    asyncio.run(load_from_gcs(media_types))


if __name__ == "__main__":
    main()

