import asyncio
import json
import os
import sys

from redis.asyncio import Redis

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.adapters.config import load_env
from src.core.normalize import document_to_redis, normalize_document


async def seed():
    # Load environment configuration
    load_env()

    # Load TMDB seed data from JSON file
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'tmdb_seed.json')
    with open(data_path) as f:
        data = json.load(f)

    tmdb_items = data.get('tmdb', [])
    print(f"Found {len(tmdb_items)} TMDB items to seed")

    # Connect to Redis
    r = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True
    )

    # Seed each item using the normalization layer
    seeded_count = 0
    skipped_count = 0

    for item in tmdb_items:
        # Normalize the document using the abstraction layer
        # Source is auto-detected from the data structure
        search_doc = normalize_document(item)

        if search_doc is None:
            skipped_count += 1
            print(f"Skipping unnormalizable item: {item.get('title', item.get('name', 'unknown'))}")
            continue

        # Convert to Redis format and store
        key = f"media:{search_doc.id}"
        redis_doc = document_to_redis(search_doc)

        await r.json().set(key, "$", redis_doc)
        seeded_count += 1

        if seeded_count % 10 == 0:
            print(f"Seeded {seeded_count} items...")

    print("\nSeeding complete:")
    print(f"  - Successfully seeded: {seeded_count} items")
    print(f"  - Skipped: {skipped_count} items")

    # Show sample document for verification
    if seeded_count > 0:
        sample_keys = await r.keys("media:*")
        if sample_keys:
            sample = await r.json().get(sample_keys[0])
            print(f"\nSample document ({sample_keys[0]}):")
            print(f"  - search_title: {sample.get('search_title')}")
            print(f"  - type: {sample.get('type')}")
            print(f"  - year: {sample.get('year')}")
            print(f"  - popularity: {sample.get('popularity')}")
            print(f"  - rating: {sample.get('rating')}")
            print(f"  - source: {sample.get('source')}")

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(seed())
