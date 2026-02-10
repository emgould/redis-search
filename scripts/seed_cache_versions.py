"""
Seed the shared cache version registry in Redis.

Writes the canonical version for every cache prefix so that all repos
sharing the same Redis instance use the same versions.  Run once per
Redis instance, or re-run after adding new prefixes.

Usage:
    source venv/bin/activate && source config/local.env
    python scripts/seed_cache_versions.py

Or via Makefile:
    make cache-version-seed
"""

import sys
from pathlib import Path

# Ensure src/ is on the path so utils imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from utils.redis_cache import (  # noqa: E402
    VERSION_REGISTRY_KEY,
    get_all_cache_versions,
    get_redis_client,
)

# Canonical prefix → version mapping.
# When a prefix needs a cache bust, bump the version here and re-run.
CACHE_VERSIONS: dict[str, str] = {
    # TMDB
    "tmdb_request": "1.0.1",
    "tmdb_func": "1.2.9",
    "tmdb": "1.4.3",
    "trending_handler": "1.0.1",
    # Watchmode
    "watchmode_request": "1.0.0",
    "watchmode": "2.1.0",
    "watchmode_wrapper": "4.1.0",
    # OpenLibrary
    "openlibrary_editions": "1.0.0",
    "openlibrary_request": "1.0.22",
    "books": "1.4.5",
    "openlibrary_func": "4.33.3",
    # Podcasts
    "rss_feed": "1.0.0",
    "podcast_request": "1.0.0",
    "podcast": "1.0.0",
    "podcast_func": "1.3.0",
    "podcast_wrapper": "4.0.5",
    # Spotify
    "spotify_request": "1.0.2",
    "spotify": "1.0.2",
    "spotify_wrapper": "1.6.0",
    # Last.fm
    "lastfm_request": "1.0.0",
    "lastfm": "2.5.0",
    "lastfm_wrapper": "3.2.1",
    # YouTube
    "youtube": "2.0.3",
    "youtube_wrapper": "4.3.0",
    # Rotten Tomatoes
    "rottentomatoes_request": "1.0.0",
    "rottentomatoes": "1.0.0",
    "rottentomatoes_wrapper": "1.0.0",
    # NewsAI  (search.py had 1.0.7, wrappers.py had 1.0.0 — use higher)
    "newsai": "1.0.7",
    # News
    "news": "2.1.1",
    "news_func": "5.0.0",
    # NY Times
    "nytimes_request": "1.0.1",
    "nytimes": "3.0.1",
    "nytimes_wrapper": "4.0.1",
    # SchedulesDirect
    "sd_schedule": "1.0.0",
    "sd_token": "1.0.0",
    "schedulesdirect_func": "1.0.3",
    "schedulesdirect_wrapper": "1.0.1",
    # FlixPatrol
    "flixpatrol": "2.0.1",
    "flixpatrol_func": "2.0.1",
    # Comscore
    "comscore": "2.0.1",
    "comscore_func": "2.0.1",
    # Google Books
    "google_books": "1.0.1",
    "google_books_func": "3.0.1",
    # TVDB
    "tvdb": "2.0.1",
    # Apple
    "apple_auth": "1.0.1",
}


def main() -> None:
    client = get_redis_client()

    # Write all versions in a single HSET call
    client.hset(VERSION_REGISTRY_KEY, mapping=CACHE_VERSIONS)  # type: ignore[arg-type]

    # Read back and display
    stored = get_all_cache_versions()
    print(f"Seeded {len(CACHE_VERSIONS)} cache versions into Redis:")
    for prefix, version in sorted(stored.items()):
        print(f"  {prefix:30s} {version}")


if __name__ == "__main__":
    main()
