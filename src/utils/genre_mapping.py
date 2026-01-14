"""
Genre Mapping Utility for TMDB genre ID to name resolution.

TMDB uses integer genre IDs in their API responses. This utility fetches
the genre list from TMDB and provides a mapping to resolve IDs to names.

Usage:
    from src.utils.genre_mapping import get_genre_mapping, resolve_genre_ids

    # Get the mapping (fetches from TMDB if not cached)
    mapping = await get_genre_mapping()

    # Resolve genre IDs to names
    names = resolve_genre_ids([35, 18, 10751], mapping)
    # Returns: ["Comedy", "Drama", "Family"]
"""

import asyncio
import logging

from api.tmdb.core import TMDBService

logger = logging.getLogger(__name__)

# Module-level cache for genre mapping
_genre_mapping_cache: dict[int, str] | None = None


async def fetch_genre_mapping() -> dict[int, str]:
    """
    Fetch genre mapping from TMDB API.

    Makes 2 API calls (bypasses cache to ensure fresh data):
    - GET /genre/movie/list
    - GET /genre/tv/list

    Returns:
        Dict mapping genre_id (int) to genre_name (str)
        Example: {35: "Comedy", 18: "Drama", 10751: "Family", ...}
    """
    from api.tmdb.tmdb_models import TMDBGenre

    service = TMDBService()

    try:
        # Bypass cache to ensure fresh data - genre lists are small and rarely change
        movie_endpoint = "genre/movie/list"
        tv_endpoint = "genre/tv/list"
        params = {"language": "en-US"}

        movie_data = await service._make_request(movie_endpoint, params, no_cache=True)
        tv_data = await service._make_request(tv_endpoint, params, no_cache=True)

        if not movie_data or not tv_data:
            logger.error("TMDB API returned empty response for genres")
            return {}

        # Parse results
        movie_genres = [TMDBGenre.model_validate(g) for g in movie_data.get("genres", [])]
        tv_genres = [TMDBGenre.model_validate(g) for g in tv_data.get("genres", [])]
        all_genres = movie_genres + tv_genres

        # Build mapping
        mapping: dict[int, str] = {}
        for genre in all_genres:
            mapping[genre.id] = genre.name

        logger.info(f"Fetched {len(mapping)} genres from TMDB API")
        return mapping

    except Exception as e:
        logger.error(f"Error fetching genre mapping from TMDB: {e}")
        # Return empty mapping on error - caller should handle gracefully
        return {}


async def get_genre_mapping(force_refresh: bool = False) -> dict[int, str]:
    """
    Get the genre mapping, using cache if available.

    Args:
        force_refresh: If True, bypass cache and fetch fresh data

    Returns:
        Dict mapping genre_id to genre_name
    """
    global _genre_mapping_cache

    if _genre_mapping_cache is None or force_refresh:
        _genre_mapping_cache = await fetch_genre_mapping()

    return _genre_mapping_cache


def resolve_genre_ids(
    genre_ids: list[int] | None,
    mapping: dict[int, str],
) -> list[str]:
    """
    Resolve a list of genre IDs to genre names.

    Args:
        genre_ids: List of TMDB genre IDs (e.g., [35, 18, 10751])
        mapping: Genre ID to name mapping from get_genre_mapping()

    Returns:
        List of genre names (e.g., ["Comedy", "Drama", "Family"])
        Unknown IDs are silently skipped.
    """
    if not genre_ids:
        return []

    names = []
    for gid in genre_ids:
        name = mapping.get(gid)
        if name:
            names.append(name)
        else:
            logger.debug(f"Unknown genre ID: {gid}")

    return names


def get_genre_mapping_sync() -> dict[int, str]:
    """
    Synchronous wrapper to get genre mapping.

    Useful for scripts that aren't async.
    Creates a new event loop if needed.
    """
    try:
        asyncio.get_running_loop()
        # If we're in an async context, we can't use run_until_complete
        raise RuntimeError("Use get_genre_mapping() in async context")
    except RuntimeError:
        # No running loop, safe to create one
        return asyncio.run(get_genre_mapping())


# Pre-defined fallback mapping for offline use or testing
# This is a snapshot of TMDB genres as of 2024
FALLBACK_GENRE_MAPPING: dict[int, str] = {
    # Movie genres
    28: "Action",
    12: "Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    9648: "Mystery",
    10749: "Romance",
    878: "Science Fiction",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
    37: "Western",
    # TV genres
    10759: "Action & Adventure",
    10762: "Kids",
    10763: "News",
    10764: "Reality",
    10765: "Sci-Fi & Fantasy",
    10766: "Soap",
    10767: "Talk",
    10768: "War & Politics",
}


async def get_genre_mapping_with_fallback(allow_fallback: bool = True) -> dict[int, str]:
    """
    Get genre mapping with optional fallback to pre-defined mapping on error.

    Args:
        allow_fallback: If True, use fallback mapping when API fails.
                       If False, raise an error instead.

    Returns:
        Dict mapping genre_id to genre_name

    Raises:
        RuntimeError: If API fails and allow_fallback is False
    """
    mapping = await get_genre_mapping(force_refresh=True)

    if not mapping:
        if allow_fallback:
            logger.warning(
                "⚠️  TMDB API unavailable - using FALLBACK genre mapping. "
                "Data may be incomplete or outdated!"
            )
            return FALLBACK_GENRE_MAPPING.copy()
        else:
            raise RuntimeError(
                "Failed to fetch genre mapping from TMDB API and fallback is disabled"
            )

    return mapping
