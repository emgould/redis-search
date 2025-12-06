"""
RottenTomatoes Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using MCSearchResponse pattern.
"""

from typing import Any, TypedDict, cast

from contracts.models import MCType

from api.rottentomatoes.core import RottenTomatoesService
from api.rottentomatoes.models import (
    MCRottenTomatoesItem,
    RottenTomatoesPeopleSearchResponse,
    RottenTomatoesSearchResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache


class RTMetrics(TypedDict):
    """Simple structure for RottenTomatoes scores."""

    critics: int | None
    audience: int | None


logger = get_logger(__name__)

# Cache for wrapper methods
RottenTomatoesWrapperCache = RedisCache(
    defaultTTL=60 * 60 * 72,  # 3 days
    prefix="rottentomatoes_wrapper",
    verbose=False,
    isClassMethod=True,
    version="1.0.0",  # Initial version
)


class RottenTomatoesWrapper:
    """Wrapper class for RottenTomatoes async operations."""

    def __init__(self):
        self.service = RottenTomatoesService()

    @RedisCache.use_cache(RottenTomatoesWrapperCache, prefix="search_content_wrapper")
    async def search_content(
        self,
        query: str,
        limit: int = 10,
        media_type: MCType | None = None,
        **kwargs: Any,
    ) -> RottenTomatoesSearchResponse:
        """
        Async wrapper function to search for movies and TV shows.

        Args:
            query: Search query string
            limit: Number of results to return (max 50)
            media_type: Filter by MCType.MOVIE or MCType.TV_SERIES (optional)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesSearchResponse: MCSearchResponse derivative containing search results
        """
        try:
            data = await self.service.search_content(
                query=query,
                limit=limit,
                media_type=media_type,
                **kwargs,
            )

            data.data_source = "search_content_wrapper"
            return cast(RottenTomatoesSearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_content wrapper: {e}")
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                data_source="search_content_wrapper",
                status_code=500,
            )

    @RedisCache.use_cache(RottenTomatoesWrapperCache, prefix="search_people_wrapper")
    async def search_people(
        self,
        query: str,
        limit: int = 10,
        **kwargs: Any,
    ) -> RottenTomatoesPeopleSearchResponse:
        """
        Async wrapper function to search for people (actors, directors, etc.).

        Args:
            query: Search query string
            limit: Number of results to return (max 50)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesPeopleSearchResponse: MCSearchResponse derivative containing search results
        """
        try:
            data = await self.service.search_people(
                query=query,
                limit=limit,
                **kwargs,
            )

            data.data_source = "search_people_wrapper"
            return cast(RottenTomatoesPeopleSearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_people wrapper: {e}")
            return RottenTomatoesPeopleSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                data_source="search_people_wrapper",
                status_code=500,
            )

    @RedisCache.use_cache(RottenTomatoesWrapperCache, prefix="search_all_wrapper")
    async def search_all(
        self,
        query: str,
        limit: int = 10,
        **kwargs: Any,
    ) -> RottenTomatoesSearchResponse:
        """
        Async wrapper function to search for both content and people.

        Args:
            query: Search query string
            limit: Number of results per type to return (max 50)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesSearchResponse: MCSearchResponse derivative containing search results
        """
        try:
            data = await self.service.search_all(
                query=query,
                limit=limit,
                **kwargs,
            )

            data.data_source = "search_all_wrapper"
            return cast(RottenTomatoesSearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_all wrapper: {e}")
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                data_source="search_all_wrapper",
                status_code=500,
            )


# Create singleton instance
rottentomatoes_wrapper = RottenTomatoesWrapper()


# Standalone async functions for direct import
async def search_content_async(
    query: str,
    limit: int = 10,
    media_type: MCType | None = None,
    **kwargs: Any,
) -> RottenTomatoesSearchResponse:
    """
    Standalone async function to search for movies and TV shows.

    Args:
        query: Search query string
        limit: Number of results to return (max 50)
        media_type: Filter by MCType.MOVIE or MCType.TV_SERIES (optional)
        **kwargs: Additional arguments (including no_cache flag)

    Returns:
        RottenTomatoesSearchResponse: MCSearchResponse derivative containing search results
    """
    result = await rottentomatoes_wrapper.search_content(
        query=query,
        limit=limit,
        media_type=media_type,
        **kwargs,
    )
    return cast(RottenTomatoesSearchResponse, result)


async def search_people_async(
    query: str,
    limit: int = 10,
    **kwargs: Any,
) -> RottenTomatoesPeopleSearchResponse:
    """
    Standalone async function to search for people.

    Args:
        query: Search query string
        limit: Number of results to return (max 50)
        **kwargs: Additional arguments (including no_cache flag)

    Returns:
        RottenTomatoesPeopleSearchResponse: MCSearchResponse derivative containing search results
    """
    result = await rottentomatoes_wrapper.search_people(
        query=query,
        limit=limit,
        **kwargs,
    )
    return cast(RottenTomatoesPeopleSearchResponse, result)


async def search_all_async(
    query: str,
    limit: int = 10,
    **kwargs: Any,
) -> RottenTomatoesSearchResponse:
    """
    Standalone async function to search for both content and people.

    Args:
        query: Search query string
        limit: Number of results per type to return (max 50)
        **kwargs: Additional arguments (including no_cache flag)

    Returns:
        RottenTomatoesSearchResponse: MCSearchResponse derivative containing search results
    """
    result = await rottentomatoes_wrapper.search_all(
        query=query,
        limit=limit,
        **kwargs,
    )
    return cast(RottenTomatoesSearchResponse, result)


async def get_rt_metrics(
    title: str,
    year: int | None = None,
    star: str | None = None,
    **kwargs: Any,
) -> RTMetrics | None:
    """
    Get RottenTomatoes critic and audience scores for a title.

    Simple lookup that returns just the scores. Searches by title and optionally
    filters by year and/or star (cast member).

    Args:
        title: The movie or TV show title to search for (required)
        year: Release year to filter by (optional)
        star: Cast member name to filter by (optional)
        **kwargs: Additional arguments (including no_cache flag)

    Returns:
        RTMetrics dict with critics and audience scores, or None if not found

    Example:
        >>> scores = await get_rt_metrics("The Matrix", year=1999)
        >>> print(scores)
        {'critics': 83, 'audience': 85}

        >>> scores = await get_rt_metrics("Inception", star="Leonardo DiCaprio")
        >>> print(scores)
        {'critics': 87, 'audience': 91}
    """
    if not title or not title.strip():
        return None

    # Determine how many results to fetch based on filters
    # If filtering, get more results to search through
    limit = 1 if not year and not star else 10

    try:
        result = await rottentomatoes_wrapper.search_content(
            query=title.strip(),
            limit=limit,
            **kwargs,
        )

        if result.status_code != 200 or not result.results:
            return None

        # Find matching result
        match: MCRottenTomatoesItem | None = None

        for item in result.results:
            # If no filters, take first result
            if not year and not star:
                match = item
                break

            # Check year filter
            if year and item.release_year != year:
                continue

            # Check star filter (case-insensitive partial match)
            if star:
                star_lower = star.lower()
                cast_match = any(star_lower in cast_name.lower() for cast_name in item.cast_names)
                if not cast_match:
                    continue

            # Passed all filters
            match = item
            break

        if not match:
            return None

        return RTMetrics(
            critics=match.critics_score,
            audience=match.audience_score,
        )

    except Exception as e:
        logger.error(f"Error in get_rt_metrics for '{title}': {e}")
        return None
