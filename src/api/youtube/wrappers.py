"""
YouTube Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

from datetime import UTC, datetime
from typing import cast

from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSearchResponse,
    MCSources,
    MCType,
)

from api.youtube.core import YouTubeService, youtube_service
from api.youtube.models import (
    YouTubeCategoriesResponse,
    YouTubePopularResponse,
    YouTubeSearchResponse,
    YouTubeTrendingResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
YouTubeWrapperCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours - same as core service
    prefix="youtube_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="4.0.3",  # Version bump for Redis migration
)


class YouTubeWrapper:
    def __init__(self):
        """Initialize YouTube wrapper. Service is created per-request with API key."""
        self.service = youtube_service

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="get_trending_videos")
    async def get_trending_videos(
        self,
        region_code: str = "US",
        language: str = "en",
        max_results: int = 50,
        category_id: str | None = None,
        query: str | None = None,
        **kwargs,
    ) -> YouTubeTrendingResponse:
        """
        Async wrapper function to get trending YouTube videos.

        Returns:
            YouTubeTrendingResponse: MCBaseItem derivative containing trending videos or error information
        """

        try:
            logger.info("🔍 Wrapper: About to call service.get_trending_videos")
            data = await self.service.get_trending_videos(
                region_code=region_code,
                language=language,
                max_results=max_results,
                category_id=category_id,
                query=query,
            )
            logger.info(f"🔍 Wrapper: Service call completed, got {data.total_results} videos")

            if data.error:
                data.status_code = 500
                return cast(YouTubeTrendingResponse, data)

            data.status_code = 200
            return cast(YouTubeTrendingResponse, data)

        except Exception as e:
            logger.error(f"Error in get_trending_videos: {e}")
            error_response = YouTubeTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=[],
                total_results=0,
                region_code=region_code,
                language=language,
                category_id=category_id,
                query=query,
                fetched_at="",
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="search_videos")
    async def search_videos(
        self,
        query: str,
        max_results: int = 25,
        order: str = "relevance",
        published_after: str | None = None,
        region_code: str = "US",
        language: str = "en",
    ) -> YouTubeSearchResponse:
        """
        Async wrapper function to search YouTube videos via the api, very expensive

        Returns:
            YouTubeSearchResponse: MCBaseItem derivative containing search results or error information
        """

        if not query:
            error_response = YouTubeSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query="",
                error="query parameter is required",
                status_code=400,
            )
            return error_response

        try:
            data = await self.service.search_videos(
                query=query,
                max_results=max_results,
                order=order,
                published_after=published_after,
                region_code=region_code,
                language=language,
            )

            if data.error:
                data.status_code = 500
                return cast(YouTubeSearchResponse, data)

            data.status_code = 200
            return cast(YouTubeSearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_videos: {e}")
            error_response = YouTubeSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="get_video_categories")
    async def get_video_categories(
        self,
        region_code: str = "US",
        language: str = "en",
    ) -> YouTubeCategoriesResponse:
        """
        Async wrapper function to get YouTube video categories.

        Returns:
            YouTubeCategoriesResponse: MCBaseItem derivative containing categories or error information
        """

        try:
            data = await self.service.get_video_categories(
                region_code=region_code,
                language=language,
            )

            if data.error:
                data.status_code = 500
                return cast(YouTubeCategoriesResponse, data)

            data.status_code = 200
            return cast(YouTubeCategoriesResponse, data)

        except Exception as e:
            logger.error(f"Error in get_video_categories: {e}")
            error_response = YouTubeCategoriesResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                categories=[],
                region_code=region_code,
                language=language,
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="get_popular_videos")
    async def get_popular_videos(
        self,
        query: str | None = None,
        max_results: int = 50,
        region_code: str = "US",
        language: str = "en",
        published_after: str | None = None,
    ) -> YouTubePopularResponse:
        """
        Async wrapper function to get popular YouTube videos.

        Returns:
            YouTubePopularResponse: MCBaseItem derivative containing popular videos or error information
        """

        try:
            data = await self.service.get_popular_videos(
                query=query,
                max_results=max_results,
                region_code=region_code,
                language=language,
                published_after=published_after,
            )

            if data.error:
                data.status_code = 500
                return cast(YouTubePopularResponse, data)

            data.status_code = 200
            return cast(YouTubePopularResponse, data)

        except Exception as e:
            logger.error(f"Error in get_popular_videos: {e}")
            error_response = YouTubePopularResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                videos=[],
                total_results=0,
                query=query or "",
                region_code=region_code,
                language=language,
                error=str(e),
                status_code=500,
            )
            return error_response

    """
    Person Wrappers
    """

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="get_person_details")
    async def get_person_details(
        self, source_id: str, page: int = 1, limit: int = 20
    ) -> MCSearchResponse:
        """Get details about a creator given a source ID

        Args:
            query: Search query
            page: Page number for pagination
            limit: Maximum number of results per page to return

        Returns:
            MCSearchResponse with results of type MCPersonItem
            Status code: 200 for success, 400/404/500 for errors
        """
        if not source_id.strip():
            return MCSearchResponse(
                results=[],
                total_results=0,
                query=source_id,
                data_type=MCType.PERSON,
                data_source="YouTube",
                error="Search query is required",
                status_code=404,
            )

        try:
            service = YouTubeService()
            response: MCSearchResponse = await service.get_person_details(source_id, limit)

            return response

        except Exception as e:
            logger.error(f"Error in person search for '{source_id}': {e}")
            return MCSearchResponse(
                results=[],
                total_results=0,
                query=source_id,
                data_type=MCType.PERSON,
                data_source="TMDB Person Search",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="search_people_async")
    async def search_people_async(
        self, query: str, page: int = 1, limit: int = 20
    ) -> MCSearchResponse:
        """Search for people/actors using TMDB's person search endpoint.

        Args:
            query: Search query
            page: Page number for pagination
            limit: Maximum number of results per page to return

        Returns:
            MCSearchResponse with results of type MCPersonItem
            Status code: 200 for success, 400/404/500 for errors
        """
        if not query.strip():
            return MCSearchResponse(
                results=[],
                total_results=0,
                query=query,
                data_type=MCType.PERSON,
                data_source="TMDB Person Search",
                error="Search query is required",
                status_code=400,
            )

        try:
            service = YouTubeService()
            result = await service.search_people(query, limit)

            # Convert TMDBSearchPersonResult to MCSearchResponse
            return MCSearchResponse(
                results=result.results,  # type: ignore[arg-type]
                total_results=result.total_results,
                query=query,
                data_type=MCType.PERSON,
                data_source="TMDB Person Search",
            )

        except Exception as e:
            logger.error(f"Error in person search for '{query}': {e}")
            return MCSearchResponse(
                results=[],
                total_results=0,
                query=query,
                data_type=MCType.PERSON,
                data_source="TMDB Person Search",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(YouTubeWrapperCache, prefix="search_person_async")
    async def search_person_async(
        self,
        request: "MCPersonSearchRequest",
        media_type: MCType = MCType.VIDEO,
        limit: int | None = None,
        **kwargs,
    ) -> "MCPersonSearchResponse":
        """Search for person works (movies and TV shows) based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

        Args:
            request: MCPersonSearchRequest with person identification details
            limit: Maximum number of works to return per type (default: 50)

        Returns:
            MCPersonSearchResponse with person details and works
            - details: MCPersonItem (person details)
            - works: list[MCMovieItem | MCTvItem] (combined movies and TV shows)
            - related: [] (empty, will be filled by search_broker)
        """

        try:
            # If source is not YouTube, fall back to name-based search
            # This allows searching for videos by people from other sources (e.g., authors, actors)
            if request.source != MCSources.YOUTUBE:
                logger.info(
                    f"Non-YouTube source detected ({request.source}), falling back to name-based channel search for: {request.name}"
                )
                # Use name-based search instead of ID-based search
                channel_response = await self.search_people_async(
                    query=request.name, limit=limit or 20
                )

                # Convert MCSearchResponse to MCPersonSearchResponse
                # If we found channels, return them as "related" (potential matches)
                if channel_response.status_code == 200 and channel_response.results:
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,  # No specific channel details
                        works=[],  # No works since we don't have a specific channel ID
                        related=channel_response.results,  # Return found channels as related
                        error=None,
                        status_code=200,
                    )
                else:
                    # No channels found or error occurred
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error=channel_response.error,
                        status_code=channel_response.status_code,
                    )

            # Validate source_id (YouTube uses channel IDs)
            person_id_str = request.source_id
            try:
                person_id = int(person_id_str)
            except ValueError:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source_id for TMDB person: {request.source_id} (must be a number)",
                    status_code=404,
                )

            if person_id <= 0:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source_id for TMDB person: {request.source_id} (must be positive)",
                    status_code=400,
                )

            # Get person details
            person_details_response: MCSearchResponse = await self.service.get_person_details(
                person_id, limit=1
            )
            if person_details_response.status_code != 200 or not person_details_response.results:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=person_details_response.error or "Person not found",
                    status_code=person_details_response.status_code or 404,
                )

            # Get person credits (includes person details, movies, and TV shows)
            credit_limit = limit if limit is not None else 50
            credits_response: MCSearchResponse = await self.service.get_channel_videos(
                person_id, limit=credit_limit
            )

            if credits_response.status_code != 200 or not credits_response.results:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=credits_response.error or "Person not found",
                    status_code=credits_response.status_code or 404,
                )

            # Extract person details and works from credits response

            # Combine movies and TV shows into works array
            works: list[MCBaseItem] = list(credits_response.results)  # type: ignore[assignment]

            # Return response with person details and works
            # related will be filled by search_broker
            return MCPersonSearchResponse(
                input=request,
                details=person_details_response.results[0],  # MCPersonItem
                works=works,  # list[MCBaseItem]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching person works for {request.name}: {e}")
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )


youtube_wrapper = YouTubeWrapper()
