"""
NewsAI Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
Drop-in replacement for news API wrappers.
"""

from typing import Any, cast

from api.newsai.event_models import TrendingEventsResponse
from api.newsai.models import (
    NewsSearchResponse,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from api.newsai.search import NewsAISearchService
from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
NewsAICache = RedisCache(
    defaultTTL=60 * 60,  # 1 hour (news changes frequently)
    prefix="newsai",
    verbose=False,
    isClassMethod=True,  # Required for class methods
)


class NewsAIWrapper:
    def __init__(self):
        self.service = NewsAISearchService()

    @RedisCache.use_cache(NewsAICache, prefix="get_trending_events_wrapper")
    async def get_trending_events(
        self,
        country: str = "us",
        query: str | None = None,
        category: str | None = None,
        page_size: int = 20,
        page: int = 1,
        **kwargs: Any,
    ) -> TrendingEventsResponse:
        """
        Async wrapper function to get trending news events.

        This is the PREFERRED method for trending news as it returns events (news stories)
        rather than individual articles. Each event contains multiple related articles.

        Returns:
            TrendingEventsResponse: MCSearchResponse derivative containing trending events data or error information
        """
        try:
            data = await self.service.get_trending_events(
                country=country,
                query=query,
                category=category,
                page_size=page_size,
                page=page,
                **kwargs,
            )

            if data is None or data.status == "error":
                return TrendingEventsResponse(
                    results=[],
                    total_results=0,
                    country=country,
                    query=query,
                    category=category,
                    page=page,
                    page_size=page_size,
                    status="error",
                    error="Failed to fetch trending events",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return TrendingEventsResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Error in get_trending_events: {e}")
            return TrendingEventsResponse(
                results=[],
                total_results=0,
                country=country,
                query=query,
                category=category,
                page=page,
                page_size=page_size,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="get_trending_news_wrapper")
    async def get_trending_news(
        self,
        country: str = "us",
        query: str | None = None,
        category: str | None = None,
        page_size: int = 20,
        page: int = 1,
        **kwargs: Any,
    ) -> TrendingNewsResponse:
        """
        Async wrapper function to get trending news.

        Returns:
            TrendingNewsResponse: MCSearchResponse derivative containing trending news data or error information
        """
        try:
            data = await self.service.get_trending_news(
                country=country,
                query=query,
                category=category,
                page_size=page_size,
                page=page,
                **kwargs,
            )

            if data is None or data.status == "error":
                return TrendingNewsResponse(
                    results=[],
                    total_results=0,
                    country=country,
                    query=query,
                    category=category,
                    page=page,
                    page_size=page_size,
                    status="error",
                    error="Failed to fetch trending news",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return TrendingNewsResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Error in get_trending_news: {e}")
            return TrendingNewsResponse(
                results=[],
                total_results=0,
                country=country,
                query=query,
                category=category,
                page=page,
                page_size=page_size,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="search_news_wrapper")
    async def search_news(
        self,
        query: str,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 20,
        page: int = 1,
        **kwargs: Any,
    ) -> NewsSearchResponse:
        """
        Async wrapper function to search news.

        Returns:
            NewsSearchResponse: MCSearchResponse derivative containing search results or error information
        """
        try:
            data = await self.service.search_news(
                query=query,
                from_date=from_date,
                to_date=to_date,
                language=language,
                sort_by=sort_by,
                page_size=page_size,
                page=page,
                **kwargs,
            )

            if data is None or data.status == "error":
                return NewsSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    language=language,
                    sort_by=sort_by,
                    from_date=from_date,
                    to_date=to_date,
                    page=page,
                    page_size=page_size,
                    status="error",
                    error="Failed to search news",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return NewsSearchResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Error in search_news: {e}")
            return NewsSearchResponse(
                results=[],
                total_results=0,
                query=query,
                language=language,
                sort_by=sort_by,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=page_size,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="get_news_sources_wrapper")
    async def get_news_sources(
        self,
        category: str | None = None,
        language: str | None = None,
        country: str | None = None,
        **kwargs: Any,
    ) -> NewsSourcesResponse:
        """
        Async wrapper function to get news sources.

        Returns:
            NewsSourcesResponse: MCSearchResponse derivative containing news sources or error information
        """
        try:
            data = await self.service.get_news_sources(
                category=category,
                language=language,
                country=country,
                **kwargs,
            )

            if data is None or data.status == "error":
                return NewsSourcesResponse(
                    results=[],
                    total_results=0,
                    total_sources=0,
                    category=category,
                    language=language,
                    country=country,
                    status="error",
                    error="Failed to fetch news sources",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return NewsSourcesResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Error in get_news_sources: {e}")
            return NewsSourcesResponse(
                results=[],
                total_results=0,
                total_sources=0,
                category=category,
                language=language,
                country=country,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="get_media_reviews_wrapper")
    async def get_media_reviews(
        self,
        title: str,
        media_type: str,
        page_size: int = 20,
        page: int = 1,
        **kwargs: Any,
    ) -> NewsSearchResponse:
        """
        Async wrapper function to get media reviews for a TV show or movie.

        Args:
            title: Title of the TV show or movie
            media_type: Media type ('movie' or 'tv')
            page_size: Number of articles to return (max 100)
            page: Page number for pagination

        Returns:
            NewsSearchResponse: MCSearchResponse derivative containing review articles or error information
        """
        try:
            from contracts.models import MCType

            # Convert string media_type to MCType enum
            if media_type.lower() == "movie":
                mc_type = MCType.MOVIE
            elif media_type.lower() in ["tv", "tv_series"]:
                mc_type = MCType.TV_SERIES
            else:
                return NewsSearchResponse(
                    results=[],
                    total_results=0,
                    query=title,
                    language="en",
                    sort_by="date",
                    page=page,
                    page_size=page_size,
                    status="error",
                    error=f"Invalid media_type: {media_type}. Must be 'movie' or 'tv'",
                    status_code=400,
                )

            data = await self.service.get_media_reviews(
                title=title,
                media_type=mc_type,
                page_size=page_size,
                page=page,
                **kwargs,
            )

            if data is None or data.status == "error":
                return NewsSearchResponse(
                    results=[],
                    total_results=0,
                    query=title,
                    language="en",
                    sort_by="date",
                    page=page,
                    page_size=page_size,
                    status="error",
                    error="Failed to fetch media reviews",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return NewsSearchResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Error in get_media_reviews: {e}")
            return NewsSearchResponse(
                results=[],
                total_results=0,
                query=title,
                language="en",
                sort_by="date",
                page=page,
                page_size=page_size,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="search_person_works")
    async def search_person_async(
        self,
        request: "MCPersonSearchRequest",
        limit: int | None = None,
    ) -> "MCPersonSearchResponse":
        """Search for news author works (articles) based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

        Args:
            request: MCPersonSearchRequest with author identification details
            limit: Maximum number of articles to return (default: 50)

        Returns:
            MCPersonSearchResponse with author details and works
            - details: MCNewsItem | None (author details, if available)
            - works: list[MCNewsItem] (articles written by the author)
            - related: [] (empty, will be filled by search_broker)
        """
        from contracts.models import MCPersonSearchResponse

        try:
            # Validate that this is a NewsAI author
            # For NewsAI/Event Registry, we use the name to search for articles by author
            author_name = request.name
            if not author_name:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="Author name is required for NewsAI search",
                    status_code=400,
                )

            # Search for articles using author name in query
            # Event Registry has authorUri parameter we could use if we had the author URI
            # For now, we'll search articles and filter by author field
            article_limit = min(limit if limit is not None else 50, 100)
            articles_response = await self.search_news(query=author_name, page_size=article_limit)

            if articles_response.status_code != 200:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=articles_response.error or "Failed to search articles",
                    status_code=articles_response.status_code or 500,
                )

            if len(articles_response.results) == 0:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="No articles found",
                    status_code=404,
                )

            # Filter articles to only include those by this author
            # Match by author field (case-insensitive, partial match)
            works: list[MCBaseItem] = []
            author_name_lower = author_name.lower().strip()

            for article in articles_response.results:
                article_author = article.author or ""
                if article_author and author_name_lower in article_author.lower():
                    works.append(article)

            # For NewsAI/Event Registry, we don't have a dedicated author model
            # Use the first article's author info to create a simple author representation
            # or return None for details
            details = None

            # Return response with author details and works
            # related will be filled by search_broker
            return MCPersonSearchResponse(
                input=request,
                details=details,  # None for NewsAI (no author model)
                works=works,  # list[MCNewsItem]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching author works for {request.name}: {e}")
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )


newsai_wrapper = NewsAIWrapper()


async def search_person_async(
    request: "MCPersonSearchRequest",
    limit: int | None = None,
) -> "MCPersonSearchResponse":
    """Search for news author works (articles) based on person search request.

    This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

    Args:
        request: MCPersonSearchRequest with author identification details
        limit: Maximum number of articles to return (default: 50)

    Returns:
        MCPersonSearchResponse with author details and works
        - details: MCNewsItem | None (author details, if available)
        - works: list[MCNewsItem] (articles written by the author)
        - related: [] (empty, will be filled by search_broker)
    """
    return cast(
        "MCPersonSearchResponse", await newsai_wrapper.search_person_async(request, limit=limit)
    )
