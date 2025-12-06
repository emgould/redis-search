"""
News Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

from typing import Any, cast

from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
)

from api.news.models import (
    NewsSearchResponse,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from api.news.search import NewsSearchService
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods - using Redis for distributed caching
NewsCache = RedisCache(
    defaultTTL=30 * 60,  # 30 minutes (news changes frequently)
    prefix="news_func",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="5.0.0",  # Bumped for Redis cache migration
)


class NewsWrapper:
    def __init__(self):
        self.service = NewsSearchService()

    @RedisCache.use_cache(NewsCache, prefix="get_trending_news_wrapper")
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

    @RedisCache.use_cache(NewsCache, prefix="search_news_wrapper")
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

    @RedisCache.use_cache(NewsCache, prefix="get_news_sources_wrapper")
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

    @RedisCache.use_cache(NewsCache, prefix="search_person_works")
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
            # Validate that this is a NewsAPI author

            # For NewsAPI, we use the name to search for articles by author
            author_name = request.name
            if not author_name:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="Author name is required for NewsAPI search",
                    status_code=400,
                )

            # Search for articles using author name in query
            # NewsAPI doesn't have a dedicated author search, so we search articles
            # and filter by author field
            article_limit = min(limit if limit is not None else 50, 100)  # NewsAPI max is 100
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

            # For NewsAPI, we don't have a dedicated author model
            # Use the first article's author info to create a simple author representation
            # or return None for details
            details = None

            # Return response with author details and works
            # related will be filled by search_broker
            return MCPersonSearchResponse(
                input=request,
                details=details,  # None for NewsAPI (no author model)
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


news_wrapper = NewsWrapper()


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
        "MCPersonSearchResponse", await news_wrapper.search_person_async(request, limit=limit)
    )
