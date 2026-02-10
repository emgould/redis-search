"""
News Search Service - Search operations for NewsAPI
Handles trending news, search, and source management.
"""

import aiohttp

from api.news.core import NewsService
from api.news.models import (
    NewsSearchResponse,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache configuration - 30 minutes for trending news (news changes frequently)
CacheExpiration = 30 * 60  # 30 minutes
NewsCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="news",
    verbose=False,
    isClassMethod=True,
)


class NewsSearchService(NewsService):
    """
    News Search Service - Handles trending news, search, and source management.
    Extends NewsService with search-specific functionality.
    """

    @RedisCache.use_cache(NewsCache, prefix="trending")
    async def get_trending_news(
        self,
        country: str = "us",
        query: str | None = None,
        category: str | None = None,
        page_size: int = 20,
        page: int = 1,
    ) -> TrendingNewsResponse:
        """
        Get trending news articles.

        Args:
            country: 2-letter ISO 3166-1 country code (default: us)
            query: Optional search query to filter articles
            category: Optional category (business, entertainment, general, health, science, sports, technology)
            page_size: Number of articles to return (max 100)
            page: Page number for pagination

        Returns:
            TrendingNewsResponse with validated article models
        """
        try:
            logger.info(f"Fetching trending news for country: {country}, query: {query}")

            # Validate API key
            if not self.news_api_key:
                logger.error("NewsAPI key is not available")
                return TrendingNewsResponse(
                    results=[],
                    total_results=0,
                    country=country,
                    query=query,
                    category=category,
                    page=page,
                    page_size=page_size,
                    status="error",
                    error="NewsAPI key is not configured",
                    status_code=500,
                )

            # Validate page_size
            page_size = min(max(page_size, 1), 100)  # NewsAPI limit is 100

            # Build request parameters
            params = {
                "country": country,
                "pageSize": page_size,
                "page": page,
                "apiKey": self.news_api_key,
            }

            if category:
                params["category"] = category

            if query:
                params["q"] = query

            # Use top headlines endpoint for trending news - make async HTTP request
            url = "https://newsapi.org/v2/top-headlines"
            headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params, headers=headers) as resp,
            ):
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"NewsAPI returned status {resp.status}: {error_text}")
                    # Check if response is HTML (error page) vs JSON
                    if error_text.strip().startswith("<!DOCTYPE") or error_text.strip().startswith(
                        "<html"
                    ):
                        logger.error("NewsAPI returned HTML error page instead of JSON response")
                    return TrendingNewsResponse(
                        results=[],
                        total_results=0,
                        country=country,
                        query=query,
                        category=category,
                        page=page,
                        page_size=page_size,
                        status="error",
                        error=f"API request failed with status {resp.status}",
                        status_code=resp.status,
                    )

                # Check content type to ensure we're getting JSON
                content_type = resp.headers.get("Content-Type", "").lower()
                if "application/json" not in content_type:
                    error_text = await resp.text()
                    logger.error(
                        f"NewsAPI returned non-JSON response. Content-Type: {content_type}, Response: {error_text[:500]}"
                    )
                    return TrendingNewsResponse(
                        results=[],
                        total_results=0,
                        country=country,
                        query=query,
                        category=category,
                        page=page,
                        page_size=page_size,
                        status="error",
                        error=f"API returned non-JSON response (Content-Type: {content_type})",
                        status_code=500,
                    )

                response = await resp.json()

            # Process articles
            articles = []
            for article_data in response.get("articles", []):
                article = self._process_article_item(article_data)
                articles.append(article)

            result = TrendingNewsResponse(
                results=articles,
                total_results=response.get("totalResults", len(articles)),
                country=country,
                query=query,
                category=category,
                page=page,
                page_size=page_size,
                status=response.get("status"),
            )

            logger.info(f"Successfully fetched {len(articles)} trending news articles")
            return result

        except Exception as e:
            logger.error(f"Unexpected error fetching trending news: {e}")
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

    @RedisCache.use_cache(NewsCache, prefix="search")
    async def search_news(
        self,
        query: str,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 20,
        page: int = 1,
    ) -> NewsSearchResponse:
        """
        Search for news articles.

        Args:
            query: Search query string
            from_date: Oldest article date (YYYY-MM-DD format)
            to_date: Newest article date (YYYY-MM-DD format)
            language: Language code (en, es, fr, de, it, etc.)
            sort_by: Sort order (relevancy, popularity, publishedAt)
            page_size: Number of articles to return (max 100)
            page: Page number for pagination

        Returns:
            NewsSearchResponse with validated article models
        """
        try:
            logger.info(f"Searching news for: {query}")

            # Validate API key
            if not self.news_api_key:
                logger.error("NewsAPI key is not available")
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
                    error="NewsAPI key is not configured",
                    status_code=500,
                )

            # Validate page_size
            page_size = min(max(page_size, 1), 100)  # NewsAPI limit is 100

            # Build request parameters
            params = {
                "q": query,
                "language": language,
                "sortBy": sort_by,
                "pageSize": page_size,
                "page": page,
                "apiKey": self.news_api_key,
            }

            if from_date:
                params["from"] = from_date
            if to_date:
                params["to"] = to_date

            # Use everything endpoint for search - make async HTTP request
            url = "https://newsapi.org/v2/everything"
            headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params, headers=headers) as resp,
            ):
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"NewsAPI returned status {resp.status}: {error_text}")
                    # Check if response is HTML (error page) vs JSON
                    if error_text.strip().startswith("<!DOCTYPE") or error_text.strip().startswith(
                        "<html"
                    ):
                        logger.error("NewsAPI returned HTML error page instead of JSON response")
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
                        error=f"API request failed with status {resp.status}",
                        status_code=resp.status,
                    )

                # Check content type to ensure we're getting JSON
                content_type = resp.headers.get("Content-Type", "").lower()
                if "application/json" not in content_type:
                    error_text = await resp.text()
                    logger.error(
                        f"NewsAPI returned non-JSON response. Content-Type: {content_type}, Response: {error_text[:500]}"
                    )
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
                        error=f"API returned non-JSON response (Content-Type: {content_type})",
                        status_code=500,
                    )

                response = await resp.json()

            # Process articles
            articles = []
            for article_data in response.get("articles", []):
                article = self._process_article_item(article_data)
                articles.append(article)

            result = NewsSearchResponse(
                results=articles,
                total_results=response.get("totalResults", len(articles)),
                query=query,
                language=language,
                sort_by=sort_by,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=page_size,
                status=response.get("status"),
            )

            logger.info(f"Successfully found {len(articles)} news articles for query: {query}")
            return result

        except Exception as e:
            logger.error(f"Unexpected error during news search: {e}")
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

    @RedisCache.use_cache(NewsCache, prefix="sources")
    async def get_news_sources(
        self,
        category: str | None = None,
        language: str | None = None,
        country: str | None = None,
    ) -> NewsSourcesResponse:
        """
        Get available news sources.

        Args:
            category: Optional category filter
            language: Optional language filter
            country: Optional country filter

        Returns:
            NewsSourcesResponse with validated source models
        """
        try:
            logger.info(
                f"Fetching news sources for category: {category}, language: {language}, country: {country}"
            )

            # Validate API key
            if not self.news_api_key:
                logger.error("NewsAPI key is not available")
                return NewsSourcesResponse(
                    results=[],
                    total_results=0,
                    total_sources=0,
                    category=category,
                    language=language,
                    country=country,
                    status="error",
                    error="NewsAPI key is not configured",
                    status_code=500,
                )

            # Build request parameters
            params = {"apiKey": self.news_api_key}
            if category:
                params["category"] = category
            if language:
                params["language"] = language
            if country:
                params["country"] = country

            # Make async HTTP request to sources endpoint
            url = "https://newsapi.org/v2/top-headlines/sources"
            headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params, headers=headers) as resp,
            ):
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"NewsAPI returned status {resp.status}: {error_text}")
                    # Check if response is HTML (error page) vs JSON
                    if error_text.strip().startswith("<!DOCTYPE") or error_text.strip().startswith(
                        "<html"
                    ):
                        logger.error("NewsAPI returned HTML error page instead of JSON response")
                    return NewsSourcesResponse(
                        results=[],
                        total_results=0,
                        total_sources=0,
                        category=category,
                        language=language,
                        country=country,
                        status="error",
                        error=f"API request failed with status {resp.status}",
                        status_code=resp.status,
                    )

                # Check content type to ensure we're getting JSON
                content_type = resp.headers.get("Content-Type", "").lower()
                if "application/json" not in content_type:
                    error_text = await resp.text()
                    logger.error(
                        f"NewsAPI returned non-JSON response. Content-Type: {content_type}, Response: {error_text[:500]}"
                    )
                    return NewsSourcesResponse(
                        results=[],
                        total_results=0,
                        total_sources=0,
                        category=category,
                        language=language,
                        country=country,
                        status="error",
                        error=f"API returned non-JSON response (Content-Type: {content_type})",
                        status_code=500,
                    )

                response = await resp.json()

            # Process sources
            sources = []
            for source_data in response.get("sources", []):
                source = NewsSourceDetails(
                    id=source_data.get("id"),
                    name=source_data.get("name", ""),
                    description=source_data.get("description"),
                    url=source_data.get("url"),
                    category=source_data.get("category"),
                    language=source_data.get("language"),
                    country=source_data.get("country"),
                )
                sources.append(source)

            result = NewsSourcesResponse(
                results=sources,
                total_results=len(sources),
                total_sources=len(sources),
                category=category,
                language=language,
                country=country,
                status=response.get("status"),
            )

            logger.info(f"Successfully fetched {len(sources)} news sources")
            return result

        except Exception as e:
            logger.error(f"Unexpected error fetching sources: {e}")
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
