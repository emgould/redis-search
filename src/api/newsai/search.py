"""
NewsAI Search Service - Search operations for Event Registry API
Handles trending news, search, and source management.
Supports both simple parameter-based queries and complex Event Registry query language.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import aiohttp
from contracts.models import MCType

from api.newsai.core import NewsAIService
from api.newsai.event_models import TrendingEventsResponse
from api.newsai.models import (
    NewsSearchResponse,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)


def _load_default_trending_query() -> dict[str, Any]:
    """Load the default trending query from config file."""
    config_path = Path(__file__).parent / "config" / "default_trending_query.json"
    try:
        with open(config_path) as f:
            config = json.load(f)
            # Remove comment fields
            return {k: v for k, v in config.items() if not k.startswith("$comment")}
    except Exception as e:
        logger.error(f"Error loading default trending query config: {e}")
        # Fallback to basic query
        return {
            "$query": {
                "$and": [
                    {"categoryUri": "dmoz/Arts/Television/Programs"},
                ]
            }
        }


# Cache configuration - 60 minutes for trending news (news changes frequently)
CacheExpiration = 60 * 60  # 60 minutes
NewsAICache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="newsai",
    verbose=False,
    isClassMethod=True,
    version="1.0.7",  # Version bump for Redis migration
)


class NewsAISearchService(NewsAIService):
    """
    NewsAI Search Service - Handles trending news, search, and source management.
    Extends NewsAIService with search-specific functionality using Event Registry API.

    Supports both simple parameter-based queries and complex Event Registry query language:
    - Simple: Pass parameters like country, category, query, etc.
    - Complex: Pass a 'complex_query' dict with Event Registry's $and/$or structure
    """

    # Language code mapping from ISO 639-1 (2-letter) to ISO 639-3 (3-letter) used by Event Registry
    LANG_MAP = {
        "en": "eng",
        "es": "spa",
        "de": "deu",
        "fr": "fra",
        "it": "ita",
        "pt": "por",
        "ru": "rus",
        "zh": "zho",
        "ja": "jpn",
        "ko": "kor",
        "ar": "ara",
        "hi": "hin",
        "nl": "nld",
        "sv": "swe",
        "no": "nor",
        "da": "dan",
        "fi": "fin",
        "pl": "pol",
        "tr": "tur",
        "cs": "ces",
        "el": "ell",
        "he": "heb",
        "id": "ind",
        "ms": "msa",
        "th": "tha",
        "vi": "vie",
        "uk": "ukr",
        "ro": "ron",
        "hu": "hun",
        "bg": "bul",
        "hr": "hrv",
        "sk": "slk",
        "sl": "slv",
        "sr": "srp",
        "ca": "cat",
        "et": "est",
        "lv": "lav",
        "lt": "lit",
    }

    # Category mapping from NewsAPI categories to Event Registry categories
    CATEGORY_MAP = {
        "business": "dmoz/Business",
        "entertainment": "dmoz/Arts/Entertainment",
        "general": None,  # No direct mapping
        "health": "dmoz/Health",
        "science": "dmoz/Science",
        "sports": "dmoz/Sports",
        "technology": "dmoz/Computers",
    }

    def _map_language(self, lang: str) -> str:
        """Map 2-letter language code to 3-letter code for Event Registry."""
        return self.LANG_MAP.get(lang.lower(), lang)

    def _map_category(self, category: str | None) -> str | None:
        """Map NewsAPI category to Event Registry category URI."""
        if not category:
            return None
        return self.CATEGORY_MAP.get(category.lower())

    @RedisCache.use_cache(NewsAICache, prefix="trending_events")
    async def get_trending_events(
        self,
        country: str = "us",
        query: str | None = None,
        category: str | None = None,
        page_size: int = 20,
        page: int = 1,
        complex_query: dict[str, Any] | None = None,
    ) -> TrendingEventsResponse:
        """
        Get trending news EVENTS (clusters of related articles) using Event Registry's getEvents endpoint.

        This is the preferred method for trending news as it returns events (news stories)
        rather than individual articles. Each event contains multiple related articles.

        Args:
            country: 2-letter ISO 3166-1 country code (default: us)
            query: Optional search query to filter events
            category: Optional category (business, entertainment, general, health, science, sports, technology)
            page_size: Number of events to return (max 50)
            page: Page number for pagination
            complex_query: Optional complex Event Registry query structure with $and/$or operators
                          If provided, simple parameters (country, query, category) are ignored.

        Returns:
            TrendingEventsResponse with validated event models
        """
        try:
            logger.info(
                f"Fetching trending events for country: {country}, query: {query}, complex_query: {bool(complex_query)}"
            )

            # Validate page_size - events endpoint max is 50
            page_size = min(max(page_size, 1), 50)

            url = f"{self.BASE_URL}/api/v1/event/getEvents"

            # If no query parameter provided, use default trending query
            if query is None and complex_query is None:
                complex_query = _load_default_trending_query()
                logger.info("Using default trending query from config")

            # Handle complex query structure
            if complex_query:
                # Use complex query structure with POST request
                query_body = {
                    "query": complex_query,
                    "resultType": "events",
                    "eventsPage": page,
                    "eventsCount": page_size,
                    "eventsSortBy": "date",
                    "eventsSortByAsc": False,
                    "includeEventTitle": True,
                    "includeEventSummary": True,
                    "includeEventSocialScore": True,
                    "includeEventSentiment": True,
                    "includeEventLocation": True,
                    "includeEventDate": True,
                    "includeEventArticleCounts": True,
                    "includeEventConcepts": True,
                    "includeEventCategories": True,
                    "eventImageCount": 1,
                    "includeSourceTitle": True,
                    "apiKey": self.newsai_api_key,
                }

                response, status_code = await self._core_async_post_request(
                    url,
                    query_body,
                    headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                    timeout=30,
                    rate_limit_max=8,
                    rate_limit_period=1.0,
                )

                if status_code != 200 or response is None:
                    return TrendingEventsResponse(
                        results=[],
                        total_results=0,
                        country=country,
                        query=query,
                        category=category,
                        page=page,
                        page_size=page_size,
                        status="error",
                        error=f"API request failed with status {status_code}",
                        status_code=status_code,
                    )
            else:
                # Use simple parameter-based query with GET request
                params: dict = {
                    "apiKey": self.newsai_api_key,
                    "resultType": "events",
                    "eventsPage": page,
                    "eventsCount": page_size,
                    "eventsSortBy": "date",
                    "eventsSortByAsc": "false",
                    "includeEventTitle": "true",
                    "includeEventSummary": "true",
                    "includeEventSocialScore": "true",
                    "includeEventSentiment": "true",
                    "includeEventLocation": "true",
                    "includeEventDate": "true",
                    "includeEventArticleCounts": "true",
                    "includeEventConcepts": "true",
                    "includeEventCategories": "true",
                    "eventImageCount": "1",
                    "includeSourceTitle": "true",
                }

                # Add location filter for country if not "us"
                if country and country.lower() != "us":
                    country_upper = country.upper()
                    params["sourceLocationUri"] = f"http://en.wikipedia.org/wiki/{country_upper}"

                # Add category filter if provided
                if category:
                    category_uri = self._map_category(category)
                    if category_uri:
                        params["categoryUri"] = category_uri

                # Add query filter if provided
                if query:
                    params["keyword"] = query

                async with (
                    aiohttp.ClientSession() as session,
                    session.get(url, params=params) as resp,
                ):
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Event Registry returned status {resp.status}: {error_text}")
                        return TrendingEventsResponse(
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

                    response = await resp.json()

            # Process events from Event Registry response
            events = []
            events_data = response.get("events", {})

            if isinstance(events_data, dict):
                # Extract results array from events object
                results_list = events_data.get("results", [])
            else:
                # If events is already a list
                results_list = events_data if isinstance(events_data, list) else []

            for event_data in results_list:
                event = self._process_event_item(event_data)
                events.append(event)

            # Extract total results
            total_results = 0
            if isinstance(events_data, dict):
                total_results = events_data.get("totalResults", len(events))
            else:
                total_results = len(events)

            result = TrendingEventsResponse(
                results=events,
                total_results=total_results,
                country=country,
                query=query,
                category=category,
                page=page,
                page_size=page_size,
                status="ok",
            )

            logger.info(f"Successfully fetched {len(events)} trending events")
            return result

        except Exception as e:
            logger.error(f"Unexpected error fetching trending events: {e}")
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

    @RedisCache.use_cache(NewsAICache, prefix="trending")
    async def get_trending_news(
        self,
        country: str = "us",
        query: str | None = None,
        category: str | None = None,
        page_size: int = 20,
        page: int = 1,
        complex_query: dict[str, Any] | None = None,
    ) -> TrendingNewsResponse:
        """
        Get trending news articles using Event Registry's getArticles endpoint.

        Args:
            country: 2-letter ISO 3166-1 country code (default: us)
            query: Optional search query to filter articles
            category: Optional category (business, entertainment, general, health, science, sports, technology)
            page_size: Number of articles to return (max 100)
            page: Page number for pagination
            complex_query: Optional complex Event Registry query structure with $and/$or operators
                          If provided, simple parameters (country, query, category) are ignored.
                          Example:
                          {
                              "$query": {
                                  "$and": [
                                      {"$or": [{"categoryUri": "dmoz/Arts/Television/Programs"}]},
                                      {"dateStart": "2025-11-04", "dateEnd": "2025-11-11"}
                                  ]
                              }
                          }

        Returns:
            TrendingNewsResponse with validated article models
        """
        try:
            logger.info(
                f"Fetching trending news for country: {country}, query: {query}, complex_query: {bool(complex_query)}"
            )

            # Validate page_size
            page_size = min(max(page_size, 1), 100)

            url = f"{self.BASE_URL}/api/v1/article/getArticles"

            # If no query parameter provided, use default trending query
            if query is None and complex_query is None:
                complex_query = _load_default_trending_query()
                logger.info("Using default trending query from config")

            # Handle complex query structure
            if complex_query:
                # Use complex query structure with POST request
                query_body = {
                    "query": complex_query,
                    "resultType": "articles",
                    "articlesPage": page,
                    "articlesCount": page_size,
                    "articlesSortBy": "date",
                    "articlesSortByAsc": False,
                    "includeArticleTitle": True,
                    "includeArticleBasicInfo": True,
                    "includeArticleBody": True,
                    "includeArticleImage": True,
                    "includeArticleSentiment": True,
                    "includeSourceTitle": True,
                    "apiKey": self.newsai_api_key,
                }

                response, status_code = await self._core_async_post_request(
                    url,
                    query_body,
                    headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                    timeout=30,
                    rate_limit_max=8,
                    rate_limit_period=1.0,
                )

                if status_code != 200 or response is None:
                    return TrendingNewsResponse(
                        results=[],
                        total_results=0,
                        country=country,
                        query=query,
                        category=category,
                        page=page,
                        page_size=page_size,
                        status="error",
                        error=f"API request failed with status {status_code}",
                        status_code=status_code,
                    )
            else:
                # Use simple parameter-based query with GET request
                params: dict = {
                    "apiKey": self.newsai_api_key,
                    "resultType": "articles",
                    "articlesPage": page,
                    "articlesCount": page_size,
                    "articlesSortBy": "date",  # Sort by date for trending
                    "articlesSortByAsc": "false",  # Most recent first
                    "includeArticleTitle": "true",
                    "includeArticleBasicInfo": "true",
                    "includeArticleBody": "true",
                    "includeArticleImage": "true",
                    "includeArticleSentiment": "true",
                    "includeSourceTitle": "true",
                }

                # Add location filter for country if not "us" (Event Registry uses location URIs)
                # For simplicity, we'll use sourceLocationUri to filter by country
                if country and country.lower() != "us":
                    # Map country code to Wikipedia URI format
                    country_upper = country.upper()
                    params["sourceLocationUri"] = f"http://en.wikipedia.org/wiki/{country_upper}"

                # Add category filter if provided
                if category:
                    category_uri = self._map_category(category)
                    if category_uri:
                        params["categoryUri"] = category_uri

                # Add query filter if provided
                if query:
                    params["keyword"] = query

                async with (
                    aiohttp.ClientSession() as session,
                    session.get(url, params=params) as resp,
                ):
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Event Registry returned status {resp.status}: {error_text}")
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

                    response = await resp.json()

            # Process articles from Event Registry response
            articles = []
            articles_data = response.get("articles", {})

            if isinstance(articles_data, dict):
                # Extract results array from articles object
                results_list = articles_data.get("results", [])
            else:
                # If articles is already a list
                results_list = articles_data if isinstance(articles_data, list) else []

            for article_data in results_list:
                article = self._process_article_item(article_data)
                articles.append(article)

            # Extract total results
            total_results = 0
            if isinstance(articles_data, dict):
                total_results = articles_data.get("totalResults", len(articles))
            else:
                total_results = len(articles)

            result = TrendingNewsResponse(
                results=articles,
                total_results=total_results,
                country=country,
                query=query,
                category=category,
                page=page,
                page_size=page_size,
                status="ok",
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

    @RedisCache.use_cache(NewsAICache, prefix="concept_uri")
    async def get_concept(self, title: str) -> str | None:
        """
        Resolve the top concept URI for a title (movie, show, person, etc.) using NewsAI's suggestConceptsFast.

        Args:
            title: Entity name to resolve (e.g. movie or TV show title)

        Returns:
            conceptUri (string) if found, otherwise None
        """
        try:
            logger.info(f"Resolving concept URI for: {title}")

            # Use suggestConceptsFast endpoint with GET request
            url = f"{self.BASE_URL}/api/v1/suggestConceptsFast"
            query = title.replace(" ", "_")
            params = {
                "prefix": query,
                "lang": "eng",
                "apiKey": self.newsai_api_key,
            }

            response = await self._core_async_request(
                url,
                params=params,
                headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                timeout=15,
                rate_limit_max=8,
                rate_limit_period=1.0,
                return_exceptions=True,
            )

            if not response:
                logger.warning(f"SuggestConceptsFast failed for {title}")
                return None

            # Response is a list of concept objects directly
            if isinstance(response, list) and len(response) > 0:
                top_concept = response[0]
                # Ensure top_concept is a dict before calling .get()
                if isinstance(top_concept, dict):
                    concept_uri = top_concept.get("uri")
                    if concept_uri and isinstance(concept_uri, str):
                        path = concept_uri.split("/")[-1]
                        if path.lower() == query.lower():
                            logger.info(f"Found concept URI for {title}: {concept_uri}")
                            return str(concept_uri)

            logger.info(f"No concept URI found for {title}")
            return None

        except Exception as e:
            logger.error(f"Error resolving concept URI for {title}: {e}")
            return None

    @RedisCache.use_cache(NewsAICache, prefix="media_reviews")
    async def get_media_reviews(
        self,
        title: str,
        media_type: MCType,
        page_size: int = 20,
        page: int = 1,
    ) -> NewsSearchResponse:
        """
        Search for recent critic reviews for a TV show or movie.
        Attempts concept-based search first, falls back to keyword search if no concept URI is found.
        """
        try:
            logger.info(f"Fetching media reviews for {media_type.value}: {title}")

            page_size = min(max(page_size, 1), 100)
            end_date = date.today()
            start_date = end_date - timedelta(days=30)

            # Define top critic/review sources
            # review_sources = [
            #     "metacritic.com",
            #     "editorial.rottentomatoes.com",
            #     "nytimes.com",
            #     "variety.com",
            #     "hollywoodreporter.com",
            #     "rogerebert.com",
            #     "slantmagazine.com",
            #     "ew.com",
            #     "buzzfeed.com",
            #     "wvnews.com",
            # ]

            # Step 1️⃣ — Try to resolve conceptUri
            concept_uri = await self.get_concept(title)

            # Step 2️⃣ — Build query differently based on availability
            if concept_uri:
                logger.info(f"Using conceptUri for {title}: {concept_uri}")
                # Note: conceptUri should be a single string in the query, not an array
                # The $or for sources should be a list of conditions
                complex_query = {
                    "$query": {
                        "$and": [
                            {"conceptUri": concept_uri},
                            # {"$or": [{"sourceUri": s} for s in review_sources]},
                        ]
                    },
                    "$filter": {
                        "forceMaxDataTimeWindow": "31",
                        # "dataType": ["news", "blog"],
                        "startSourceRankPercentile": 0,
                        "endSourceRankPercentile": 30,
                    },
                }

            else:
                logger.info(f"No concept found for {title}, falling back to keyword search")
                # Use exact keyword match without extra terms
                category_uri = (
                    "dmoz/Arts/Movies" if media_type == MCType.MOVIE else "dmoz/Arts/Television"
                )

                complex_query = {
                    "$query": {
                        "$and": [
                            {
                                "keyword": title,
                                "keywordSearchMode": "exact",
                            },
                            {"categoryUri": category_uri},
                            {"lang": "eng"},
                        ]
                    },
                    "$filter": {
                        "forceMaxDataTimeWindow": "31",
                        "dataType": ["news", "pr", "blog"],
                        "startSourceRankPercentile": 0,
                        "endSourceRankPercentile": 50,
                        "hasDuplicate": "skipHasDuplicates",
                    },
                }

            # Step 3️⃣ — Build payload
            url = f"{self.BASE_URL}/api/v1/article/getArticles"
            query_body = {
                "query": complex_query,
                "articlesCount": page_size,
                "articlesSortByAsc": False,
                "minRelevance": 60,
                "includeArticleTitle": True,
                "includeArticleBasicInfo": True,
                "includeArticleEventUri": False,
                "includeArticleImage": True,
                "includeArticleVideos": True,
                "resultType": "articles",
                "articlesSortBy": "rel",
                "apiKey": self.newsai_api_key,
            }

            # Step 4️⃣ — Execute request
            response, status_code = await self._core_async_post_request(
                url,
                query_body,
                headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                timeout=30,
                rate_limit_max=8,
                rate_limit_period=1.0,
            )

            if status_code != 200 or not response:
                logger.error(
                    f"Event Registry returned status {status_code} for media reviews query"
                )
                return NewsSearchResponse(
                    results=[],
                    total_results=0,
                    query=title,
                    language="en",
                    sort_by="rel",
                    from_date=start_date.isoformat(),
                    to_date=end_date.isoformat(),
                    page=page,
                    page_size=page_size,
                    status="error",
                    error=f"API request failed with status {status_code}",
                    status_code=status_code,
                )

            # Step 5️⃣ — Process and normalize articles
            articles_data = response.get("articles", {})
            if isinstance(articles_data, dict):
                results_list = articles_data.get("results", [])
            else:
                results_list = articles_data if isinstance(articles_data, list) else []

            articles = [self._process_article_item(a) for a in results_list]
            total_results = (
                articles_data.get("totalResults", len(articles))
                if isinstance(articles_data, dict)
                else len(articles)
            )

            logger.info(
                f"Fetched {len(articles)} review articles for {title} ({'concept' if concept_uri else 'keyword'} mode)"
            )

            return NewsSearchResponse(
                results=articles,
                total_results=total_results,
                query=title,
                language="en",
                sort_by="rel",
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat(),
                page=page,
                page_size=page_size,
                status="ok",
            )

        except Exception as e:
            logger.error(f"Unexpected error fetching media reviews for {title}: {e}")
            return NewsSearchResponse(
                results=[],
                total_results=0,
                query=title,
                language="en",
                sort_by="rel",
                from_date=start_date.isoformat(),
                to_date=end_date.isoformat(),
                page=page,
                page_size=page_size,
                status="error",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NewsAICache, prefix="search")
    async def search_news(
        self,
        query: str,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str = "en",
        sort_by: str = "publishedAt",
        page_size: int = 20,
        page: int = 1,
        complex_query: dict[str, Any] | None = None,
    ) -> NewsSearchResponse:
        """
        Search for news articles using Event Registry's getArticles endpoint.

        Args:
            query: Search query string
            from_date: Oldest article date (YYYY-MM-DD format)
            to_date: Newest article date (YYYY-MM-DD format)
            language: Language code (en, es, fr, de, it, etc.)
            sort_by: Sort order (relevancy, popularity, publishedAt)
            page_size: Number of articles to return (max 100)
            page: Page number for pagination
            complex_query: Optional complex Event Registry query structure with $and/$or operators
                          If provided, simple parameters are ignored.

        Returns:
            NewsSearchResponse with validated article models
        """
        try:
            logger.info(f"Searching news for: {query}, complex_query: {bool(complex_query)}")

            # Validate page_size
            page_size = min(max(page_size, 1), 100)

            url = f"{self.BASE_URL}/api/v1/article/getArticles"

            # Map sort_by to Event Registry format
            sort_by_map = {
                "relevancy": "rel",
                "popularity": "socialScore",
                "publishedAt": "date",
            }
            er_sort_by = sort_by_map.get(sort_by, "date")

            # Handle complex query structure
            if complex_query:
                # Use complex query structure with POST request
                query_body = {
                    "query": complex_query,
                    "resultType": "articles",
                    "articlesPage": page,
                    "articlesCount": page_size,
                    "articlesSortBy": er_sort_by,
                    "articlesSortByAsc": False,
                    "includeArticleTitle": True,
                    "includeArticleBasicInfo": True,
                    "includeArticleBody": True,
                    "includeArticleImage": True,
                    "includeArticleSentiment": True,
                    "includeSourceTitle": True,
                    "apiKey": self.newsai_api_key,
                }

                response, status_code = await self._core_async_post_request(
                    url,
                    query_body,
                    headers={"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"},
                    timeout=30,
                    rate_limit_max=8,
                    rate_limit_period=1.0,
                )

                if status_code != 200 or response is None:
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
                        error=f"API request failed with status {status_code}",
                        status_code=status_code,
                    )
            else:
                # Use simple parameter-based query with GET request
                # Map language to 3-letter code
                lang_code = self._map_language(language)

                params: dict = {
                    "apiKey": self.newsai_api_key,
                    "resultType": "articles",
                    "articlesPage": page,
                    "articlesCount": page_size,
                    "articlesSortBy": er_sort_by,
                    "articlesSortByAsc": "false",  # Most relevant/recent first
                    "keyword": query,
                    "lang": lang_code,
                    "includeArticleTitle": "true",
                    "includeArticleBasicInfo": "true",
                    "includeArticleBody": "true",
                    "includeArticleImage": "true",
                    "includeArticleSentiment": "true",
                    "includeSourceTitle": "true",
                }

                # Add date filters if provided
                if from_date:
                    params["dateStart"] = from_date
                if to_date:
                    params["dateEnd"] = to_date

                async with (
                    aiohttp.ClientSession() as session,
                    session.get(url, params=params) as resp,
                ):
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Event Registry returned status {resp.status}: {error_text}")
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

                    response = await resp.json()

            # Process articles from Event Registry response
            articles = []
            articles_data = response.get("articles", {})

            if isinstance(articles_data, dict):
                # Extract results array from articles object
                results_list = articles_data.get("results", [])
            else:
                # If articles is already a list
                results_list = articles_data if isinstance(articles_data, list) else []

            for article_data in results_list:
                article = self._process_article_item(article_data)
                articles.append(article)

            # Extract total results
            total_results = 0
            if isinstance(articles_data, dict):
                total_results = articles_data.get("totalResults", len(articles))
            else:
                total_results = len(articles)

            result = NewsSearchResponse(
                results=articles,
                total_results=total_results,
                query=query,
                language=language,
                sort_by=sort_by,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=page_size,
                status="ok",
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

    @RedisCache.use_cache(NewsAICache, prefix="sources")
    async def get_news_sources(
        self,
        category: str | None = None,
        language: str | None = None,
        country: str | None = None,
    ) -> NewsSourcesResponse:
        """
        Get available news sources using Event Registry's suggestSourcesFast endpoint.

        Note: Event Registry doesn't have a direct equivalent to NewsAPI's sources endpoint.
        This implementation uses the autosuggest endpoint to get popular sources.

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

            # Build request parameters for Event Registry autosuggest
            params: dict = {
                "apiKey": self.newsai_api_key,
                "prefix": "",  # Empty prefix to get popular sources
            }

            # Event Registry's suggestSourcesFast doesn't support filtering by category/language/country
            # We'll fetch sources and filter client-side if needed
            url = f"{self.BASE_URL}/api/v1/suggestSourcesFast"

            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Event Registry returned status {resp.status}: {error_text}")
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

                response = await resp.json()

            # Process sources from Event Registry response
            sources = []

            # Response is an array of source objects
            if isinstance(response, list):
                for source_data in response[:100]:  # Limit to 100 sources
                    # Event Registry source format: {uri, dataType, title}
                    source = NewsSourceDetails(
                        uri=source_data.get("uri"),
                        id=source_data.get("uri"),
                        name=source_data.get("title", ""),
                        title=source_data.get("title"),
                        data_type=source_data.get("dataType"),
                    )
                    sources.append(source)

            result = NewsSourcesResponse(
                results=sources,
                total_results=len(sources),
                total_sources=len(sources),
                category=category,
                language=language,
                country=country,
                status="ok",
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
