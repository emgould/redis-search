"""
RottenTomatoes Core Service - Base service for RottenTomatoes API operations via Algolia.
Handles core API communication and search operations.
"""

from typing import Any, cast

from api.rottentomatoes.models import (
    AlgoliaMultiQueryResponse,
    ContentRtHit,
    MCRottenTomatoesItem,
    MCRottenTomatoesPersonItem,
    PeopleRtHit,
    RottenTomatoesPeopleSearchResponse,
    RottenTomatoesSearchResponse,
    SearchResultContent,
    SearchResultPeople,
)
from contracts.models import MCType
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 12 hours for search data
CacheExpiration = 12 * 60 * 60  # 12 hours

# Request cache for raw API responses
RottenTomatoesRequestCache = RedisCache(
    defaultTTL=12 * 60 * 60,  # 12 hours - search data stable
    prefix="rottentomatoes_request",
    verbose=False,
    isClassMethod=True,
)

# Cache for processed search results
RottenTomatoesCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="rottentomatoes",
    verbose=False,
    isClassMethod=True,
)

logger = get_logger(__name__)

# Algolia API configuration for RottenTomatoes
ALGOLIA_APP_ID = "79FRDP12PN"
ALGOLIA_API_KEY = "175588f6e5f8319b27702e4cc4013561"


class RottenTomatoesService(BaseAPIClient):
    """
    Core RottenTomatoes service for API communication via Algolia.
    Handles search operations for movies, TV shows, and people.
    """

    # Rate limiter configuration: Algolia is generous but we'll be conservative
    # Using 10 requests per second to be safe
    _rate_limit_max = 2
    _rate_limit_period = 1

    def __init__(self):
        """Initialize RottenTomatoes service."""
        super().__init__()
        self.algolia_url = (
            f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"
            f"?x-algolia-agent=Algolia%20for%20JavaScript%20(4.25.3)%3B%20Browser%20(lite)"
            f"&x-algolia-api-key={ALGOLIA_API_KEY}"
            f"&x-algolia-application-id={ALGOLIA_APP_ID}"
        )

    def _build_search_params(self, query: str, hits_per_page: int = 10) -> str:
        """Build encoded search parameters for Algolia request."""
        encoded_query = query.replace(" ", "+")
        # Note: RT Algolia expects spaces around the equals sign in the filter
        # isEmsSearchable = 1 (encoded as isEmsSearchable%20%3D%201)
        return (
            f"analyticsTags=%5B%22header_search%22%5D"
            f"&clickAnalytics=true"
            f"&filters=isEmsSearchable%20%3D%201"
            f"&hitsPerPage={hits_per_page}"
            f"&query={encoded_query}"
        )

    @RedisCache.use_cache(RottenTomatoesRequestCache, prefix="rt_search")
    async def _make_search_request(
        self,
        query: str,
        include_content: bool = True,
        include_people: bool = True,
        hits_per_page: int = 10,
        **kwargs: Any,
    ) -> AlgoliaMultiQueryResponse:
        """
        Make async search request to RottenTomatoes Algolia API.

        Args:
            query: Search query string
            include_content: Whether to include content (movies/TV) results
            include_people: Whether to include people results
            hits_per_page: Number of results per page (default: 10)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            AlgoliaMultiQueryResponse with search results
        """
        params = self._build_search_params(query, hits_per_page)

        # Build request payload
        requests: list[dict[str, Any]] = []
        if include_content:
            requests.append(
                {
                    "indexName": "content_rt",
                    "params": params,
                    "query": query,
                }
            )
        if include_people:
            requests.append(
                {
                    "indexName": "people_rt",
                    "params": params,
                    "query": query,
                }
            )

        payload = {"requests": requests}

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": "https://www.rottentomatoes.com",
            "Referer": "https://www.rottentomatoes.com/",
        }

        try:
            response, status_code = await self._core_async_post_request(
                url=self.algolia_url,
                json_body=payload,
                headers=headers,
                timeout=30,
                max_retries=3,
                rate_limit_max=self._rate_limit_max,
                rate_limit_period=self._rate_limit_period,
                return_exceptions=True,
            )

            if not response or status_code != 200:
                logger.error(f"RottenTomatoes search failed with status {status_code}")
                return AlgoliaMultiQueryResponse(results=[])

            return AlgoliaMultiQueryResponse.model_validate(response)

        except Exception as e:
            logger.error(f"Error making RottenTomatoes search request: {e}")
            return AlgoliaMultiQueryResponse(results=[])

    @RedisCache.use_cache(RottenTomatoesCache, prefix="search_content")
    async def search_content(
        self,
        query: str,
        limit: int = 10,
        media_type: MCType | None = None,
        **kwargs: Any,
    ) -> RottenTomatoesSearchResponse:
        """
        Search for movies and TV shows on RottenTomatoes.

        Args:
            query: Search query string
            limit: Maximum number of results (default: 10)
            media_type: Filter by MCType.MOVIE or MCType.TV_SERIES (optional)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesSearchResponse with content results
        """
        if not query or not query.strip():
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error="Search query is required",
                status_code=400,
            )

        try:
            # Make search request
            response = await self._make_search_request(
                query=query,
                include_content=True,
                include_people=False,
                hits_per_page=limit,
                **kwargs,
            )

            # Extract content results
            results: list[MCRottenTomatoesItem] = []
            content_hits = 0

            for result in response.results:
                # Check if this is content results (has ContentRtHit type hits)
                if isinstance(result, SearchResultContent) or (
                    hasattr(result, "hits")
                    and result.hits
                    and isinstance(result.hits[0], ContentRtHit)
                ):
                    content_result = cast(SearchResultContent, result)
                    content_hits = content_result.nbHits or 0

                    for hit in content_result.hits[:limit]:
                        item = MCRottenTomatoesItem.from_content_hit(hit)

                        # Filter by media_type if specified
                        if media_type:
                            if media_type == MCType.MOVIE and item.mc_type != MCType.MOVIE:
                                continue
                            if media_type == MCType.TV_SERIES and item.mc_type != MCType.TV_SERIES:
                                continue

                        results.append(item)

            return RottenTomatoesSearchResponse(
                results=results,
                total_results=len(results),
                query=query,
                content_hits=content_hits,
                people_hits=0,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching RottenTomatoes content: {e}")
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(RottenTomatoesCache, prefix="search_people")
    async def search_people(
        self,
        query: str,
        limit: int = 10,
        **kwargs: Any,
    ) -> RottenTomatoesPeopleSearchResponse:
        """
        Search for people (actors, directors, etc.) on RottenTomatoes.

        Args:
            query: Search query string
            limit: Maximum number of results (default: 10)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesPeopleSearchResponse with people results
        """
        if not query or not query.strip():
            return RottenTomatoesPeopleSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error="Search query is required",
                status_code=400,
            )

        try:
            # Make search request
            response = await self._make_search_request(
                query=query,
                include_content=False,
                include_people=True,
                hits_per_page=limit,
                **kwargs,
            )

            # Extract people results
            results: list[MCRottenTomatoesPersonItem] = []

            for result in response.results:
                # Check if this is people results
                if isinstance(result, SearchResultPeople) or (
                    hasattr(result, "hits")
                    and result.hits
                    and isinstance(result.hits[0], PeopleRtHit)
                ):
                    people_result = cast(SearchResultPeople, result)

                    for hit in people_result.hits[:limit]:
                        item = MCRottenTomatoesPersonItem.from_people_hit(hit)
                        results.append(item)

            return RottenTomatoesPeopleSearchResponse(
                results=results,
                total_results=len(results),
                query=query,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching RottenTomatoes people: {e}")
            return RottenTomatoesPeopleSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(RottenTomatoesCache, prefix="search_all")
    async def search_all(
        self,
        query: str,
        limit: int = 10,
        **kwargs: Any,
    ) -> RottenTomatoesSearchResponse:
        """
        Search for both content and people on RottenTomatoes.

        Args:
            query: Search query string
            limit: Maximum number of results per type (default: 10)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            RottenTomatoesSearchResponse with all results
        """
        if not query or not query.strip():
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error="Search query is required",
                status_code=400,
            )

        try:
            # Make search request for both content and people
            response = await self._make_search_request(
                query=query,
                include_content=True,
                include_people=True,
                hits_per_page=limit,
                **kwargs,
            )

            # Extract all results
            results: list[MCRottenTomatoesItem] = []
            content_hits = 0

            for result in response.results:
                if isinstance(result, SearchResultContent) or (
                    hasattr(result, "hits") and result.hits and hasattr(result.hits[0], "rtId")
                ):
                    content_result = cast(SearchResultContent, result)
                    content_hits = content_result.nbHits or 0

                    for hit in content_result.hits[:limit]:
                        item = MCRottenTomatoesItem.from_content_hit(hit)
                        results.append(item)

            return RottenTomatoesSearchResponse(
                results=results,
                total_results=len(results),
                query=query,
                content_hits=content_hits,
                people_hits=0,
                data_type=MCType.MIXED,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching RottenTomatoes: {e}")
            return RottenTomatoesSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )


# Create singleton instance
rottentomatoes_service = RottenTomatoesService()
