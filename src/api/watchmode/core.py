"""
Watchmode Core Service - Base service for Watchmode API operations.
Handles new releases, title details, and streaming availability.
"""

from typing import Any, cast

from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 6 hours for new releases data
CacheExpiration = 6 * 60 * 60  # 6 hours

# Request cache - separate from other caches, independent refresh
WatchmodeRequestCache = RedisCache(
    defaultTTL=4 * 60 * 60,  # 4 hours - streaming data changes frequently
    prefix="watchmode_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.0",  # Request cache version - independent from other caches
)

WatchmodeCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="watchmode",
    verbose=False,
    isClassMethod=True,
    version="2.1.0",  # Bumped for refactored modular structure
)

logger = get_logger(__name__)


class WatchmodeService(BaseAPIClient):
    """
    Base Watchmode service for all Watchmode API operations.
    Provides core utilities for fetching streaming data.
    """

    # Default rate limit (no specific limit documented, using conservative defaults)
    _rate_limit_max = 10
    _rate_limit_period = 1

    def __init__(self, api_key: str):
        """Initialize Watchmode service with API key.

        Args:
            api_key: Watchmode API key (required)

        Raises:
            ValueError: If API key is not provided
        """
        if not api_key:
            raise ValueError("Watchmode API key is required")

        self.api_key = api_key
        self.base_url = "https://api.watchmode.com/v1"

    @RedisCache.use_cache(WatchmodeRequestCache, prefix="watchmode_api")
    async def _make_request(
        self, endpoint: str, params: dict | None = None, max_retries: int = 3
    ) -> Any:
        """Make an HTTP request to the Watchmode API with retry logic.

        This method brokers the call to _core_async_request with Watchmode-specific config.

        Args:
            endpoint: API endpoint (e.g., '/releases/')
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            JSON response (dict or list) or None on error
        """
        if params is None:
            params = {}

        # Add API key to params
        params["apiKey"] = self.api_key

        url = f"{self.base_url}{endpoint}"

        result = await self._core_async_request(
            url=url,
            params=params,
            headers=None,
            timeout=30,
            max_retries=max_retries,
            rate_limit_max=self._rate_limit_max,
            rate_limit_period=self._rate_limit_period,
        )
        # Cast to expected type since return_status_code=False
        return result

    @RedisCache.use_cache(WatchmodeCache, prefix="new_releases")
    async def get_new_releases(
        self, region: str = "US", types: str = "movie,tv", limit: int = 50
    ) -> dict | None:
        """Get new releases from streaming api.

        Args:
            region: Country code (default: US)
            types: Content types (default: movie,tv)
                   Note: The /releases/ endpoint uses different type names than /search/
                   Valid values: "movie", "tv", "tv_special", "tv_miniseries", "short_film"
            limit: Number of results (default: 50)

        Returns:
            Dict with releases data or None on error
        """
        from datetime import datetime, timedelta

        # Calculate date range for "this week" - last 7 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        params = {
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "regions": region,
            "types": types,
            "limit": limit,
        }

        result = await self._make_request("/releases/", params)
        return cast(dict[Any, Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="title_details")
    async def get_title_details(self, watchmode_id: int) -> dict | None:
        """Get detailed information for a specific title.

        Args:
            watchmode_id: Watchmode title ID

        Returns:
            Dict with title details or None on error
        """
        result = await self._make_request(f"/title/{watchmode_id}/details/")
        return cast(dict[Any, Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="title_sources")
    async def get_title_streaming_sources(
        self, watchmode_id: int, region: str = "US"
    ) -> list | None:
        """Get streaming sources for a specific title.

        Args:
            watchmode_id: Watchmode title ID
            region: Country code (default: US)

        Returns:
            List of streaming sources or None on error.
            Note: The Watchmode API always returns a list for this endpoint.
        """
        params = {"regions": region}
        result = await self._make_request(f"/title/{watchmode_id}/sources/", params)
        # The sources endpoint always returns a list
        return cast(list[Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="search")
    async def search_titles(self, query: str, types: str = "movie,tv") -> dict | None:
        """Search for titles by name.

        Args:
            query: Search query string
            types: Content types to search (default: movie,tv)
                   Valid values: "movie", "tv", "person" or comma-separated combinations

        Returns:
            Dict with search results or None on error
        """
        params = {"search_field": "name", "search_value": query, "types": types}
        result = await self._make_request("/search/", params)
        return cast(dict[Any, Any] | None, result)
