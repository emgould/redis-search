"""
Streaming Availability Core Service - Base service for Streaming Availability API operations.
Handles streaming availability lookups by TMDB ID via RapidAPI.

API Documentation: https://rapidapi.com/movie-of-the-night-movie-of-the-night-default/api/streaming-availability
"""

from typing import Any, Literal, cast

import aiohttp

from api.streaming_availability.auth import streaming_availability_auth
from api.streaming_availability.rapidapi_key_pool import (
    RapidAPIQuotaExhaustedError,
    pick_rapidapi_key,
    record_rapidapi_rate_limit_headers,
)
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 4 hours for streaming data (changes frequently)
CacheExpiration = 4 * 60 * 60  # 4 hours

# Request cache - separate from other caches, independent refresh
StreamingAvailabilityRequestCache = RedisCache(
    defaultTTL=4 * 60 * 60,  # 4 hours - streaming data changes frequently
    prefix="sa_request",
    verbose=False,
    isClassMethod=True,
)

StreamingAvailabilityCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="streaming_availability",
    verbose=False,
    isClassMethod=True,
)

logger = get_logger(__name__)

_REQUEST_TIMEOUT_SECONDS = 30
_CHANGES_TIMEOUT_SECONDS = 90


def _query_params(params: dict[str, Any]) -> dict[str, str]:
    return {key: str(value) for key, value in params.items()}


def _status_result(status: int, body: object) -> object:
    """Map one HTTP result. HTTP 429 is quota exhaustion for a single fixed key."""
    if status == 200:
        return body
    if status == 429:
        raise RapidAPIQuotaExhaustedError
    return None


async def _streaming_availability_get(
    url: str,
    params: dict[str, str],
    api_key: str,
    api_host: str,
    timeout_seconds: int = _REQUEST_TIMEOUT_SECONDS,
) -> tuple[int, object, dict[str, str]]:
    """One Streaming Availability GET. Does not retry HTTP 429."""
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host,
    }
    async with (
        aiohttp.ClientSession(timeout=timeout) as session,
        session.get(url, params=params, headers=headers) as response,
    ):
        response_headers = dict(response.headers.items())
        if response.status == 200:
            body: object = await response.json()
            return response.status, body, response_headers
        await response.read()
        return response.status, None, response_headers


class StreamingAvailabilityService(BaseAPIClient):
    """
    Base Streaming Availability service for all API operations.
    Provides core utilities for fetching streaming availability data via RapidAPI.
    """

    def __init__(self, rapidapi_key: str | None = None) -> None:
        """Initialize Streaming Availability service.

        Args:
            rapidapi_key: Optional fixed key for tests. When omitted, each HTTP
                request randomly selects a key from the configured RAPIDAPI_KEY pool.
        """
        self._rapidapi_key_override = rapidapi_key
        self.base_url = "https://streaming-availability.p.rapidapi.com"
        self.api_host = "streaming-availability.p.rapidapi.com"

    async def _request_with_key(
        self,
        url: str,
        params: dict[str, str],
        api_key: str,
        *,
        record_quota: bool,
        timeout_seconds: int = _REQUEST_TIMEOUT_SECONDS,
    ) -> tuple[int, object]:
        status, body, headers = await _streaming_availability_get(
            url,
            params,
            api_key,
            self.api_host,
            timeout_seconds=timeout_seconds,
        )
        if record_quota:
            record_rapidapi_rate_limit_headers(api_key, headers, http_status=status)
        return status, body

    @RedisCache.use_cache(StreamingAvailabilityRequestCache, prefix="sa_api")
    async def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3,
        timeout_seconds: int = _REQUEST_TIMEOUT_SECONDS,
    ) -> Any:
        """Fetch one Streaming Availability resource, rotating keys on HTTP 429.

        Does not use BaseAPIClient's shared 429 retry loop. ``max_retries`` is
        retained so existing cache keys stay stable; server errors are not retried.

        Raises:
            RapidAPIQuotaExhaustedError: Every pooled key is cooled or the last status was 429.
        """
        del max_retries
        if params is None:
            params = {}
        query = _query_params(params)
        url = f"{self.base_url}{endpoint}"

        if self._rapidapi_key_override:
            status, body = await self._request_with_key(
                url,
                query,
                self._rapidapi_key_override,
                record_quota=False,
                timeout_seconds=timeout_seconds,
            )
            return _status_result(status, body)

        candidates = streaming_availability_auth.configured_rapidapi_keys()
        if not candidates:
            raise RapidAPIQuotaExhaustedError
        remaining = list(candidates)
        last_status = 0
        for _attempt in range(len(candidates)):
            api_key = pick_rapidapi_key(remaining)
            if api_key is None:
                break
            remaining = [key for key in remaining if key != api_key]
            status, body = await self._request_with_key(
                url,
                query,
                api_key,
                record_quota=True,
                timeout_seconds=timeout_seconds,
            )
            last_status = status
            if status == 200:
                return body
            if status != 429:
                return None
        if last_status in (0, 429):
            raise RapidAPIQuotaExhaustedError
        return None

    @RedisCache.use_cache(StreamingAvailabilityCache, prefix="show_by_tmdb")
    async def get_show_by_tmdb_id(
        self,
        tmdb_id: int,
        content_type: Literal["movie", "tv"] = "movie",
        country: str | None = None,
    ) -> dict[str, Any] | None:
        """Get streaming availability for a title using TMDB ID.

        The Streaming Availability API uses format: /shows/{tmdb_type}/{tmdb_id}
        where tmdb_type is "movie" or "tv".

        Args:
            tmdb_id: TMDB ID of the title
            content_type: "movie" or "tv"
            country: Optional 2-letter country code to filter results (e.g., "us", "gb")

        Returns:
            Dict with show details and streaming options, or None if not found
        """
        # Build the endpoint path
        # API expects: /shows/movie/{id} or /shows/tv/{id}
        endpoint = f"/shows/{content_type}/{tmdb_id}"

        params: dict[str, Any] = {}
        if country:
            params["country"] = country.lower()

        result = await self._make_request(endpoint, params)
        return cast(dict[str, Any] | None, result)

    @RedisCache.use_cache(StreamingAvailabilityCache, prefix="show_by_imdb")
    async def get_show_by_imdb_id(
        self,
        imdb_id: str,
        country: str | None = None,
    ) -> dict[str, Any] | None:
        """Get streaming availability for a title using IMDB ID.

        Args:
            imdb_id: IMDB ID of the title (e.g., "tt0068646")
            country: Optional 2-letter country code to filter results

        Returns:
            Dict with show details and streaming options, or None if not found
        """
        # Ensure IMDB ID has 'tt' prefix
        if not imdb_id.startswith("tt"):
            imdb_id = f"tt{imdb_id}"

        endpoint = f"/shows/{imdb_id}"

        params: dict[str, Any] = {}
        if country:
            params["country"] = country.lower()

        result = await self._make_request(endpoint, params)
        return cast(dict[str, Any] | None, result)

    @RedisCache.use_cache(StreamingAvailabilityCache, prefix="changes")
    async def get_changes(
        self,
        country: str = "us",
        change_type: Literal["new", "removed", "updated"] = "new",
        item_type: Literal["show", "movie", "series"] = "show",
        show_type: Literal["movie", "series"] | None = None,
        catalogs: list[str] | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any] | None:
        """Get recent changes (new additions, removals, updates) on streaming services.

        This is the key endpoint for "what's new" functionality.

        Args:
            country: 2-letter country code (default: "us")
            change_type: Type of change - "new", "removed", or "updated" (default: "new")
            item_type: "show" (both), "movie", or "series" (default: "show")
            show_type: Optional "movie" or "series" when item_type is "show"
            catalogs: Optional list of service catalogs to filter (e.g., ["netflix", "prime"])
            from_timestamp: Optional Unix timestamp for start of time range
            to_timestamp: Optional Unix timestamp for end of time range
            cursor: Optional cursor for pagination

        Returns:
            Dict with changes data including 'changes' array and optional 'nextCursor'
        """
        endpoint = "/changes"

        params: dict[str, Any] = {
            "country": country.lower(),
            "change_type": change_type,
            "item_type": item_type,
        }

        if show_type is not None:
            params["show_type"] = show_type
        if catalogs:
            params["catalogs"] = ",".join(catalogs)
        if from_timestamp:
            params["from"] = from_timestamp
        if to_timestamp:
            params["to"] = to_timestamp
        if cursor:
            params["cursor"] = cursor

        result = await self._make_request(
            endpoint,
            params,
            timeout_seconds=_CHANGES_TIMEOUT_SECONDS,
        )
        return cast(dict[str, Any] | None, result)

    @RedisCache.use_cache(StreamingAvailabilityCache, prefix="search_filters")
    async def search_by_filters(
        self,
        country: str = "us",
        catalogs: list[str] | None = None,
        show_type: Literal["movie", "series"] | None = None,
        genres: list[str] | None = None,
        year_min: int | None = None,
        year_max: int | None = None,
        rating_min: int | None = None,
        order_by: str = "popularity_1year",
        cursor: str | None = None,
    ) -> dict[str, Any] | None:
        """Search for shows using various filters.

        Args:
            country: 2-letter country code (default: "us")
            catalogs: Optional list of service catalogs (e.g., ["netflix", "prime"])
            show_type: "movie" or "series"
            genres: Optional list of genre IDs
            year_min: Minimum release year
            year_max: Maximum release year
            rating_min: Minimum rating (0-100)
            order_by: Sort order (default: "popularity_1year")
            cursor: Optional cursor for pagination

        Returns:
            Dict with search results including 'shows' array and optional 'nextCursor'
        """
        endpoint = "/shows/search/filters"

        params: dict[str, Any] = {
            "country": country.lower(),
            "order_by": order_by,
        }

        if catalogs:
            params["catalogs"] = ",".join(catalogs)
        if show_type:
            params["show_type"] = show_type
        if genres:
            params["genres"] = ",".join(genres)
        if year_min:
            params["year_min"] = year_min
        if year_max:
            params["year_max"] = year_max
        if rating_min:
            params["rating_min"] = rating_min
        if cursor:
            params["cursor"] = cursor

        result = await self._make_request(endpoint, params)
        return cast(dict[str, Any] | None, result)
