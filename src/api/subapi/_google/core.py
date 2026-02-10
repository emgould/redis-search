"""
Google Books Core Service - Base service for Google Books API operations
Handles core API communication and basic operations.
"""

from typing import Any

import aiohttp

from api.subapi._google.auth import google_books_auth
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 7 days for book data (same as TMDB)
CacheExpiration = 7 * 24 * 60 * 60  # 7 days
GoogleBooksCache: RedisCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="google_books",
    verbose=False,
    isClassMethod=True,
)

logger = get_logger(__name__)


class GoogleBooksService:
    """
    Core Google Books service for API communication.
    Handles basic Google Books API operations.
    """

    def __init__(self):
        """Initialize Google Books service."""
        self.base_url = "https://www.googleapis.com/books/v1"

    async def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> tuple[dict[str, Any], int | None]:
        """
        Make an async request to the Google Books API.

        Args:
            endpoint: API endpoint (e.g., 'volumes')
            params: Optional query parameters
            method: HTTP method (default: GET)

        Returns:
            tuple: (response_data, error_code) - error_code is None on success
        """
        url = f"{self.base_url}/{endpoint}"

        # Add API key to params if available
        if params is None:
            params = {}

        params["key"] = google_books_auth.google_books_api_key

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.request(method, url, params=params) as response,
            ):
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    return data, None
                elif response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    logger.warning(
                        f"Google Books API rate limit exceeded. Retry after: {retry_after}"
                    )
                    return (
                        {
                            "error": "Rate limit exceeded",
                            "retry_after": retry_after,
                        },
                        429,
                    )
                elif response.status == 403:
                    error_text = await response.text()
                    logger.error(f"Google Books API access forbidden: {error_text}")
                    return (
                        {
                            "error": "API access forbidden - check API key",
                            "details": error_text,
                        },
                        403,
                    )
                elif response.status == 404:
                    return {"error": "Not found"}, 404
                else:
                    error_text = await response.text()
                    logger.warning(f"Google Books API returned {response.status}: {error_text}")
                    return (
                        {"error": f"API request failed: {error_text}"},
                        response.status,
                    )

        except aiohttp.ClientError as e:
            logger.error(f"Network error in Google Books API request: {e}")
            return {"error": "Network request failed"}, 500
        except Exception as e:
            logger.error(f"Unexpected error in Google Books API request: {e}")
            return {"error": "Internal server error"}, 500

    @RedisCache.use_cache(GoogleBooksCache, prefix="volume_by_id")
    async def get_volume_by_id(
        self,
        volume_id: str,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], int | None]:
        """
        Get a specific volume by its Google Books ID.

        Args:
            volume_id: Google Books volume ID
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (volume_data, error_code or None)
        """
        endpoint = f"volumes/{volume_id}"
        return await self._make_request(endpoint)

    @RedisCache.use_cache(GoogleBooksCache, prefix="volume_by_isbn")
    async def get_volume_by_isbn(
        self,
        isbn: str,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], int | None]:
        """
        Get a volume by ISBN.

        Args:
            isbn: ISBN-10 or ISBN-13
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (search_results, error_code or None)
        """
        params = {
            "q": f"isbn:{isbn}",
            "maxResults": 1,
        }
        return await self._make_request("volumes", params)

    def _extract_year_from_date(self, date_str: str | None) -> int | None:
        """
        Extract year from various date formats.

        Args:
            date_str: Date string in various formats (YYYY, YYYY-MM, YYYY-MM-DD)

        Returns:
            Year as integer or None
        """
        if not date_str:
            return None

        try:
            # Try to extract first 4 digits as year
            year_str = date_str.split("-")[0]
            if len(year_str) == 4 and year_str.isdigit():
                return int(year_str)
        except (ValueError, IndexError, AttributeError):
            pass

        return None

    def _ensure_https(self, url: str | None) -> str | None:
        """
        Ensure URL uses HTTPS (React Native requirement).

        Args:
            url: URL string

        Returns:
            URL with https:// or None
        """
        if not url:
            return None
        return url.replace("http://", "https://")


google_books_service = GoogleBooksService()
