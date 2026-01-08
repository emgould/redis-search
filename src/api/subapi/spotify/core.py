"""
Spotify Core Service - Base service with core utilities.
Provides foundation for search operations.
"""

import logging
from typing import Any, cast

import aiohttp

from api.subapi.spotify.auth import spotify_auth
from contracts.models import MCImage, MCLink, MCUrlType
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__, level=logging.WARNING)

# Cache configuration - 24 hours for Spotify data
CacheExpiration = 24 * 60 * 60  # 24 hours

# Request cache - separate from other caches, independent refresh
SpotifyRequestCache = RedisCache(
    defaultTTL=12 * 60 * 60,  # 12 hours - music data fairly stable
    prefix="spotify_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.1",  # Version bump for Redis migration
)

SpotifyCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="spotify",
    verbose=False,
    isClassMethod=True,
    version="1.0.1",  # Version bump for Redis migration
)

# Rate limiter configuration: Spotify API limits
# Spotify allows ~180 requests per 30-second rolling window (6 per second average)
# Using 5 requests per second to stay safely under the limit
# Note: This controls RATE (requests/time), not concurrency
_SPOTIFY_RATE_LIMIT_MAX = 25
_SPOTIFY_RATE_LIMIT_PERIOD = 1


class SpotifyService(BaseAPIClient):
    """
    Base Spotify music service with core utilities.
    Provides foundation for search operations.
    """

    def __init__(self):
        """Initialize Spotify service."""
        pass  # noqa: PIE790

    @RedisCache.use_cache(SpotifyRequestCache, prefix="spotify_api")
    async def _make_spotify_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        """
        Make a rate-limited Spotify API request with retry logic for 429 errors.

        This method brokers the call to _core_async_request with Spotify-specific config.
        The session parameter is kept for compatibility but BaseAPIClient manages its own session.

        Args:
            session: aiohttp ClientSession (kept for compatibility, auth may use it)
            url: Spotify API URL
            params: Query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            Parsed JSON data if successful, None otherwise
        """
        # Get headers using the provided session (for auth token management)
        headers = await spotify_auth.get_spotify_headers(session)
        if not headers:
            return None

        result = await self._core_async_request(
            url=url,
            params=params,
            headers=headers,
            timeout=10,
            max_retries=max_retries,
            rate_limit_max=_SPOTIFY_RATE_LIMIT_MAX,
            rate_limit_period=_SPOTIFY_RATE_LIMIT_PERIOD,
        )
        # Cast to expected type since return_status_code=False
        return cast(dict[str, Any] | None, result)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Calculate the Levenshtein edit distance between two strings.

        Args:
            s1: First string
            s2: Second string

        Returns:
            The minimum number of single-character edits (insertions, deletions, substitutions)
            required to transform s1 into s2
        """
        # Create a matrix to store distances
        len1, len2 = len(s1), len(s2)

        # Initialize the matrix
        dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        # Initialize first row and column
        for i in range(len1 + 1):
            dp[i][0] = i
        for j in range(len2 + 1):
            dp[0][j] = j

        # Fill the matrix using dynamic programming
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if s1[i - 1] == s2[j - 1]:
                    # Characters match, no edit needed
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    # Take minimum of insert, delete, or substitute
                    dp[i][j] = 1 + min(
                        dp[i - 1][j],  # deletion
                        dp[i][j - 1],  # insertion
                        dp[i - 1][j - 1],  # substitution
                    )

        return dp[len1][len2]


def process_spotify_images(images_data: list[dict[str, Any]]) -> tuple[list[MCImage], str | None]:
    """
    Process Spotify images array and convert to MCImage list.

    Args:
        images_data: List of image dictionaries from Spotify API

    Returns:
        Tuple of (list of MCImage objects, default_image_url or None)
    """
    if not images_data:
        return [], None

    images = []
    for image in images_data:
        width = image.get("width") or 0
        height = image.get("height") or 0
        url = image.get("url")
        if not url:
            continue

        # Determine image size key based on width
        if width <= 160:
            key = "s"
        elif width <= 320:
            key = "m"
        else:
            key = "l"

        images.append(
            MCImage(
                url=url,
                key=key,
                type=MCUrlType.URL,
                description=f"{width}x{height}",
            )
        )

    # Return the largest image (last in Spotify's sorted order) as default
    default_image = images[-1].url if images else None
    return images, default_image


def process_spotify_links(data: dict[str, Any]) -> tuple[list[MCLink], str | None]:
    """
    Process Spotify external URLs and href to create MCLink list.

    Args:
        data: Dictionary containing external_urls and href from Spotify API

    Returns:
        Tuple of (list of MCLink objects, spotify_url or None)
    """
    links = []
    external_urls = data.get("external_urls", {})

    # Process external_urls dictionary (key-value pairs where value is URL string)
    for key, url in external_urls.items():
        if url:
            links.append(
                MCLink(
                    url=url,
                    key=key,
                    description=key,
                )
            )

    # Add href if present
    href = data.get("href")
    if href:
        links.append(MCLink(url=href, key="href", description="Spotify Href"))

    # Extract Spotify URL specifically
    spotify_url = external_urls.get("spotify") or None
    return links, spotify_url
