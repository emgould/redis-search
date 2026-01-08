"""
LastFM Core Service - Base service with core utilities.
Provides foundation for enrichment and search operations.
"""

import os
from typing import Any, cast

import aiohttp

from api.lastfm.auth import Auth
from contracts.models import MCType, generate_mc_id
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 24 hours for trending albums
CacheExpiration = 24 * 60 * 60  # 24 hours

# Request cache - separate from other caches, independent refresh
LastFMRequestCache = RedisCache(
    defaultTTL=12 * 60 * 60,  # 12 hours - music data fairly stable
    prefix="lastfm_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.0",  # Request cache version - independent from other caches
)

LastFMCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="lastfm",
    verbose=True,
    version="2.4.0",  # Fixed: image fallback now uses largest image (index 0) instead of smallest
)

logger = get_logger(__name__)


class LastFMService(Auth, BaseAPIClient):
    """
    Base LastFM music service with core utilities.
    Provides foundation for enrichment and search operations.
    """

    # Default rate limit (no specific limit documented, using conservative defaults)
    _rate_limit_max = 50
    _rate_limit_period = 1

    def __init__(
        self,
        spotify_client_id: str | None = None,
        spotify_client_secret: str | None = None,
    ):
        """
        Initialize LastFM service.

        Args:
            spotify_client_id: Spotify client ID (optional, for enrichment)
            spotify_client_secret: Spotify client secret (optional, for enrichment)
        """
        super().__init__()

        # Spotify credentials (optional - enrichment will be skipped if not provided)
        self.spotify_client_id = spotify_client_id or os.getenv("SPOTIFY_CLIENT_ID")
        self.spotify_client_secret = spotify_client_secret or os.getenv("SPOTIFY_CLIENT_SECRET")

    def _process_album_item(self, album_data: dict) -> dict:
        """
        Process and normalize an album item from Last.fm.

        Args:
            album_data: Raw album data from Last.fm API

        Returns:
            Processed album dictionary with standardized fields
        """
        try:
            # Extract basic album information
            artist_info = album_data.get("artist", {})
            artist_name = (
                artist_info.get("name", "") if isinstance(artist_info, dict) else str(artist_info)
            )

            mbid = album_data.get("mbid", "")

            # Get the best quality image (extralarge preferred)
            image_url = None
            images = album_data.get("image", [])
            if images:
                for img in images:
                    if img.get("size") == "extralarge" and img.get("#text"):
                        image_url = img["#text"]
                        break
                # Fallback to any available image if extralarge not found
                if not image_url:
                    for img in images:
                        if img.get("#text"):
                            image_url = img["#text"]
                            break

            processed_album = {
                "title": album_data.get("name", ""),
                "artist": artist_name,
                "listeners": int(album_data.get("listeners", 0))
                if album_data.get("listeners")
                else 0,
                "playcount": int(album_data.get("playcount", 0))
                if album_data.get("playcount")
                else 0,
                "image": image_url,
                "url": album_data.get("url", ""),
                "mbid": mbid,
                "artist_url": artist_info.get("url") if isinstance(artist_info, dict) else None,
                "streamable": album_data.get("streamable"),
                "spotify_url": None,
                "apple_music_url": None,
            }

            # Add MediaCircle standardized fields
            processed_album["mc_id"] = generate_mc_id(processed_album, MCType.MUSIC_ALBUM)
            processed_album["mc_type"] = MCType.MUSIC_ALBUM.value

            return processed_album

        except Exception as e:
            logger.error(f"Error processing album item: {e}")
            return {
                "title": album_data.get("name", "Error processing album"),
                "artist": "Unknown",
                "error": str(e),
            }

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Calculate the Levenshtein edit distance between two strings.

        Used for fuzzy matching in search results.

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

    def _process_spotify_result(self, data: list[dict], type: str) -> list[dict]:
        """
        Process Spotify search results and format for MediaCircle.

        Args:
            data: List of Spotify items (artists, albums, or playlists)
            type: Type of items ('music_artist', 'music_album', 'music_playlist')

        Returns:
            List of processed items with standardized fields
        """
        data = [d for d in data if d is not None]
        # Sort by popularity (descending)
        data.sort(key=lambda x: x.get("popularity", 0), reverse=True)

        # Format results to match app's expected structure
        results = []
        for item in data:
            images = item.get("images", [])
            # Get the largest image (first one is usually 640x640)
            image_url = images[0].get("url") if images else None

            # Get followers count (only artists have this field)
            followers_data = item.get("followers")
            followers_count = followers_data.get("total", 0) if followers_data else 0

            # Get external URLs (safely handle None)
            external_urls = item.get("external_urls") or {}
            spotify_url = external_urls.get("spotify")

            response_data = {
                # Core fields
                "id": item.get("id"),
                "name": item.get("name"),
                # Spotify specific
                "spotify_url": spotify_url,
                "popularity": item.get("popularity", 0),
                "followers": followers_count,
                "genres": item.get("genres", []),
                # Image
                "image": image_url,
                "images": images,  # Keep all image sizes
                # For compatibility with existing music display
                "artist": item.get("name"),  # For cards that expect 'artist' field
                "title": item.get("name"),  # For cards that expect 'title' field
            }
            results.append(response_data)
        return results

    @RedisCache.use_cache(LastFMRequestCache, prefix="lastfm_api")
    async def _make_request(
        self,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> tuple[dict[str, Any] | None, int]:
        """
        Make an async request to the Last.fm API.

        This method brokers the call to _core_async_request with LastFM-specific config.
        Returns tuple format for compatibility with existing code.

        Args:
            params: Query parameters (must include 'method' and will auto-add api_key and format)
            method: HTTP method (default: GET)

        Returns:
            tuple: (response_data, status_code) - response_data is None on error
        """
        if not self.base_url:
            logger.error("base_url is not set")
            return None, 500

        if params is None:
            params = {}

        # Add required Last.fm API parameters
        params["api_key"] = self.lastfm_api_key
        params["format"] = "json"

        # Note: LastFM API only uses GET requests, method parameter kept for compatibility
        if method != "GET":
            logger.warning(f"LastFM API only supports GET requests, ignoring method={method}")

        result = await self._core_async_request(
            url=self.base_url,
            params=params,
            headers=None,
            timeout=10,
            max_retries=3,
            rate_limit_max=self._rate_limit_max,
            rate_limit_period=self._rate_limit_period,
        )

        # Cast to expected type since return_status_code=False
        result_dict = cast(dict[str, Any] | None, result)

        if result_dict is None:
            return None, 500

        return result_dict, 200

    @RedisCache.use_cache(LastFMRequestCache, prefix="odesli_api")
    async def _odesli_make_request(self, spotify_url: str, **kwargs) -> dict[str, Any] | None:
        """
        Make an async request to the Odesli (Songlink) API.

        Uses _core_async_request for rate limiting, retry logic, and error handling.

        Args:
            spotify_url: Spotify URL to expand to all streaming platforms

        Returns:
            Dict with platform links or None on error
        """
        if not spotify_url:
            return None

        odesli_url = "https://api.song.link/v1-alpha.1/links"
        params = {"url": spotify_url}

        # Use conservative rate limits for Odesli (free API, no documented limits)
        # Using same rate limit as LastFM for consistency
        data = await self._core_async_request(
            url=odesli_url,
            params=params,
            headers=None,
            timeout=10,
            max_retries=3,
            rate_limit_max=5,  # Conservative rate limit for free API
            rate_limit_period=1.0,
        )

        if data is None or not isinstance(data, dict):
            return None

        # Extract platform links from response
        links = {}

        # Try direct "linksByPlatform"
        links_by_platform = data.get("linksByPlatform")
        if isinstance(links_by_platform, dict):
            for platform, platform_data in links_by_platform.items():
                # Normalize platform keys to lowercase for consistency
                platform_key = platform.lower()
                if isinstance(platform_data, dict):
                    links[platform_key] = platform_data.get("url")

        # Fallback: nested inside entitiesByUniqueId
        elif "entitiesByUniqueId" in data:
            entities = data.get("entitiesByUniqueId")
            if isinstance(entities, dict):
                for entity in entities.values():
                    if isinstance(entity, dict) and "linksByPlatform" in entity:
                        entity_links = entity.get("linksByPlatform")
                        if isinstance(entity_links, dict):
                            for platform, platform_data in entity_links.items():
                                # Normalize platform keys to lowercase for consistency
                                platform_key = platform.lower()
                                if isinstance(platform_data, dict):
                                    links[platform_key] = platform_data.get("url")

        return links if links else None

    async def _mb_make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any] | None, int]:
        """
        Make an async request to the MusicBrainz API.

        Args:
            endpoint: API endpoint URL (full URL or path)
            params: Optional query parameters
            headers: Optional headers (User-Agent will be added if not provided)

        Returns:
            tuple: (response_data, status_code) - response_data is None on error
        """
        # Ensure endpoint is a full URL
        if not endpoint.startswith("http"):
            mb_base_url = "https://musicbrainz.org/ws/2"
            endpoint = f"{mb_base_url}/{endpoint}"

        # Add default headers
        if headers is None:
            headers = {}
        if "User-Agent" not in headers:
            headers["User-Agent"] = "MediaCircle/1.0"

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response,
            ):
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    return data, 200
                else:
                    logger.error(f"MusicBrainz API returned status {response.status}")
                    return None, response.status

        except aiohttp.ClientError as e:
            logger.error(f"Network error in MusicBrainz API request: {e}")
            return None, 500
        except Exception as e:
            logger.error(f"Unexpected error in MusicBrainz API request: {e}")
            return None, 500
