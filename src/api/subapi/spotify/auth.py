"""
Spotify Auth Service - Centralized Spotify authentication and token management.
Provides Spotify credentials and token generation for use by other services
(LastFM, Podcast, etc.).
"""

import asyncio
import base64
import logging
import os
import time

import aiohttp
from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__, level=logging.WARNING)

# Spotify API credentials secret parameters
SPOTIFY_CLIENT_ID = SecretParam("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = SecretParam("SPOTIFY_CLIENT_SECRET")


class SpotifyAuth:
    """
    Centralized Spotify authentication service.
    Handles client credentials flow and token caching.
    """

    _spotify_token: str | None = None
    _spotify_token_expires_at: float | None = None

    def __init__(self):
        """Initialize Spotify auth service."""
        self._spotify_token = None
        self._spotify_token_expires_at = None

    @property
    def spotify_client_id(self) -> str | None:
        """Lazy-load Spotify client ID from Firebase secrets."""
        try:
            return SPOTIFY_CLIENT_ID.value or os.getenv("SPOTIFY_CLIENT_ID")
        except Exception:
            logger.error("SPOTIFY_CLIENT_ID not available in SecretParam or environment")
            return None

    @property
    def spotify_client_secret(self) -> str | None:
        """Lazy-load Spotify client secret from Firebase secrets."""
        try:
            return SPOTIFY_CLIENT_SECRET.value or os.getenv("SPOTIFY_CLIENT_SECRET")
        except Exception:
            logger.error("SPOTIFY_CLIENT_SECRET not available in SecretParam or environment")
            return None

    def _is_spotify_token_expired(self) -> bool:
        """
        Check if the cached Spotify token is expired or will expire soon.

        Returns:
            True if token is expired or will expire within 5 minutes, False otherwise
        """
        if not self._spotify_token or not self._spotify_token_expires_at:
            return True

        # Check if token expires within 5 minutes (300 seconds) to refresh proactively
        current_time = time.time()
        return current_time >= (self._spotify_token_expires_at - 300)

    def get_spotify_token_status(self) -> dict:
        """
        Get status information about the cached Spotify token.
        Useful for debugging and testing token expiration.

        Returns:
            Dictionary with token status information:
            - has_token: bool - Whether a token is cached
            - is_expired: bool - Whether token is expired or expiring soon
            - expires_at: float | None - Unix timestamp when token expires
            - expires_in: int | None - Seconds until expiration (negative if expired)
            - expires_at_readable: str | None - Human-readable expiration time
        """
        if not self._spotify_token or not self._spotify_token_expires_at:
            return {
                "has_token": False,
                "is_expired": True,
                "expires_at": None,
                "expires_in": None,
                "expires_at_readable": None,
            }

        current_time = time.time()
        expires_in = int(self._spotify_token_expires_at - current_time)
        is_expired = self._is_spotify_token_expired()

        # Format readable expiration time
        expires_at_readable = None
        if self._spotify_token_expires_at:
            from datetime import datetime

            expires_at_readable = datetime.fromtimestamp(self._spotify_token_expires_at).isoformat()

        return {
            "has_token": True,
            "is_expired": is_expired,
            "expires_at": self._spotify_token_expires_at,
            "expires_in": expires_in,
            "expires_at_readable": expires_at_readable,
        }

    async def get_spotify_headers(self, session: aiohttp.ClientSession) -> dict | None:
        """Get Spotify headers with authorization token."""
        token = await self.get_spotify_token(session)
        if not token:
            return None
        return {"Authorization": f"Bearer {token}"}

    async def get_spotify_token(
        self, session: aiohttp.ClientSession, max_retries: int = 3
    ) -> str | None:
        """
        Get Spotify API access token using client credentials flow with retry logic.
        Tokens expire after 1 hour (3600 seconds). This method checks expiration and
        refreshes the token if needed.

        Args:
            session: aiohttp ClientSession for making requests
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            Spotify access token string, or None if authentication failed

        Raises:
            aiohttp.ClientError: If all retry attempts fail
        """
        # Return cached token if still valid
        if not self._is_spotify_token_expired():
            return self._spotify_token

        client_id = self.spotify_client_id
        client_secret = self.spotify_client_secret

        if not client_id or not client_secret:
            logger.error("Spotify credentials not available")
            return None

        # Base64 encode client_id:client_secret
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        token_url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}
        timeout = aiohttp.ClientTimeout(total=10)  # 10 second timeout

        # Retry logic for token requests
        last_error = None
        last_status = None
        for attempt in range(max_retries):
            try:
                async with session.post(
                    token_url, headers=headers, data=data, timeout=timeout
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self._spotify_token = data["access_token"]
                        expires_in = data.get("expires_in", 3600)  # Default to 1 hour
                        self._spotify_token_expires_at = time.time() + expires_in
                        logger.info(
                            f"Successfully obtained Spotify token (expires in {expires_in}s)"
                        )
                        return self._spotify_token
                    else:
                        error_text = await resp.text()
                        status = resp.status
                        last_status = status
                        # Only log retry attempts at DEBUG level to reduce log noise
                        # Final failure will be logged after all retries are exhausted
                        logger.debug(
                            f"Spotify token request failed (attempt {attempt + 1}/{max_retries}): "
                            f"Status {status}, Response: {error_text}"
                        )
                        last_error = f"HTTP {status}: {error_text}"

                        # For 503 (Service Unavailable), use longer backoff
                        # Server is overloaded, give it more time
                        if status == 503:
                            wait_time = 2 ** (attempt + 2)  # 4s, 8s, 16s instead of 1s, 2s, 4s
                        else:
                            wait_time = 2**attempt  # Standard exponential backoff

                        # Only wait if we have more retries left
                        if attempt < max_retries - 1:
                            await asyncio.sleep(wait_time)
            except TimeoutError:
                # Only log retry attempts at DEBUG level
                logger.debug(f"Spotify token request timeout (attempt {attempt + 1}/{max_retries})")
                last_error = "Request timeout"
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)
            except Exception as e:
                # Only log retry attempts at DEBUG level
                logger.debug(
                    f"Spotify token request exception (attempt {attempt + 1}/{max_retries}): {e}"
                )
                last_error = str(e)
                # Exponential backoff between retries
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)

        # Only log final failure after all retries are exhausted
        logger.error(
            f"Failed to obtain Spotify token after {max_retries} attempts: {last_error}"
            + (f" (Status: {last_status})" if last_status else "")
        )
        return None


# Singleton instance for use across the application
spotify_auth = SpotifyAuth()
