"""
Podcast Core Service - Base service with core utilities.
Provides foundation for search and discovery operations.
"""

import base64
import os

from api.podcast.podcastindex import (
    PODCASTINDEX_API_KEY,
    PODCASTINDEX_API_SECRET,
    PodcastIndexClient,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)


class Auth:
    """
    Base Podcast service with core utilities.
    Provides foundation for search and discovery operations.
    """

    api_key: str | None = None
    api_secret: str | None = None
    base_url: str | None = None

    def __init__(self):
        self.api_key = None
        self.api_secret = None
        self.base_url = "https://api.podcastindex.org/api/1.0"
        # Initialize private attributes for lazy-loading properties
        self._podcast_api_key = None
        self._podcast_api_secret = None

    @property
    def podcast_api_key(self):
        """Lazy-load PodcastIndex API key from Firebase secrets."""
        if self._podcast_api_key is None:
            try:
                self._podcast_api_key = PODCASTINDEX_API_KEY.value
                logger.info(f"Loaded API key via SecretParam: {self._podcast_api_key}")
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                self._podcast_api_key = os.getenv("PODCASTINDEX_API_KEY")
                if self._podcast_api_key:
                    logger.info(f"Loaded API key via env var: {self._podcast_api_key}")
                else:
                    logger.error("PODCASTINDEX_API_KEY not available in SecretParam or environment")
        return self._podcast_api_key

    @property
    def podcast_api_secret(self):
        """Lazy-load PodcastIndex API secret from Firebase secrets."""
        if self._podcast_api_secret is None:
            try:
                self._podcast_api_secret = PODCASTINDEX_API_SECRET.value
                logger.info(
                    f"Loaded API secret via SecretParam, length: {len(self._podcast_api_secret) if self._podcast_api_secret else 0}"
                )
                # Check if we got a truncated secret (Firebase emulator issue with special chars)
                if self._podcast_api_secret and len(self._podcast_api_secret) < 30:
                    logger.error(
                        "API secret appears truncated, falling back to environment variable"
                    )
                    # Try base64 encoded version first (for Firebase emulator compatibility)
                    base64_secret = os.getenv("PODCASTINDEX_API_SECRET_BASE64")
                    if base64_secret:
                        try:
                            self._podcast_api_secret = (
                                base64.b64decode(base64_secret).decode("utf-8").strip()
                            )
                            logger.info(
                                f"Loaded API secret via base64 env var, length: {len(self._podcast_api_secret)}"
                            )
                        except Exception as decode_e:
                            logger.warning(f"Base64 decoding failed: {decode_e}")
                            self._podcast_api_secret = None

                    # Fallback to regular env var
                    if not self._podcast_api_secret:
                        self._podcast_api_secret = os.getenv("PODCASTINDEX_API_SECRET")
                        # Handle escaped characters (e.g., \# becomes #)
                        if self._podcast_api_secret:
                            self._podcast_api_secret = self._podcast_api_secret.replace("\\#", "#")
                        logger.info(
                            f"Loaded API secret via regular env var, length: {len(self._podcast_api_secret) if self._podcast_api_secret else 0}"
                        )
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                # Try base64 encoded version first (for Firebase emulator compatibility)
                base64_secret = os.getenv("PODCASTINDEX_API_SECRET_BASE64")
                if base64_secret:
                    try:
                        self._podcast_api_secret = (
                            base64.b64decode(base64_secret).decode("utf-8").strip()
                        )
                        logger.info(
                            f"Loaded API secret via base64 env var, length: {len(self._podcast_api_secret)}"
                        )
                    except Exception as decode_e:
                        logger.warning(f"Base64 decoding failed: {decode_e}")
                        self._podcast_api_secret = None

                # Fallback to regular env var
                if not self._podcast_api_secret:
                    self._podcast_api_secret = os.getenv("PODCASTINDEX_API_SECRET")
                    # Handle escaped characters (e.g., \# becomes #)
                    if self._podcast_api_secret:
                        self._podcast_api_secret = self._podcast_api_secret.replace("\\#", "#")
                    logger.info(
                        f"Loaded API secret via regular env var, length: {len(self._podcast_api_secret) if self._podcast_api_secret else 0}"
                    )

            if not self._podcast_api_secret:
                logger.error("PODCASTINDEX_API_SECRET not available in SecretParam or environment")
        return self._podcast_api_secret

    async def get_client(self) -> PodcastIndexClient:
        """
        Get PodcastIndex client instance.

        Returns:
            PodcastIndexClient configured with API credentials

        Raises:
            ValueError: If API key or secret is not available
        """
        # Access properties to trigger lazy loading
        api_key = self.podcast_api_key
        api_secret = self.podcast_api_secret

        if not api_key:
            raise ValueError("PodcastIndex API key is not available")
        if not api_secret:
            raise ValueError("PodcastIndex API secret is not available")

        return PodcastIndexClient(
            api_key=api_key, api_secret=api_secret, user_agent="MediaCircle/1.0"
        )
