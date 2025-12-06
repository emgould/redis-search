"""
YouTube Auth Service - Base service with authentication utilities.
Provides foundation for YouTube Data API operations.
"""

import os

from firebase_functions.params import SecretParam
from googleapiclient.discovery import build

from utils.get_logger import get_logger

logger = get_logger(__name__)

# YouTube API Key secret parameter
YOUTUBE_API_KEY = SecretParam("YOUTUBE_API_KEY")


class Auth:
    """
    Base YouTube service with authentication utilities.
    Provides foundation for YouTube Data API operations.
    """

    _youtube_api_key: str | None = None
    _youtube: build = None

    def __init__(self):
        """Initialize YouTube auth service."""
        self._youtube_api_key = None
        self._youtube = None

    @property
    def youtube_api_key(self) -> str | None:
        """Lazy-load YouTube API key from Firebase secrets."""
        if self._youtube_api_key is None:
            try:
                self._youtube_api_key = YOUTUBE_API_KEY.value
                logger.info("Loaded YouTube API key via SecretParam")
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                self._youtube_api_key = os.getenv("YOUTUBE_API_KEY")
                if self._youtube_api_key:
                    logger.info("Loaded YouTube API key via env var")
                else:
                    logger.error("YOUTUBE_API_KEY not available in SecretParam or environment")
                    raise ValueError("YOUTUBE_API_KEY not available in SecretParam or environment")
        return self._youtube_api_key

    @property
    def youtube(self) -> build:
        """Lazy-load YouTube client."""
        if self._youtube is None:
            self._youtube = build("youtube", "v3", developerKey=self.youtube_api_key)
        return self._youtube
