"""
LastFM Auth Service - Base service with authentication utilities.
Provides foundation for Last.fm API operations.
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# LastFM API Key secret parameter
LASTFM_API_KEY = SecretParam("LASTFM_API_KEY")


class Auth:
    """
    Base LastFM service with authentication utilities.
    Provides foundation for Last.fm API operations.
    """

    _lastfm_api_key: str | None = None
    base_url: str | None = None

    def __init__(self):
        self.base_url = "https://ws.audioscrobbler.com/2.0/"
        # Initialize private attribute for lazy-loading property
        self._lastfm_api_key = None

    @property
    def lastfm_api_key(self):
        """Lazy-load Last.fm API key from Firebase secrets."""
        if self._lastfm_api_key is None:
            try:
                self._lastfm_api_key = LASTFM_API_KEY.value or os.getenv("LASTFM_API_KEY")
                logger.info(f"Loaded API key via SecretParam: {self._lastfm_api_key}")
            except Exception:
                logger.error("LASTFM_API_KEY not available in SecretParam or environment")
        return self._lastfm_api_key
