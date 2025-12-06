"""
Watchmode Auth Service - Centralized Watchmode authentication and API key management.
Provides Watchmode API key for use by other services (core, wrappers, handlers).
"""

import os

from firebase_functions.params import SecretParam

from api.tmdb.auth import TMDB_READ_TOKEN
from utils.get_logger import get_logger

logger = get_logger(__name__)

# Watchmode API key secret parameter
WATCHMODE_API_KEY = SecretParam("WATCHMODE_API_KEY")


class WatchmodeAuth:
    """
    Centralized Watchmode authentication service.
    Handles API key management from Firebase secrets or environment variables.
    """

    _watchmode_api_key: str | None = None
    _tmdb_read_token: str | None = None

    def __init__(self):
        """Initialize Watchmode auth service."""
        self._watchmode_api_key = None
        self._tmdb_read_token = None

    @property
    def watchmode_api_key(self) -> str | None:
        """Lazy-load Watchmode API key from Firebase secrets."""
        if self._watchmode_api_key is None:
            try:
                self._watchmode_api_key = WATCHMODE_API_KEY.value
                logger.info("Loaded Watchmode API key via SecretParam")
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                self._watchmode_api_key = os.getenv("WATCHMODE_API_KEY")
                if self._watchmode_api_key:
                    logger.info("Loaded Watchmode API key via env var")
                else:
                    logger.error("WATCHMODE_API_KEY not available in SecretParam or environment")
        return self._watchmode_api_key

    @property
    def tmdb_read_token(self) -> str | None:
        """Lazy-load TMDB read token from Firebase secrets."""
        if self._tmdb_read_token is None:
            try:
                self._tmdb_read_token = TMDB_READ_TOKEN.value
                logger.info("Loaded TMDB read token via SecretParam")
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                self._tmdb_read_token = os.getenv("TMDB_READ_TOKEN")
                if self._tmdb_read_token:
                    logger.info("Loaded TMDB read token via env var")
                else:
                    logger.error("TMDB_READ_TOKEN not available in SecretParam or environment")
        return self._tmdb_read_token


# Singleton instance for use across the application
watchmode_auth = WatchmodeAuth()
