"""
TMDB Auth Service - Base service with authentication utilities.
Provides foundation for TMDB API operations.
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# TMDB API token secret parameter
TMDB_READ_TOKEN = SecretParam("TMDB_READ_TOKEN")


class Auth:
    """
    Base TMDB service with authentication utilities.
    Provides foundation for TMDB API operations.
    """

    _tmdb_read_token: str | None = None
    base_url: str | None = None
    image_base_url: str | None = None

    def __init__(self):
        self.base_url = "https://api.themoviedb.org/3"
        self.image_base_url = "https://image.tmdb.org/t/p/"
        # Initialize private attribute for lazy-loading property
        self._tmdb_read_token = None

    @property
    def tmdb_read_token(self):
        """Lazy-load TMDB read token from Firebase secrets."""
        if self._tmdb_read_token is None:
            try:
                self._tmdb_read_token = TMDB_READ_TOKEN.value

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

    def auth_headers(self):
        """Return the authorization headers for TMDB API requests."""
        return {
            "Authorization": f"Bearer {self.tmdb_read_token}",
            "Content-Type": "application/json",
        }
