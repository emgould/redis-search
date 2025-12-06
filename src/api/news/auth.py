"""
News Auth Service - Base service with authentication utilities.
Provides foundation for NewsAPI operations.
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# NewsAPI Key secret parameter
NEWS_API_KEY = SecretParam("NEWS_API_KEY")


class Auth:
    """
    Base News service with authentication utilities.
    Provides foundation for NewsAPI operations.
    """

    _news_api_key: str | None = None

    def __init__(self):
        # Initialize private attribute for lazy-loading property
        self._news_api_key = None

    @property
    def news_api_key(self):
        """Lazy-load News API key from Firebase secrets."""
        if self._news_api_key is None:
            try:
                self._news_api_key = NEWS_API_KEY.value or os.getenv("NEWS_API_KEY")
                logger.info(f"Loaded API key via SecretParam: {self._news_api_key}")
            except Exception:
                logger.error("NEWS_API_KEY not available in SecretParam or environment")
        return self._news_api_key
