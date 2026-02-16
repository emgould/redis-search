"""
NewsAI Auth Service - Base service with authentication utilities.
Provides foundation for NewsAI (Event Registry) API operations.
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# NewsAI API Key secret parameter
NEWSAI_API_KEY = SecretParam("NEWS_AI_API_KEY")


class Auth:
    """
    Base NewsAI service with authentication utilities.
    Provides foundation for NewsAI (Event Registry) API operations.
    """

    _newsai_api_key: str | None = None

    def __init__(self):
        # Initialize private attribute for lazy-loading property
        self._newsai_api_key = None

    @property
    def newsai_api_key(self):
        """Lazy-load NewsAI API key from Firebase secrets."""
        if self._newsai_api_key is None:
            try:
                self._newsai_api_key = NEWSAI_API_KEY.value or os.getenv("NEWSAI_API_KEY")
                if self._newsai_api_key:
                    logger.info("Loaded NewsAI API key successfully")
                else:
                    logger.error("NEWSAI_API_KEY not available in SecretParam or environment")
            except Exception as e:
                logger.error(f"Error loading NEWSAI_API_KEY: {e}")
        return self._newsai_api_key
