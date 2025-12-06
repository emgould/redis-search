"""
Google Books Auth Service - Base service with authentication utilities.
Provides foundation for Google Books API operations.
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# Google Books API Key secret parameter
GOOGLE_BOOKS_API_KEY = SecretParam("GOOGLE_BOOK_API_KEY")


class Auth:
    """
    Base Google Books service with authentication utilities.
    Provides foundation for Google Books API operations.
    """

    _google_books_api_key: str | None = None
    base_url: str | None = None

    def __init__(self):
        self.base_url = "https://www.googleapis.com/books/v1"
        # Initialize private attribute for lazy-loading property
        self._google_books_api_key = None

    @property
    def google_books_api_key(self) -> str | None:
        """Lazy-load Google Books API key from Firebase secrets."""
        if self._google_books_api_key is None:
            try:
                self._google_books_api_key = GOOGLE_BOOKS_API_KEY.value
                logger.info("Loaded Google Books API key via SecretParam")
            except Exception as e:
                logger.warning(
                    f"SecretParam access failed: {e}, falling back to environment variable"
                )
                self._google_books_api_key = os.getenv("GOOGLE_BOOK_API_KEY")
                if self._google_books_api_key:
                    logger.info("Loaded Google Books API key via env var")
                else:
                    logger.warning(
                        "GOOGLE_BOOK_API_KEY not available in SecretParam or environment - "
                        "some operations may be rate limited"
                    )
        return self._google_books_api_key


google_books_auth = Auth()
