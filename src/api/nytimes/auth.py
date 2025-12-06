"""
NYTimes Auth Service - Centralized NYTimes authentication and API key management.
Provides NYTimes API key for use by other services (core, wrappers, handlers).
"""

import os

from firebase_functions.params import SecretParam

from utils.get_logger import get_logger

logger = get_logger(__name__)

# NYTimes API key secret parameter
NYTIMES_API_KEY = SecretParam("NYTIMES_API_KEY")


class NYTimesAuth:
    """
    Centralized NYTimes authentication service.
    Handles API key management from Firebase secrets or environment variables.
    """

    _nytimes_api_key: str | None = None

    def __init__(self):
        """Initialize NYTimes auth service."""
        self._nytimes_api_key = None

    @property
    def nytimes_api_key(self) -> str | None:
        """Lazy-load NYTimes API key from environment variables or Firebase secrets.

        Prioritizes environment variables (for tests/local dev) over SecretParam (for production).
        """
        if self._nytimes_api_key is None:
            # Check environment variable first (for tests/local dev)
            env_key = os.getenv("NYTIMES_API_KEY")
            if env_key:
                self._nytimes_api_key = env_key
                logger.info("Loaded NYTimes API key via env var")
            else:
                # Fall back to SecretParam (for production)
                try:
                    secret_value = NYTIMES_API_KEY.value
                    self._nytimes_api_key = secret_value
                    if self._nytimes_api_key:
                        logger.info("Loaded NYTimes API key via SecretParam")
                    else:
                        logger.error("NYTIMES_API_KEY not available in SecretParam or environment")
                except Exception as e:
                    logger.warning(f"SecretParam access failed: {e}")
                    logger.error("NYTIMES_API_KEY not available in SecretParam or environment")
        return self._nytimes_api_key

    def get_api_key_status(self) -> dict:
        """
        Get status information about the NYTimes API key.
        Useful for debugging and testing.

        Returns:
            Dictionary with API key status information:
            - has_key: bool - Whether an API key is available
            - key_length: int | None - Length of the API key (if available)
            - key_prefix: str | None - First 4 characters of key (if available)
        """
        if not self._nytimes_api_key:
            return {
                "has_key": False,
                "key_length": None,
                "key_prefix": None,
            }

        return {
            "has_key": True,
            "key_length": len(self._nytimes_api_key),
            "key_prefix": self._nytimes_api_key[:4] if len(self._nytimes_api_key) >= 4 else None,
        }

    def get_request_params(self, additional_params: dict | None = None) -> dict:
        """
        Get request parameters with API key included.

        Args:
            additional_params: Optional additional parameters to include

        Returns:
            Dictionary with API key and any additional parameters
        """
        params = additional_params or {}
        api_key = self.nytimes_api_key
        if api_key:
            params["api-key"] = api_key
        return params


# Singleton instance for use across the application
nytimes_auth = NYTimesAuth()
