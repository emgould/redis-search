"""
Streaming Availability Auth Service - Centralized authentication and API key management.
Provides RapidAPI key for use by other services (core, wrappers, handlers).
"""

import os

from firebase_functions.params import SecretParam

from api.streaming_availability.rapidapi_key_pool import (
    pick_rapidapi_key as select_pooled_rapidapi_key,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)

# RapidAPI key secret parameter for Streaming Availability API
RAPIDAPI_KEY = SecretParam("RAPIDAPI_KEY")


def parse_rapidapi_key_pool(raw: str | None) -> list[str]:
    """Split a comma-separated RapidAPI key pool into non-empty keys."""
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


class StreamingAvailabilityAuth:
    """
    Centralized Streaming Availability authentication service.
    Handles API key management from Firebase secrets or environment variables.
    """

    def __init__(self) -> None:
        """Initialize Streaming Availability auth service."""
        self._pool_loaded = False
        self._key_pool: list[str] = []

    def _load_key_pool(self) -> None:
        if self._pool_loaded:
            return

        raw: str | None = None
        try:
            raw = RAPIDAPI_KEY.value
            logger.info("Loaded RapidAPI key material via SecretParam")
        except Exception as e:
            logger.warning(f"SecretParam access failed: {e}, falling back to environment variable")
            raw = os.getenv("RAPIDAPI_KEY")
            if raw:
                logger.info("Loaded RapidAPI key material via env var")

        self._key_pool = parse_rapidapi_key_pool(raw)
        self._pool_loaded = True

        if not self._key_pool:
            logger.error("RAPIDAPI_KEY not available in SecretParam or environment")
        elif len(self._key_pool) > 1:
            logger.info("RapidAPI key pool configured with %s keys", len(self._key_pool))
        else:
            logger.info("RapidAPI single key configured")

    def has_rapidapi_keys(self) -> bool:
        """Return True when at least one RapidAPI key is configured."""
        self._load_key_pool()
        return bool(self._key_pool)

    def configured_rapidapi_keys(self) -> list[str]:
        """Return the configured pool, including keys currently in cooldown."""
        self._load_key_pool()
        return list(self._key_pool)

    def pick_rapidapi_key(self) -> str | None:
        """Return a random configured key that is not in monthly cooldown."""
        return select_pooled_rapidapi_key(self.configured_rapidapi_keys())

    @property
    def rapidapi_key(self) -> str | None:
        """Random key from the pool (same as pick_rapidapi_key)."""
        return self.pick_rapidapi_key()


# Singleton instance for use across the application
streaming_availability_auth = StreamingAvailabilityAuth()
