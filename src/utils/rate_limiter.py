"""
Rate Limiter Utility - Per-event-loop rate limiting per API.

For Firebase function instances, this provides a rate limiter per API configuration
per event loop. Each API (TMDB, Spotify, NYTimes, etc.) gets its own limiter with
its own rate limit configuration, and limiters are scoped to event loops to prevent
"attached to a different loop" errors.

Usage:
    from utils.rate_limiter import get_rate_limiter

    # Get a limiter for TMDB (per event loop)
    limiter = get_rate_limiter(max_rate=35, time_period=1)

    # Use it in async context
    async with limiter:
        # Make your API request
        pass
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from typing import Any

from aiolimiter import AsyncLimiter

from utils.get_logger import get_logger

logger = get_logger(__name__)

# Module-level lock for thread safety
_lock = threading.Lock()

# Store limiters per (max_rate, time_period, loop_id) combination
# Key: (max_rate, time_period, loop_id), Value: ResilientRateLimiter instance
# This ensures each event loop gets its own resilient limiter wrapper
_limiters: dict[tuple[int, float, int], ResilientRateLimiter] = {}

# Track limiter creation for monitoring
_limiter_creation_count = 0


class ResilientRateLimiter:
    """
    Wrapper around AsyncLimiter that gracefully handles cross-loop errors.

    If a RuntimeError occurs due to event loop mismatch, this wrapper
    automatically creates a new limiter for the current loop and retries.
    """

    def __init__(self, max_rate: int, time_period: float):
        self.max_rate = max_rate
        self.time_period = time_period
        self._limiter: AsyncLimiter | None = None
        self._loop_id: int | None = None
        self._token_lock = threading.Lock()
        self._active_tokens: weakref.WeakKeyDictionary[
            asyncio.Task[Any], tuple[AsyncLimiter, int]
        ] = weakref.WeakKeyDictionary()

    def _ensure_limiter(self) -> AsyncLimiter:
        """Ensure we have a limiter for the current event loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        current_loop_id = id(loop)

        # Create new limiter if we don't have one or if we're on a different loop
        if self._limiter is None or self._loop_id != current_loop_id:
            self._limiter = AsyncLimiter(self.max_rate, self.time_period)
            self._loop_id = current_loop_id
            logger.debug(
                f"Created rate limiter for loop {current_loop_id}: "
                f"{self.max_rate} requests per {self.time_period}s"
            )

        return self._limiter

    async def __aenter__(self) -> ResilientRateLimiter:
        """Acquire rate limit token with automatic retry on loop mismatch."""
        max_retries = 2
        last_error: RuntimeError | None = None

        for attempt in range(max_retries):
            try:
                limiter = self._ensure_limiter()
                await limiter.__aenter__()
                task = asyncio.current_task()
                if task is not None:
                    with self._token_lock:
                        self._active_tokens[task] = (
                            limiter,
                            self._loop_id or id(asyncio.get_running_loop()),
                        )
                return self
            except RuntimeError as e:
                last_error = e
                error_msg = str(e).lower()
                # Check if this is a cross-loop error
                if "loop" in error_msg or "future" in error_msg or "task" in error_msg:
                    logger.warning(
                        f"Rate limiter loop mismatch detected (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    # Force recreation of limiter on next attempt
                    self._limiter = None
                    self._loop_id = None
                    if attempt < max_retries - 1:
                        continue
                # Not a loop error, re-raise immediately
                raise

        # All retries exhausted - last_error is guaranteed to be set here
        assert last_error is not None, "last_error should be set after exhausting retries"
        logger.error(f"Rate limiter failed after {max_retries} attempts: {last_error}")
        raise last_error

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Release rate limit token."""
        task = asyncio.current_task()
        token: tuple[AsyncLimiter, int] | None = None
        if task is not None:
            with self._token_lock:
                token = self._active_tokens.pop(task, None)

        limiter_to_release = token[0] if token else self._limiter

        if limiter_to_release is not None:
            try:
                await limiter_to_release.__aexit__(exc_type, exc_val, exc_tb)
            except RuntimeError as e:
                # Log but don't fail on release errors
                logger.warning(f"Error releasing rate limiter: {e}")


def get_rate_limiter(max_rate: int, time_period: float = 1.0) -> ResilientRateLimiter:
    """
    Get or create a resilient rate limiter per API configuration per event loop.

    This returns a ResilientRateLimiter that automatically handles event loop
    mismatches by recreating the underlying AsyncLimiter on the current loop.

    Different APIs get different limiters:
    - TMDB: get_rate_limiter(35, 1) → one limiter per event loop for all TMDB requests
    - Spotify: get_rate_limiter(25, 1) → one limiter per event loop for all Spotify requests
    - NYTimes: get_rate_limiter(5, 60) → one limiter per event loop for all NYTimes requests

    NOTE: Limiters are per-event-loop. In typical Firebase Cloud Functions deployments,
    this means 1-2 limiters per API. Rate limits are set conservatively (e.g., 35/sec
    for TMDB's 40/sec limit) to provide headroom for multiple loops.

    Args:
        max_rate: Maximum number of requests allowed
        time_period: Time period in seconds (default: 1.0)

    Returns:
        ResilientRateLimiter instance that handles cross-loop errors gracefully

    Example:
        >>> limiter = get_rate_limiter(max_rate=35, time_period=1)
        >>> async with limiter:
        ...     # Make TMDB API request - automatically retries on loop errors
        ...     pass
    """
    global _limiter_creation_count

    # Get the current event loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

    # Cache key includes loop ID to ensure limiters are per-event-loop
    cache_key = (max_rate, time_period, id(loop))

    # Double-checked locking pattern for thread safety
    if cache_key not in _limiters:
        with _lock:
            if cache_key not in _limiters:
                # Create a resilient limiter for this specific event loop
                limiter = ResilientRateLimiter(max_rate, time_period)
                _limiters[cache_key] = limiter
                _limiter_creation_count += 1

                # Log limiter creation for monitoring
                logger.info(
                    f"Created rate limiter #{_limiter_creation_count} for loop {id(loop)}: "
                    f"{max_rate} requests per {time_period}s"
                )

    return _limiters[cache_key]
