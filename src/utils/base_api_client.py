"""
Base API Client - Shared request handling with caching, deduplication, and retry logic.
All API services should inherit from this or use its _core_async_request method.
"""

import asyncio
import json
import os
import random
import sys
from typing import Any, Protocol

import aiohttp

from utils.get_logger import get_logger
from utils.rate_limiter import get_rate_limiter

logger = get_logger(__name__)

# Check if we're in test environment
# Rate limiting should be DISABLED only for unit tests (mocked API calls)
# Rate limiting should be ENABLED for integration tests (real API calls)
_IS_TEST_ENV = os.getenv("ENVIRONMENT", "").lower() == "test"
_ENABLE_CACHE_FOR_TESTS = os.getenv("ENABLE_CACHE_FOR_TESTS", "").lower() == "1"
# Check if this is an integration test by looking for "integration" in pytest args
_IS_INTEGRATION_TEST = any(
    "integration" in arg.lower() for arg in sys.argv if "test" in arg.lower()
)
# Skip rate limiting ONLY for unit tests (test env + not integration test)
# Integration tests ALWAYS use real rate limiting since they make real API calls
_SKIP_RATE_LIMITING = _IS_TEST_ENV and not _IS_INTEGRATION_TEST


class RateLimiterProtocol(Protocol):
    """Protocol for rate limiters (both real and no-op)."""

    async def __aenter__(self) -> Any: ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...


class BaseAPIClient:
    """
    Base class for API clients with shared request handling.
    Provides caching, deduplication, rate limiting, and retry logic.
    """

    # Request deduplication: track pending requests to prevent duplicate concurrent calls
    # Class-level dictionary shared across all instances
    _pending_requests: dict[str, asyncio.Task] = {}
    _pending_lock: asyncio.Lock | None = None

    # Concurrency control: limit simultaneous in-flight requests per API per event loop
    # Key: (rate_limit_max, rate_limit_period, loop_id), Value: asyncio.Semaphore
    # This works WITH rate limiting to prevent bursts that exceed rolling window limits
    # Semaphores are per-event-loop to avoid "bound to different event loop" errors
    _concurrency_semaphores: dict[tuple[int, float, int], asyncio.Semaphore] = {}
    _semaphore_lock: asyncio.Lock | None = None

    @classmethod
    def _get_pending_lock(cls) -> asyncio.Lock:
        """Get or create the pending requests lock."""
        if cls._pending_lock is None:
            try:
                loop = asyncio.get_event_loop()
                cls._pending_lock = asyncio.Lock()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                cls._pending_lock = asyncio.Lock()
        return cls._pending_lock

    @classmethod
    def _get_semaphore_lock(cls) -> asyncio.Lock:
        """Get or create the semaphore lock."""
        if cls._semaphore_lock is None:
            try:
                loop = asyncio.get_event_loop()
                cls._semaphore_lock = asyncio.Lock()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                cls._semaphore_lock = asyncio.Lock()
        return cls._semaphore_lock

    @classmethod
    def _get_concurrency_semaphore(
        cls, rate_limit_max: int, rate_limit_period: float
    ) -> asyncio.Semaphore:
        """
        Get or create a concurrency semaphore for the given rate limit config.

        The semaphore limits the number of SIMULTANEOUS in-flight requests,
        working together with the rate limiter to prevent bursts.

        Semaphore limit is set to rate_limit_max to ensure we never have more
        concurrent requests than our rate limit allows per period.

        IMPORTANT: Semaphores are event-loop specific. We create one per (config, loop) pair.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, try to get the current loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # No event loop at all, create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        # Cache key includes loop ID to ensure semaphores are per-event-loop
        cache_key = (rate_limit_max, rate_limit_period, id(loop))

        # Create semaphore if it doesn't exist for this loop
        if cache_key not in cls._concurrency_semaphores:
            # Create semaphore with limit = rate_limit_max
            # This ensures max concurrent requests = max requests per period
            cls._concurrency_semaphores[cache_key] = asyncio.Semaphore(rate_limit_max)

        return cls._concurrency_semaphores[cache_key]

    async def _core_async_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        rate_limit_max: int = 10,
        rate_limit_period: float = 1.0,
        return_exceptions: bool = False,
        return_status_code: bool = False,
    ) -> Any:
        # Type annotation: when return_status_code=True, returns tuple, otherwise dict | list | None
        """
        Core async HTTP GET request with deduplication, rate limiting, and retry logic.

        This method handles:
        - Request deduplication: prevents duplicate concurrent requests
        - Rate limiting: coordinates requests to stay within limits
        - Retry logic: exponential backoff with jitter for rate limits
        - Error handling: graceful handling of network and API errors

        Args:
            url: Full URL to request
            params: Optional query parameters
            headers: Optional HTTP headers
            timeout: Request timeout in seconds (default: 60)
            max_retries: Maximum retry attempts (default: 3)
            rate_limit_max: Maximum requests per period (default: 10)
            rate_limit_period: Time period in seconds (default: 1.0)
            return_exceptions: If True, return exceptions instead of raising (default: False)

        Returns:
            JSON response (dict, list, or other JSON type) or None on error.
            If return_status_code=True, returns tuple (response | None, status_code)
        """
        # Create unique key for request deduplication
        params_str = json.dumps(params, sort_keys=True) if params else "{}"
        headers_str = json.dumps(headers, sort_keys=True) if headers else "{}"
        request_key = f"GET|{url}|{params_str}|{headers_str}"

        # Get the lock
        lock = self._get_pending_lock()

        # Check if there's already a pending request for this key
        async with lock:
            if request_key in self._pending_requests:
                pending_task = self._pending_requests[request_key]
            else:
                pending_task = None

        if pending_task is not None:
            try:
                # Wait for the pending request to complete
                result = await pending_task
                # Handle tuple return (when return_status_code=True)
                if isinstance(result, tuple):
                    return result
                if result is not None:
                    return dict(result)
                return None, 500
            except Exception:
                # If pending request failed, remove it and continue to make new request
                async with lock:
                    self._pending_requests.pop(request_key, None)

        # Create the actual request task
        async def _fetch_data() -> Any:
            try:
                request_timeout = aiohttp.ClientTimeout(total=timeout)

                # Get rate limiter AND concurrency semaphore - both shared across all requests to the same API
                # Skip rate limiting only for unit tests (test env + cache disabled)
                # Integration tests (test env + cache enabled) should use real rate limiting
                if _SKIP_RATE_LIMITING:
                    # Use a no-op context manager for unit tests to avoid rate limiting delays
                    # Unit tests are mocked and don't make real API calls, so rate limiting isn't needed
                    class NoOpRateLimiter:
                        _value = 999  # Fake value for logging compatibility

                        async def __aenter__(self) -> Any:
                            return self

                        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
                            return None

                    rate_limiter: Any = NoOpRateLimiter()
                    concurrency_semaphore: Any = NoOpRateLimiter()
                else:
                    # Use real rate limiting AND concurrency control for production and integration tests
                    # Rate limiter: controls requests per time period (e.g., 5 per second)
                    # Semaphore: limits simultaneous in-flight requests (prevents bursts)
                    rate_limiter = get_rate_limiter(rate_limit_max, rate_limit_period)
                    concurrency_semaphore = self._get_concurrency_semaphore(
                        rate_limit_max, rate_limit_period
                    )

                # Track 429 retries separately - we'll retry 429s more aggressively
                rate_limit_retries = 0
                max_rate_limit_retries = 10  # Maximum 429 retries before giving up
                attempt = 0
                total_requests = 0
                max_total_requests = max_retries + max_rate_limit_retries + 5  # Safety limit

                while attempt < max_retries:
                    # Safety check to prevent infinite loops
                    total_requests += 1
                    if total_requests > max_total_requests:
                        logger.error(
                            f"Maximum total requests ({max_total_requests}) exceeded for {url}. "
                            f"Stopping to prevent infinite loop."
                        )
                        if return_status_code:
                            return None, 500
                        return None
                    try:
                        # CONCURRENCY CONTROL: Limit simultaneous in-flight requests
                        # This prevents bursts that exceed rolling window limits
                        # The semaphore ensures max concurrent requests = rate_limit_max
                        async with concurrency_semaphore:
                            # RATE LIMITING: Control requests per time period
                            # AsyncLimiter will queue requests - only rate_limit_max requests
                            # will proceed per rate_limit_period seconds
                            # This must happen INSIDE the retry loop so each retry attempt consumes a token
                            async with (  # noqa: SIM117
                                rate_limiter,
                                aiohttp.ClientSession() as session,
                                session.get(
                                    url,
                                    headers=headers,
                                    params=params,
                                    timeout=request_timeout,
                                ) as response,
                            ):
                                status = response.status

                                if status == 429:
                                    # Rate limit hit - wait and retry without incrementing attempt
                                    # Note: We've already exited the rate_limiter context, so the next
                                    # retry will acquire a new token (which is correct for rate limiting)
                                    rate_limit_retries += 1
                                    if rate_limit_retries > max_rate_limit_retries:
                                        logger.error(
                                            f"Rate limit retries exhausted ({max_rate_limit_retries}) for {url}"
                                        )
                                        # Ensure response body is consumed to properly close connection
                                        try:
                                            await response.read()
                                        except Exception:
                                            pass  # Ignore errors when consuming response body
                                        if return_status_code:
                                            return None, 429
                                        return None

                                    retry_after_header = response.headers.get("Retry-After", 2)
                                    retry_after = (
                                        int(retry_after_header)
                                        if isinstance(retry_after_header, (int, str))
                                        else 2
                                    )
                                    # Add jitter to prevent simultaneous retries
                                    jitter = random.uniform(0.1, 0.5)
                                    wait_time = retry_after + jitter
                                    if rate_limit_retries >= max_rate_limit_retries - 1:
                                        logger.warning(
                                            f"Rate limit hit for {url} (retry {rate_limit_retries}/{max_rate_limit_retries}). "
                                            f"Waiting {wait_time:.2f}s before retry..."
                                        )
                                    # Ensure response body is consumed to properly close connection
                                    try:
                                        await response.read()
                                    except Exception:
                                        pass  # Ignore errors when consuming response body
                                    await asyncio.sleep(wait_time)
                                    # Don't increment attempt - retry immediately (will acquire new rate limiter token)
                                    continue

                                if status != 200:
                                    # 404s are expected (resource doesn't exist) - log as DEBUG
                                    # Other errors are unexpected - log as WARNING
                                    if status == 404:
                                        logger.debug(
                                            f"API returned status {response.status} for {url} (resource not found)"
                                        )
                                    else:
                                        logger.warning(
                                            f"API returned status {response.status} for {url}"
                                        )
                                    # Don't retry 4xx errors (client errors) except 429 (rate limit)
                                    # These are not transient and retrying won't help
                                    if 400 <= status < 500 and status != 429:
                                        # Ensure response body is consumed to properly close connection
                                        try:
                                            await response.read()
                                        except Exception:
                                            pass  # Ignore errors when consuming response body
                                        if return_status_code:
                                            return None, status
                                        return None
                                    # For 5xx errors and other non-200 responses, retry with exponential backoff
                                    attempt += 1
                                    if attempt < max_retries:
                                        # Ensure response body is consumed to properly close connection
                                        try:
                                            await response.read()
                                        except Exception:
                                            pass  # Ignore errors when consuming response body
                                        backoff_time = 2 ** (attempt - 1)
                                        await asyncio.sleep(backoff_time)
                                        continue
                                    # Return status code if requested (for APIs that need it)
                                    # Ensure response body is consumed to properly close connection
                                    try:
                                        await response.read()
                                    except Exception:
                                        pass  # Ignore errors when consuming response body
                                    if return_status_code:
                                        return None, status
                                    return None

                                # Read and parse JSON response
                                clean_result = await response.json()

                            # Return outside the context manager to ensure no async references
                            if return_status_code:
                                return clean_result, status
                            return clean_result

                    except RuntimeError as e:
                        # Handle case where event loop is shutting down
                        error_msg = str(e)
                        if "cannot schedule" in error_msg or "Event loop is closed" in error_msg:
                            logger.warning(
                                f"Event loop is shutting down, aborting request to {url}: {e}"
                            )
                            if return_exceptions:
                                if return_status_code:
                                    return None, 500
                                return None
                            # Don't raise - this is expected during shutdown
                            if return_status_code:
                                return None, 500
                            return None
                        # Re-raise if it's a different RuntimeError
                        raise
                    except asyncio.CancelledError:
                        # Don't retry on cancellation - propagate it immediately
                        raise
                    except (TimeoutError, aiohttp.ClientError) as e:
                        attempt += 1
                        is_last_attempt = attempt >= max_retries

                        if is_last_attempt:
                            logger.error(
                                f"Error making request to {url} after {max_retries} attempts: {e}"
                            )
                            if return_exceptions:
                                if return_status_code:
                                    return None, 500
                                return None
                            raise

                        # Exponential backoff: 2^(attempt-1) seconds (1s, 2s, 4s, ...)
                        backoff_time = 2 ** (attempt - 1)
                        # Include exception type for better debugging when message is empty
                        # error_detail = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
                        # logger.warning(
                        #     f"Request to {url} failed (attempt {attempt}/{max_retries}): {error_detail}. "
                        #     f"Retrying in {backoff_time}s..."
                        # )
                        await asyncio.sleep(backoff_time)

                    except Exception as e:
                        logger.error(f"Unexpected error making request to {url}: {e}")
                        if return_exceptions:
                            if return_status_code:
                                return None, 500
                            return None
                        raise

                # All retries exhausted
                if return_status_code:
                    return None, 500
                return None
            finally:
                # Always remove from pending requests when done
                async with lock:
                    self._pending_requests.pop(request_key, None)

        # Create task and store it for deduplication (with lock)
        async with lock:
            # Double-check pattern: another request might have started while we were waiting
            if request_key in self._pending_requests:
                pending_task = self._pending_requests[request_key]
            else:
                task = asyncio.create_task(_fetch_data())
                self._pending_requests[request_key] = task
                pending_task = task

        return await pending_task

    async def _core_async_mutation_request(
        self,
        method: str,
        url: str,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_max: int = 10,
        rate_limit_period: float = 1.0,
        return_exceptions: bool = False,
        return_status_code: bool = True,
    ) -> tuple[dict[str, Any] | None, int]:
        """
        Core async HTTP mutation request (POST, PUT, DELETE) with rate limiting and retry logic.

        This method handles:
        - Rate limiting: coordinates requests to stay within limits
        - Retry logic: exponential backoff with jitter for rate limits
        - Error handling: graceful handling of network and API errors

        Args:
            method: HTTP method ("POST", "PUT", "DELETE")
            url: Full URL to request
            json_body: Optional JSON body to send (for POST/PUT requests)
            headers: Optional HTTP headers (Content-Type: application/json is added automatically for POST/PUT)
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum retry attempts (default: 3)
            rate_limit_max: Maximum requests per period (default: 10)
            rate_limit_period: Time period in seconds (default: 1.0)
            return_exceptions: If True, return exceptions instead of raising (default: False)
            return_status_code: Always returns tuple (response | None, status_code) (default: True)

        Returns:
            tuple: (response_data | None, status_code)
        """
        method = method.upper()
        if method not in ("POST", "PUT", "DELETE"):
            raise ValueError(f"Unsupported HTTP method: {method}. Use POST, PUT, or DELETE.")

        try:
            request_timeout = aiohttp.ClientTimeout(total=timeout)

            # Ensure Content-Type header is set for POST/PUT requests with body
            if method in ("POST", "PUT") and json_body is not None:
                if headers is None:
                    headers = {}
                if "Content-Type" not in headers:
                    headers["Content-Type"] = "application/json"

            # Get rate limiter AND concurrency semaphore
            if _SKIP_RATE_LIMITING:
                # Use no-op for unit tests
                class NoOpRateLimiter:
                    async def __aenter__(self) -> Any:
                        return self

                    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
                        return None

                rate_limiter: Any = NoOpRateLimiter()
                concurrency_semaphore: Any = NoOpRateLimiter()
            else:
                rate_limiter = get_rate_limiter(rate_limit_max, rate_limit_period)
                concurrency_semaphore = self._get_concurrency_semaphore(
                    rate_limit_max, rate_limit_period
                )

            # Track 429 retries separately
            rate_limit_retries = 0
            max_rate_limit_retries = 10
            attempt = 0

            while attempt < max_retries:
                try:
                    async with (  # noqa: SIM117
                        concurrency_semaphore,
                        rate_limiter,
                        aiohttp.ClientSession() as session,
                    ):
                        # Select the appropriate session method based on HTTP method
                        if method == "POST":
                            request_ctx = session.post(
                                url, json=json_body, timeout=request_timeout, headers=headers
                            )
                        elif method == "PUT":
                            request_ctx = session.put(
                                url, json=json_body, timeout=request_timeout, headers=headers
                            )
                        else:  # DELETE
                            request_ctx = session.delete(
                                url, timeout=request_timeout, headers=headers
                            )

                        async with request_ctx as response:
                            status = response.status

                            if status == 429:
                                # Rate limit hit - wait and retry
                                rate_limit_retries += 1
                                if rate_limit_retries > max_rate_limit_retries:
                                    logger.error(
                                        f"Rate limit retries exhausted ({max_rate_limit_retries}) for {url}"
                                    )
                                    try:
                                        await response.read()
                                    except Exception:
                                        pass
                                    return None, 429

                                retry_after_header = response.headers.get("Retry-After", 2)
                                retry_after = (
                                    int(retry_after_header)
                                    if isinstance(retry_after_header, (int, str))
                                    else 2
                                )
                                jitter = random.uniform(0.1, 0.5)
                                wait_time = retry_after + jitter
                                logger.warning(
                                    f"Rate limit hit for {url} (retry {rate_limit_retries}/{max_rate_limit_retries}). "
                                    f"Waiting {wait_time:.2f}s before retry..."
                                )
                                try:
                                    await response.read()
                                except Exception:
                                    pass
                                await asyncio.sleep(wait_time)
                                continue

                            if status != 200:
                                # Don't retry 4xx errors (client errors) except 429
                                if 400 <= status < 500 and status != 429:
                                    # Parse error response body to include error details
                                    try:
                                        error_data = await response.json()
                                        return error_data, status
                                    except Exception:
                                        # If JSON parsing fails, just return None
                                        try:
                                            await response.read()
                                        except Exception:
                                            pass
                                        return None, status

                                # For 5xx errors, retry with exponential backoff
                                attempt += 1
                                if attempt < max_retries:
                                    try:
                                        await response.read()
                                    except Exception:
                                        pass
                                    backoff_time = 2 ** (attempt - 1)
                                    await asyncio.sleep(backoff_time)
                                    continue
                                try:
                                    await response.read()
                                except Exception:
                                    pass
                                return None, status

                            # Success - try to read and parse JSON response
                            # Some endpoints (especially DELETE) may return empty bodies
                            try:
                                data = await response.json()
                                return data, status
                            except Exception:
                                # Empty response body is acceptable
                                return None, status

                except asyncio.CancelledError:
                    # Don't retry on cancellation - propagate it immediately
                    raise
                except (TimeoutError, aiohttp.ClientError) as e:
                    attempt += 1
                    is_last_attempt = attempt >= max_retries

                    if is_last_attempt:
                        logger.error(
                            f"Error making {method} request to {url} after {max_retries} attempts: {e}"
                        )
                        if return_exceptions:
                            return None, 500
                        raise

                    backoff_time = 2 ** (attempt - 1)
                    logger.warning(
                        f"{method} request to {url} failed (attempt {attempt}/{max_retries}): {e}. "
                        f"Retrying in {backoff_time}s..."
                    )
                    await asyncio.sleep(backoff_time)

                except Exception as e:
                    logger.error(f"Unexpected error making {method} request to {url}: {e}")
                    if return_exceptions:
                        return None, 500
                    raise

            # All retries exhausted
            return None, 500

        except Exception as e:
            logger.error(f"Fatal error in _core_async_mutation_request ({method}): {e}")
            return None, 500

    # Convenience methods that wrap _core_async_mutation_request
    async def _core_async_post_request(
        self,
        url: str,
        json_body: dict[str, Any],
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_max: int = 10,
        rate_limit_period: float = 1.0,
        return_exceptions: bool = False,
        return_status_code: bool = True,
    ) -> tuple[dict[str, Any] | None, int]:
        """Convenience wrapper for POST requests."""
        return await self._core_async_mutation_request(
            method="POST",
            url=url,
            json_body=json_body,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_max=rate_limit_max,
            rate_limit_period=rate_limit_period,
            return_exceptions=return_exceptions,
            return_status_code=return_status_code,
        )

    async def _core_async_put_request(
        self,
        url: str,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_max: int = 10,
        rate_limit_period: float = 1.0,
        return_exceptions: bool = False,
        return_status_code: bool = True,
    ) -> tuple[dict[str, Any] | None, int]:
        """Convenience wrapper for PUT requests."""
        return await self._core_async_mutation_request(
            method="PUT",
            url=url,
            json_body=json_body,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_max=rate_limit_max,
            rate_limit_period=rate_limit_period,
            return_exceptions=return_exceptions,
            return_status_code=return_status_code,
        )

    async def _core_async_delete_request(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_max: int = 10,
        rate_limit_period: float = 1.0,
        return_exceptions: bool = False,
        return_status_code: bool = True,
    ) -> tuple[dict[str, Any] | None, int]:
        """Convenience wrapper for DELETE requests."""
        return await self._core_async_mutation_request(
            method="DELETE",
            url=url,
            json_body=None,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_max=rate_limit_max,
            rate_limit_period=rate_limit_period,
            return_exceptions=return_exceptions,
            return_status_code=return_status_code,
        )
