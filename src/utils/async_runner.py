"""
Async Runner Utility - Safely run async functions in Firebase Functions.

This utility handles the case where an event loop may or may not already be running,
which can occur in deployed Firebase Functions environments or with gunicorn workers.

Uses nest_asyncio to patch asyncio to allow nested event loops, which is necessary
when gunicorn workers fork and need to share event loops.
"""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)

# Apply nest_asyncio patch at module load time to allow nested event loops
# This is required because:
# 1. gunicorn uses a pre-fork worker model
# 2. When workers fork, they may inherit event loops that are still running
# 3. nest_asyncio allows asyncio.run() to work even if an event loop is already running
_nest_asyncio_applied = False


def _ensure_nest_asyncio():
    """Ensure nest_asyncio is applied exactly once."""
    global _nest_asyncio_applied
    if not _nest_asyncio_applied:
        try:
            import nest_asyncio

            nest_asyncio.apply()
            _nest_asyncio_applied = True
            logger.debug("nest_asyncio patch applied successfully")
        except ImportError:
            logger.warning(
                "nest_asyncio not installed, falling back to standard asyncio. "
                "Install with: pip install nest-asyncio"
            )
        except RuntimeError as e:
            # Already applied or other runtime issue
            if "already been applied" in str(e).lower():
                _nest_asyncio_applied = True
            else:
                logger.warning(f"Failed to apply nest_asyncio: {e}")


# Apply nest_asyncio at import time
_ensure_nest_asyncio()


def run_async(coro: Coroutine[Any, Any, Any]) -> Any:
    """
    Safely run an async coroutine in Firebase Functions.

    This function handles multiple scenarios:
    1. When no event loop is running: uses asyncio.run() to create and run a new loop
    2. When an event loop is already running: uses nest_asyncio to allow nested runs
    3. When event loop is shutting down: handles gracefully with proper error message

    Args:
        coro: The coroutine to run

    Returns:
        The result of the coroutine

    Example:
        >>> async def my_async_function():
        ...     return "result"
        >>> result = run_async(my_async_function())
    """
    # Ensure nest_asyncio is applied (in case it wasn't at import time)
    _ensure_nest_asyncio()

    try:
        # Try to get the running loop
        try:
            loop = asyncio.get_running_loop()
            # If we have a running loop and nest_asyncio is applied, we can use run_until_complete
            if _nest_asyncio_applied and not loop.is_closed():
                return loop.run_until_complete(coro)
        except RuntimeError:
            # No running loop, we'll create one below
            pass

        # Try to use asyncio.run() - this works when no loop is running
        # or when nest_asyncio is applied
        return asyncio.run(coro)

    except RuntimeError as e:
        error_msg = str(e).lower()

        # Handle event loop shutdown scenarios
        if "cannot schedule" in error_msg or "event loop is closed" in error_msg:
            logger.warning(f"Event loop shutdown detected: {e}")
            # Don't raise - return None to allow graceful degradation
            return None

        # Handle case where asyncio.run() cannot be called from running event loop
        # This should not happen with nest_asyncio, but keep as fallback
        if "cannot be called from a running event loop" in error_msg:
            import concurrent.futures

            def run_in_new_loop():
                """Create a new event loop in this thread and run the coroutine."""
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    # Don't close the loop - let it linger for reuse
                    pass

            # Run in a thread pool executor with its own event loop
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_in_new_loop)
                return future.result(timeout=120)  # 2 minute timeout

        # Re-raise if it's a different RuntimeError
        raise


def run_async_safe(coro: Coroutine[Any, Any, Any], default: Any = None) -> Any:
    """
    Safely run an async coroutine, returning a default value on failure.

    This is useful when you want to gracefully handle event loop issues
    without raising exceptions.

    Args:
        coro: The coroutine to run
        default: The default value to return on failure

    Returns:
        The result of the coroutine, or the default value on failure
    """
    try:
        result = run_async(coro)
        return result if result is not None else default
    except Exception as e:
        logger.warning(f"run_async_safe caught exception: {e}")
        return default
