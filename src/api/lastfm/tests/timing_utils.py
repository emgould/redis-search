"""
Timing utilities for integration tests to identify slow operations.
"""

import contextlib
import sys
import time
from collections.abc import Generator


@contextlib.contextmanager
def time_operation(operation_name: str, threshold: float = 0.5) -> Generator[None, None, None]:
    """
    Context manager to time operations and log if they exceed threshold.

    Args:
        operation_name: Name of the operation being timed
        threshold: Minimum duration (seconds) to log (default: 0.5s)

    Example:
        with time_operation("Spotify API call"):
            result = await service.search_albums("Pink Floyd")
    """
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        if duration > threshold:
            # Use stderr so pytest doesn't capture it
            sys.stderr.write(f"  ⏱️  {operation_name}: {duration:.2f}s\n")
            sys.stderr.flush()
