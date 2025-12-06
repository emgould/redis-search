"""
Shared fixtures and utilities for News service tests.

This conftest uses real API data that has been captured from NewsAPI.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

# Set environment to test mode FIRST, before any imports
import os

# Set test environment variables BEFORE any imports that might use them
# This prevents the api.news modules from failing when they try to
# instantiate NewsService at import time
if "NEWS_API_KEY" not in os.environ:
    os.environ["NEWS_API_KEY"] = "test_newsapi_key_12345"

# Set ENVIRONMENT to test to disable caching (must be set before utils.cache is imported)
os.environ["ENVIRONMENT"] = "test"
# Also disable cache for tests to prevent cache pollution between tests
os.environ["ENABLE_CACHE_FOR_TESTS"] = "0"

import json
from pathlib import Path

import pytest


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"

    # Pre-create snapshot directories to avoid pytest-snapshots directory modification errors
    # pytest-snapshots is strict about directory metadata, so we create directories upfront
    snapshots_dir = Path(__file__).parent / "snapshots"
    if snapshots_dir.exists():
        # Ensure common snapshot directories exist
        test_wrappers_dir = snapshots_dir / "test_wrappers"
        if test_wrappers_dir.exists():
            # Pre-create directories for known wrapper tests
            for test_name in [
                "test_get_news_sources_async",
                "test_get_trending_news_async",
                "test_search_news_async",
            ]:
                test_dir = test_wrappers_dir / test_name
                test_dir.mkdir(parents=True, exist_ok=True)


# Load fixtures from JSON files
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filename: str) -> dict:
    """Load a fixture from JSON file.

    Args:
        filename: Name of the fixture file relative to fixtures directory

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}\n"
            f"Run './fixtures/seed_mock_data.sh' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_newsapi_key():
    """Mock NewsAPI key for testing.

    Note: In production, this comes from NEWS_API_KEY environment variable.
    """
    return "test_newsapi_key_12345"


@pytest.fixture(autouse=True)
def clear_wrapper_cache():
    """Clear wrapper cache before each test to prevent cache pollution between tests."""
    from api.news.wrappers import NewsCache

    # Clear the cache before each test
    NewsCache.clear_memory_cache()

    yield

    # Clear cache after test as well
    NewsCache.clear_memory_cache()
