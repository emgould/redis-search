"""
Shared fixtures and utilities for LastFM service tests.

This conftest uses real API data that has been captured from Last.fm and Spotify.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

# Set test environment variables BEFORE any imports that might use them
# This prevents the api.lastfm.__init__ module from failing when it tries to
# instantiate MusicService at import time
if "LASTFM_API_KEY" not in os.environ:
    os.environ["LASTFM_API_KEY"] = "test_lastfm_api_key_12345"
if "SPOTIFY_CLIENT_ID" not in os.environ:
    os.environ["SPOTIFY_CLIENT_ID"] = "test_spotify_client_id"
if "SPOTIFY_CLIENT_SECRET" not in os.environ:
    os.environ["SPOTIFY_CLIENT_SECRET"] = "test_spotify_client_secret"

# Load fixtures from JSON files
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"


def load_fixture(filename: str) -> dict:
    """Load a fixture from JSON file.

    Args:
        filename: Name of the fixture file

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}\n"
            f"Run 'python generate_mock_data.py' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_lastfm_api_key():
    """Mock Last.fm API key."""
    return "test_lastfm_api_key_12345"


@pytest.fixture
def mock_spotify_credentials():
    """Mock Spotify client credentials."""
    return {
        "client_id": "test_spotify_client_id",
        "client_secret": "test_spotify_client_secret",
    }


@pytest.fixture
def mock_auth(mock_lastfm_api_key, mock_spotify_credentials):
    """Mock Auth class properties for LastFM API credentials."""
    with (
        patch("api.lastfm.auth.Auth.lastfm_api_key", new_callable=PropertyMock) as mock_key,
    ):
        mock_key.return_value = mock_lastfm_api_key
        yield {
            "api_key": mock_lastfm_api_key,
            "spotify_client_id": mock_spotify_credentials["client_id"],
            "spotify_client_secret": mock_spotify_credentials["client_secret"],
        }


@pytest.fixture(autouse=True)
def mock_logger_errors():
    """Mock logger.error to suppress expected error messages during tests.

    This prevents test output from being cluttered with expected error logs
    when testing error handling paths.
    """
    with (
        patch("api.lastfm.wrappers.logger.error") as mock_error,
        patch("api.lastfm.search.logger.error") as mock_search_error,
    ):
        yield {
            "wrappers_error": mock_error,
            "search_error": mock_search_error,
        }


@pytest.fixture(autouse=True)
def log_test_timing(request):
    """Log timing information for integration tests to identify slow operations."""
    if "integration" in request.keywords:
        start_time = time.time()
        yield
        duration = time.time() - start_time
        if duration > 1.0:  # Only log tests that take more than 1 second
            # Use stderr so pytest doesn't capture it
            sys.stderr.write(f"\n⏱️  {request.node.name} took {duration:.2f}s\n")
            sys.stderr.flush()
    else:
        yield


def pytest_runtest_setup(item):
    """Log when integration tests start."""
    if "integration" in item.keywords:
        # Use stderr so pytest doesn't capture it
        sys.stderr.write(f"\n🔄 Running: {item.name}\n")
        sys.stderr.flush()


def pytest_runtest_teardown(item, nextitem):
    """Log when integration tests complete."""
    if "integration" in item.keywords:
        pass  # Timing is logged in the fixture
