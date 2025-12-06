"""
Shared fixtures and utilities for Podcast service tests.

This conftest uses real API data that has been captured from PodcastIndex.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

# Set environment to test mode FIRST, before any imports
import json
import os
from pathlib import Path
from unittest.mock import PropertyMock, patch

import pytest


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"


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
def mock_podcast_api_key():
    """Mock PodcastIndex API key."""
    return "test_api_key_12345"


@pytest.fixture
def mock_podcast_api_secret():
    """Mock PodcastIndex API secret."""
    return "test_api_secret_67890"


@pytest.fixture
def mock_auth(mock_podcast_api_key, mock_podcast_api_secret):
    """Mock Auth class properties for podcast API credentials."""
    with (
        patch("api.podcast.auth.Auth.podcast_api_key", new_callable=PropertyMock) as mock_key,
        patch("api.podcast.auth.Auth.podcast_api_secret", new_callable=PropertyMock) as mock_secret,
    ):
        mock_key.return_value = mock_podcast_api_key
        mock_secret.return_value = mock_podcast_api_secret
        yield {"api_key": mock_podcast_api_key, "api_secret": mock_podcast_api_secret}


@pytest.fixture
def podcast_credentials():
    """
    Real PodcastIndex API credentials for integration tests.

    Requires environment variables:
    - PODCASTINDEX_API_KEY
    - PODCASTINDEX_API_SECRET
    """
    api_key = os.getenv("PODCASTINDEX_API_KEY")
    api_secret = os.getenv("PODCASTINDEX_API_SECRET")

    if not api_key or not api_secret:
        pytest.skip(
            "Integration tests require PODCASTINDEX_API_KEY and PODCASTINDEX_API_SECRET "
            "environment variables to be set"
        )

    return {"api_key": api_key, "api_secret": api_secret}
