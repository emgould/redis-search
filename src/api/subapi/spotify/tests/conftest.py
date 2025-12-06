"""
Shared fixtures and utilities for Spotify service tests.

This conftest uses real API data that has been captured from Spotify.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

# Set test environment variables BEFORE any imports that might use them
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
        filename: Name of the fixture file (relative to fixtures/ directory)

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}\n"
            f"Run 'python fixtures/generate_mock_data.py' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_spotify_credentials():
    """Mock Spotify client credentials."""
    return {
        "client_id": "test_spotify_client_id",
        "client_secret": "test_spotify_client_secret",
    }


@pytest.fixture
def mock_spotify_auth(mock_spotify_credentials):
    """Mock Spotify auth service."""
    with (
        patch(
            "api.subapi.spotify.auth.SpotifyAuth.spotify_client_id", new_callable=PropertyMock
        ) as mock_id,
        patch(
            "api.subapi.spotify.auth.SpotifyAuth.spotify_client_secret", new_callable=PropertyMock
        ) as mock_secret,
        patch(
            "api.subapi.spotify.auth.spotify_auth.get_spotify_token", new_callable=AsyncMock
        ) as mock_token,
        patch(
            "api.subapi.spotify.auth.spotify_auth.get_spotify_headers", new_callable=AsyncMock
        ) as mock_headers,
    ):
        mock_id.return_value = mock_spotify_credentials["client_id"]
        mock_secret.return_value = mock_spotify_credentials["client_secret"]
        mock_token.return_value = "test_access_token"
        mock_headers.return_value = {"Authorization": "Bearer test_access_token"}
        yield {
            "client_id": mock_spotify_credentials["client_id"],
            "client_secret": mock_spotify_credentials["client_secret"],
            "token": "test_access_token",
            "headers": {"Authorization": "Bearer test_access_token"},
        }


@pytest.fixture
def mock_auth(mock_spotify_auth):
    """Alias for mock_spotify_auth for compatibility."""
    return mock_spotify_auth
