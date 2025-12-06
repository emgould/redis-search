"""
Shared fixtures and utilities for YouTube service tests.

This conftest uses real API data that has been captured from YouTube Data API.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

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
def mock_youtube_api_key():
    """Mock YouTube API key."""
    return "test_youtube_api_key_12345"


@pytest.fixture
def mock_youtube_client():
    """Mock YouTube API client."""
    mock_client = MagicMock()
    return mock_client
