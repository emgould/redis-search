"""
Shared fixtures and utilities for Watchmode service tests.

This conftest uses real API data that has been captured from Watchmode.
Mock data is generated using generate_mock_data.py and stored in fixtures/.
"""

# Set environment to test mode FIRST, before any imports
import os

os.environ["ENVIRONMENT"] = "test"

import json
from pathlib import Path

import pytest


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"


# Load fixtures from JSON files
FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
            f"Run 'python fixtures/generate_mock_data.py' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_watchmode_api_key():
    """Mock Watchmode API key."""
    return "test_watchmode_key_12345"


@pytest.fixture
def mock_tmdb_token():
    """Mock TMDB API token."""
    return "test_tmdb_token_12345"
