"""
Shared fixtures and utilities for Google Books API tests.

This conftest uses real API data that has been captured from Google Books.
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
        filename: Name of the fixture file (relative to fixtures/)

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Fixture file not found: {fixture_path}\n"
            f"Run 'python generate_mock_data.py --all' to generate fixtures from real API data."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_google_books_api_key():
    """Mock Google Books API key for testing."""
    return "test_google_books_api_key_12345"


@pytest.fixture
def google_books_api_key():
    """Get Google Books API key from environment (for integration tests)."""
    api_key = os.getenv("GOOGLE_BOOK_API_KEY")
    if not api_key:
        pytest.skip("GOOGLE_BOOK_API_KEY not set in environment")
    return api_key


@pytest.fixture
def sample_book_query():
    """Sample book search query."""
    return "Harry Potter and the Philosopher's Stone"


@pytest.fixture
def sample_isbn():
    """Sample ISBN for testing."""
    return "9780439708180"  # Harry Potter and the Sorcerer's Stone


@pytest.fixture
def sample_volume_id():
    """Sample Google Books volume ID."""
    return "wrOQLV6xB-wC"  # Harry Potter and the Sorcerer's Stone


@pytest.fixture
def sample_author():
    """Sample author name."""
    return "J.K. Rowling"
