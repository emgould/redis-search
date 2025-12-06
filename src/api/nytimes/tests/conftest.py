"""
Shared fixtures and utilities for NYTimes service tests.
"""

# Set environment to test mode FIRST, before any imports
import os

os.environ["ENVIRONMENT"] = "test"
# Don't set NYTIMES_API_KEY here - will be set in pytest_configure based on test type

import asyncio
import json
from pathlib import Path

import pytest


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"

    # Detect if we're running unit tests only (not integration tests)
    # Unit tests should use mocked API key, integration tests use real key from .env
    marker_expr = config.getoption("-m", default=None)
    is_unit_tests_only = marker_expr and "not integration" in marker_expr

    # If running unit tests only, ensure test API key is set
    # If running integration tests, use real key from environment (don't override)
    if is_unit_tests_only:
        # Unit tests: use mocked API key
        os.environ["NYTIMES_API_KEY"] = "test_nytimes_api_key_12345"
    elif "NYTIMES_API_KEY" not in os.environ:
        # If no marker specified and no env var set, default to test key (for unit tests)
        os.environ["NYTIMES_API_KEY"] = "test_nytimes_api_key_12345"
    # Otherwise, keep existing NYTIMES_API_KEY from environment (for integration tests)


@pytest.fixture(autouse=True)
async def delay_between_integration_tests(request):
    """Add a 1-second delay between integration tests to avoid rate limiting.

    This fixture automatically runs for all integration tests (marked with
    @pytest.mark.integration) and adds a delay after each test completes.

    The 1-second delay balances test speed with avoiding NYTimes API rate limits.
    Cover enrichment is done sequentially with 0.1s delays between books.
    """
    # Check if this is an integration test
    if "integration" in request.keywords:
        # Delay happens after the test via yield
        yield
        # Add 1 second delay after integration test completes
        await asyncio.sleep(1)
    else:
        # For non-integration tests, no delay needed
        yield


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
            f"Create fixtures from real API responses for testing."
        )

    with open(fixture_path) as f:
        return json.load(f)


@pytest.fixture
def mock_nytimes_api_key():
    """Mock NYTimes API key."""
    return "test_nytimes_api_key_12345"


@pytest.fixture(autouse=True)
def clear_nytimes_cache():
    """Clear NYTimes cache before each test to prevent cache pollution between tests."""
    from api.nytimes.core import NYTimesCache, NYTimesRequestCache

    # Clear both caches before each test
    NYTimesCache.clear_memory_cache()
    NYTimesRequestCache.clear_memory_cache()

    yield

    # Clear caches after test as well
    NYTimesCache.clear_memory_cache()
    NYTimesRequestCache.clear_memory_cache()


@pytest.fixture
def mock_bestseller_list_response():
    """Mock response for bestseller list endpoint."""
    return {
        "status": "OK",
        "copyright": "Copyright (c) 2024 The New York Times Company.",
        "num_results": 2,
        "results": {
            "bestsellers_date": "2024-01-13",
            "published_date": "2024-01-28",
            "published_date_description": "latest",
            "previous_published_date": "2024-01-21",
            "next_published_date": "",
            "list_name": "Combined Print and E-Book Fiction",
            "list_name_encoded": "combined-print-and-e-book-fiction",
            "display_name": "Combined Print & E-Book Fiction",
            "updated": "WEEKLY",
            "books": [
                {
                    "rank": 1,
                    "rank_last_week": 2,
                    "weeks_on_list": 3,
                    "asterisk": 0,
                    "dagger": 0,
                    "primary_isbn10": "1234567890",
                    "primary_isbn13": "9781234567890",
                    "publisher": "Test Publisher",
                    "description": "Test description",
                    "price": "30.00",
                    "title": "TEST BOOK",
                    "author": "Test Author",
                    "contributor": "by Test Author",
                    "contributor_note": "",
                    "book_image": "https://example.com/book.jpg",
                    "book_image_width": 330,
                    "book_image_height": 500,
                    "amazon_product_url": "https://amazon.com/test",
                    "age_group": "",
                    "book_review_link": "",
                    "first_chapter_link": "",
                    "sunday_review_link": "",
                    "article_chapter_link": "",
                    "isbns": [{"isbn10": "1234567890", "isbn13": "9781234567890"}],
                    "buy_links": [{"name": "Amazon", "url": "https://amazon.com/test"}],
                    "book_uri": "nyt://book/test",
                },
                {
                    "rank": 2,
                    "rank_last_week": 1,
                    "weeks_on_list": 5,
                    "asterisk": 0,
                    "dagger": 0,
                    "primary_isbn10": "0987654321",
                    "primary_isbn13": "9780987654321",
                    "publisher": "Another Publisher",
                    "description": "Another test description",
                    "price": "28.00",
                    "title": "ANOTHER TEST BOOK",
                    "author": "Another Author",
                    "contributor": "by Another Author",
                    "contributor_note": "",
                    "book_image": "https://example.com/another.jpg",
                    "book_image_width": 330,
                    "book_image_height": 500,
                    "amazon_product_url": "https://amazon.com/another",
                    "age_group": "",
                    "book_review_link": "",
                    "first_chapter_link": "",
                    "sunday_review_link": "",
                    "article_chapter_link": "",
                    "isbns": [{"isbn10": "0987654321", "isbn13": "9780987654321"}],
                    "buy_links": [{"name": "Amazon", "url": "https://amazon.com/another"}],
                    "book_uri": "nyt://book/another",
                },
            ],
            "corrections": [],
        },
    }


@pytest.fixture
def mock_overview_response():
    """Mock response for overview endpoint."""
    return {
        "status": "OK",
        "copyright": "Copyright (c) 2024 The New York Times Company.",
        "num_results": 1,
        "results": {
            "bestsellers_date": "2024-01-13",
            "published_date": "2024-01-28",
            "published_date_description": "latest",
            "previous_published_date": "2024-01-21",
            "next_published_date": "",
            "lists": [
                {
                    "list_id": 704,
                    "list_name": "Combined Print and E-Book Fiction",
                    "list_name_encoded": "combined-print-and-e-book-fiction",
                    "display_name": "Combined Print & E-Book Fiction",
                    "updated": "WEEKLY",
                    "list_image": None,
                    "list_image_width": None,
                    "list_image_height": None,
                    "books": [
                        {
                            "rank": 1,
                            "rank_last_week": 2,
                            "weeks_on_list": 3,
                            "asterisk": 0,
                            "dagger": 0,
                            "primary_isbn10": "1234567890",
                            "primary_isbn13": "9781234567890",
                            "publisher": "Test Publisher",
                            "description": "Test description",
                            "price": "30.00",
                            "title": "TEST BOOK",
                            "author": "Test Author",
                            "contributor": "by Test Author",
                            "contributor_note": "",
                            "book_image": "https://example.com/book.jpg",
                            "book_image_width": 330,
                            "book_image_height": 500,
                            "amazon_product_url": "https://amazon.com/test",
                            "age_group": "",
                            "book_review_link": "",
                            "first_chapter_link": "",
                            "sunday_review_link": "",
                            "article_chapter_link": "",
                            "isbns": [{"isbn10": "1234567890", "isbn13": "9781234567890"}],
                            "buy_links": [{"name": "Amazon", "url": "https://amazon.com/test"}],
                            "book_uri": "nyt://book/test",
                        }
                    ],
                }
            ],
        },
    }


@pytest.fixture
def mock_list_names_response():
    """Mock response for list names endpoint."""
    return {
        "status": "OK",
        "copyright": "Copyright (c) 2024 The New York Times Company.",
        "num_results": 2,
        "results": [
            {
                "list_name": "Combined Print and E-Book Fiction",
                "display_name": "Combined Print & E-Book Fiction",
                "list_name_encoded": "combined-print-and-e-book-fiction",
                "oldest_published_date": "2011-02-13",
                "newest_published_date": "2024-01-28",
                "updated": "WEEKLY",
            },
            {
                "list_name": "Combined Print and E-Book Nonfiction",
                "display_name": "Combined Print & E-Book Nonfiction",
                "list_name_encoded": "combined-print-and-e-book-nonfiction",
                "oldest_published_date": "2011-02-13",
                "newest_published_date": "2024-01-28",
                "updated": "WEEKLY",
            },
        ],
    }


@pytest.fixture
def mock_reviews_response():
    """Mock response for reviews endpoint."""
    return {
        "status": "OK",
        "copyright": "Copyright (c) 2024 The New York Times Company.",
        "num_results": 1,
        "results": [
            {
                "url": "https://www.nytimes.com/2024/01/15/books/review/test-book.html",
                "publication_dt": "2024-01-15",
                "byline": "Test Reviewer",
                "book_title": "Test Book",
                "book_author": "Test Author",
                "summary": "A compelling test book review.",
                "isbn13": ["9781234567890"],
            }
        ],
    }
