"""
Unit tests for OpenLibrary Core Service.
Tests OpenLibraryService base class functionality.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.openlibrary.core import OpenLibraryService

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filepath: str) -> dict:
    """Load a JSON fixture file."""
    with open(FIXTURES_DIR / filepath) as f:
        return json.load(f)


class TestOpenLibraryService:
    """Tests for OpenLibraryService class."""

    def test_init_without_api_key(self):
        """Test service initialization without API key."""
        service = OpenLibraryService()
        # Should not raise error, API key is optional
        assert service.base_url == "https://openlibrary.org"
        assert service.search_url == "https://openlibrary.org/search.json"
        assert service.covers_url == "https://covers.openlibrary.org/b"

    def test_init_matches_fixture(self):
        """Test that service initialization matches fixture data."""
        service = OpenLibraryService()
        fixture_data = load_fixture("core/service_init.json")

        assert service.base_url == fixture_data["base_url"]
        assert service.search_url == fixture_data["search_url"]
        assert service.covers_url == fixture_data["covers_url"]
        assert fixture_data["service_initialized"] is True

    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request."""
        service = OpenLibraryService()
        mock_response_data = {"docs": [{"title": "Test Book"}]}

        # Mock the BaseAPIClient._core_async_request method
        with patch.object(service, "_core_async_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response_data

            result, error = await service._make_request("https://openlibrary.org/search.json")

            assert result == mock_response_data
            assert error is None

    @pytest.mark.asyncio
    async def test_make_request_404(self):
        """Test API request with 404 status."""
        service = OpenLibraryService()

        # Mock the BaseAPIClient._core_async_request to return None (which indicates an error)
        with patch.object(service, "_core_async_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = None

            result, error = await service._make_request("https://openlibrary.org/invalid")

            assert error == 500
            assert "error" in result
            assert result["error"] == "API request failed"

    @pytest.mark.asyncio
    async def test_make_request_rate_limit(self):
        """Test API request with rate limit (429 status)."""
        service = OpenLibraryService()

        # Mock the BaseAPIClient._core_async_request to return None (simulating failure)
        with patch.object(service, "_core_async_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = None

            result, error = await service._make_request(
                "https://openlibrary.org/search.json", max_retries=0
            )

            assert error == 500
            assert "error" in result
            assert result["error"] == "API request failed"

    @pytest.mark.asyncio
    async def test_make_request_network_error(self):
        """Test API request with network error."""
        service = OpenLibraryService()

        # Mock the BaseAPIClient._core_async_request to return None (simulating network error)
        with patch.object(service, "_core_async_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = None

            result, error = await service._make_request(
                "https://openlibrary.org/search.json", max_retries=0
            )

            assert error == 500
            assert "error" in result
            assert result["error"] == "API request failed"
