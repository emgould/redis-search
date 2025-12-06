"""
Unit tests for Google Books Core Service.
Tests GoogleBooksService base class functionality.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.subapi._google.core import GoogleBooksService
from api.subapi._google.search import GoogleBooksSearchService
from api.subapi._google.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestGoogleBooksService:
    """Tests for GoogleBooksService class."""

    def test_extract_year_from_date(self):
        """Test year extraction from various date formats."""
        service = GoogleBooksService()

        assert service._extract_year_from_date("2020") == 2020
        assert service._extract_year_from_date("2020-05") == 2020
        assert service._extract_year_from_date("2020-05-15") == 2020
        assert service._extract_year_from_date(None) is None
        assert service._extract_year_from_date("invalid") is None

    def test_ensure_https(self):
        """Test HTTPS URL conversion."""
        service = GoogleBooksService()

        assert service._ensure_https("http://example.com") == "https://example.com"
        assert service._ensure_https("https://example.com") == "https://example.com"
        assert service._ensure_https(None) is None

    @pytest.mark.asyncio
    async def test_get_volume_by_id(self):
        """Test getting a volume by ID using mocked data."""
        service = GoogleBooksService()

        # Load fixture
        mock_response = load_fixture("core/volume_by_id.json")

        # Mock the _make_request method
        with patch.object(service, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = (mock_response, None)

            result, error = await service.get_volume_by_id("wrOQLV6xB-wC")

            assert error is None
            assert result["id"] == "wrOQLV6xB-wC"
            assert "volumeInfo" in result
            assert result["volumeInfo"]["title"]
            mock_request.assert_called_once_with("volumes/wrOQLV6xB-wC")

    @pytest.mark.asyncio
    async def test_get_volume_by_isbn(self):
        """Test getting a volume by ISBN using mocked data."""
        service = GoogleBooksService()

        # Load fixture
        mock_response = load_fixture("core/volume_by_isbn.json")

        # Mock the _make_request method
        with patch.object(service, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = (mock_response, None)

            result, error = await service.get_volume_by_isbn("9780439708180")

            assert error is None
            assert "items" in result
            assert len(result["items"]) > 0
            mock_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request."""
        service = GoogleBooksService()

        mock_response_data = {"kind": "books#volume", "id": "test123"}

        # Create mock response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create mock session
        mock_session_instance = MagicMock()
        mock_session_instance.request = MagicMock(return_value=mock_response)
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result, error = await service._make_request("volumes/test123")

            assert error is None
            assert result == mock_response_data

    @pytest.mark.asyncio
    async def test_make_request_rate_limit(self):
        """Test API request with rate limit error."""
        service = GoogleBooksService()

        # Create mock response
        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = MagicMock()
        mock_response.headers.get = MagicMock(return_value="60")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create mock session
        mock_session_instance = MagicMock()
        mock_session_instance.request = MagicMock(return_value=mock_response)
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result, error = await service._make_request("volumes/test123")

            assert error == 429
            assert "error" in result
            assert "Rate limit exceeded" in result["error"]

    @pytest.mark.asyncio
    async def test_make_request_not_found(self):
        """Test API request with 404 error."""
        service = GoogleBooksService()

        # Create mock response
        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create mock session
        mock_session_instance = MagicMock()
        mock_session_instance.request = MagicMock(return_value=mock_response)
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session_instance):
            result, error = await service._make_request("volumes/nonexistent")

            assert error == 404
            assert "error" in result
            assert "Not found" in result["error"]
