"""
Unit tests for NYTimes Core Service.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.nytimes.auth import nytimes_auth
from api.nytimes.core import NYTimesService
from api.nytimes.models import NYTimesBook

pytestmark = pytest.mark.unit


class TestNYTimesService:
    """Tests for NYTimesService class."""

    def test_init_with_auth_service(self, mock_nytimes_api_key):
        """Test service initialization using auth service."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            assert service.api_key == mock_nytimes_api_key
            assert service.base_url == "https://api.nytimes.com/svc/books/v3"
            assert service.covers_url == "https://covers.openlibrary.org/b"

    def test_init_without_api_key_raises_error(self):
        """Test service initialization without API key raises error."""
        import os

        original_key = os.environ.get("NYTIMES_API_KEY")
        try:
            # Remove env var if it exists
            if "NYTIMES_API_KEY" in os.environ:
                del os.environ["NYTIMES_API_KEY"]
            # Clear cached key and set to None
            with patch.object(nytimes_auth, "_nytimes_api_key", None):
                with pytest.raises(ValueError, match="NYTimes API key is required"):
                    NYTimesService()
        finally:
            # Restore original env var
            if original_key:
                os.environ["NYTIMES_API_KEY"] = original_key

    @pytest.mark.asyncio
    async def test_make_request_success(self, mock_nytimes_api_key):
        """Test successful API request."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            mock_response_data = {"status": "OK", "results": []}

            # Mock get_rate_limiter to prevent blocking in tests
            # BaseAPIClient imports get_rate_limiter from utils.rate_limiter
            with (
                patch("utils.base_api_client.get_rate_limiter") as mock_get_limiter,
                patch("aiohttp.ClientSession") as mock_session_class,
            ):
                # Create a no-op limiter context manager
                mock_limiter = AsyncMock()
                mock_limiter.__aenter__ = AsyncMock(return_value=None)
                mock_limiter.__aexit__ = AsyncMock(return_value=None)
                mock_get_limiter.return_value = mock_limiter

                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json = AsyncMock(return_value=mock_response_data)

                mock_session = MagicMock()
                mock_session.get.return_value.__aenter__.return_value = mock_response
                mock_session.__aenter__.return_value = mock_session
                mock_session.__aexit__.return_value = AsyncMock()

                mock_session_class.return_value = mock_session

                result, error_code = await service._make_request("lists/names.json", no_cache=True)

                assert result == mock_response_data
                assert error_code is None
                mock_session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_make_request_404_status(self, mock_nytimes_api_key):
        """Test API request with 404 status."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            # Mock get_rate_limiter to prevent blocking in tests
            # BaseAPIClient imports get_rate_limiter from utils.rate_limiter
            with (
                patch("utils.base_api_client.get_rate_limiter") as mock_get_limiter,
                patch("aiohttp.ClientSession") as mock_session_class,
            ):
                # Create a no-op limiter context manager
                mock_limiter = AsyncMock()
                mock_limiter.__aenter__ = AsyncMock(return_value=None)
                mock_limiter.__aexit__ = AsyncMock(return_value=None)
                mock_get_limiter.return_value = mock_limiter
                mock_response = AsyncMock()
                mock_response.status = 404

                mock_session = MagicMock()
                mock_session.get.return_value.__aenter__.return_value = mock_response
                mock_session.__aenter__.return_value = mock_session
                mock_session.__aexit__.return_value = AsyncMock()

                mock_session_class.return_value = mock_session

                result, error_code = await service._make_request("lists/invalid.json", no_cache=True)

                assert error_code == 404
                assert result == {"error": "Endpoint not found"}

    @pytest.mark.asyncio
    async def test_make_request_exception(self, mock_nytimes_api_key):
        """Test API request with exception."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            # Mock get_rate_limiter to prevent blocking in tests
            # BaseAPIClient imports get_rate_limiter from utils.rate_limiter
            with (
                patch("utils.base_api_client.get_rate_limiter") as mock_get_limiter,
                patch("aiohttp.ClientSession") as mock_session_class,
            ):
                # Create a no-op limiter context manager
                mock_limiter = AsyncMock()
                mock_limiter.__aenter__ = AsyncMock(return_value=None)
                mock_limiter.__aexit__ = AsyncMock(return_value=None)
                mock_get_limiter.return_value = mock_limiter
                mock_session_class.side_effect = Exception("Network error")

                result, error_code = await service._make_request("lists/names.json", no_cache=True)

                assert error_code == 500
                assert "error" in result

    @pytest.mark.asyncio
    async def test_enrich_books_with_covers(self, mock_nytimes_api_key):
        """Test enriching books with cover images."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            books = [
                NYTimesBook(
                    title="Test Book",
                    author="Test Author",
                    primary_isbn13="9781234567890",
                    book_image="https://example.com/cover.jpg",
                )
            ]

            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_response = AsyncMock()
                mock_response.status = 200

                mock_session = MagicMock()
                mock_session.head.return_value.__aenter__.return_value = mock_response
                mock_session.__aenter__.return_value = mock_session
                mock_session.__aexit__.return_value = AsyncMock()

                mock_session_class.return_value = mock_session

                enriched_books, count = await service.enrich_books_with_covers(books)

                assert len(enriched_books) == 1
                assert enriched_books[0].cover_available is True
                assert count >= 0

    @pytest.mark.asyncio
    async def test_get_bestseller_lists_with_list_name(
        self, mock_nytimes_api_key, mock_bestseller_list_response
    ):
        """Test getting bestseller lists with specific list name."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            with patch.object(
                service, "_make_request", return_value=(mock_bestseller_list_response, None)
            ):
                result, error_code = await service.get_bestseller_lists(
                    list_name="combined-print-and-e-book-fiction"
                )

                assert error_code is None
                assert result is not None

    @pytest.mark.asyncio
    async def test_get_list_names(self, mock_nytimes_api_key, mock_list_names_response):
        """Test getting list names."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            with patch.object(
                service, "_make_request", return_value=(mock_list_names_response, None)
            ):
                result, error_code = await service.get_list_names()

                assert error_code is None
                assert result is not None

    @pytest.mark.asyncio
    async def test_get_book_reviews(self, mock_nytimes_api_key, mock_reviews_response):
        """Test getting book reviews."""
        with patch.object(nytimes_auth, "_nytimes_api_key", mock_nytimes_api_key):
            service = NYTimesService()

            with patch.object(service, "_make_request", return_value=(mock_reviews_response, None)):
                result, error_code = await service.get_book_reviews(author="Test Author")

                assert error_code is None
                assert result is not None
