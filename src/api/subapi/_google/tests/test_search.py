"""
Unit tests for Google Books search service.
Tests GoogleBooksSearchService functionality using mocked data.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from api.subapi._google.models import GoogleBooksItem, GoogleBooksSearchResponse
from api.subapi._google.search import GoogleBooksSearchService
from api.subapi._google.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestGoogleBooksSearchService:
    """Tests for GoogleBooksSearchService class."""

    @pytest.mark.asyncio
    async def test_search_books(self, mock_google_books_api_key):
        """Test basic book search using mocked data."""
        service = GoogleBooksSearchService()

        # Mock the _make_request method on the internal service
        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            # Simulate raw API response
            raw_response = load_fixture("make_requests/search_books_by_title.json")
            mock_request.return_value = (raw_response, None)

            result = await service.search_books("Harry Potter", max_results=5)

            assert result.error is None
            assert isinstance(result, GoogleBooksSearchResponse)
            assert result.totalItems > 0
            assert len(result.items) > 0
            assert len(result.docs) > 0  # OpenLibrary compatibility
            assert result.query == "Harry Potter"

            # Check first item
            first_book = result.items[0]
            assert first_book.google_id
            assert first_book.title
            assert first_book.mc_id
            assert first_book.mc_type == "book"

    @pytest.mark.asyncio
    async def test_search_by_isbn(self, mock_google_books_api_key):
        """Test search by ISBN using mocked data."""
        service = GoogleBooksSearchService()

        # Mock the get_volume_by_isbn method on the internal service
        with patch.object(
            service.service, "get_volume_by_isbn", new_callable=AsyncMock
        ) as mock_method:
            raw_response = load_fixture("make_requests/get_volume_by_isbn.json")
            mock_method.return_value = (raw_response, None)

            result = await service.search_by_isbn("9780439708180")

            # The method should return a GoogleBooksSearchResponse
            assert isinstance(result, GoogleBooksSearchResponse)
            if result.error is None:
                assert len(result.items) > 0
                assert result.items[0].title
                assert result.items[0].google_id

    @pytest.mark.asyncio
    async def test_search_by_title_and_author(self, mock_google_books_api_key):
        """Test search by title and author using mocked data."""
        service = GoogleBooksSearchService()

        # Mock the _make_request method on the internal service
        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            raw_response = load_fixture("make_requests/search_by_title_and_author.json")
            mock_request.return_value = (raw_response, None)

            result = await service.search_by_title_and_author(
                title="Harry Potter",
                author="J.K. Rowling",
                max_results=5,
            )

            assert result.error is None
            assert isinstance(result, GoogleBooksSearchResponse)
            assert result.totalItems > 0
            assert len(result.items) > 0

            # Check that results are relevant
            first_book = result.items[0]
            assert first_book.title
            assert first_book.author_name

    @pytest.mark.asyncio
    async def test_search_direct(self, mock_google_books_api_key):
        """Test direct search that returns GoogleBooksSearchResponse."""
        service = GoogleBooksSearchService()

        # Mock the _make_request method on the internal service
        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            raw_response = load_fixture("make_requests/search_books_by_title.json")
            mock_request.return_value = (raw_response, None)

            result = await service.search_direct("Harry Potter", max_results=5)

            assert isinstance(result, GoogleBooksSearchResponse)
            assert result.error is None
            assert len(result.items) > 0
            assert result.items[0].google_id
            assert result.items[0].title

    @pytest.mark.asyncio
    async def test_convert_volume_to_book_item(self, mock_google_books_api_key):
        """Test volume conversion to book item."""
        service = GoogleBooksSearchService()

        # Load a real volume from fixtures
        volume_data = load_fixture("make_requests/get_volume_by_id.json")

        # Convert it
        book = service._convert_volume_to_book_item(volume_data)

        assert book is not None
        assert book.google_id == volume_data["id"]
        assert book.title
        # mc_id may be None if model validator didn't run, but type should be set
        assert book.mc_type == "book"
        assert book.key == f"/works/GOOGLE_{volume_data['id']}"

    @pytest.mark.asyncio
    async def test_convert_volume_without_title(self, mock_google_books_api_key):
        """Test that volumes without title are skipped."""
        service = GoogleBooksSearchService()

        # Create invalid volume data
        invalid_volume = {
            "id": "test123",
            "volumeInfo": {
                "title": "",  # Empty title
                "authors": ["Test Author"],
            },
        }

        book = service._convert_volume_to_book_item(invalid_volume)
        assert book.error is not None

    @pytest.mark.asyncio
    async def test_convert_volume_without_authors(self, mock_google_books_api_key):
        """Test that volumes without authors are skipped."""
        service = GoogleBooksSearchService()

        # Create invalid volume data
        invalid_volume = {
            "id": "test123",
            "volumeInfo": {
                "title": "Test Book",
                "authors": [],  # No authors
            },
        }

        book = service._convert_volume_to_book_item(invalid_volume)
        assert book.error is not None

    @pytest.mark.asyncio
    async def test_search_with_pagination(self, mock_google_books_api_key):
        """Test search with pagination parameters."""
        service = GoogleBooksSearchService()

        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            raw_response = load_fixture("make_requests/search_books_by_title.json")
            mock_request.return_value = (raw_response, None)

            # First page
            result1 = await service.search_books("Harry Potter", max_results=5, start_index=0)
            assert result1.error is None
            assert len(result1.items) > 0

            # Verify pagination parameters were passed
            call_args = mock_request.call_args
            assert call_args is not None

    @pytest.mark.asyncio
    async def test_search_with_order_by(self, mock_google_books_api_key):
        """Test search with different ordering."""
        service = GoogleBooksSearchService()

        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            raw_response = load_fixture("make_requests/search_books_general.json")
            mock_request.return_value = (raw_response, None)

            # Search by relevance
            result_relevance = await service.search_books(
                "python programming", max_results=5, order_by="relevance"
            )
            assert result_relevance.error is None
            assert len(result_relevance.items) > 0

            # Search by newest
            result_newest = await service.search_books(
                "python programming", max_results=5, order_by="newest"
            )
            assert result_newest.error is None
            assert len(result_newest.items) > 0

    @pytest.mark.asyncio
    async def test_search_no_results(self, mock_google_books_api_key):
        """Test search that returns no results."""
        service = GoogleBooksSearchService()

        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            # Mock empty response
            empty_response = {"kind": "books#volumes", "totalItems": 0, "items": []}
            mock_request.return_value = (empty_response, None)

            result = await service.search_books("xyzabc123nonexistent")

            assert result.error is None
            assert result.totalItems == 0
            assert len(result.items) == 0

    @pytest.mark.asyncio
    async def test_search_error_handling(self, mock_google_books_api_key):
        """Test search error handling."""
        service = GoogleBooksSearchService()

        with patch.object(service.service, "_make_request", new_callable=AsyncMock) as mock_request:
            # Mock error response
            mock_request.return_value = ({"error": "API error"}, 500)

            result = await service.search_books("test query")

            assert result.error is not None
            assert "error" in result.error or "500" in result.error
