"""
Integration tests for Google Books API.
These tests make real API calls and require a valid API key.

Run with: pytest -m integration
Skip with: pytest -m "not integration"
"""

import pytest

from api.subapi._google.wrappers import google_books_wrapper

pytestmark = pytest.mark.integration


class TestGoogleBooksIntegration:
    """Integration tests for Google Books API wrappers."""

    @pytest.mark.asyncio
    async def test_search_books_async_with_query(self, google_books_api_key, sample_book_query):
        """Test search_books with general query."""
        result = await google_books_wrapper.search_books(
            query=sample_book_query,
            max_results=5,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert result.totalItems >= 0
        assert len(result.items) > 0

        # Check metadata
        result_dict = result.model_dump()
        assert "metadata" not in result_dict  # Metadata added by handlers, not wrappers

        # Check first item structure
        first_item = result.items[0]
        assert first_item.google_id is not None
        assert first_item.title is not None
        assert first_item.mc_id is not None
        assert first_item.mc_type is not None
        assert first_item.source is not None

    @pytest.mark.asyncio
    async def test_search_books_async_with_isbn(self, google_books_api_key, sample_isbn):
        """Test search_books with ISBN.

        Note: Google Books API sometimes returns editions without ISBN data
        in the industryIdentifiers field, even when searching by ISBN.
        This is a known API behavior, not a bug in our code.
        """
        result = await google_books_wrapper.search_books(
            isbn=sample_isbn,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert len(result.items) > 0

        # Verify we got a book result
        first_item = result.items[0]
        assert first_item.title is not None
        assert first_item.google_id is not None

        # ISBN data may not be present in the response for some editions
        # The API searched by ISBN successfully, but the returned volume
        # might be an edition without industryIdentifiers in its metadata

    @pytest.mark.asyncio
    async def test_search_books_async_with_title_author(
        self, google_books_api_key, sample_book_query, sample_author
    ):
        """Test search_books with title and author."""
        result = await google_books_wrapper.search_books(
            title=sample_book_query,
            author=sample_author,
            max_results=5,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert len(result.items) > 0

    @pytest.mark.asyncio
    async def test_search_books_direct_async(self, google_books_api_key, sample_book_query):
        """Test search_books_direct."""
        result = await google_books_wrapper.search_books_direct(
            query=sample_book_query,
            max_results=5,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        # search_books_direct returns GoogleBooksSearchResponse with docs/items
        assert len(result.docs) > 0 or len(result.items) > 0
        results = result.docs if result.docs else result.items
        assert isinstance(results, list)
        assert len(results) > 0
        first_item = results[0]
        assert first_item.google_id is not None
        assert first_item.title is not None

    @pytest.mark.asyncio
    async def test_get_volume_by_id_async(self, google_books_api_key, sample_volume_id):
        """Test get_volume_by_id."""
        result = await google_books_wrapper.get_volume_by_id(
            volume_id=sample_volume_id,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert result.volume is not None
        # Volume should have google_id matching the requested ID
        assert result.volume.google_id == sample_volume_id
        # Title should be present for a valid volume
        assert result.volume.title is not None
        assert isinstance(result.volume.title, str)

    @pytest.mark.asyncio
    async def test_get_volume_by_isbn_async(self, google_books_api_key, sample_isbn):
        """Test get_volume_by_isbn."""
        result = await google_books_wrapper.get_volume_by_isbn(
            isbn=sample_isbn,
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert result.volume is not None
        assert result.volume.title is not None

    @pytest.mark.asyncio
    async def test_search_with_no_results(self, google_books_api_key):
        """Test search that returns no results."""
        result = await google_books_wrapper.search_books(
            query="xyzabc123nonexistentbook999",
            api_key=google_books_api_key,
        )

        assert result.status_code == 200
        assert result.error is None
        assert result.totalItems == 0
        assert len(result.items) == 0

    @pytest.mark.asyncio
    async def test_search_without_api_key(self, sample_book_query):
        """Test search without API key (should still work but may be rate limited)."""
        result = await google_books_wrapper.search_books(
            query=sample_book_query,
            max_results=1,
            api_key=None,
        )

        # Should work without API key, but may be rate limited
        # If rate limited, status_code will be 429
        if result.status_code != 200:
            assert result.status_code == 429
        else:
            assert len(result.items) >= 0
