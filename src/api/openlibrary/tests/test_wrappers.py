"""
Tests for OpenLibrary async wrapper functions.
Tests the wrapper layer that converts models to dicts and returns (dict, error_code) tuples.
"""

from unittest.mock import AsyncMock, patch

import pytest

from api.openlibrary.models import (
    MCBookItem,
    OpenLibraryAuthorSearchResponse,
    OpenLibraryCoverUrlsResponse,
    OpenLibrarySearchResponse,
)
from api.openlibrary.wrappers import openlibrary_wrapper


class TestSearchBooksAsync:
    """Tests for search_books_async wrapper."""

    @pytest.mark.asyncio
    async def test_search_books_async_success(self):
        """Test successful book search async wrapper."""
        mock_books = [
            MCBookItem(key="/works/OL1W", title="Book 1", author_name=["Author 1"]),
            MCBookItem(key="/works/OL2W", title="Book 2", author_name=["Author 2"]),
        ]
        mock_response = OpenLibrarySearchResponse(
            results=mock_books, total_results=2, query="test", data_source="OpenLibrary Search"
        )

        with patch.object(
            openlibrary_wrapper.service, "search_books", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_books(query="test", limit=10, no_cache=True)

            assert isinstance(result, OpenLibrarySearchResponse)
            assert result.status_code == 200
            assert len(result.results) == 2
            assert result.results[0].title == "Book 1"
            assert result.data_source == "search_books_async"
            assert result.data_type is not None
            # The wrapper passes all parameters, including None values
            mock_search.assert_called_once()
            call_kwargs = mock_search.call_args[1]
            assert call_kwargs["query"] == "test"
            assert call_kwargs["limit"] == 10
            assert call_kwargs["offset"] == 0

    @pytest.mark.asyncio
    async def test_search_books_async_handles_error(self):
        """Test error handling in book search async wrapper."""
        mock_response = OpenLibrarySearchResponse(
            results=[], total_results=0, query="test", error="API error"
        )

        with patch.object(
            openlibrary_wrapper.service, "search_books", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_books(query="test", no_cache=True)

            assert isinstance(result, OpenLibrarySearchResponse)
            assert result.status_code == 500
            assert result.error == "API error"
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_books_async_exception_handling(self):
        """Test exception handling in book search async wrapper."""
        with patch.object(
            openlibrary_wrapper.service, "search_books", new_callable=AsyncMock
        ) as mock_search:
            mock_search.side_effect = Exception("Test error")

            result = await openlibrary_wrapper.search_books(query="test", no_cache=True)

            assert isinstance(result, OpenLibrarySearchResponse)
            assert result.status_code == 500
            assert result.error == "Test error"
            assert len(result.results) == 0


class TestGetCoverUrlsAsync:
    """Tests for get_cover_urls_async wrapper."""

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_isbn(self):
        """Test get_cover_urls_async wrapper with ISBN parameter."""
        # Mock aiohttp HEAD request to return 200
        # The code uses nested async context managers: ClientSession() and session.head()
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200

            # Mock the head() call's context manager
            # Create a proper async context manager class
            class MockHeadContext:
                def __init__(self, response):
                    self.response = response

                async def __aenter__(self):
                    return self.response

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    return None

            # Mock the session (which is returned by ClientSession context manager)
            mock_session = AsyncMock()
            # head() must return the context manager synchronously (not a coroutine)
            mock_session.head = lambda *args, **kwargs: MockHeadContext(mock_response)
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            # ClientSession() returns the session object (which is also a context manager)
            mock_session_class.return_value = mock_session

            result = await openlibrary_wrapper.get_cover_urls(isbn="9780743273565", no_cache=True)

            assert isinstance(result, OpenLibraryCoverUrlsResponse)
            assert result.status_code == 200
            assert len(result.results) == 1
            assert result.results[0].identifier["type"] == "isbn"
            assert result.results[0].identifier["value"] == "9780743273565"
            assert result.results[0].covers_available is True
            assert result.data_type is not None

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_olid(self):
        """Test get_cover_urls_async wrapper with OLID parameter."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200

            mock_head_context = AsyncMock()
            mock_head_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_head_context.__aexit__ = AsyncMock(return_value=None)

            mock_session = AsyncMock()
            mock_session.head.return_value = mock_head_context
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_session_class.return_value = mock_session

            result = await openlibrary_wrapper.get_cover_urls(olid="OL82563W", no_cache=True)

            assert isinstance(result, OpenLibraryCoverUrlsResponse)
            assert result.status_code == 200
            assert result.results[0].identifier["type"] == "olid"
            assert result.results[0].identifier["value"] == "OL82563W"

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_cover_id(self):
        """Test get_cover_urls_async wrapper with cover_id parameter."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200

            mock_head_context = AsyncMock()
            mock_head_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_head_context.__aexit__ = AsyncMock(return_value=None)

            mock_session = AsyncMock()
            mock_session.head.return_value = mock_head_context
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_session_class.return_value = mock_session

            result = await openlibrary_wrapper.get_cover_urls(cover_id="8739161", no_cache=True)

            assert isinstance(result, OpenLibraryCoverUrlsResponse)
            assert result.status_code == 200
            assert result.results[0].identifier["type"] == "id"
            assert result.results[0].identifier["value"] == "8739161"

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_no_identifier(self):
        """Test get_cover_urls_async wrapper without identifier."""
        result = await openlibrary_wrapper.get_cover_urls(no_cache=True)

        assert isinstance(result, OpenLibraryCoverUrlsResponse)
        assert result.status_code == 400
        assert result.error == "At least one identifier is required"

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_covers_not_available(self):
        """Test get_cover_urls_async when covers are not available."""
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 404  # Cover not found

            mock_head_context = AsyncMock()
            mock_head_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_head_context.__aexit__ = AsyncMock(return_value=None)

            mock_session = AsyncMock()
            mock_session.head.return_value = mock_head_context
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)

            mock_session_class.return_value = mock_session

            result = await openlibrary_wrapper.get_cover_urls(isbn="9780743273565", no_cache=True)

            assert isinstance(result, OpenLibraryCoverUrlsResponse)
            assert result.status_code == 200
            assert result.results[0].covers_available is False
            assert result.results[0].cover_urls is None

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_exception_handling(self):
        """Test exception handling in get_cover_urls_async wrapper."""
        # Test exception that escapes the inner try/except (e.g., during identifier determination)
        # This would be caught by the outer exception handler
        with patch("builtins.print"):  # Suppress any print statements
            # Force an exception before the cover check by patching something that fails early
            # Actually, exceptions during cover check are caught internally, so test a different scenario
            # For now, test that the function handles cover check exceptions gracefully
            with patch("aiohttp.ClientSession") as mock_session_class:
                # Mock ClientSession to raise exception during context manager entry
                mock_session = AsyncMock()
                mock_session.__aenter__.side_effect = Exception("Network error")
                mock_session_class.return_value = mock_session

                result = await openlibrary_wrapper.get_cover_urls(
                    isbn="9780743273565", no_cache=True
                )

                # Exception during cover check is caught internally, function continues successfully
                assert isinstance(result, OpenLibraryCoverUrlsResponse)
                assert result.status_code == 200
                assert result.results[0].covers_available is False


class TestSearchAuthorsAsync:
    """Tests for search_authors_async wrapper."""

    @pytest.mark.asyncio
    async def test_search_authors_async_success(self):
        """Test successful author search async wrapper."""
        from api.openlibrary.models import MCAuthorItem

        mock_authors = [
            MCAuthorItem(key="/authors/OL1A", name="Author 1"),
            MCAuthorItem(key="/authors/OL2A", name="Author 2"),
        ]
        mock_response = OpenLibraryAuthorSearchResponse(
            results=mock_authors, total_results=2, query="test"
        )

        with patch.object(
            openlibrary_wrapper.service, "search_authors", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_authors(query="test", limit=10, no_cache=True)

            assert isinstance(result, OpenLibraryAuthorSearchResponse)
            assert result.status_code == 200
            assert len(result.results) == 2
            assert result.results[0].name == "Author 1"
            assert result.data_type is not None
            mock_search.assert_called_once_with(query="test", limit=10, offset=0)

    @pytest.mark.asyncio
    async def test_search_authors_async_handles_error(self):
        """Test error handling in author search async wrapper."""
        mock_response = OpenLibraryAuthorSearchResponse(
            results=[], total_results=0, query="test", error="API error"
        )

        with patch.object(
            openlibrary_wrapper.service, "search_authors", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_authors(query="test", no_cache=True)

            assert isinstance(result, OpenLibraryAuthorSearchResponse)
            assert result.status_code == 500
            assert result.error == "API error"
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_authors_async_exception_handling(self):
        """Test exception handling in author search async wrapper."""
        with patch.object(
            openlibrary_wrapper.service, "search_authors", new_callable=AsyncMock
        ) as mock_search:
            mock_search.side_effect = Exception("Test error")

            result = await openlibrary_wrapper.search_authors(query="test", no_cache=True)

            assert isinstance(result, OpenLibraryAuthorSearchResponse)
            assert result.status_code == 500
            assert result.error == "Test error"
            assert len(result.results) == 0


class TestWrapperReturnFormat:
    """Tests for wrapper return format consistency."""

    @pytest.mark.asyncio
    async def test_all_wrappers_return_mcsearchresponse(self):
        """Test that all wrappers return MCSearchResponse derivatives."""
        with (
            patch.object(
                openlibrary_wrapper.service, "search_books", new_callable=AsyncMock
            ) as mock_search_books,
            patch.object(
                openlibrary_wrapper.service, "search_authors", new_callable=AsyncMock
            ) as mock_search_authors,
        ):
            mock_search_books.return_value = OpenLibrarySearchResponse(
                results=[], total_results=0, query="test"
            )
            mock_search_authors.return_value = OpenLibraryAuthorSearchResponse(
                results=[], total_results=0, query="test"
            )

            with patch("aiohttp.ClientSession") as mock_session_class:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_session = AsyncMock()
                mock_session.head.return_value.__aenter__.return_value = mock_response
                mock_session.__aenter__.return_value = mock_session
                mock_session.__aexit__.return_value = AsyncMock()
                mock_session_class.return_value = mock_session

                # Test all wrappers
                result1 = await openlibrary_wrapper.search_books(query="test", no_cache=True)
                result2 = await openlibrary_wrapper.search_authors(query="test", no_cache=True)
                result3 = await openlibrary_wrapper.get_cover_urls(isbn="123", no_cache=True)

                # All should return MCSearchResponse derivatives
                assert isinstance(result1, OpenLibrarySearchResponse)
                assert isinstance(result2, OpenLibraryAuthorSearchResponse)
                assert isinstance(result3, OpenLibraryCoverUrlsResponse)

                # All should have status_code and data_type
                assert isinstance(result1.status_code, int)
                assert isinstance(result2.status_code, int)
                assert isinstance(result3.status_code, int)
                assert result1.data_type is not None
                assert result2.data_type is not None
                assert result3.data_type is not None


class TestWrapperRequiredFields:
    """Tests for required fields in wrapper responses."""

    @pytest.mark.asyncio
    async def test_search_books_has_required_fields(self):
        """Test that book search results have mc_id, mc_type, source, and source_id."""
        mock_books = [
            MCBookItem(
                key="/works/OL1W",
                title="Book 1",
                author_name=["Author 1"],
                source_id="/works/OL1W",
            ),
            MCBookItem(
                key="/works/OL2W",
                title="Book 2",
                author_name=["Author 2"],
                source_id="/works/OL2W",
            ),
        ]
        mock_response = OpenLibrarySearchResponse(results=mock_books, total_results=2, query="test")

        with patch.object(
            openlibrary_wrapper.service, "search_books", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_books(query="test", limit=10, no_cache=True)

            assert len(result.results) == 2
            for book in result.results:
                # Verify required fields are present and not None/empty
                assert book.mc_id, f"mc_id is missing or empty for book: {book.title}"
                assert book.mc_type, f"mc_type is missing or empty for book: {book.title}"
                assert book.source, f"source is missing or empty for book: {book.title}"
                assert book.source_id, f"source_id is missing or empty for book: {book.title}"

                # Verify correct values
                assert book.mc_type.value == "book"
                assert book.source.value == "openlibrary"

    @pytest.mark.asyncio
    async def test_search_authors_has_required_fields(self):
        """Test that author search results have mc_id, mc_type, source, and source_id."""
        from api.openlibrary.models import MCAuthorItem

        mock_authors = [
            MCAuthorItem(
                key="/authors/OL1A",
                name="Author 1",
                source_id="/authors/OL1A",
            ),
            MCAuthorItem(
                key="/authors/OL2A",
                name="Author 2",
                source_id="/authors/OL2A",
            ),
        ]
        mock_response = OpenLibraryAuthorSearchResponse(
            results=mock_authors, total_results=2, query="test"
        )

        with patch.object(
            openlibrary_wrapper.service, "search_authors", new_callable=AsyncMock
        ) as mock_search:
            mock_search.return_value = mock_response

            result = await openlibrary_wrapper.search_authors(query="test", limit=10, no_cache=True)

            assert len(result.results) == 2
            for author in result.results:
                # Verify required fields are present and not None/empty
                assert author.mc_id, f"mc_id is missing or empty for author: {author.name}"
                assert author.mc_type, f"mc_type is missing or empty for author: {author.name}"
                assert author.source, f"source is missing or empty for author: {author.name}"
                assert author.source_id, f"source_id is missing or empty for author: {author.name}"

                # Verify correct values
                assert author.mc_type.value == "person"
                assert author.source.value == "openlibrary"
