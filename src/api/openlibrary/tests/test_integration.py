"""
Integration tests for OpenLibrary service.
These tests hit the actual OpenLibrary API endpoints (no mocks).

Requirements:
- Internet connection required
- Optional: GOOGLE_BOOK_API_KEY environment variable for Google Books integration tests

Run with: pytest api/openlibrary/tests/test_integration.py -v
"""

import json
from unittest.mock import MagicMock

import pytest
from contracts.models import (
    MCSources,
    MCSubType,
    MCType,
)
from firebase_functions import https_fn

from api.openlibrary.handlers import OpenLibraryHandler
from api.openlibrary.models import MCAuthorItem, MCBookItem
from api.openlibrary.wrappers import openlibrary_wrapper, search_person_async
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def openlibrary_handler():
    """Create OpenLibraryHandler instance."""
    return OpenLibraryHandler()


@pytest.fixture
def mock_request():
    """Create a mock Firebase Functions Request object."""

    def _create_mock_request(args: dict[str, str | None] | None = None):
        mock_req = MagicMock(spec=https_fn.Request)
        # Make args support .get() method like a dict
        args_dict = args or {}
        mock_req.args = MagicMock()
        mock_req.args.get = lambda key, default=None: args_dict.get(key, default)
        return mock_req

    return _create_mock_request


class TestHandlers:
    """Integration tests for all OpenLibrary handlers."""

    @pytest.mark.asyncio
    async def test_search_books_handler_by_title(self, openlibrary_handler, mock_request):
        """Test search_books handler with title parameter."""
        req = mock_request({"title": "The Great Gatsby", "limit": "5"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        assert data["results"][0]["title"] is not None
        write_snapshot(data, "search_books_handler_by_title_result.json")

    @pytest.mark.asyncio
    async def test_search_books_handler_by_author(self, openlibrary_handler, mock_request):
        """Test search_books handler with author parameter."""
        req = mock_request({"author": "F. Scott Fitzgerald", "limit": "5"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        write_snapshot(data, "search_books_handler_by_author_result.json")

    @pytest.mark.asyncio
    async def test_search_books_handler_by_isbn(
        self, openlibrary_handler: OpenLibraryHandler, mock_request
    ):
        """Test search_books handler with ISBN parameter."""
        req = mock_request({"isbn": "9780743273565", "limit": "1"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        write_snapshot(data, "search_books_handler_by_isbn_result.json")

    @pytest.mark.asyncio
    async def test_search_books_handler_by_query(
        self, openlibrary_handler: OpenLibraryHandler, mock_request
    ):
        """Test search_books handler with query parameter."""
        req = mock_request({"query": "Dune", "limit": "3"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert len(data["results"]) > 0
        write_snapshot(data, "search_books_handler_by_query_result.json")

    @pytest.mark.asyncio
    async def test_search_books_handler_with_offset(self, openlibrary_handler, mock_request):
        """Test search_books handler with offset parameter."""
        req = mock_request({"title": "Python", "limit": "5", "offset": "5"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        write_snapshot(data, "search_books_handler_with_offset_result.json")

    @pytest.mark.asyncio
    async def test_search_books_handler_no_params(self, openlibrary_handler, mock_request):
        """Test search_books handler without parameters returns error."""
        req = mock_request({})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_search_books_handler_invalid_limit(self, openlibrary_handler, mock_request):
        """Test search_books handler with invalid limit parameter."""
        req = mock_request({"title": "Test", "limit": "101"})
        response = await openlibrary_handler.search_books(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_by_isbn(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler with ISBN parameter."""
        req = mock_request({"isbn": "9780743273565"})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "data" in data
        assert "metadata" in data
        assert data["metadata"]["source"] == "OpenLibrary Covers API"
        write_snapshot(data, "get_book_covers_handler_by_isbn_result.json")

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_by_olid(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler with OLID parameter."""
        req = mock_request({"olid": "OL82563W"})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "data" in data
        assert "metadata" in data
        write_snapshot(data, "get_book_covers_handler_by_olid_result.json")

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_by_cover_id(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler with cover_id parameter."""
        req = mock_request({"cover_id": "8739161"})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "data" in data
        assert "metadata" in data
        write_snapshot(data, "get_book_covers_handler_by_cover_id_result.json")

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_by_oclc(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler with OCLC parameter."""
        req = mock_request({"oclc": "459294"})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "data" in data
        assert "metadata" in data
        write_snapshot(data, "get_book_covers_handler_by_oclc_result.json")

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_by_lccn(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler with LCCN parameter."""
        req = mock_request({"lccn": "2003052119"})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "data" in data
        assert "metadata" in data
        write_snapshot(data, "get_book_covers_handler_by_lccn_result.json")

    @pytest.mark.asyncio
    async def test_get_book_covers_handler_no_params(self, openlibrary_handler, mock_request):
        """Test get_book_covers handler without parameters returns error."""
        req = mock_request({})
        response = await openlibrary_handler.get_book_covers(req)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data


class TestWrappers:
    """Integration tests for all OpenLibrary wrappers."""

    @pytest.mark.asyncio
    async def test_search_books_async_by_title(self):
        """Test search_books wrapper with title parameter."""
        result = await openlibrary_wrapper.search_books(title="The Great Gatsby", limit=5)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.data_source == "search_books_async"
        assert result.query == "The Great Gatsby"

        # Check first result has expected fields including required fields
        first_book = result.results[0]
        assert first_book.title is not None
        assert first_book.key is not None

        # Verify required fields
        assert first_book.mc_id is not None, "mc_id is required"
        assert first_book.mc_type == "book", "mc_type must be 'book'"
        assert first_book.source is not None, "source is required"
        assert first_book.source.value == "openlibrary", "source must be 'openlibrary'"
        assert first_book.source_id is not None, "source_id is required"
        assert len(first_book.source_id) > 0, "source_id cannot be empty"

        write_snapshot(result.model_dump(), "search_books_async_by_title_result.json")

    @pytest.mark.asyncio
    async def test_search_books_async_by_author(self):
        """Test search_books_async wrapper with author parameter."""
        result = await openlibrary_wrapper.search_books(author="F. Scott Fitzgerald", limit=5)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.data_source == "search_books_async"
        assert result.query == "F. Scott Fitzgerald"

        # Verify all results have required fields
        for book in result.results:
            assert book.mc_id is not None, f"mc_id is required for book: {book.title}"
            assert book.mc_type == "book", f"mc_type must be 'book' for book: {book.title}"
            assert book.source is not None, f"source is required for book: {book.title}"
            assert book.source.value == "openlibrary", (
                f"source must be 'openlibrary' for book: {book.title}"
            )
            assert book.source_id is not None, f"source_id is required for book: {book.title}"
            assert len(book.source_id) > 0, f"source_id cannot be empty for book: {book.title}"

        write_snapshot(result.model_dump(), "search_books_async_by_author_result.json")

    @pytest.mark.asyncio
    async def test_search_books_async_by_isbn(self):
        """Test search_books_async wrapper with ISBN parameter."""
        result = await openlibrary_wrapper.search_books(isbn="9780743273565", limit=1)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.data_source == "search_books_async"
        assert result.query == "9780743273565"

        # Verify all results have required fields
        for book in result.results:
            assert book.mc_id is not None, f"mc_id is required for book: {book.title}"
            assert book.mc_type == "book", f"mc_type must be 'book' for book: {book.title}"
            assert book.source is not None, f"source is required for book: {book.title}"
            assert book.source.value == "openlibrary", (
                f"source must be 'openlibrary' for book: {book.title}"
            )
            assert book.source_id is not None, f"source_id is required for book: {book.title}"
            assert len(book.source_id) > 0, f"source_id cannot be empty for book: {book.title}"

        write_snapshot(result.model_dump(), "search_books_async_by_isbn_result.json")

    @pytest.mark.asyncio
    async def test_search_books_async_by_query(self):
        """Test search_books_async wrapper with query parameter."""
        result = await openlibrary_wrapper.search_books(query="Dune", limit=3)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.data_source == "search_books_async"
        assert result.query == "Dune"

        # Verify all results have required fields
        for book in result.results:
            assert book.mc_id is not None, f"mc_id is required for book: {book.title}"
            assert book.mc_type == "book", f"mc_type must be 'book' for book: {book.title}"
            assert book.source is not None, f"source is required for book: {book.title}"
            assert book.source.value == "openlibrary", (
                f"source must be 'openlibrary' for book: {book.title}"
            )
            assert book.source_id is not None, f"source_id is required for book: {book.title}"
            assert len(book.source_id) > 0, f"source_id cannot be empty for book: {book.title}"

        write_snapshot(result.model_dump(), "search_books_async_by_query_result.json")

    @pytest.mark.asyncio
    async def test_search_books_async_with_offset(self):
        """Test search_books_async wrapper with offset parameter."""
        result = await openlibrary_wrapper.search_books(title="Python", limit=5, offset=5)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0

        # Verify all results have required fields
        for book in result.results:
            assert book.mc_id is not None, f"mc_id is required for book: {book.title}"
            assert book.mc_type == "book", f"mc_type must be 'book' for book: {book.title}"
            assert book.source is not None, f"source is required for book: {book.title}"
            assert book.source.value == "openlibrary", (
                f"source must be 'openlibrary' for book: {book.title}"
            )
            assert book.source_id is not None, f"source_id is required for book: {book.title}"
            assert len(book.source_id) > 0, f"source_id cannot be empty for book: {book.title}"

        write_snapshot(result.model_dump(), "search_books_async_with_offset_result.json")

    @pytest.mark.asyncio
    async def test_search_books_async_no_params(self):
        """Test search_books_async wrapper without parameters returns error."""
        result = await openlibrary_wrapper.search_books()

        assert result.status_code == 400
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_isbn(self):
        """Test get_cover_urls_async wrapper with ISBN parameter."""
        result = await openlibrary_wrapper.get_cover_urls(isbn="9780743273565")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) == 1
        assert result.results[0].identifier["type"] == "isbn"
        assert result.results[0].identifier["value"] == "9780743273565"
        # Note: covers_available may vary depending on OpenLibrary's data
        write_snapshot(result.model_dump(), "get_cover_urls_async_by_isbn_result.json")

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_olid(self):
        """Test get_cover_urls_async wrapper with OLID parameter."""
        result = await openlibrary_wrapper.get_cover_urls(olid="OL82563W")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) == 1
        assert result.results[0].identifier["type"] == "olid"
        assert result.results[0].identifier["value"] == "OL82563W"
        write_snapshot(result.model_dump(), "get_cover_urls_async_by_olid_result.json")

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_cover_id(self):
        """Test get_cover_urls_async wrapper with cover_id parameter."""
        result = await openlibrary_wrapper.get_cover_urls(cover_id="8739161")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) == 1
        assert result.results[0].identifier["type"] == "id"
        assert result.results[0].identifier["value"] == "8739161"
        write_snapshot(result.model_dump(), "get_cover_urls_async_by_cover_id_result.json")

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_oclc(self):
        """Test get_cover_urls_async wrapper with OCLC parameter."""
        result = await openlibrary_wrapper.get_cover_urls(oclc="459294")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) == 1
        assert result.results[0].identifier["type"] == "oclc"
        assert result.results[0].identifier["value"] == "459294"
        write_snapshot(result.model_dump(), "get_cover_urls_async_by_oclc_result.json")

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_by_lccn(self):
        """Test get_cover_urls_async wrapper with LCCN parameter."""
        result = await openlibrary_wrapper.get_cover_urls(lccn="2003052119")

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) == 1
        assert result.results[0].identifier["type"] == "lccn"
        assert result.results[0].identifier["value"] == "2003052119"
        write_snapshot(result.model_dump(), "get_cover_urls_async_by_lccn_result.json")

    @pytest.mark.asyncio
    async def test_get_cover_urls_async_no_params(self):
        """Test get_cover_urls_async wrapper without parameters returns error."""
        result = await openlibrary_wrapper.get_cover_urls()

        assert result.status_code == 400
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_search_authors_async_by_query(self):
        """Test search_authors_async wrapper with query parameter."""
        result = await openlibrary_wrapper.search_authors(query="F. Scott Fitzgerald", limit=5)

        assert result.status_code == 200
        assert result is not None
        assert len(result.results) > 0
        assert result.query == "F. Scott Fitzgerald"

        # Check first result has expected fields
        first_author = result.results[0]
        assert first_author.name is not None
        assert first_author.key is not None

        # Verify all results have required fields
        for author in result.results:
            assert author.mc_id is not None, f"mc_id is required for author: {author.name}"
            assert author.mc_type == MCType.PERSON, f"mc_type must be 'person' for author: {author.name}"
            assert author.source is not None, f"source is required for author: {author.name}"
            assert author.source.value == "openlibrary", (
                f"source must be 'openlibrary' for author: {author.name}"
            )
            assert author.source_id is not None, f"source_id is required for author: {author.name}"
            assert len(author.source_id) > 0, f"source_id cannot be empty for author: {author.name}"

        write_snapshot(result.model_dump(), "search_authors_async_by_query_result.json")

    @pytest.mark.asyncio
    async def test_search_authors_async_no_params(self):
        """Test search_authors_async wrapper without parameters returns error."""
        result = await openlibrary_wrapper.search_authors()

        assert result.status_code == 400
        assert result.error is not None
        assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_person_async(self):
        """Test search_person_async wrapper function."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request for J.K. Rowling
        # Use a non-existent author key so it falls back to name search
        person_request = MCPersonSearchRequest(
            source_id="/authors/OL999999999A",  # Non-existent key - will fall back to name search
            source=MCSources.OPENLIBRARY,
            mc_type=MCType.PERSON,
            mc_id="author_jk_rowling",
            mc_subtype=MCSubType.AUTHOR,
            name="J.K. Rowling",
        )

        # Call the wrapper function
        result = await search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # Validate author details
        assert result.details is not None
        author = MCAuthorItem.model_validate(result.details.model_dump())
        assert author.mc_type == MCType.PERSON
        assert author.mc_subtype == MCSubType.AUTHOR
        assert "Rowling" in author.name or "rowling" in author.name.lower()
        # Verify required MCBaseItem fields for author
        assert author.mc_id is not None, f"mc_id is missing for author: {author.name}"
        assert author.mc_type == MCType.PERSON, f"mc_type is wrong for author: {author.name}"
        assert author.mc_subtype == MCSubType.AUTHOR, f"mc_subtype is wrong for author: {author.name}"
        assert author.source is not None, f"source is missing for author: {author.name}"
        assert author.source_id is not None, f"source_id is missing for author: {author.name}"

        # Validate works array contains books
        assert len(result.works) > 0, "works array should not be empty"

        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
            item_validated = MCBookItem.model_validate(work_dict)
            assert item_validated.mc_type == MCType.BOOK
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for book: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.BOOK, (
                f"mc_type is wrong for book: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for book: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for book: {item_validated.title}"
            )

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works_openlibrary.json")

    @pytest.mark.asyncio
    async def test_search_person_async_valid_key(self):
        """Test search_person_async with a valid author key - should use direct lookup."""
        from contracts.models import MCPersonSearchRequest

        # First, search for J.K. Rowling to get her real author key
        search_result = await openlibrary_wrapper.search_authors(query="J.K. Rowling", limit=1)
        assert search_result.status_code == 200
        assert len(search_result.results) > 0
        rowling_author = search_result.results[0]
        rowling_key = (
            rowling_author.key or rowling_author.source_id
        )  # Use key field which has full path
        # Normalize to ensure it has the /authors/ prefix to match what will be returned
        if not rowling_key.startswith("/authors/"):
            rowling_key = f"/authors/{rowling_key}"

        # Create request with valid author key - name doesn't matter when key is valid
        person_request = MCPersonSearchRequest(
            source_id=rowling_key,  # Valid OpenLibrary author key for J.K. Rowling
            source=MCSources.OPENLIBRARY,
            mc_type=MCType.PERSON,
            mc_id=f"author_{rowling_key.replace('/', '_')}",
            mc_subtype=MCSubType.AUTHOR,
            name="Wrong Name That Should Be Ignored",  # Name doesn't matter when key is valid
        )

        # Call wrapper
        result = await search_person_async(person_request, limit=20)

        # Validate response - should succeed using key, ignoring name
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # Validate author details - should be J.K. Rowling from key, not "Wrong Name"
        assert result.details is not None
        author = MCAuthorItem.model_validate(result.details.model_dump())
        assert author.mc_type == MCType.PERSON
        assert author.mc_subtype == MCSubType.AUTHOR
        # Should be J.K. Rowling (from key), not "Wrong Name"
        assert "rowling" in author.name.lower()
        # The returned author's source_id should match what we passed in
        assert author.source_id == person_request.source_id
        # Verify required MCBaseItem fields for author
        assert author.mc_id is not None, f"mc_id is missing for author: {author.name}"
        assert author.mc_type == MCType.PERSON, f"mc_type is wrong for author: {author.name}"
        assert author.mc_subtype == MCSubType.AUTHOR, f"mc_subtype is wrong for author: {author.name}"
        assert author.source is not None, f"source is missing for author: {author.name}"
        assert author.source_id is not None, f"source_id is missing for author: {author.name}"

        # Validate works array contains books
        assert len(result.works) > 0, "works array should not be empty"

        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)
            item_validated = MCBookItem.model_validate(work_dict)
            assert item_validated.mc_type == MCType.BOOK
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for book: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.BOOK, (
                f"mc_type is wrong for book: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for book: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for book: {item_validated.title}"
            )

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source(self):
        """Test search_person_async with invalid source."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with invalid source (not OpenLibrary)
        person_request = MCPersonSearchRequest(
            source_id="/authors/OL123456A",
            source=MCSources.TMDB,  # Invalid for OpenLibrary wrapper
            mc_type=MCType.PERSON,
            mc_id="author_123",
            mc_subtype=MCSubType.AUTHOR,
            name="Test Author",
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_missing_name(self):
        """Test search_person_async with missing author name."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request without name
        # Use a non-existent key so that when key lookup fails and name is empty, it returns 404
        person_request = MCPersonSearchRequest(
            source_id="/authors/OL999999999A",  # Non-existent key
            source=MCSources.OPENLIBRARY,
            mc_type=MCType.PERSON,
            mc_id="author_123",
            mc_subtype=MCSubType.AUTHOR,
            name="",  # Empty name
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 404
        assert result.error is not None
        assert "name" in result.error.lower()
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_author_not_found(self):
        """Test search_person_async with non-existent author."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with non-existent author
        # Use a clearly invalid key format that won't exist
        person_request = MCPersonSearchRequest(
            source_id="/authors/OL999999999999999999A",  # Non-existent author key (invalid format)
            source=MCSources.OPENLIBRARY,
            mc_type=MCType.PERSON,
            mc_id="author_nonexistent",
            mc_subtype=MCSubType.AUTHOR,
            name="XyZqWrTpLmN123456789",  # Very unlikely to exist
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 404  # Not found should always return 404
        assert result.error is not None
        assert result.details is None
        assert result.works == []
        assert result.related == []
