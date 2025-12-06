"""
Tests for OpenLibrary search service (search.py only).
Tests cover all methods defined in search.py:
- _process_book_doc
- _calculate_blended_score
- get_cover_urls
- search_books
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from api.openlibrary.models import (
    MCBookItem,
    OpenLibraryAuthorSearchResponse,
    OpenLibrarySearchResponse,
)
from api.openlibrary.search import OpenLibrarySearchService
from utils.pytest_utils import write_snapshot

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filepath: str) -> dict:
    """Load a JSON fixture file."""
    with open(FIXTURES_DIR / filepath) as f:
        return json.load(f)


class TestOpenLibrarySearchService:
    """Tests for OpenLibrarySearchService."""

    @pytest.fixture
    def service(self):
        """Create a search service instance."""
        return OpenLibrarySearchService()

    @pytest.fixture
    def mock_search_response(self):
        """Mock OpenLibrary search response."""
        return load_fixture("make_requests/search_gatsby.json")

    def test_service_initialization(self, service):
        """Test service initialization."""
        assert service.base_url == "https://openlibrary.org"
        assert service.search_url == "https://openlibrary.org/search.json"

    def test_process_book_doc_basic(self, service):
        """Test basic book document processing."""
        doc = {
            "key": "/works/OL468516W",
            "title": "The Great Gatsby",
            "author_name": ["F. Scott Fitzgerald"],
            "first_publish_year": 1925,
        }

        processed = service._process_book_doc(doc)

        assert processed.title == "The Great Gatsby"
        assert processed.openlibrary_url == "https://openlibrary.org/works/OL468516W"
        assert processed.openlibrary_key == "/works/OL468516W"
        assert processed.author == "F. Scott Fitzgerald"
        assert processed.mc_id is not None
        assert processed.mc_type == "book"

    def test_process_book_doc_with_cover(self, service):
        """Test book document processing with cover image."""
        doc = {
            "key": "/works/OL468516W",
            "title": "The Great Gatsby",
            "cover_i": 8235847,
        }

        processed = service._process_book_doc(doc)

        assert processed.cover_available is True
        assert processed.cover_urls is not None
        assert "small" in processed.cover_urls
        assert "medium" in processed.cover_urls
        assert "large" in processed.cover_urls
        assert processed.book_image == processed.cover_urls["medium"]

    def test_process_book_doc_with_isbns(self, service):
        """Test book document processing with ISBNs."""
        doc = {
            "key": "/works/OL468516W",
            "title": "The Great Gatsby",
            "isbn": ["9780743273565", "0743273565", "9780141182636"],
        }

        processed = service._process_book_doc(doc)

        assert processed.primary_isbn13 == "9780743273565"
        assert processed.primary_isbn10 == "0743273565"
        assert processed.isbns is not None
        assert len(processed.isbns) > 0

    def test_process_book_doc_with_subjects(self, service):
        """Test book document processing with subjects."""
        doc = {
            "key": "/works/OL468516W",
            "title": "The Great Gatsby",
            "subject": ["Fiction", "Classic Literature", "American fiction"],
        }

        processed = service._process_book_doc(doc)

        assert processed.subjects is not None
        assert "Fiction" in processed.subjects
        assert len(processed.subjects) <= 10  # Should be limited

    def test_process_book_doc_with_language(self, service):
        """Test book document processing with language."""
        doc = {"key": "/works/OL1W", "title": "Test", "language": ["eng"]}

        processed = service._process_book_doc(doc)
        assert processed.language == "English"

        doc2 = {"key": "/works/OL2W", "title": "Test", "language": ["spa"]}
        processed2 = service._process_book_doc(doc2)
        assert processed2.language == "Spanish"

    def test_calculate_blended_score(self, service):
        """Test blended score calculation."""
        book1 = MCBookItem(
            key="/works/OL1W", title="Book 1", readinglog_count=1000, first_publish_year=2020
        )
        book2 = MCBookItem(
            key="/works/OL2W", title="Book 2", readinglog_count=500, first_publish_year=1950
        )

        score1 = service._calculate_blended_score(book1, 0, 10, 1000)
        score2 = service._calculate_blended_score(book2, 5, 10, 1000)

        # First doc should score higher (more relevant position, more popular, more recent)
        assert score1 > score2

    @pytest.mark.asyncio
    async def test_search_books_basic(
        self, service: OpenLibrarySearchService, mock_search_response
    ):
        """Test basic book search."""
        with patch.object(service, "_make_request", return_value=(mock_search_response, None)):
            response = await service.search_books(query="The Great Gatsby")

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error is None
            assert len(response.results) > 0
            assert "The Great Gatsby" in response.results[0].title
            assert response.results[0].mc_id is not None
            write_snapshot(
                {"docs": [r.model_dump() for r in response.results]},
                "search_books_basic_result.json",
            )

    @pytest.mark.asyncio
    async def test_search_books_by_title(
        self, service: OpenLibrarySearchService, mock_search_response
    ):
        """Test search by title."""
        with patch.object(service, "_make_request", return_value=(mock_search_response, None)):
            response = await service.search_books(title="The Great Gatsby")

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error is None
            assert len(response.results) > 0

    @pytest.mark.asyncio
    async def test_search_books_by_author(
        self, service: OpenLibrarySearchService, mock_search_response
    ):
        """Test search by author."""
        with patch.object(service, "_make_request", return_value=(mock_search_response, None)):
            response = await service.search_books(author="F. Scott Fitzgerald")

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error is None
            assert len(response.results) > 0
            write_snapshot(
                {"docs": [r.model_dump() for r in response.results]},
                "search_books_by_author_result.json",
            )

    @pytest.mark.asyncio
    async def test_search_books_by_isbn(self, service):
        """Test search by ISBN."""
        isbn_response = load_fixture("make_requests/search_isbn.json")

        with patch.object(service, "_make_request", return_value=(isbn_response, None)):
            response = await service.search_books(isbn="9780743273565")

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error is None
            assert len(response.results) > 0

    @pytest.mark.asyncio
    async def test_search_books_with_limit(self, service, mock_search_response):
        """Test search with limit parameter."""
        with patch.object(service, "_make_request", return_value=(mock_search_response, None)):
            response = await service.search_books(query="test", limit=5)

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error is None
            # Limit should be respected in the request params

    @pytest.mark.asyncio
    async def test_search_books_no_params(self, service):
        """Test search without any parameters."""
        response = await service.search_books()

        assert isinstance(response, OpenLibrarySearchResponse)
        assert response.error == "At least one search parameter is required"
        assert len(response.results) == 0

    @pytest.mark.asyncio
    async def test_search_books_api_error(self, service):
        """Test search with API error."""
        with patch.object(service, "_make_request", return_value=({"error": "API error"}, 500)):
            response = await service.search_books(query="test")

            assert isinstance(response, OpenLibrarySearchResponse)
            assert response.error == "API error"  # Error message from result dict
            assert len(response.results) == 0

    @pytest.mark.asyncio
    async def test_search_authors_basic(self, service):
        """Test basic author search."""
        mock_author_response = {
            "docs": [
                {
                    "key": "/authors/OL34184A",
                    "name": "F. Scott Fitzgerald",
                    "top_subjects": ["Fiction", "American literature"],
                    "work_count": 20,
                }
            ],
            "num_found": 1,
        }

        with patch.object(service, "_make_request") as mock_request:
            # Mock the initial search request
            mock_request.side_effect = [
                (mock_author_response, None),  # Search request
                (
                    {"name": "F. Scott Fitzgerald", "bio": "American novelist"},
                    None,
                ),  # Detail request
            ]

            response = await service.search_authors(query="F. Scott Fitzgerald", limit=5)

            assert isinstance(response, OpenLibraryAuthorSearchResponse)
            assert response.error is None
            assert len(response.results) > 0
            # Results are MCAuthorItem objects (Pydantic converts dicts to instances)
            assert response.results[0].name == "F. Scott Fitzgerald"

    @pytest.mark.asyncio
    async def test_search_authors_no_params(self, service):
        """Test author search without parameters."""
        response = await service.search_authors()

        assert isinstance(response, OpenLibraryAuthorSearchResponse)
        assert response.error == "At least one search parameter is required"
        assert len(response.results) == 0

    @pytest.mark.asyncio
    async def test_search_authors_api_error(self, service):
        """Test author search with API error."""
        with patch.object(service, "_make_request", return_value=({"error": "API error"}, 500)):
            response = await service.search_authors(query="test")

            assert isinstance(response, OpenLibraryAuthorSearchResponse)
            assert response.error == "API error"  # Error message from result dict
            assert len(response.results) == 0

    @pytest.mark.asyncio
    async def test_search_authors_no_results(self, service):
        """Test author search with no results."""
        with patch.object(service, "_make_request", return_value=({"docs": []}, None)):
            response = await service.search_authors(query="nonexistent author")

            assert isinstance(response, OpenLibraryAuthorSearchResponse)
            assert response.error is None
            assert len(response.results) == 0


class TestSearchRanking:
    """Tests for search ranking and sorting."""

    @pytest.fixture
    def service(self):
        """Create a search service instance."""
        return OpenLibrarySearchService()

    def test_blended_score_relevance(self, service):
        """Test that relevance affects score."""
        book = MCBookItem(
            key="/works/OL1W", title="Book", readinglog_count=1000, first_publish_year=2020
        )

        # First position should score higher than last position
        score_first = service._calculate_blended_score(book, 0, 10, 1000)
        score_last = service._calculate_blended_score(book, 9, 10, 1000)

        assert score_first > score_last

    def test_blended_score_popularity(self, service):
        """Test that popularity affects score."""
        book_popular = MCBookItem(
            key="/works/OL1W", title="Popular", readinglog_count=1000, first_publish_year=2020
        )
        book_unpopular = MCBookItem(
            key="/works/OL2W", title="Unpopular", readinglog_count=100, first_publish_year=2020
        )

        score_popular = service._calculate_blended_score(book_popular, 0, 10, 1000)
        score_unpopular = service._calculate_blended_score(book_unpopular, 0, 10, 1000)

        assert score_popular > score_unpopular

    def test_blended_score_recency(self, service):
        """Test that recency affects score."""
        book_recent = MCBookItem(
            key="/works/OL1W", title="Recent", readinglog_count=1000, first_publish_year=2020
        )
        book_old = MCBookItem(
            key="/works/OL2W", title="Old", readinglog_count=1000, first_publish_year=1950
        )

        score_recent = service._calculate_blended_score(book_recent, 0, 10, 1000)
        score_old = service._calculate_blended_score(book_old, 0, 10, 1000)

        assert score_recent > score_old
