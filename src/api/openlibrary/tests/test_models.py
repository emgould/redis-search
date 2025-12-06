"""
Tests for OpenLibrary Pydantic models.
"""

import pytest
from pydantic import ValidationError

from api.openlibrary.models import (
    BookSearchResponse,
    CoverUrlsResponse,
    MCBookItem,
)
from contracts.models import MCType


class TestMCBookItem:
    """Tests for MCBookItem model."""

    def test_book_item_minimal(self):
        """Test MCBookItem with minimal required fields."""
        book = MCBookItem(key="/works/OL123456W", title="Test Book")

        assert book.key == "/works/OL123456W"
        assert book.title == "Test Book"
        assert book.author_name == []
        assert book.isbn == []

    def test_book_item_full(self):
        """Test MCBookItem with all fields."""
        book = MCBookItem(
            key="/works/OL468516W",
            title="The Great Gatsby",
            openlibrary_key="/works/OL468516W",
            openlibrary_url="https://openlibrary.org/works/OL468516W",
            author_name=["F. Scott Fitzgerald"],
            author="F. Scott Fitzgerald",
            isbn=["9780743273565", "0743273565"],
            primary_isbn13="9780743273565",
            primary_isbn10="0743273565",
            first_publish_year=1925,
            publisher="Scribner",
            description="A classic American novel",
            cover_i=8235847,
            cover_available=True,
            subject=["Fiction", "Classic Literature"],
            number_of_pages=180,
            ratings_average=3.9,
            ratings_count=2456,
        )

        assert book.title == "The Great Gatsby"
        assert book.author_name == ["F. Scott Fitzgerald"]
        assert book.first_publish_year == 1925
        assert book.cover_available is True

    def test_mc_id_generation(self):
        """Test automatic mc_id generation."""
        book = MCBookItem(
            key="/works/OL468516W",
            title="The Great Gatsby",
            primary_isbn13="9780743273565",
        )

        assert book.mc_id is not None
        assert book.mc_type == MCType.BOOK.value
        assert "9780743273565" in book.mc_id or "OL468516W" in book.mc_id

    def test_mc_id_with_openlibrary_key(self):
        """Test mc_id generation with OpenLibrary key."""
        book = MCBookItem(
            key="/works/OL468516W",
            title="Test Book",
            openlibrary_key="/works/OL468516W",
        )

        assert book.mc_id is not None
        assert book.mc_type == MCType.BOOK.value

    def test_mc_id_with_isbn(self):
        """Test mc_id generation with ISBN."""
        book = MCBookItem(
            key="/works/OL123W",
            title="Test Book",
            primary_isbn13="9780743273565",
            primary_isbn10="0743273565",
        )

        assert book.mc_id is not None
        # mc_id will use openlibrary_key from key field if ISBNs aren't in the dict passed to generate_mc_id
        assert "OL123W" in book.mc_id or "9780743273565" in book.mc_id or "0743273565" in book.mc_id

    def test_model_dump(self):
        """Test model serialization."""
        book = MCBookItem(
            key="/works/OL468516W",
            title="The Great Gatsby",
            author_name=["F. Scott Fitzgerald"],
        )

        data = book.model_dump()
        assert isinstance(data, dict)
        assert data["title"] == "The Great Gatsby"
        assert data["mc_id"] is not None
        assert data["mc_type"] == "book"

    def test_model_dump_json(self):
        """Test JSON serialization."""
        book = MCBookItem(
            key="/works/OL468516W",
            title="The Great Gatsby",
            author_name=["F. Scott Fitzgerald"],
        )

        json_str = book.model_dump_json()
        assert isinstance(json_str, str)
        assert "The Great Gatsby" in json_str
        assert "mc_id" in json_str


class TestBookSearchResponse:
    """Tests for BookSearchResponse model."""

    def test_search_response_empty(self):
        """Test empty search response."""
        response = BookSearchResponse(docs=[], num_found=0)

        assert response.docs == []
        assert response.num_found == 0
        assert response.offset == 0

    def test_search_response_with_books(self):
        """Test search response with books."""
        books = [
            MCBookItem(key="/works/OL1W", title="Book 1"),
            MCBookItem(key="/works/OL2W", title="Book 2"),
        ]
        response = BookSearchResponse(docs=books, num_found=2, offset=0, query="test")

        assert len(response.docs) == 2
        assert response.num_found == 2
        assert response.query == "test"

    def test_search_response_validation(self):
        """Test search response validation."""
        # Should accept valid data
        response = BookSearchResponse(
            docs=[MCBookItem(key="/works/OL1W", title="Test")], num_found=1
        )
        assert response.num_found == 1

    def test_model_dump(self):
        """Test response serialization."""
        response = BookSearchResponse(
            docs=[MCBookItem(key="/works/OL1W", title="Test")], num_found=1, query="test"
        )

        data = response.model_dump()
        assert isinstance(data, dict)
        assert data["num_found"] == 1
        assert len(data["docs"]) == 1


class TestCoverUrlsResponse:
    """Tests for CoverUrlsResponse model."""

    def test_cover_urls_available(self):
        """Test cover URLs when available."""
        response = CoverUrlsResponse(
            identifier={"type": "isbn", "value": "9780743273565"},
            covers_available=True,
            cover_urls={
                "small": "https://covers.openlibrary.org/b/isbn/9780743273565-S.jpg",
                "medium": "https://covers.openlibrary.org/b/isbn/9780743273565-M.jpg",
                "large": "https://covers.openlibrary.org/b/isbn/9780743273565-L.jpg",
            },
        )

        assert response.covers_available is True
        assert response.cover_urls is not None
        assert "small" in response.cover_urls
        assert "medium" in response.cover_urls
        assert "large" in response.cover_urls

    def test_cover_urls_not_available(self):
        """Test cover URLs when not available."""
        response = CoverUrlsResponse(
            identifier={"type": "isbn", "value": "1234567890"},
            covers_available=False,
            cover_urls=None,
        )

        assert response.covers_available is False
        assert response.cover_urls is None


class TestModelIntegration:
    """Integration tests for models working together."""

    def test_book_item_in_search_response(self):
        """Test MCBookItem within BookSearchResponse."""
        books = [
            MCBookItem(
                key="/works/OL468516W",
                title="The Great Gatsby",
                author_name=["F. Scott Fitzgerald"],
                primary_isbn13="9780743273565",
            )
        ]
        response = BookSearchResponse(docs=books, num_found=1, query="gatsby")

        assert len(response.docs) == 1
        assert response.docs[0].mc_id is not None
        assert response.docs[0].mc_type == "book"
