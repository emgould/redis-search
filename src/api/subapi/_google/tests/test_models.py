"""
Unit tests for Google Books models.
Tests Pydantic model validation and transformations using fixture data.
"""

from pathlib import Path

import pytest

from api.subapi._google.models import (
    GoogleBooksItem,
    GoogleBooksSearchResponse,
    GoogleBooksVolumeRaw,
)
from api.subapi._google.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestGoogleBooksItem:
    """Tests for GoogleBooksItem model."""

    def test_create_from_search_fixture(self):
        """Test creating GoogleBooksItem from search fixture data."""
        # Load a search result fixture
        search_fixture = load_fixture("search/search_books_by_title.json")

        # Get first item
        first_item_data = search_fixture["items"][0]

        # Create model from fixture
        item = GoogleBooksItem.model_validate(first_item_data)

        assert item.google_id
        assert item.title
        assert item.mc_id
        assert item.mc_type == "book"
        assert item.key.startswith("/works/GOOGLE_")

    def test_item_has_required_fields(self):
        """Test that GoogleBooksItem has all required fields from fixture."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        item_data = search_fixture["items"][0]

        item = GoogleBooksItem.model_validate(item_data)

        # Core fields
        assert item.google_id
        assert item.title
        assert item.key

        # Auto-generated fields
        assert item.mc_id is not None
        assert item.mc_type == "book"

        # Author fields
        assert item.author_name
        assert len(item.author_name) > 0

    def test_item_with_isbn(self):
        """Test GoogleBooksItem with ISBN data from search results."""
        search_fixture = load_fixture("search/search_books_by_title.json")

        # Find an item with ISBN data
        item_with_isbn = None
        for item_data in search_fixture["items"]:
            if (
                item_data.get("primary_isbn13")
                or item_data.get("primary_isbn10")
                or item_data.get("isbn")
            ):
                item_with_isbn = GoogleBooksItem.model_validate(item_data)
                break

        # If we found one with ISBN, test it
        if item_with_isbn:
            assert item_with_isbn.google_id
            assert item_with_isbn.title
            assert (
                item_with_isbn.primary_isbn13
                or item_with_isbn.primary_isbn10
                or len(item_with_isbn.isbn) > 0
            )

    def test_mc_id_generation_from_fixture(self):
        """Test that mc_id is auto-generated from real data."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        item_data = search_fixture["items"][0]

        item = GoogleBooksItem.model_validate(item_data)

        assert item.mc_id is not None
        assert item.mc_type == "book"
        # mc_id should contain either ISBN or google_id
        assert (
            (item.primary_isbn13 and item.primary_isbn13 in item.mc_id)
            or (item.primary_isbn10 and item.primary_isbn10 in item.mc_id)
            or item.google_id in item.mc_id
        )

    def test_item_cover_images(self):
        """Test that cover image fields are properly populated."""
        search_fixture = load_fixture("search/search_books_by_title.json")

        # Find an item with cover images
        item_with_cover = None
        for item_data in search_fixture["items"]:
            if item_data.get("cover_available"):
                item_with_cover = GoogleBooksItem.model_validate(item_data)
                break

        if item_with_cover:
            assert item_with_cover.cover_available is True
            assert item_with_cover.cover_urls
            assert "medium" in item_with_cover.cover_urls or "large" in item_with_cover.cover_urls

    def test_item_openlibrary_compatibility(self):
        """Test OpenLibrary compatibility fields."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        item_data = search_fixture["items"][0]

        item = GoogleBooksItem.model_validate(item_data)

        # OpenLibrary compatibility fields
        assert item.key
        assert item.openlibrary_key
        assert item.rank == 0
        assert item.rank_last_week == 0
        assert item.weeks_on_list == 0
        assert item.price == "0.00"


class TestGoogleBooksSearchResponse:
    """Tests for GoogleBooksSearchResponse model."""

    def test_create_from_search_fixture(self):
        """Test creating GoogleBooksSearchResponse from search fixture."""
        search_fixture = load_fixture("search/search_books_by_title.json")

        # Create model from fixture
        response = GoogleBooksSearchResponse.model_validate(search_fixture)

        assert response.totalItems > 0
        assert len(response.items) > 0
        assert len(response.docs) == len(response.items)
        assert response.num_found == response.totalItems
        assert response.data_source == "Google Books API"

    def test_search_response_items_validation(self):
        """Test that all items in search response are valid GoogleBooksItems."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        response = GoogleBooksSearchResponse.model_validate(search_fixture)

        # All items should be GoogleBooksItem instances
        for item in response.items:
            assert isinstance(item, GoogleBooksItem)
            assert item.google_id
            assert item.title
            # mc_id should be generated (may be None if validator didn't run, but type should be set)
            assert item.mc_type == "book"

    def test_docs_items_sync(self):
        """Test that docs and items are synchronized."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        response = GoogleBooksSearchResponse.model_validate(search_fixture)

        # docs should be auto-populated from items
        assert len(response.docs) == len(response.items)
        for i, doc in enumerate(response.docs):
            assert doc == response.items[i]

    def test_search_response_metadata(self):
        """Test search response metadata fields."""
        search_fixture = load_fixture("search/search_books_by_title.json")
        response = GoogleBooksSearchResponse.model_validate(search_fixture)

        assert response.kind == "books#volumes"
        assert response.totalItems > 0
        assert response.num_found == response.totalItems
        assert response.query is not None
        assert response.data_source == "Google Books API"

    def test_general_search_response(self):
        """Test with general search fixture (different query)."""
        search_fixture = load_fixture("search/search_books_general.json")
        response = GoogleBooksSearchResponse.model_validate(search_fixture)

        assert response.totalItems > 0
        assert len(response.items) > 0
        assert all(isinstance(item, GoogleBooksItem) for item in response.items)

    def test_direct_search_response(self):
        """Test with direct search fixture."""
        search_fixture = load_fixture("search/search_direct.json")

        # Direct search returns list of dicts, not a search response
        assert isinstance(search_fixture, list)
        assert len(search_fixture) > 0

        # Each item should be convertible to GoogleBooksItem
        for item_data in search_fixture:
            item = GoogleBooksItem.model_validate(item_data)
            assert item.google_id
            assert item.title


class TestGoogleBooksVolumeRaw:
    """Tests for GoogleBooksVolumeRaw model."""

    def test_create_from_api_response(self):
        """Test creating GoogleBooksVolumeRaw from raw API response."""
        # Load raw API response
        volume_fixture = load_fixture("make_requests/get_volume_by_id.json")

        # Create model from fixture
        volume = GoogleBooksVolumeRaw.model_validate(volume_fixture)

        assert volume.kind == "books#volume"
        assert volume.id
        assert volume.volumeInfo.title
        assert volume.volumeInfo.authors
        assert len(volume.volumeInfo.authors) > 0

    def test_volume_info_fields(self):
        """Test that volumeInfo contains expected fields."""
        volume_fixture = load_fixture("make_requests/get_volume_by_id.json")
        volume = GoogleBooksVolumeRaw.model_validate(volume_fixture)

        # Check volumeInfo fields
        assert volume.volumeInfo.title
        assert volume.volumeInfo.authors
        assert volume.volumeInfo.publishedDate
        assert volume.volumeInfo.description

        # Check ISBNs
        if volume.volumeInfo.industryIdentifiers:
            assert len(volume.volumeInfo.industryIdentifiers) > 0
            first_isbn = volume.volumeInfo.industryIdentifiers[0]
            assert first_isbn.type in ["ISBN_10", "ISBN_13"]
            assert first_isbn.identifier

    def test_image_links(self):
        """Test image links parsing."""
        volume_fixture = load_fixture("make_requests/get_volume_by_id.json")
        volume = GoogleBooksVolumeRaw.model_validate(volume_fixture)

        if volume.volumeInfo.imageLinks:
            # Should have at least thumbnail
            assert volume.volumeInfo.imageLinks.thumbnail is not None

    def test_sale_info(self):
        """Test sale info parsing."""
        volume_fixture = load_fixture("make_requests/get_volume_by_id.json")
        volume = GoogleBooksVolumeRaw.model_validate(volume_fixture)

        if volume.saleInfo:
            assert volume.saleInfo.country is not None
            assert volume.saleInfo.saleability is not None

    def test_volume_by_isbn_response(self):
        """Test parsing volume by ISBN response."""
        # This returns a search result with items array
        isbn_fixture = load_fixture("make_requests/get_volume_by_isbn.json")

        assert "items" in isbn_fixture
        assert len(isbn_fixture["items"]) > 0

        # First item should be a valid volume
        first_volume = GoogleBooksVolumeRaw.model_validate(isbn_fixture["items"][0])
        assert first_volume.kind == "books#volume"
        assert first_volume.id
        assert first_volume.volumeInfo.title

    def test_volume_metadata_fields(self):
        """Test volume metadata fields."""
        volume_fixture = load_fixture("make_requests/get_volume_by_id.json")
        volume = GoogleBooksVolumeRaw.model_validate(volume_fixture)

        # Check metadata
        assert volume.id
        assert volume.selfLink
        assert volume.kind == "books#volume"

        # Check volume info has key fields
        assert volume.volumeInfo.title
        assert volume.volumeInfo.authors
        # Publisher and publishedDate are optional
        assert hasattr(volume.volumeInfo, "publisher")
        assert hasattr(volume.volumeInfo, "publishedDate")

    def test_search_result_volumes(self):
        """Test parsing volumes from search results."""
        search_fixture = load_fixture("make_requests/search_books_by_title.json")

        assert "items" in search_fixture
        assert len(search_fixture["items"]) > 0

        # All items should be valid volumes
        for item_data in search_fixture["items"]:
            volume = GoogleBooksVolumeRaw.model_validate(item_data)
            assert volume.kind == "books#volume"
            assert volume.id
            assert volume.volumeInfo.title
            assert volume.volumeInfo.authors
