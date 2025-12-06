"""
Tests for TVDB core service.

These tests verify the TVDBService class methods using mocked responses
and real fixture data.
"""

from unittest.mock import MagicMock, patch

import pytest

from api.subapi._tvdb.core import TVDBService


class TestTVDBServiceInit:
    """Test TVDBService initialization."""

    def test_init_without_api_key(self):
        """Test initialization fails without API key."""
        with pytest.raises(ValueError, match="TVDB API key is required"):
            TVDBService("")

    @patch("api.subapi._tvdb.core.TVDB")
    def test_init_with_api_key(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test successful initialization with API key."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = [
            {"id": 1, "name": "Banner"},
            {"id": 2, "name": "Poster"},
        ]
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        assert service.api_key == mock_tvdb_api_key
        assert service.client == mock_client
        assert len(service.art_type_map) == 2


class TestTVDBServiceSearch:
    """Test TVDBService search method."""

    @patch("api.subapi._tvdb.core.TVDB")
    def test_search_empty_query(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test search with empty query raises ValueError."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)

        with pytest.raises(ValueError, match="Search query cannot be empty"):
            service.search("")

    @patch("api.subapi._tvdb.core.TVDB")
    def test_search_returns_list(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test search with list response."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.search.return_value = [
            {
                "id": 12345,
                "name": "Test Show",
                "overview": "Test overview",
                "first_air_time": "2020-01-01",
                "status": "Continuing",
                "network": "Test Network",
                "primary_language": "en",
                "score": 8.5,
            }
        ]
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        results = service.search("test", limit=10)

        assert len(results) == 1
        assert results[0]["id"] == 12345
        assert results[0]["name"] == "Test Show"

    @patch("api.subapi._tvdb.core.TVDB")
    def test_search_returns_dict(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test search with dict response."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.search.return_value = {
            "data": [
                {
                    "tvdb_id": 12345,
                    "name": "Test Show",
                    "overview": "Test overview",
                }
            ]
        }
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        results = service.search("test", limit=10)

        assert len(results) == 1
        assert results[0]["id"] == 12345


class TestTVDBServiceShowDetails:
    """Test TVDBService get_show_details method."""

    @patch("api.subapi._tvdb.core.TVDB")
    def test_get_show_details_basic(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test getting basic show details."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.get_series.return_value = {
            "id": 12345,
            "name": "Test Show",
            "slug": "test-show",
            "overview": "Test overview",
            "year": 2020,
            "score": 8.5,
            "genres": [{"name": "Drama"}],
        }
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.get_show_details(12345, extended=False)

        assert result is not None
        assert result["id"] == 12345
        assert result["name"] == "Test Show"
        assert len(result["genres"]) == 1

    @patch("api.subapi._tvdb.core.TVDB")
    def test_get_show_details_extended(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test getting extended show details."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.get_series_extended.return_value = {
            "id": 12345,
            "name": "Test Show",
            "slug": "test-show",
            "overview": "Test overview",
            "year": 2020,
            "score": 8.5,
            "genres": [{"name": "Drama"}, {"name": "Comedy"}],
            "contentRatings": [{"name": "TV-14", "country": "USA"}],
            "seasons": [{"id": 1, "number": 1, "name": "Season 1"}],
            "episodes": [{"id": 1, "name": "Episode 1"}],
            "remoteIds": [{"sourceName": "IMDB", "id": "tt1234567"}],
            "tags": [{"id": 1, "name": "test", "tagName": "Test"}],
        }
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.get_show_details(12345, extended=True)

        assert result is not None
        assert len(result["genres"]) == 2
        assert len(result["content_ratings"]) == 1
        assert result["seasons_count"] == 1
        assert result["episodes_count"] == 1
        assert "imdb" in result["external_ids"]

    @patch("api.subapi._tvdb.core.TVDB")
    def test_get_show_details_not_found(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test getting details for non-existent show."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.get_series.return_value = None
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.get_show_details(99999, extended=False)

        assert result is None


class TestTVDBServiceImages:
    """Test TVDBService image methods."""

    @patch("api.subapi._tvdb.core.TVDB")
    def test_get_show_images_with_tvdb_id(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test getting show images with TVDB ID."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = [
            {"id": 2, "name": "Poster"},
            {"id": 23, "name": "Logo"},
        ]
        mock_client.get_series.return_value = {
            "id": 12345,
            "name": "Test Show",
            "network": "Test Network",
        }
        mock_client.get_series_extended.return_value = {
            "artworks": [
                {
                    "type": 2,
                    "image": "https://example.com/poster.jpg",
                    "thumbnail": "https://example.com/poster_thumb.jpg",
                    "language": "eng",
                    "score": 10,
                    "width": 1000,
                    "height": 1500,
                },
                {
                    "type": 23,
                    "image": "https://example.com/logo.png",
                    "thumbnail": "https://example.com/logo_thumb.png",
                    "language": "eng",
                    "score": 9,
                    "width": 500,
                    "height": 200,
                },
            ]
        }
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.get_show_images(
            query="", tvdb_id=12345, lang="eng", image_types=["poster", "logo"]
        )

        assert result["tvdbid"] == 12345
        assert result["show_name"] == "Test Show"
        assert result["poster"] == "https://example.com/poster.jpg"
        assert result["logo"] == "https://example.com/logo.png"

    @patch("api.subapi._tvdb.core.TVDB")
    def test_get_all_images(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test getting all images for a show."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = [
            {"id": 2, "name": "Poster"},
            {"id": 23, "name": "Logo"},
        ]
        mock_client.get_series_extended.return_value = {
            "artworks": [
                {
                    "id": 1,
                    "type": 2,
                    "image": "https://example.com/poster1.jpg",
                    "thumbnail": "https://example.com/poster1_thumb.jpg",
                    "language": "eng",
                    "score": 10,
                    "width": 1000,
                    "height": 1500,
                    "includesText": False,
                },
                {
                    "id": 2,
                    "type": 2,
                    "image": "https://example.com/poster2.jpg",
                    "thumbnail": "https://example.com/poster2_thumb.jpg",
                    "language": "eng",
                    "score": 8,
                    "width": 1000,
                    "height": 1500,
                    "includesText": False,
                },
            ]
        }
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.get_all_images(12345, lang="eng")

        assert "Poster" in result
        assert len(result["Poster"]) == 2
        # Should be sorted by score descending
        assert result["Poster"][0]["score"] == 10
        assert result["Poster"][1]["score"] == 8


class TestTVDBServiceExternalId:
    """Test TVDBService search_by_external_id method."""

    @patch("api.subapi._tvdb.core.TVDB")
    def test_search_by_external_id(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test searching by external ID."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.search_by_remote_id.return_value = [
            {
                "tvdb_id": 12345,
                "name": "Test Show",
                "overview": "Test overview",
                "first_air_time": "2020-01-01",
                "status": "Continuing",
                "network": "Test Network",
            }
        ]
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.search_by_external_id("tt1234567", source="imdb")

        assert result is not None
        assert result["id"] == 12345
        assert result["external_id"] == "tt1234567"
        assert result["external_source"] == "imdb"

    @patch("api.subapi._tvdb.core.TVDB")
    def test_search_by_external_id_not_found(self, mock_tvdb_class, mock_tvdb_api_key):
        """Test searching by external ID with no results."""
        mock_client = MagicMock()
        mock_client.get_artwork_types.return_value = []
        mock_client.search_by_remote_id.return_value = []
        mock_tvdb_class.return_value = mock_client

        service = TVDBService(mock_tvdb_api_key)
        result = service.search_by_external_id("tt9999999", source="imdb")

        assert result is None


# Integration tests that use real fixture data
class TestTVDBServiceWithFixtures:
    """Test TVDB service with real fixture data."""

    @pytest.mark.skip(reason="Requires fixture data - run after seeding")
    def test_search_from_fixture(self, load_fixture, mock_tvdb_api_key):
        """Test search using fixture data."""
        fixture_data = load_fixture("fixtures/core/search_shows.json")
        # This would test with real fixture data
        assert fixture_data is not None

    @pytest.mark.skip(reason="Requires fixture data - run after seeding")
    def test_show_details_from_fixture(self, load_fixture, mock_tvdb_api_key):
        """Test show details using fixture data."""
        fixture_data = load_fixture("fixtures/core/get_show_details_extended.json")
        # This would test with real fixture data
        assert fixture_data is not None
