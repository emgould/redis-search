"""
Tests for TVDB Pydantic models.

These tests verify that our Pydantic models can correctly parse and validate
real TVDB API response data.
"""

import pytest
from pydantic import ValidationError

from api.subapi._tvdb.models import (
    TVDBImageData,
    TVDBSearchResponse,
    TVDBSearchResult,
    TVDBShow,
)


class TestTVDBModels:
    """Test TVDB Pydantic models."""

    def test_tvdb_search_result_valid(self):
        """Test TVDBSearchResult with valid data."""
        data = {
            "id": 12345,
            "name": "Test Show",
            "overview": "A test show overview",
            "first_air_date": "2020-01-01",
            "status": "Continuing",
            "network": "Test Network",
            "original_language": "en",
            "score": 8.5,
        }
        result = TVDBSearchResult(**data)
        assert result.id == 12345
        assert result.name == "Test Show"
        assert result.score == 8.5

    def test_tvdb_search_result_minimal(self):
        """Test TVDBSearchResult with minimal required fields."""
        data = {
            "id": 12345,
            "name": "Test Show",
        }
        result = TVDBSearchResult(**data)
        assert result.id == 12345
        assert result.name == "Test Show"
        assert result.overview == ""  # Default value
        assert result.status == "Unknown"  # Default value

    def test_tvdb_search_response(self):
        """Test TVDBSearchResponse."""
        data = {
            "shows": [
                {"id": 1, "name": "Show 1"},
                {"id": 2, "name": "Show 2"},
            ],
            "total_count": 2,
            "query": "test",
        }
        response = TVDBSearchResponse(**data)
        assert len(response.shows) == 2
        assert response.total_count == 2
        assert response.query == "test"

    def test_tvdb_show_basic(self):
        """Test TVDBShow with basic data."""
        data = {
            "id": 12345,
            "tvdb_id": 12345,
            "name": "Test Show",
            "overview": "Test overview",
        }
        show = TVDBShow(**data)
        assert show.id == 12345
        assert show.tvdb_id == 12345
        assert show.name == "Test Show"
        assert show.network == "Unknown"  # Default value

    def test_tvdb_show_extended(self):
        """Test TVDBShow with extended data."""
        data = {
            "id": 12345,
            "tvdb_id": 12345,
            "name": "Test Show",
            "slug": "test-show",
            "overview": "Test overview",
            "year": 2020,
            "score": 8.5,
            "first_aired": "2020-01-01",
            "status": "Continuing",
            "genres": ["Drama", "Comedy"],
            "seasons_count": 5,
            "episodes_count": 50,
            "external_ids": {"imdb": "tt1234567"},
        }
        show = TVDBShow(**data)
        assert show.id == 12345
        assert show.year == 2020
        assert len(show.genres) == 2
        assert show.seasons_count == 5
        assert show.external_ids["imdb"] == "tt1234567"

    def test_tvdb_image_data(self):
        """Test TVDBImageData."""
        data = {
            "tvdbid": 12345,
            "platform": "Test Network",
            "show_name": "Test Show",
            "poster": "https://example.com/poster.jpg",
            "logo": "https://example.com/logo.png",
        }
        image_data = TVDBImageData(**data)
        assert image_data.tvdbid == 12345
        assert image_data.poster == "https://example.com/poster.jpg"
        assert image_data.logo == "https://example.com/logo.png"

    def test_tvdb_image_data_minimal(self):
        """Test TVDBImageData with minimal data."""
        data = {}
        image_data = TVDBImageData(**data)
        assert image_data.tvdbid is None
        assert image_data.platform == "Unknown"
        assert image_data.poster is None

    def test_tvdb_show_with_tmdb_data(self):
        """Test TVDBShow with TMDB enrichment data."""
        data = {
            "id": 12345,
            "tvdb_id": 12345,
            "name": "Test Show",
            "tmdb_id": 67890,
            "tmdb_popularity": 100.5,
            "tmdb_vote_average": 8.5,
            "tmdb_vote_count": 1000,
            "watch_providers": {"primary_provider": {"provider_name": "Netflix"}},
            "streaming_platform": "Netflix",
        }
        show = TVDBShow(**data)
        assert show.tmdb_id == 67890
        assert show.tmdb_popularity == 100.5
        assert show.streaming_platform == "Netflix"

    def test_tvdb_show_validation_error(self):
        """Test TVDBShow validation error for missing required fields."""
        data = {
            "name": "Test Show",
            # Missing required 'id' and 'tvdb_id'
        }
        with pytest.raises(ValidationError):
            TVDBShow(**data)


# Integration tests that use real fixture data
class TestTVDBModelsWithFixtures:
    """Test TVDB models with real fixture data."""

    @pytest.mark.skip(reason="Requires fixture data - run after seeding")
    def test_search_result_from_fixture(self, load_fixture):
        """Test parsing a search result from fixture data."""
        fixture_data = load_fixture("fixtures/models/tvdb_search_result.json")
        result = TVDBSearchResult(**fixture_data)
        assert result.id is not None
        assert result.name is not None

    @pytest.mark.skip(reason="Requires fixture data - run after seeding")
    def test_show_from_fixture(self, load_fixture):
        """Test parsing a show from fixture data."""
        fixture_data = load_fixture("fixtures/models/tvdb_show.json")
        show = TVDBShow(**fixture_data)
        assert show.id is not None
        assert show.name is not None

    @pytest.mark.skip(reason="Requires fixture data - run after seeding")
    def test_image_data_from_fixture(self, load_fixture):
        """Test parsing image data from fixture."""
        fixture_data = load_fixture("fixtures/models/tvdb_image_data.json")
        image_data = TVDBImageData(**fixture_data)
        assert image_data.tvdbid is not None
