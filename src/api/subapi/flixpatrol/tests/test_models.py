"""
Unit tests for FlixPatrol Models.
Tests Pydantic model validation and serialization.
"""

import pytest
from pydantic import ValidationError

from api.subapi.flixpatrol.models import (
    FlixPatrolMediaItem,
    FlixPatrolMetadata,
    FlixPatrolParsedData,
    FlixPatrolPlatformData,
    FlixPatrolResponse,
)

pytestmark = pytest.mark.unit


class TestFlixPatrolMediaItem:
    """Tests for FlixPatrolMediaItem model."""

    def test_media_item_with_minimal_data(self):
        """Test media item with minimal required data."""
        item_data = {"rank": 1, "title": "Test Show", "score": 1000}

        item = FlixPatrolMediaItem(**item_data)

        assert item.rank == 1
        assert item.title == "Test Show"
        assert item.score == 1000
        assert item.platform is None
        assert item.content_type is None

    def test_media_item_with_full_data(self):
        """Test media item with complete data."""
        item_data = {
            "rank": 1,
            "title": "Breaking Bad",
            "score": 5000,
            "platform": "netflix",
            "content_type": "tv",
        }

        item = FlixPatrolMediaItem(**item_data)

        assert item.rank == 1
        assert item.title == "Breaking Bad"
        assert item.score == 5000
        assert item.platform == "netflix"
        assert item.content_type == "tv"

    def test_media_item_missing_required_fields(self):
        """Test that media item requires essential fields."""
        with pytest.raises(ValidationError):
            FlixPatrolMediaItem(rank=1)  # Missing title and score


class TestFlixPatrolPlatformData:
    """Tests for FlixPatrolPlatformData model."""

    def test_platform_data_with_full_data(self):
        """Test platform data with complete data."""
        platform_data = {
            "platform": "netflix",
            "shows": [
                {"rank": 1, "title": "Test Show 1", "score": 1000},
                {"rank": 2, "title": "Test Show 2", "score": 900},
            ],
            "movies": [
                {"rank": 1, "title": "Test Movie 1", "score": 2000},
                {"rank": 2, "title": "Test Movie 2", "score": 1800},
            ],
        }

        platform = FlixPatrolPlatformData(**platform_data)

        assert platform.platform == "netflix"
        assert len(platform.shows) == 2
        assert len(platform.movies) == 2
        assert platform.shows[0].title == "Test Show 1"
        assert platform.movies[0].title == "Test Movie 1"

    def test_platform_data_with_minimal_data(self):
        """Test platform data with minimal data."""
        platform_data = {"platform": "hbo"}

        platform = FlixPatrolPlatformData(**platform_data)

        assert platform.platform == "hbo"
        assert platform.shows == []
        assert platform.movies == []


class TestFlixPatrolMetadata:
    """Tests for FlixPatrolMetadata model."""

    def test_metadata_with_defaults(self):
        """Test metadata with default values."""
        metadata = FlixPatrolMetadata()

        assert metadata.source == "FlixPatrol"
        assert metadata.total_shows == 0
        assert metadata.total_movies == 0
        assert metadata.platforms == []

    def test_metadata_with_custom_values(self):
        """Test metadata with custom values."""
        metadata = FlixPatrolMetadata(
            source="Custom Source",
            total_shows=10,
            total_movies=20,
            platforms=["netflix", "hbo"],
        )

        assert metadata.source == "Custom Source"
        assert metadata.total_shows == 10
        assert metadata.total_movies == 20
        assert metadata.platforms == ["netflix", "hbo"]


class TestFlixPatrolResponse:
    """Tests for FlixPatrolResponse model."""

    def test_response_with_full_data(self):
        """Test response with complete data."""
        response_data = {
            "date": "2024-01-01",
            "shows": {
                "netflix": [{"rank": 1, "title": "Test Show 1", "score": 1000}],
                "hbo": [{"rank": 1, "title": "Test Show 2", "score": 900}],
            },
            "movies": {
                "netflix": [{"rank": 1, "title": "Test Movie 1", "score": 2000}],
                "hbo": [{"rank": 1, "title": "Test Movie 2", "score": 1800}],
            },
            "top_trending_tv_shows": [{"rank": 1, "title": "Test Show 1", "score": 1000}],
            "top_trending_movies": [{"rank": 1, "title": "Test Movie 1", "score": 2000}],
            "metadata": {
                "source": "FlixPatrol",
                "total_shows": 2,
                "total_movies": 2,
                "platforms": ["netflix", "hbo"],
            },
        }

        response = FlixPatrolResponse(**response_data)

        assert response.date == "2024-01-01"
        assert "netflix" in response.shows
        assert "hbo" in response.shows
        assert len(response.top_trending_tv_shows) == 1
        assert len(response.top_trending_movies) == 1
        assert response.metadata is not None
        assert response.metadata.total_shows == 2

    def test_response_with_minimal_data(self):
        """Test response with minimal data."""
        response_data = {"date": "2024-01-01"}

        response = FlixPatrolResponse(**response_data)

        assert response.date == "2024-01-01"
        assert response.shows == {}
        assert response.movies == {}
        assert response.top_trending_tv_shows == []
        assert response.top_trending_movies == []
        assert response.metadata is None


class TestFlixPatrolParsedData:
    """Tests for FlixPatrolParsedData model."""

    def test_parsed_data_with_full_data(self):
        """Test parsed data with complete data."""
        parsed_data = {
            "date": "2024-01-01",
            "shows": {
                "netflix": [{"rank": 1, "title": "Test Show 1", "score": 1000}],
            },
            "movies": {
                "netflix": [{"rank": 1, "title": "Test Movie 1", "score": 2000}],
            },
        }

        parsed = FlixPatrolParsedData(**parsed_data)

        assert parsed.date == "2024-01-01"
        assert "netflix" in parsed.shows
        assert "netflix" in parsed.movies

    def test_parsed_data_with_minimal_data(self):
        """Test parsed data with minimal data."""
        parsed_data = {"date": "2024-01-01"}

        parsed = FlixPatrolParsedData(**parsed_data)

        assert parsed.date == "2024-01-01"
        assert parsed.shows == {}
        assert parsed.movies == {}
