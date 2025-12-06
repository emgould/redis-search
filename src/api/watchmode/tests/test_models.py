"""
Unit tests for Watchmode Pydantic models.

Tests model validation and serialization.
"""

import pytest
from pydantic import ValidationError

from api.watchmode.models import (
    WatchmodeRelease,
    WatchmodeSearchResponse,
    WatchmodeSearchResult,
    WatchmodeStreamingSource,
    WatchmodeTitleDetails,
    WatchmodeTitleDetailsResponse,
    WatchmodeWhatsNewResponse,
)


class TestWatchmodeStreamingSource:
    """Test WatchmodeStreamingSource model."""

    def test_valid_source(self):
        """Test creating a valid streaming source."""
        source = WatchmodeStreamingSource(
            source_id=1,
            name="Netflix",
            type="subscription",
            region="US",
        )
        assert source.source_id == 1
        assert source.name == "Netflix"
        assert source.type == "subscription"
        assert source.region == "US"

    def test_source_with_optional_fields(self):
        """Test streaming source with optional fields."""
        source = WatchmodeStreamingSource(
            source_id=1,
            name="iTunes",
            type="purchase",
            region="US",
            price=9.99,
            format="HD",
        )
        assert source.price == 9.99
        assert source.format == "HD"


class TestWatchmodeRelease:
    """Test WatchmodeRelease model."""

    def test_valid_release(self):
        """Test creating a valid release."""
        release = WatchmodeRelease(
            id=123,
            title="Test Movie",
            type="movie",
        )
        assert release.id == 123
        assert release.title == "Test Movie"
        assert release.type == "movie"

    def test_release_with_tmdb_id(self):
        """Test release with TMDB ID."""
        release = WatchmodeRelease(
            id=123,
            tmdb_id=456,
            title="Test Movie",
            type="movie",
        )
        assert release.tmdb_id == 456


class TestWatchmodeTitleDetails:
    """Test WatchmodeTitleDetails model."""

    def test_valid_title_details(self):
        """Test creating valid title details."""
        details = WatchmodeTitleDetails(
            id=123,
            title="Test Movie",
            type="movie",
        )
        assert details.id == 123
        assert details.title == "Test Movie"
        assert details.type == "movie"

    def test_title_details_with_ratings(self):
        """Test title details with rating information."""
        details = WatchmodeTitleDetails(
            id=123,
            title="Test Movie",
            type="movie",
            user_rating=8.5,
            critic_score=85,
            us_rating="PG-13",
        )
        assert details.user_rating == 8.5
        assert details.critic_score == 85
        assert details.us_rating == "PG-13"


class TestWatchmodeSearchResult:
    """Test WatchmodeSearchResult model."""

    def test_valid_search_result(self):
        """Test creating a valid search result."""
        result = WatchmodeSearchResult(
            id=123,
            name="Test Show",
            type="tv_series",
            result_type="title",
        )
        assert result.id == 123
        assert result.name == "Test Show"
        assert result.type == "tv_series"
        assert result.result_type == "title"


class TestWatchmodeWhatsNewResponse:
    """Test WatchmodeWhatsNewResponse model."""

    def test_valid_whats_new_response(self):
        """Test creating a valid what's new response."""
        response = WatchmodeWhatsNewResponse(
            results=[],
            total_results=0,
            generated_at="2024-01-01T00:00:00",
        )
        assert response.results == []
        assert response.total_results == 0
        assert response.region == "US"
        assert response.data_source == "watchmode_list + tmdb_complete"


class TestWatchmodeTitleDetailsResponse:
    """Test WatchmodeTitleDetailsResponse model."""

    def test_valid_title_details_response(self):
        """Test creating a valid title details response with streaming sources."""
        response = WatchmodeTitleDetailsResponse(
            id=123,
            title="Test Movie",
            type="movie",
            streaming_sources=[],
        )
        assert response.id == 123
        assert response.title == "Test Movie"
        assert response.streaming_sources == []


class TestWatchmodeSearchResponse:
    """Test WatchmodeSearchResponse model."""

    def test_valid_search_response(self):
        """Test creating a valid search response."""
        response = WatchmodeSearchResponse(
            results=[],
            total_results=0,
            query="test",
        )
        assert response.results == []
        assert response.total_results == 0
        assert response.query == "test"
