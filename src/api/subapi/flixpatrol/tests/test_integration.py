"""
Integration tests for FlixPatrol service.

These tests hit the actual FlixPatrol website and verify real data.
"""

import pytest

from api.subapi.flixpatrol.core import FlixPatrolService
from api.subapi.flixpatrol.models import FlixPatrolResponse
from api.subapi.flixpatrol.wrappers import flixpatrol_wrapper
from utils.pytest_utils import write_snapshot


class TestFlixPatrolIntegration:
    """Integration tests for FlixPatrol service."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_integration(self):
        """Test getting complete FlixPatrol data."""
        service = FlixPatrolService()
        result = await service.get_flixpatrol_data()

        # Verify basic structure
        assert result.date
        assert result.metadata
        assert result.metadata.source == "FlixPatrol"

        # Should have some data
        assert result.shows or result.movies
        assert result.top_trending_tv_shows or result.top_trending_movies

        # Verify metadata
        assert result.metadata.total_shows >= 0
        assert result.metadata.total_movies >= 0
        assert len(result.metadata.platforms) > 0

        # Check that we have some major platforms
        major_platforms = {"netflix", "hbo", "disney+", "amazon prime"}
        found_platforms = set(result.metadata.platforms)
        assert len(major_platforms & found_platforms) > 0
        write_snapshot(result.model_dump(), "test_get_flixpatrol_data_integration.json")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_with_custom_providers(self):
        """Test getting FlixPatrol data with custom providers."""
        service = FlixPatrolService()
        result = await service.get_flixpatrol_data(providers=["netflix", "hbo"])

        # Should only have Netflix and HBO in trending
        if result.top_trending_tv_shows:
            platforms = {show.platform for show in result.top_trending_tv_shows}
            assert platforms.issubset({"netflix", "hbo"})

        if result.top_trending_movies:
            platforms = {movie.platform for movie in result.top_trending_movies}
            assert platforms.issubset({"netflix", "hbo"})
        write_snapshot(result.model_dump(), "test_get_flixpatrol_data_with_custom_providers.json")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_flixpatrol_data_sorting(self):
        """Test that FlixPatrol data is properly sorted."""
        service = FlixPatrolService()
        result = await service.get_flixpatrol_data()

        # Check that trending shows are sorted by score (descending)
        if len(result.top_trending_tv_shows) > 1:
            for i in range(len(result.top_trending_tv_shows) - 1):
                assert (
                    result.top_trending_tv_shows[i].score
                    >= result.top_trending_tv_shows[i + 1].score
                )

        # Check that trending movies are sorted by score (descending)
        if len(result.top_trending_movies) > 1:
            for i in range(len(result.top_trending_movies) - 1):
                assert (
                    result.top_trending_movies[i].score >= result.top_trending_movies[i + 1].score
                )

        # Check that per-platform data is sorted by rank (ascending)
        for _platform, shows in result.shows.items():
            if len(shows) > 1:
                for i in range(len(shows) - 1):
                    assert shows[i].rank <= shows[i + 1].rank

        for _platform, movies in result.movies.items():
            if len(movies) > 1:
                for i in range(len(movies) - 1):
                    assert movies[i].rank <= movies[i + 1].rank


class TestFlixPatrolWrappersIntegration:
    """Integration tests for FlixPatrol async wrappers."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_async_real_api(self):
        """Test async wrapper with real API."""
        result = await flixpatrol_wrapper.get_flixpatrol_data()

        assert isinstance(result, FlixPatrolResponse)
        assert result.status_code == 200
        assert result.error is None
        assert result.date is not None
        assert result.shows is not None or result.movies is not None
