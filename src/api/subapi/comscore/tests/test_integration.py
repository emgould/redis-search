"""
Integration tests for Comscore Service.
Tests against actual Comscore API endpoints.

Note: These tests make real API calls and may be slower than unit tests.
Run with: pytest tests/test_integration.py -v
"""

import pytest

from api.subapi.comscore.core import ComscoreService
from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking
from api.subapi.comscore.wrappers import comscore_wrapper
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


class TestComscoreServiceIntegration:
    """Integration tests for ComscoreService against real API."""

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_real_api(self):
        """Test fetching real box office rankings from Comscore API."""
        service = ComscoreService()
        result = await service.get_domestic_rankings()

        # Verify we got valid data
        assert result is not None
        assert isinstance(result, BoxOfficeData)
        assert len(result.rankings) > 0
        assert result.exhibition_week is not None
        assert result.fetched_at is not None

        # Verify first ranking has expected structure
        first_ranking = result.rankings[0]
        assert isinstance(first_ranking, BoxOfficeRanking)
        assert first_ranking.rank == 1
        assert first_ranking.title_name is not None
        assert len(first_ranking.title_name) > 0
        assert first_ranking.weekend_estimate is not None
        write_snapshot(result, "get_domestic_rankings_real_api.json", hierarchical=False)

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_caching(self):
        """Test that caching works for domestic rankings."""
        service = ComscoreService()

        # First call - should fetch from API
        result1 = await service.get_domestic_rankings()
        assert result1 is not None

        # Second call - should use cache
        result2 = await service.get_domestic_rankings()
        assert result2 is not None

        # Results should be identical (from cache)
        assert result1.exhibition_week == result2.exhibition_week
        assert len(result1.rankings) == len(result2.rankings)

    @pytest.mark.asyncio
    async def test_match_movie_to_ranking_real_data(self):
        """Test movie matching with real box office data."""
        service = ComscoreService()
        box_office_data = await service.get_domestic_rankings()

        assert box_office_data is not None
        assert len(box_office_data.rankings) > 0

        # Try to match the #1 movie
        top_movie = box_office_data.rankings[0]
        matched_ranking = service.match_movie_to_ranking(
            top_movie.title_name, box_office_data.rankings
        )

        assert matched_ranking is not None
        assert matched_ranking.rank == 1
        assert matched_ranking.title_name == top_movie.title_name

        write_snapshot(matched_ranking, "match_movie_to_ranking_real_data.json", hierarchical=False)


class TestComscoreWrappersIntegration:
    """Integration tests for Comscore async wrappers."""

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_async_real_api(self):
        """Test async wrapper with real API."""
        result = await comscore_wrapper.get_domestic_rankings()

        assert isinstance(result, BoxOfficeData)
        assert result.status_code == 200
        assert result.error is None
        assert len(result.rankings) > 0


class TestComscoreDataQuality:
    """Integration tests to verify data quality from Comscore API."""

    @pytest.mark.asyncio
    async def test_rankings_are_ordered(self):
        """Test that rankings are in correct order (1, 2, 3, ...)."""
        service = ComscoreService()
        result = await service.get_domestic_rankings()

        assert result is not None
        assert len(result.rankings) > 0

        # Verify rankings are in order
        for i, ranking in enumerate(result.rankings, 1):
            assert ranking.rank == i

    @pytest.mark.asyncio
    async def test_all_rankings_have_titles(self):
        """Test that all rankings have non-empty titles."""
        service = ComscoreService()
        result = await service.get_domestic_rankings()

        assert result is not None

        for ranking in result.rankings:
            assert ranking.title_name is not None
            assert len(ranking.title_name) > 0

    @pytest.mark.asyncio
    async def test_all_rankings_have_estimates(self):
        """Test that all rankings have weekend estimates."""
        service = ComscoreService()
        result = await service.get_domestic_rankings()

        assert result is not None

        for ranking in result.rankings:
            assert ranking.weekend_estimate is not None
            assert len(ranking.weekend_estimate) > 0

    @pytest.mark.asyncio
    async def test_exhibition_week_format(self):
        """Test that exhibition week is in expected format."""
        service = ComscoreService()
        result = await service.get_domestic_rankings()

        assert result is not None
        assert result.exhibition_week is not None
        assert len(result.exhibition_week) > 0

        # Should be a date-like string (e.g., "2025-01-03")
        # Just verify it's not empty and has reasonable length
        assert len(result.exhibition_week) >= 8
