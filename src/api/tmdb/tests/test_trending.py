"""
Unit tests for TMDB Trending Functions.
Tests trending.py functions for getting trending movies and TV shows.
"""

from unittest.mock import AsyncMock, patch

import pytest
from contracts.models import MCType

from api.subapi.flixpatrol.models import FlixPatrolMediaItem, FlixPatrolResponse
from api.tmdb.models import (
    MCBaseMediaItem,
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCMovieItem,
    MCTvItem,
)
from api.tmdb.search import tmdb_search_service
from api.tmdb.tests.conftest import load_fixture
from api.tmdb.trending import (
    _get_tmdb_trending,
    get_trending_movies,
    get_trending_tv_shows,
)

pytestmark = pytest.mark.unit


class TestGetTrendingMovies:
    """Tests for get_trending_movies function."""

    @pytest.mark.asyncio
    async def test_get_trending_movies_success(self):
        """Test getting trending movies successfully."""
        mock_trending_movies = load_fixture("make_requests/get_trending_movie.json")
        mock_trending_movies["results"] = mock_trending_movies["results"][:2]

        with (
            patch("api.tmdb.trending.flixpatrol_wrapper.get_flixpatrol_data") as mock_flixpatrol,
            patch("api.tmdb.trending._enrich_flixpatrol_with_tmdb") as mock_enrich,
        ):
            mock_flixpatrol.return_value = FlixPatrolResponse(
                date="2025-01-15",
                shows={},
                movies={},
                top_trending_tv_shows=[],
                top_trending_movies=[
                    FlixPatrolMediaItem(
                        id="test:Test Movie:movie",
                        rank=1,
                        title="Test Movie",
                        score=1000,
                        platform="test",
                        content_type="movie",
                    )
                ],
                metadata=None,
                status_code=200,
            )
            # Create mock enriched items
            mock_movie_item = MCMovieItem(
                tmdb_id=550,
                name="Fight Club",
                title="Fight Club",
                media_type="movie",
                content_type="movie",
                mc_type=MCType.MOVIE,
            )
            mock_enrich.return_value = [mock_movie_item]

            result = await get_trending_movies(limit=10)

            assert isinstance(result, MCGetTrendingMovieResult)
            assert len(result.results) == 1
            assert result.total_results == 1
            assert result.query == "limit:10"
            assert result.data_source == "top_trending_movies(10)"
            # Verify mc_id and mc_type are set
            assert result.results[0].mc_id is not None
            assert isinstance(result.results[0].mc_id, str)
            assert len(result.results[0].mc_id) > 0
            assert result.results[0].mc_type == "movie"

    @pytest.mark.asyncio
    async def test_get_trending_movies_fallback_to_tmdb(self):
        """Test getting trending movies falls back to TMDB when FlixPatrol fails."""
        mock_trending_movies = load_fixture("make_requests/get_trending_movie.json")
        mock_trending_movies["results"] = mock_trending_movies["results"][:2]

        with (
            patch("api.tmdb.trending.flixpatrol_wrapper.get_flixpatrol_data") as mock_flixpatrol,
            patch("api.tmdb.trending._get_tmdb_trending") as mock_tmdb_trending,
        ):
            mock_flixpatrol.return_value = FlixPatrolResponse(
                date="2025-01-15",
                shows={},
                movies={},
                top_trending_tv_shows=[],
                top_trending_movies=[],
                metadata=None,
                error="FlixPatrol error",
                status_code=500,
            )
            mock_movie_item = MCMovieItem(
                tmdb_id=550,
                name="Fight Club",
                title="Fight Club",
                media_type="movie",
                content_type="movie",
                mc_type=MCType.MOVIE,
            )
            mock_tmdb_trending.return_value = [mock_movie_item]

            result = await get_trending_movies(limit=10)

            assert isinstance(result, MCGetTrendingMovieResult)
            assert len(result.results) == 1
            mock_tmdb_trending.assert_called_once_with(media_type=MCType.MOVIE, limit=10)

    @pytest.mark.asyncio
    async def test_get_trending_movies_empty_results(self):
        """Test getting trending movies with empty results."""
        with (
            patch("api.tmdb.trending.flixpatrol_wrapper.get_flixpatrol_data") as mock_flixpatrol,
            patch("api.tmdb.trending._enrich_flixpatrol_with_tmdb") as mock_enrich,
        ):
            mock_flixpatrol.return_value = FlixPatrolResponse(
                date="2025-01-15",
                shows={},
                movies={},
                top_trending_tv_shows=[],
                top_trending_movies=[],
                metadata=None,
                status_code=200,
            )
            mock_enrich.return_value = []

            result = await get_trending_movies(limit=10)

            assert isinstance(result, MCGetTrendingMovieResult)
            assert len(result.results) == 0
            assert result.total_results == 0


class TestGetTrendingTvShows:
    """Tests for get_trending_tv_shows function."""

    @pytest.mark.asyncio
    async def test_get_trending_tv_shows_success(self):
        """Test getting trending TV shows successfully."""
        mock_trending_tv = load_fixture("make_requests/get_trending_tv.json")
        mock_trending_tv["results"] = mock_trending_tv["results"][:2]

        with (
            patch("api.tmdb.trending.flixpatrol_wrapper.get_flixpatrol_data") as mock_flixpatrol,
            patch("api.tmdb.trending._enrich_flixpatrol_with_tmdb") as mock_enrich,
        ):
            mock_flixpatrol.return_value = FlixPatrolResponse(
                date="2025-01-15",
                shows={},
                movies={},
                top_trending_tv_shows=[
                    FlixPatrolMediaItem(
                        id="test:Breaking Bad:tv",
                        rank=1,
                        title="Breaking Bad",
                        score=1000,
                        platform="test",
                        content_type="tv",
                    )
                ],
                top_trending_movies=[],
                metadata=None,
                status_code=200,
            )
            # Create mock enriched items
            mock_tv_item = MCTvItem(
                tmdb_id=1396,
                name="Breaking Bad",
                title="Breaking Bad",
                media_type="tv",
                content_type="tv",
                mc_type=MCType.TV_SERIES,
            )
            mock_enrich.return_value = [mock_tv_item]

            result = await get_trending_tv_shows(limit=10)

            assert isinstance(result, MCGetTrendingShowResult)
            assert len(result.results) == 1
            assert result.total_results == 1
            assert result.query == "limit:10"
            assert result.data_source == "top_trending_tv_shows(10)"
            # Verify mc_id and mc_type are set
            assert result.results[0].mc_id is not None
            assert isinstance(result.results[0].mc_id, str)
            assert len(result.results[0].mc_id) > 0
            assert result.results[0].mc_type == MCType.TV_SERIES

    @pytest.mark.asyncio
    async def test_get_trending_tv_shows_fallback_to_tmdb(self):
        """Test getting trending TV shows falls back to TMDB when FlixPatrol fails."""
        mock_trending_tv = load_fixture("make_requests/get_trending_tv.json")
        mock_trending_tv["results"] = mock_trending_tv["results"][:2]

        with (
            patch("api.tmdb.trending.flixpatrol_wrapper.get_flixpatrol_data") as mock_flixpatrol,
            patch("api.tmdb.trending._get_tmdb_trending") as mock_tmdb_trending,
        ):
            mock_flixpatrol.return_value = FlixPatrolResponse(
                date="2025-01-15",
                shows={},
                movies={},
                top_trending_tv_shows=[],
                top_trending_movies=[],
                metadata=None,
                error="FlixPatrol error",
                status_code=500,
            )
            mock_tv_item = MCTvItem(
                tmdb_id=1396,
                name="Breaking Bad",
                title="Breaking Bad",
                media_type="tv",
                content_type="tv",
                mc_type=MCType.TV_SERIES,
            )
            mock_tmdb_trending.return_value = [mock_tv_item]

            result = await get_trending_tv_shows(limit=10)

            assert isinstance(result, MCGetTrendingShowResult)
            assert len(result.results) == 1
            mock_tmdb_trending.assert_called_once_with(media_type=MCType.TV_SERIES, limit=10)


class TestGetTmdbTrending:
    """Tests for _get_tmdb_trending function."""

    @pytest.mark.asyncio
    async def test_get_tmdb_trending_movies(self):
        """Test getting trending movies from TMDB."""
        mock_trending_movies = load_fixture("make_requests/get_trending_movie.json")
        mock_trending_movies["results"] = mock_trending_movies["results"][:2]

        with patch.object(
            tmdb_search_service, "_make_request", new=AsyncMock(return_value=mock_trending_movies)
        ):
            result = await _get_tmdb_trending(
                media_type=MCType.MOVIE, limit=10, include_details=False
            )

            assert isinstance(result, list)
            assert len(result) > 0
            assert all(isinstance(item, MCBaseMediaItem) for item in result)
            # Verify mc_id and mc_type are set for all items
            for item in result:
                assert item.mc_id is not None
                assert isinstance(item.mc_id, str)
                assert len(item.mc_id) > 0
                assert item.mc_type == "movie"

    @pytest.mark.asyncio
    async def test_get_tmdb_trending_tv(self):
        """Test getting trending TV shows from TMDB."""
        mock_trending_tv = load_fixture("make_requests/get_trending_tv.json")
        mock_trending_tv["results"] = mock_trending_tv["results"][:2]

        with patch.object(
            tmdb_search_service, "_make_request", new=AsyncMock(return_value=mock_trending_tv)
        ):
            result = await _get_tmdb_trending(
                media_type=MCType.TV_SERIES, limit=10, include_details=False
            )

            assert isinstance(result, list)
            assert len(result) > 0
            assert all(isinstance(item, MCBaseMediaItem) for item in result)
            # Verify mc_id and mc_type are set for all items
            for item in result:
                assert item.mc_id is not None
                assert isinstance(item.mc_id, str)
                assert len(item.mc_id) > 0
                assert item.mc_type == MCType.TV_SERIES

    @pytest.mark.asyncio
    async def test_get_tmdb_trending_with_details(self):
        """Test getting trending movies with detailed information."""
        mock_trending_movies = load_fixture("make_requests/get_trending_movie.json")
        mock_trending_movies["results"] = mock_trending_movies["results"][:1]
        # Get the movie ID from the trending fixture
        movie_id = mock_trending_movies["results"][0]["id"]
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        # Update the movie details to match the trending movie ID
        mock_movie_details["id"] = movie_id
        mock_movie_details["title"] = mock_trending_movies["results"][0]["title"]
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_movie.json")

        async def mock_request(endpoint, params=None):
            if "trending" in endpoint:
                return mock_trending_movies
            if endpoint == f"movie/{movie_id}":
                return mock_movie_details
            if endpoint == f"movie/{movie_id}/credits":
                return mock_cast_data
            if endpoint == f"movie/{movie_id}/videos":
                return {"results": []}
            if endpoint == f"movie/{movie_id}/watch/providers":
                return {
                    "results": {
                        "US": {"flatrate": [{"provider_id": 8, "provider_name": "Netflix"}]}
                    }
                }
            if endpoint == f"movie/{movie_id}/keywords":
                return {"keywords": []}
            # Return empty dict for other endpoints instead of None
            return {}

        with (
            patch.object(
                tmdb_search_service, "_make_request", new=AsyncMock(side_effect=mock_request)
            ),
            patch.object(tmdb_search_service, "is_vaild_movie", return_value=True),
        ):
            result = await _get_tmdb_trending(
                media_type=MCType.MOVIE, limit=1, include_details=True
            )

            assert isinstance(result, list)
            assert len(result) > 0
            # Verify items have mc_id and mc_type
            for item in result:
                assert item.mc_id is not None
                assert isinstance(item.mc_id, str)
                assert len(item.mc_id) > 0
                assert item.mc_type == "movie"

    @pytest.mark.asyncio
    async def test_get_tmdb_trending_multiple_pages(self):
        """Test getting trending content across multiple pages."""
        # Create mock data for two pages
        page1 = {
            "results": [
                {
                    "id": 1,
                    "title": "Movie 1",
                    "poster_path": "/test1.jpg",
                    "overview": "Test",
                    "release_date": "2023-01-01",
                }
            ]
            * 20,
            "page": 1,
            "total_pages": 2,
        }
        page2 = {
            "results": [
                {
                    "id": 2,
                    "title": "Movie 2",
                    "poster_path": "/test2.jpg",
                    "overview": "Test",
                    "release_date": "2023-01-02",
                }
            ]
            * 20,
            "page": 2,
            "total_pages": 2,
        }

        call_count = [0]

        async def mock_request(endpoint, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return page1
            return page2

        with patch.object(
            tmdb_search_service, "_make_request", new=AsyncMock(side_effect=mock_request)
        ):
            result = await _get_tmdb_trending(
                media_type=MCType.MOVIE, limit=30, include_details=False
            )

            assert len(result) <= 30
            # Verify all items have mc_id and mc_type
            for item in result:
                assert item.mc_id is not None
                assert isinstance(item.mc_id, str)
                assert len(item.mc_id) > 0
                assert item.mc_type == "movie"

    @pytest.mark.asyncio
    async def test_get_tmdb_trending_error_handling(self):
        """Test error handling in _get_tmdb_trending."""
        with patch.object(
            tmdb_search_service, "_make_request", new=AsyncMock(side_effect=Exception("API Error"))
        ):
            result = await _get_tmdb_trending(media_type=MCType.MOVIE, limit=10)

            assert isinstance(result, list)
            assert len(result) == 0
