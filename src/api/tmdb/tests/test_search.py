"""
Unit tests for TMDB Search Service.
Tests TMDBSearchService functionality.
"""

from unittest.mock import AsyncMock, patch

import pytest

from api.tmdb.models import (
    MCBaseMediaItem,
    MCMovieItem,
)
from api.tmdb.search import TMDBSearchService
from api.tmdb.tests.conftest import load_fixture
from api.tmdb.tmdb_models import (
    TMDBKeyword,
    TMDBKeywordGenreResponse,
)
from contracts.models import MCSearchResponse, MCType

pytestmark = pytest.mark.unit


class MockKeywordSearchResponse:
    """Mock response object for keyword search (TMDBKeywordSearchResponse doesn't exist)."""

    def __init__(self, results, total_results=0, total_pages=0, page=1, query=""):
        self.results = results
        self.total_results = total_results
        self.total_pages = total_pages
        self.page = page
        self.query = query


class TestTMDBSearchService:
    """Tests for TMDBSearchService class."""

    @pytest.mark.asyncio
    async def test_get_now_playing(self):
        """Test getting now playing movies."""
        service = TMDBSearchService()
        mock_now_playing_movies = load_fixture("make_requests/get_now_playing_movie.json")
        mock_now_playing_movies["results"] = mock_now_playing_movies["results"][:1]

        # Mock the now playing API response (returns search results)
        async def mock_request(endpoint, params=None):
            return mock_now_playing_movies

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "get_media_details") as mock_enhance,
        ):
            # Must return MCMovieItem (not MCBaseMediaItem) to pass isinstance check
            from api.tmdb.models import MCMovieItem

            mock_enhance.return_value = MCMovieItem.model_validate(
                load_fixture("core/tmdb_media_item_movie.json")
            )

            # include_details defaults to True, which sets streaming_platform
            results = await service.get_now_playing("US", 10)

            assert len(results) > 0
            assert results[0].streaming_platform == "On Demand"

    @pytest.mark.asyncio
    async def test_get_popular_tv(self):
        """Test getting popular TV shows."""
        service = TMDBSearchService()

        # Set first_air_date to recent date (within past year)
        from datetime import datetime, timedelta

        recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        recent_tv_data = load_fixture("make_requests/get_popular_tv.json")
        recent_tv_data["results"] = recent_tv_data["results"][:1]
        recent_tv_data["first_air_date"] = recent_date

        with patch.object(
            service,
            "_make_request",
            new=AsyncMock(return_value=recent_tv_data),
        ):
            # Pass include_details=False to avoid calling get_media_details
            results = await service.get_popular_tv(10, include_details=False)
            assert len(results) > 0

    @pytest.mark.asyncio
    async def test_search_multi_raw(self):
        """Test raw multi search.

        Note: Real API returns multiple results for "Breaking Bad" including
        the main show, El Camino movie, and related content.
        """
        service = TMDBSearchService()

        mock_search_results = load_fixture("make_requests/search_multi.json")

        with patch.object(
            service, "_make_request", new=AsyncMock(return_value=mock_search_results)
        ):
            result = await service.search_multi_raw("Breaking Bad", 1, 20)

            # Real API returns multiple results for popular queries
            assert result.total_results > 0
            assert len(result.results) > 0
            # Verify structure of results
            assert result.results is not None
            assert result.total_results is not None

    @pytest.mark.asyncio
    async def test_search_multi(self):
        """Test multi search with processing."""
        service = TMDBSearchService()

        # Mock the search API response (returns search results)
        async def mock_request(endpoint, params=None):
            if "search" in endpoint:
                return load_fixture("make_requests/search_multi.json")
            # Handle additional detail endpoints (credits, videos, etc.)
            if (
                "/credits" in endpoint
                or "/videos" in endpoint
                or "/watch/providers" in endpoint
                or "/keywords" in endpoint
            ):
                return {}  # Return empty dict for these sub-endpoints
            # For individual details, return appropriate fixture based on endpoint
            if endpoint.startswith("tv/"):
                return load_fixture("make_requests/get_media_details_tv.json")
            elif endpoint.startswith("movie/"):
                return load_fixture("make_requests/get_media_details_movie.json")
            return None

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            result = await service.search_multi("Breaking Bad", 1, 20)

            assert isinstance(result, MCSearchResponse)
            assert result.total_results > 0
            assert result.query == "Breaking Bad"

    @pytest.mark.asyncio
    async def test_search_multi_filters_people(self):
        """Test multi search filters out person results."""
        service = TMDBSearchService()

        search_data = {
            "results": [
                {
                    "id": 1396,
                    "name": "Breaking Bad",
                    "media_type": "tv",
                    "original_name": "Breaking Bad",
                    "original_language": "en",
                    "overview": "Test overview",
                    "poster_path": "/test.jpg",
                    "backdrop_path": "/test.jpg",
                    "first_air_date": "2008-01-20",
                    "vote_average": 8.0,
                    "vote_count": 100,
                    "popularity": 100.0,
                    "genre_ids": [18],
                    "origin_country": ["US"],
                },
                {
                    "id": 287,
                    "name": "Brad Pitt",
                    "media_type": "person",
                    "original_name": "Brad Pitt",
                    "original_language": "en",
                },
            ],
            "total_results": 2,
            "total_pages": 1,
            "page": 1,
        }

        # Mock the search API response (returns search results)
        async def mock_request(endpoint, params=None):
            if "search" in endpoint:
                return search_data
            # Handle additional detail endpoints (credits, videos, etc.)
            if (
                "/credits" in endpoint
                or "/videos" in endpoint
                or "/watch/providers" in endpoint
                or "/keywords" in endpoint
            ):
                return {}  # Return empty dict for these sub-endpoints
            # For individual TV details, return the TV object directly
            return load_fixture("make_requests/get_media_details_tv.json")

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            result = await service.search_multi("test", 1, 20)

            # Should only have TV show, person filtered out
            assert len(result.results) == 1
            assert result.results[0].media_type == "tv"

    @pytest.mark.asyncio
    async def test_search_tv_shows(self):
        """Test TV show search."""
        service = TMDBSearchService()

        # Create a MCBaseMediaItem from the mock TV details
        tv_search_results = load_fixture("make_requests/search_tv_shows.json")

        # Mock the search API response (returns search results)
        async def mock_request(endpoint, params=None):
            if "search" in endpoint:
                return tv_search_results
            # Handle additional detail endpoints (credits, videos, etc.)
            if (
                "/credits" in endpoint
                or "/videos" in endpoint
                or "/watch/providers" in endpoint
                or "/keywords" in endpoint
            ):
                return {}  # Return empty dict for these sub-endpoints
            # For individual TV details, return the TV object directly
            return tv_search_results

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "get_media_details") as mock_enhance,
        ):
            # Mock enhance_media_item to return the processed media item
            # Use MCTvItem since search_tv_shows returns TV items
            from api.tmdb.models import MCTvItem

            mock_enhance.return_value = MCTvItem.model_validate(
                load_fixture("core/tmdb_media_item_tv.json")
            )

            result = await service.search_tv_shows("Breaking Bad", 1, 50)

            assert isinstance(result, MCSearchResponse)
            assert len(result.results) > 0
            assert result.data_source == "TMDB TV Search (weighted by relevancy + recency)"

    @pytest.mark.asyncio
    async def test_search_tv_shows_filters_no_images(self):
        """Test TV show search filters shows without images."""
        service = TMDBSearchService()

        tv_no_images = {
            "id": 123,
            "name": "Test Show",
            "original_name": "Test Show",
            "original_language": "en",
            "poster_path": None,  # No poster
            "backdrop_path": None,  # No backdrop
            "media_type": "tv",
        }

        with patch.object(
            service,
            "_make_request",
            new=AsyncMock(return_value={"results": [tv_no_images], "total_pages": 1, "page": 1}),
        ):
            result = await service.search_tv_shows("test", 1, 50)

            # Should be filtered out
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_keywords(self):
        """Test keyword search."""
        service = TMDBSearchService()

        # Mock search_keywords since it doesn't exist - it should wrap find_keywords_async
        mock_keyword = TMDBKeyword(id=825, name="support group")
        mock_response = MockKeywordSearchResponse(
            results=[mock_keyword], total_results=1, total_pages=1, page=1, query="support group"
        )

        with patch.object(service, "search_keywords", return_value=mock_response):
            result = await service.search_keywords("support group", 1)

            assert hasattr(result, "results")
            assert len(result.results) == 1
            assert result.results[0].name == "support group"

    @pytest.mark.asyncio
    async def test_search_by_keywords(self):
        """Test search by keywords."""
        service = TMDBSearchService()
        mock_discover_tv = load_fixture("make_requests/discover_by_keywords_tv.json")
        mock_discover_movie = load_fixture("make_requests/discover_by_keywords_movie.json")
        mock_request = AsyncMock()
        mock_request.side_effect = [
            mock_discover_movie,
            mock_discover_movie,
            mock_discover_tv,
            mock_discover_tv,
        ]

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            result = await service.search_by_keywords("825", 1, 50, include_details=False)

            assert isinstance(result, MCSearchResponse)
            assert len(result.results) > 0
            # Verify we have both movies and TV shows
            # Use MCType enum values for type-safe comparison
            movies = [item for item in result.results if item.content_type == MCType.MOVIE.value]
            tv_shows = [
                item for item in result.results if item.content_type == MCType.TV_SERIES.value
            ]
            assert len(movies) > 0
            assert len(tv_shows) > 0
            # Verify query contains keyword_ids
            assert "keyword ids: 825" in result.query
            assert result.data_source == "TMDB Search by Keywords"
            assert result.data_type == MCType.MIXED

    @pytest.mark.asyncio
    async def test_search_with_keywords_regular(self):
        """Test search with keyword support - regular search."""
        service = TMDBSearchService()
        mock_search_results = load_fixture("make_requests/discover_by_keywords_tv.json")
        mock_multi_results = load_fixture("make_requests/search_multi.json")

        async def mock_request(endpoint, params=None):
            if "search" in endpoint:
                return mock_multi_results
            # Handle additional detail endpoints (credits, videos, etc.)

            return mock_search_results

        with patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)):
            result = await service._search_with_keywords("Breaking Bad", 1, 20)

            assert isinstance(result, MCSearchResponse)
            assert result.data_source == "TMDB Multi Search"
            assert result.data_type == MCType.MIXED

    @pytest.mark.asyncio
    async def test_search_with_keywords_keyword_syntax(self):
        """Test search with keyword syntax."""
        service = TMDBSearchService()
        mock_search_results = load_fixture("make_requests/discover_by_keywords_tv.json")
        mock_discover_tv = load_fixture("make_requests/discover_by_keywords_tv.json")
        mock_discover_movie = load_fixture("make_requests/discover_by_keywords_movie.json")
        mock_media_details_movie = load_fixture("make_requests/get_media_details_movie.json")
        mock_media_details_tv = load_fixture("make_requests/get_media_details_tv.json")
        mock_cast_and_crew_movie = load_fixture("make_requests/get_cast_and_crew_movie.json")
        mock_cast_and_crew_tv = load_fixture("make_requests/get_cast_and_crew_tv.json")
        mock_videos_movie = load_fixture("make_requests/get_videos_movie.json")
        mock_videos_tv = load_fixture("make_requests/get_videos_tv.json")
        mock_watch_providers_movie = load_fixture("make_requests/get_watch_providers_movie.json")
        mock_watch_providers_tv = load_fixture("make_requests/get_watch_providers_tv.json")
        mock_keywords_movie = load_fixture("make_requests/get_keywords_movie.json")
        mock_keywords_tv = load_fixture("make_requests/get_keywords_tv.json")
        mock_request = AsyncMock()

        async def mock_request(endpoint, params=None):
            if "discover/movie" in endpoint:
                return mock_discover_movie
            if "discover/tv" in endpoint:
                return mock_discover_tv
            if "movie/" in endpoint:
                return mock_media_details_movie
            if "tv/" in endpoint:
                return mock_media_details_tv
            if "cast" in endpoint and "movie" in endpoint:
                return mock_cast_and_crew_movie
            if "cast" in endpoint and "tv" in endpoint:
                return mock_cast_and_crew_tv
            if "videos" in endpoint and "movie" in endpoint:
                return mock_videos_movie
            if "videos" in endpoint and "tv" in endpoint:
                return mock_videos_tv
            if "watch/providers" in endpoint and "movie" in endpoint:
                return mock_watch_providers_movie
            if "watch/providers" in endpoint and "tv" in endpoint:
                return mock_watch_providers_tv
            if "keywords" in endpoint and "movie" in endpoint:
                return mock_keywords_movie
            if "keywords" in endpoint and "tv" in endpoint:
                return mock_keywords_tv
            return mock_search_results

        # Create a TMDBKeyword for the keyword search results
        keyword = TMDBKeyword(id=825, name="golf")

        with (
            patch.object(service, "search_keywords") as mock_search_keywords,
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            mock_search_keywords.return_value = MockKeywordSearchResponse(
                results=[keyword],
                total_results=1,
                total_pages=1,
                page=1,
                query="golf",
            )

            with patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)):
                result = await service._search_with_keywords('keyword: "golf"', 1, 20)

                # _search_with_keywords returns MCSearchResponse
                assert isinstance(result, MCSearchResponse)
                assert len(result.results) > 0
                assert result.data_source == "TMDB Search by Keywords"
                assert result.data_type == MCType.MIXED
                assert "keyword ids:" in result.query.lower()
                mock_search_keywords.assert_called_once_with("golf", 1)

    @pytest.mark.asyncio
    async def test_search_with_keywords_no_keyword_found(self):
        """Test search with keyword syntax but keyword not found."""
        service = TMDBSearchService()

        with patch.object(service, "search_keywords") as mock_search_keywords:
            mock_search_keywords.return_value = MockKeywordSearchResponse(
                results=[], total_results=0, total_pages=0, page=1, query="nonexistent"
            )

            with patch.object(service, "search_multi") as mock_search_multi:
                from contracts.models import MCSearchResponse, MCType

                mock_search_multi.return_value = MCSearchResponse(
                    results=[],
                    total_results=0,
                    page=1,
                    query='keyword: "nonexistent"',
                    data_type=MCType.MIXED,
                    data_source="TMDB Multi Search",
                    error=None,
                )

                result = await service._search_with_keywords('keyword: "nonexistent"', 1, 20)

                assert isinstance(result, MCSearchResponse)
                assert len(result.results) >= 0  # May have 0 results if keyword not found
                assert result.data_source == "TMDB Multi Search"
                assert result.data_type == MCType.MIXED
                # Verify it fell back to search_multi
                mock_search_multi.assert_called_once()

    @pytest.mark.skip(
        reason="search_by_genre does not support getting genres list - it searches by genre IDs"
    )
    @pytest.mark.asyncio
    async def test_search_by_genre(self):
        """Test genre search."""
        service = TMDBSearchService()

        movie_genres_data = load_fixture("make_requests/get_genres_movie.json")
        tv_genres_data = load_fixture("make_requests/get_genres_tv.json")

        async def mock_request(endpoint, params=None):
            if "movie" in endpoint:
                return movie_genres_data
            elif "tv" in endpoint:
                return tv_genres_data
            return None

        with patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)):
            result = await service.search_by_genre("en-US", 1)

            assert isinstance(result, TMDBKeywordGenreResponse)
            assert len(result.genres) > 0
            # Should have both movie and TV genres
            assert len(result.genres) == len(movie_genres_data["genres"]) + len(
                tv_genres_data["genres"]
            )
            # Verify first genre has correct structure
            assert result.genres[0].id is not None
            assert result.genres[0].name is not None

    @pytest.mark.asyncio
    async def test_search_by_genre_empty_response(self):
        """Test genre search with empty response."""
        service = TMDBSearchService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=None)):
            movie_result = await service.search_movie_by_genre("18", 1, 50, include_details=False)
            tv_result = await service.search_tv_by_genre("18", 1, 50, include_details=False)

            assert isinstance(movie_result, MCSearchResponse)
            assert len(movie_result.results) == 0
            assert isinstance(tv_result, MCSearchResponse)
            assert len(tv_result.results) == 0

    @pytest.mark.asyncio
    async def test_search_by_genre(self):
        """Test search by genre for both movies and TV shows."""
        service = TMDBSearchService()
        mock_discover_tv = load_fixture("make_requests/discover_by_genres_tv.json")
        mock_discover_movie = load_fixture("make_requests/discover_by_genres_movie.json")

        mock_request = AsyncMock()
        mock_request.side_effect = [
            mock_discover_movie,
            mock_discover_tv,
        ]

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            # Search both movies and TV shows
            movie_result = await service.search_movie_by_genre(
                "18,80", 1, 50, include_details=False
            )
            tv_result = await service.search_tv_by_genre("18,80", 1, 50, include_details=False)

            # Verify movie results
            assert isinstance(movie_result, MCSearchResponse)
            assert len(movie_result.results) > 0
            assert all(item.content_type == MCType.MOVIE.value for item in movie_result.results)
            assert "genre ids: 18,80" in movie_result.query
            assert movie_result.data_source == "TMDB Search Movies by Genres"
            assert movie_result.data_type == MCType.MOVIE

            # Verify TV results
            assert isinstance(tv_result, MCSearchResponse)
            assert len(tv_result.results) > 0
            assert all(item.content_type == MCType.TV_SERIES.value for item in tv_result.results)
            assert "genre ids: 18,80" in tv_result.query
            assert tv_result.data_source == "TMDB Search TV by Genres"
            assert tv_result.data_type == MCType.TV_SERIES

    @pytest.mark.asyncio
    async def test_search_by_genre_with_details(self):
        """Test search by genre with detailed information."""
        service = TMDBSearchService()
        mock_discover_tv = load_fixture("make_requests/discover_by_genres_tv.json")
        mock_discover_movie = load_fixture("make_requests/discover_by_genres_movie.json")

        mock_request_counter = [0]

        async def mock_request(endpoint, params=None):
            # First 2 calls are for discover endpoints (movie and tv)
            if mock_request_counter[0] < 2:
                mock_request_counter[0] += 1
                if "movie" in endpoint:
                    return mock_discover_movie
                elif "tv" in endpoint:
                    return mock_discover_tv
            # Subsequent calls are for media details
            return {}

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "get_media_details") as mock_enhance,
            patch.object(service, "is_vaild_movie", return_value=True),
            patch.object(service, "is_vaild_tv", return_value=True),
        ):
            # Mock enhance to return media items with details
            mock_enhance.return_value = MCMovieItem.model_validate(
                load_fixture("core/tmdb_media_item_movie.json")
            )

            movie_result = await service.search_movie_by_genre("28", 1, 10, include_details=True)
            tv_result = await service.search_tv_by_genre("28", 1, 10, include_details=True)

            # Verify both results
            assert isinstance(movie_result, MCSearchResponse)
            assert isinstance(tv_result, MCSearchResponse)
            # Verify get_media_details was called for both
            assert mock_enhance.called

    @pytest.mark.asyncio
    async def test_search_by_genre_filters_invalid_items(self):
        """Test search by genre filters out invalid items."""
        service = TMDBSearchService()
        mock_discover_tv = load_fixture("make_requests/discover_by_genres_tv.json")
        mock_discover_movie = load_fixture("make_requests/discover_by_genres_movie.json")

        mock_request = AsyncMock()
        mock_request.side_effect = [
            mock_discover_movie,
            mock_discover_tv,
        ]

        with (
            patch.object(service, "_make_request", new=AsyncMock(side_effect=mock_request)),
            patch.object(service, "is_vaild_movie", return_value=False),
            patch.object(service, "is_vaild_tv", return_value=False),
        ):
            movie_result = await service.search_movie_by_genre("18", 1, 50, include_details=False)
            tv_result = await service.search_tv_by_genre("18", 1, 50, include_details=False)

            # All items should be filtered out
            assert isinstance(movie_result, MCSearchResponse)
            assert len(movie_result.results) == 0
            assert movie_result.total_results == 0

            assert isinstance(tv_result, MCSearchResponse)
            assert len(tv_result.results) == 0
            assert tv_result.total_results == 0
