"""
Unit tests for TMDB Core Service.
Tests TMDBService base class functionality.
"""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from contracts.models import MCType

from api.tmdb.core import TMDBService
from api.tmdb.models import MCBaseMediaItem, MCMovieItem, MCTvItem
from api.tmdb.tests.conftest import load_fixture
from api.tmdb.tmdb_models import (
    TMDBMovieDetailsResult,
    TMDBMovieMultiSearch,
    TMDBTvDetailsResult,
    TMDBTVMultiSearch,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"

pytestmark = pytest.mark.unit


class TestProcessMediaItem:
    """Tests for model class methods that replaced process_media_item."""

    def test_process_movie_item(self):
        """Test processing a movie item from search results."""
        trend_results = load_fixture("make_requests/get_trending_movie.json")
        fixture = TMDBMovieMultiSearch.model_validate(trend_results["results"][1])
        result = MCMovieItem.from_movie_search(fixture, image_base_url=IMAGE_BASE_URL)

        assert isinstance(result, MCMovieItem)
        assert isinstance(result, MCBaseMediaItem)
        assert result.tmdb_id == trend_results["results"][1]["id"]
        assert result.title == trend_results["results"][1]["title"]
        assert result.media_type == "movie"
        assert result.release_date == trend_results["results"][1]["release_date"]
        assert result.mc_id is not None
        assert isinstance(result.mc_id, str)
        assert len(result.mc_id) > 0
        assert result.mc_type == MCType.MOVIE

    def test_process_tv_item(self):
        """Test processing a TV show item from search results."""
        trend_results = load_fixture("make_requests/get_trending_tv.json")
        fixture = TMDBTVMultiSearch.model_validate(trend_results["results"][0])
        result = MCTvItem.from_tv_search(fixture, image_base_url=IMAGE_BASE_URL)

        assert isinstance(result, MCTvItem)
        assert isinstance(result, MCBaseMediaItem)
        assert result.tmdb_id == trend_results["results"][0]["id"]
        assert result.title == trend_results["results"][0]["name"]
        assert result.media_type == "tv"
        assert result.first_air_date == trend_results["results"][0]["first_air_date"]
        assert result.mc_id is not None
        assert isinstance(result.mc_id, str)
        assert len(result.mc_id) > 0
        assert result.mc_type == MCType.TV_SERIES
        assert result.genre_ids == trend_results["results"][0]["genre_ids"]

    def test_process_movie_details(self):
        """Test processing a movie item from detail results.

        Note: TMDB movie detail endpoint doesn't include 'media_type' field.
        That field only appears in search results. When processing detail
        responses, use from_movie_details method.
        """
        results = load_fixture("make_requests/get_media_details_movie.json")
        mock_movie_data = TMDBMovieDetailsResult.model_validate(results)
        result = MCMovieItem.from_movie_details(mock_movie_data, image_base_url=IMAGE_BASE_URL)
        assert result.media_type == "movie"
        assert isinstance(result, MCMovieItem)
        assert result.mc_id is not None
        assert isinstance(result.mc_id, str)
        assert len(result.mc_id) > 0
        assert result.mc_type == MCType.MOVIE


class TestTMDBService:
    """Tests for TMDBService class."""

    def test_init_with_token(self):
        """Test service initialization with token."""
        service = TMDBService()
        os.environ["TMDB_READ_TOKEN"] = "test_token"
        assert service.tmdb_read_token == "test_token"
        assert service.base_url == "https://api.themoviedb.org/3"
        assert service.image_base_url == "https://image.tmdb.org/t/p/"
        headers = service.auth_headers()
        assert "Authorization" in headers
        assert f"Bearer {service.tmdb_read_token}" in headers["Authorization"]

    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request."""
        service = TMDBService()

        mock_response_data = {"id": 550, "title": "Fight Club"}

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)

            mock_session = MagicMock()
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.__aenter__.return_value = mock_session
            mock_session.__aexit__.return_value = AsyncMock()

            mock_session_class.return_value = mock_session

            result = await service._make_request("movie/550", no_cache=True)

            assert result == mock_response_data
            mock_session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_make_request_non_200_status(self):
        """Test API request with non-200 status."""
        service = TMDBService()

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 404

            mock_session = MagicMock()
            mock_session.get.return_value.__aenter__.return_value = mock_response
            mock_session.__aenter__.return_value = mock_session
            mock_session.__aexit__.return_value = AsyncMock()

            mock_session_class.return_value = mock_session

            result = await service._make_request("movie/999999", no_cache=True)

            assert result is None

    @pytest.mark.asyncio
    async def test_make_request_exception(self):
        """Test API request with exception."""
        service = TMDBService()

        with patch("utils.base_api_client.aiohttp.ClientSession") as mock_session_class:
            mock_session_class.side_effect = Exception("Network error")

            with pytest.raises(Exception, match="Network error"):
                await service._make_request("movie/550", no_cache=True)

    def test_get_sort_date_with_release_date(self):
        """Test _get_sort_date with release_date."""
        service = TMDBService()

        item = MCMovieItem(
            tmdb_id=550,
            name="Fight Club",
            title="Fight Club",
            media_type="movie",
            content_type="movie",
            release_date="1999-10-15",
            mc_type=MCType.MOVIE,
        )
        assert item.mc_id is not None
        assert isinstance(item.mc_id, str)
        assert len(item.mc_id) > 0
        assert item.mc_type == MCType.MOVIE
        result = service._get_sort_date(item)

        assert isinstance(result, float)
        assert result < 0  # Negative timestamp

    def test_get_sort_date_with_first_air_date(self):
        """Test _get_sort_date with first_air_date."""
        service = TMDBService()

        item = MCTvItem(
            tmdb_id=1396,
            name="Breaking Bad",
            title="Breaking Bad",
            media_type="tv",
            content_type="tv",
            first_air_date="2008-01-20",
            mc_type=MCType.TV_SERIES,
        )
        assert item.mc_id is not None
        assert isinstance(item.mc_id, str)
        assert len(item.mc_id) > 0
        assert item.mc_type == MCType.TV_SERIES
        result = service._get_sort_date(item)

        assert isinstance(result, float)
        assert result < 0

    def test_get_sort_date_without_date(self):
        """Test _get_sort_date without date."""
        service = TMDBService()

        item = MCBaseMediaItem(
            tmdb_id=123,
            name="Test",
            title="Test",
            media_type="movie",
            content_type="movie",
            mc_type=MCType.MOVIE,
        )
        assert item.mc_id is not None
        assert isinstance(item.mc_id, str)
        assert len(item.mc_id) > 0
        assert item.mc_type == MCType.MOVIE
        result = service._get_sort_date(item)

        assert result == 0

    def test_get_sort_date_with_invalid_date(self):
        """Test _get_sort_date with invalid date."""
        service = TMDBService()

        item = MCBaseMediaItem(
            tmdb_id=123,
            name="Test",
            title="Test",
            media_type="movie",
            content_type="movie",
            release_date="invalid-date",
            mc_type=MCType.MOVIE,
        )
        assert item.mc_id is not None
        assert isinstance(item.mc_id, str)
        assert len(item.mc_id) > 0
        assert item.mc_type == MCType.MOVIE
        result = service._get_sort_date(item)

        assert result == 0

    @pytest.mark.asyncio
    async def test_get_media_details_movie(self):
        """Test getting movie details."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_movie.json")

        service = TMDBService()
        mock_request = AsyncMock()
        mock_request.side_effect = [
            mock_movie_details,  # Basic details
            mock_cast_data,  # Cast
            {"results": []},  # Videos
            {"results": {}},  # Watch providers
            {"keywords": []},  # Keywords
        ]

        with patch.object(service, "_make_request", mock_request):
            result = await service.get_media_details(550, MCType.MOVIE)

            assert result is not None
            assert result.tmdb_id == 550
            assert result.title == "Fight Club"
            assert result.mc_id is not None
            assert isinstance(result.mc_id, str)
            assert len(result.mc_id) > 0
            assert result.mc_type == MCType.MOVIE
            assert result.tmdb_cast is not None

    @pytest.mark.asyncio
    async def test_get_media_details_tv(self):
        """Test getting TV show details."""
        mock_tv_details = load_fixture("make_requests/get_media_details_tv.json")
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_tv.json")

        service = TMDBService()
        mock_request = AsyncMock()
        mock_request.side_effect = [
            mock_tv_details,  # Basic details (full TV details from /tv/{id} endpoint)
            mock_cast_data,  # Cast
            {"results": []},  # Videos
            {"results": {}},  # Watch providers
            {"results": []},  # Keywords (TV uses 'results')
        ]

        with patch.object(service, "_make_request", mock_request):
            result = await service.get_media_details(1396, MCType.TV_SERIES)

            assert result is not None
            assert result.tmdb_id == 1396
            assert result.name == "Breaking Bad"
            assert result.mc_id is not None
            assert isinstance(result.mc_id, str)
            assert len(result.mc_id) > 0
            assert result.mc_type == MCType.TV_SERIES
            assert result.number_of_seasons == 5

    @pytest.mark.asyncio
    async def test_get_media_details_not_found(self):
        """Test getting details for non-existent media."""
        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=None)):
            result = await service.get_media_details(999999, MCType.MOVIE)

            assert result is not None
            assert isinstance(result, MCBaseMediaItem)
            assert result.status_code == 404
            assert result.error is not None
            assert "not found" in result.error.lower()

    @pytest.mark.asyncio
    async def test_enhance_media_item(self):
        """Test enhancing a media item."""
        service = TMDBService()
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_movie.json")
        mock_videos_data = load_fixture("make_requests/get_videos_movie.json")
        mock_watch_providers_data = load_fixture("make_requests/get_watch_providers_movie.json")
        mock_keywords_data = load_fixture("make_requests/get_keywords_movie.json")

        mock_movie_data = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        media_item = MCMovieItem.from_movie_details(mock_movie_data, image_base_url=IMAGE_BASE_URL)

        with (
            patch.object(service, "_get_watch_providers") as mock_providers,
            patch.object(service, "_get_cast_and_crew") as mock_cast,
            patch.object(service, "_get_videos") as mock_videos,
            patch.object(service, "_get_keywords") as mock_keywords,
        ):
            mock_providers.return_value = mock_watch_providers_data
            mock_cast.return_value = mock_cast_data
            mock_videos.return_value = mock_videos_data
            mock_keywords.return_value = mock_keywords_data

            result = await service.enhance_media_item(media_item)

            assert isinstance(result, MCBaseMediaItem)
            assert result.tmdb_id == mock_movie_details["id"]
            assert result.mc_id is not None
            assert isinstance(result.mc_id, str)
            assert len(result.mc_id) > 0
            assert result.mc_type == MCType.MOVIE
            assert result.watch_providers is not None
            assert result.tmdb_cast is not None

    @pytest.mark.asyncio
    async def test_get_cast_and_crew(self):
        """Test getting cast and crew."""
        service = TMDBService()
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_movie.json")

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_cast_data)):
            result = await service._get_cast_and_crew(550, "movie")

            assert "tmdb_cast" in result
            assert "main_cast" in result
            assert len(result["main_cast"]) <= 5
            # Check cast structure (real API data may have different order)
            assert len(result["tmdb_cast"]["cast"]) > 0
            assert "name" in result["tmdb_cast"]["cast"][0]
            # Check director exists and has expected structure
            assert result["director"]["name"] == "David Fincher"

    @pytest.mark.asyncio
    async def test_get_cast_and_crew_with_limit(self):
        mock_cast_data = load_fixture("make_requests/get_cast_and_crew_movie.json")
        """Test getting cast with limit."""
        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_cast_data)):
            result = await service._get_cast_and_crew(550, "movie", limit=1)

            assert len(result["tmdb_cast"]["cast"]) == 1

    @pytest.mark.asyncio
    async def test_get_videos(self):
        mock_videos_data = load_fixture("make_requests/get_videos_movie.json")
        """Test getting videos."""
        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_videos_data)):
            result = await service._get_videos(550, "movie")

            assert "tmdb_videos" in result
            assert "trailers" in result["tmdb_videos"]
            assert result["tmdb_videos"]["total_videos"] == len(mock_videos_data["results"])
            assert "primary_trailer" in result

    @pytest.mark.asyncio
    async def test_get_watch_providers(self):
        mock_watch_providers_data = load_fixture("make_requests/get_watch_providers_movie.json")
        """Test getting watch providers."""
        service = TMDBService()
        from contracts.models import MCType

        with patch.object(
            service,
            "_make_request",
            new=AsyncMock(return_value=mock_watch_providers_data),
        ):
            result = await service._get_watch_providers(550, MCType.MOVIE, "US")

            assert "watch_providers" in result
            assert result["watch_providers"]["region"] == "US"
            # Check that streaming platform is set (real API data changes over time)
            assert "streaming_platform" in result
            assert isinstance(result["streaming_platform"], str)
            assert len(result["streaming_platform"]) > 0

    @pytest.mark.asyncio
    async def test_get_watch_providers_no_data(self):
        """Test getting watch providers with no data."""
        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value={"results": {}})):
            result = await service._get_watch_providers(550, "movie", "US")

            assert result == {}

    @pytest.mark.asyncio
    async def test_get_keywords_movie(self):
        mock_keywords_data = load_fixture("make_requests/get_keywords_movie.json")
        """Test getting keywords for movie."""
        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_keywords_data)):
            result = await service._get_keywords(550, "movie")

            assert "keywords" in result
            assert result["keywords_count"] == len(mock_keywords_data["keywords"])
            # Check keywords structure (real API data may have different order)
            assert len(result["keywords"]) == len(mock_keywords_data["keywords"])
            assert "name" in result["keywords"][0]
            assert isinstance(result["keywords"][0]["name"], str)

    @pytest.mark.asyncio
    async def test_get_keywords_tv(self):
        """Test getting keywords for TV show (uses 'results' key)."""
        service = TMDBService()

        with patch.object(
            service,
            "_make_request",
            new=AsyncMock(return_value={"results": [{"id": 825, "name": "support group"}]}),
        ):
            result = await service._get_keywords(1396, "tv")

            assert "keywords" in result
            assert result["keywords_count"] == 1


class TestProviderFunctions:
    """Tests for streaming provider functions."""

    @pytest.mark.asyncio
    async def test_get_tv_providers_success(self):
        """Test getting TV providers successfully."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()
        mock_data = load_fixture("make_requests/get_tv_providers.json")

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_data)):
            result = await service.get_providers(MCType.TV_SERIES, region="US", no_cache=True)

            assert result is not None
            assert isinstance(result, TMDBProvidersResponse)
            assert result.list_type == "tv"
            assert len(result.results) == 5  # All 5 providers (none have "channel" in name)

            # Verify sorting by display_priority
            assert result.results[0].provider_name == "Netflix"  # priority 8
            assert result.results[1].provider_name == "Amazon Prime Video"  # priority 9
            assert result.results[2].provider_name == "Hulu"  # priority 15

            # Verify all expected fields are present
            for provider in result.results:
                assert hasattr(provider, "provider_id")
                assert hasattr(provider, "provider_name")
                assert hasattr(provider, "logo_path")
                assert hasattr(provider, "display_priority")
                assert provider.provider_id is not None
                assert provider.provider_name is not None

    @pytest.mark.asyncio
    async def test_get_tv_providers_filters_channels(self):
        """Test that TV providers with 'channel' in name are filtered out."""
        service = TMDBService()
        mock_data = {
            "results": [
                {
                    "provider_id": 1,
                    "provider_name": "Netflix",
                    "display_priority": 8,
                    "logo_path": "/test.jpg",
                },
                {
                    "provider_id": 2,
                    "provider_name": "HBO Channel",
                    "display_priority": 10,
                    "logo_path": "/test2.jpg",
                },
                {
                    "provider_id": 3,
                    "provider_name": "Discovery Channel",
                    "display_priority": 5,
                    "logo_path": "/test3.jpg",
                },
            ]
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_data)):
            from api.tmdb.tmdb_models import TMDBProvidersResponse

            result = await service.get_providers(MCType.TV_SERIES, region="US", no_cache=True)

            assert result is not None
            assert isinstance(result, TMDBProvidersResponse)
            assert len(result.results) == 1  # Only Netflix, channels filtered out
            assert result.results[0].provider_name == "Netflix"

    @pytest.mark.asyncio
    async def test_get_tv_providers_empty_results(self):
        """Test get_tv_providers with empty results."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value={"results": []})):
            result = await service.get_providers(MCType.TV_SERIES, region="US", no_cache=True)
            assert isinstance(result, TMDBProvidersResponse)
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_get_tv_providers_error_handling(self):
        """Test get_tv_providers handles errors gracefully."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()

        with patch.object(
            service, "_make_request", new=AsyncMock(side_effect=Exception("API Error"))
        ):
            result = await service.get_providers(MCType.TV_SERIES, region="US", no_cache=True)
            assert isinstance(result, TMDBProvidersResponse)
            assert result.status_code == 500
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_get_movie_providers_success(self):
        """Test getting movie providers successfully."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()
        mock_data = load_fixture("make_requests/get_movie_providers.json")

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_data)):
            result = await service.get_providers(MCType.MOVIE, region="US", no_cache=True)

            assert result is not None
            assert isinstance(result, TMDBProvidersResponse)
            assert result.list_type == "movie"
            # Should filter out "HBO Channel" (provider_id 999)
            assert len(result.results) == 4

            # Verify sorting by display_priority
            assert result.results[0].provider_name == "Apple TV"  # priority 2
            assert result.results[1].provider_name == "Google Play Movies"  # priority 3
            assert result.results[2].provider_name == "Netflix"  # priority 8

            # Verify "HBO Channel" was filtered out
            provider_names = [p.provider_name for p in result.results]
            assert "HBO Channel" not in provider_names

    @pytest.mark.asyncio
    async def test_get_movie_providers_filters_channels(self):
        """Test that movie providers with 'channel' in name are filtered out."""
        service = TMDBService()
        mock_data = {
            "results": [
                {
                    "provider_id": 1,
                    "provider_name": "Netflix",
                    "display_priority": 8,
                    "logo_path": "/test.jpg",
                },
                {
                    "provider_id": 2,
                    "provider_name": "Starz Channel",
                    "display_priority": 10,
                    "logo_path": "/test2.jpg",
                },
            ]
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_data)):
            from api.tmdb.tmdb_models import TMDBProvidersResponse

            result = await service.get_providers(MCType.MOVIE, region="US", no_cache=True)

            assert result is not None
            assert isinstance(result, TMDBProvidersResponse)
            assert len(result.results) == 1
            assert result.results[0].provider_name == "Netflix"

    @pytest.mark.asyncio
    async def test_get_movie_providers_empty_results(self):
        """Test get_movie_providers with empty results."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value={"results": []})):
            result = await service.get_providers(MCType.MOVIE, region="US", no_cache=True)
            assert isinstance(result, TMDBProvidersResponse)
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_get_movie_providers_error_handling(self):
        """Test get_movie_providers handles errors gracefully."""
        from api.tmdb.tmdb_models import TMDBProvidersResponse

        service = TMDBService()

        with patch.object(
            service, "_make_request", new=AsyncMock(side_effect=Exception("API Error"))
        ):
            result = await service.get_providers(MCType.MOVIE, region="US", no_cache=True)
            assert isinstance(result, TMDBProvidersResponse)
            assert result.status_code == 500
            assert len(result.results) == 0
