"""
Integration tests for TMDB handler endpoints.
These tests hit the actual TMDB API endpoints through handler methods (no mocks).

Requirements:
- TMDB_READ_TOKEN environment variable must be set
- Internet connection required
- Tests may be slower due to actual API calls

Run with: pytest api/tmdb/tests/test_integration.py -v
"""

import json
import os
from unittest.mock import MagicMock

import pytest
from contracts.models import (
    MCSearchResponse,
    MCSources,
    MCSubType,
    MCType,
)
from firebase_functions import https_fn

from api.tmdb.handlers import TMDBHandler
from api.tmdb.models import (
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCMovieItem,
    MCPersonCreditsResponse,
    MCPersonDetailsResponse,
    MCPersonItem,
    MCTvItem,
    TMDBSearchGenreResponse,
    TMDBSearchMultiResponse,
    TMDBSearchTVResponse,
)
from api.tmdb.wrappers import search_person_async
from utils.pytest_utils import write_snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def real_tmdb_token():
    """Get real TMDB token from environment."""
    token = os.getenv("TMDB_READ_TOKEN")
    if not token:
        pytest.skip("TMDB_READ_TOKEN environment variable not set")
    return token


@pytest.fixture
def tmdb_handler():
    """Create TMDBHandler instance."""
    return TMDBHandler()


@pytest.fixture
def mock_request():
    """Create a mock Firebase Functions Request object."""

    def _create_mock_request(args: dict[str, str | None] | None = None):
        mock_req = MagicMock(spec=https_fn.Request)
        # Make args support .get() method like a dict
        args_dict = args or {}
        mock_req.args = MagicMock()
        mock_req.args.get = lambda key, default=None: args_dict.get(key, default)
        return mock_req

    return _create_mock_request


@pytest.fixture
def mock_callable_request():
    """Create a mock Firebase Functions CallableRequest object."""

    def _create_mock_callable_request(data: dict | None = None, auth: dict | None = None):
        mock_req = MagicMock(spec=https_fn.CallableRequest)
        mock_req.data = data or {}
        mock_req.auth = auth
        return mock_req

    return _create_mock_callable_request


class TestTMDBHandlers:
    """Integration tests for all TMDB handler endpoints."""

    @pytest.mark.asyncio
    async def test_get_trending_movies(self, tmdb_handler, mock_request):
        """Test get_trending handler for movies."""
        req = mock_request({"media_type": "movie", "limit": "10"})
        response = await tmdb_handler.get_trending(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = MCGetTrendingMovieResult.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate each result is a MCMovieItem
        # Results from model validation are MCBaseMediaItem instances - convert to dict then validate as specific type
        for item in result.results:
            item_dict = item.model_dump() if hasattr(item, "model_dump") else dict(item)
            item_validated = MCMovieItem.model_validate(item_dict)
            assert item_validated.mc_type == MCType.MOVIE
            assert item_validated.content_type == "movie"
            assert item_validated.tmdb_id is not None
            assert item_validated.title is not None
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for movie: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.MOVIE, (
                f"mc_type is wrong for movie: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for movie: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for movie: {item_validated.title}"
            )

        write_snapshot(data, "get_trending_movies.json")

    @pytest.mark.asyncio
    async def test_get_trending_tv(self, tmdb_handler, mock_request):
        """Test get_trending handler for TV shows."""
        req = mock_request({"media_type": "tv", "limit": "10"})
        response = await tmdb_handler.get_trending(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = MCGetTrendingShowResult.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate each result is a MCTvItem
        # Results from model validation are MCBaseMediaItem instances - convert to dict then validate as specific type
        for item in result.results:
            item_dict = item.model_dump() if hasattr(item, "model_dump") else dict(item)
            item_validated = MCTvItem.model_validate(item_dict)
            assert item_validated.mc_type == MCType.TV_SERIES
            assert item_validated.content_type == "tv"
            assert item_validated.tmdb_id is not None
            assert item_validated.name is not None
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for TV show: {item_validated.name}"
            )
            assert item_validated.mc_type == MCType.TV_SERIES, (
                f"mc_type is wrong for TV show: {item_validated.name}"
            )
            assert item_validated.source is not None, (
                f"source is missing for TV show: {item_validated.name}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for TV show: {item_validated.name}"
            )

        write_snapshot(data, "get_trending_tv.json")

    @pytest.mark.asyncio
    async def test_search_multi(self, tmdb_handler, mock_request):
        """Test search_multi handler."""
        req = mock_request({"query": "Breaking Bad", "limit": "10", "page": "1"})
        response = await tmdb_handler.search_multi(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = TMDBSearchMultiResponse.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate results contain valid media items
        # Results are already properly typed as MCMovieItem | MCTvItem
        for item in result.results:
            assert isinstance(item, (MCMovieItem, MCTvItem))
            if isinstance(item, MCMovieItem):
                assert item.mc_type == MCType.MOVIE
            elif isinstance(item, MCTvItem):
                assert item.mc_type == MCType.TV_SERIES
            # Verify required MCBaseItem fields
            item_name = item.title if isinstance(item, MCMovieItem) else item.name
            assert item.mc_id is not None, f"mc_id is missing for item: {item_name}"
            assert item.mc_type in [MCType.MOVIE, MCType.TV_SERIES], (
                f"mc_type is wrong for item: {item_name}"
            )
            assert item.source is not None, f"source is missing for item: {item_name}"
            assert item.source_id is not None, f"source_id is missing for item: {item_name}"

        write_snapshot(data, "search_multi.json")

    @pytest.mark.asyncio
    async def test_search_tv_shows(self, tmdb_handler, mock_request):
        """Test search_tv_shows handler."""
        req = mock_request({"query": "The Office", "limit": "10", "page": "1"})
        response = await tmdb_handler.search_tv_shows(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = TMDBSearchTVResponse.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate each result is a MCTvItem
        # Results are already properly typed as MCTvItem
        for item in result.results:
            assert isinstance(item, MCTvItem)
            assert item.mc_type == MCType.TV_SERIES
            assert item.content_type == "tv"
            assert item.tmdb_id is not None
            assert item.name is not None
            # Verify required MCBaseItem fields
            assert item.mc_id is not None, f"mc_id is missing for TV show: {item.name}"
            assert item.mc_type == MCType.TV_SERIES, f"mc_type is wrong for TV show: {item.name}"
            assert item.source is not None, f"source is missing for TV show: {item.name}"
            assert item.source_id is not None, f"source_id is missing for TV show: {item.name}"

        write_snapshot(data, "search_tv_shows.json")

    @pytest.mark.asyncio
    async def test_search_by_genre(self, tmdb_handler, mock_request):
        """Test search_by_genre handler."""
        req = mock_request({"genre_ids": "18,80", "limit": "10", "page": "1"})
        response = await tmdb_handler.search_by_genre(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = TMDBSearchGenreResponse.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate results contain valid media items
        # Results are already properly typed as MCMovieItem | MCTvItem
        for item in result.results:
            assert isinstance(item, (MCMovieItem, MCTvItem))
            if isinstance(item, MCMovieItem):
                assert item.mc_type == MCType.MOVIE
            elif isinstance(item, MCTvItem):
                assert item.mc_type == MCType.TV_SERIES
            # Verify required MCBaseItem fields
            item_name = item.title if isinstance(item, MCMovieItem) else item.name
            assert item.mc_id is not None, f"mc_id is missing for item: {item_name}"
            assert item.mc_type in [MCType.MOVIE, MCType.TV_SERIES], (
                f"mc_type is wrong for item: {item_name}"
            )
            assert item.source is not None, f"source is missing for item: {item_name}"
            assert item.source_id is not None, f"source_id is missing for item: {item_name}"

        write_snapshot(data, "search_by_genre.json")

    @pytest.mark.asyncio
    async def test_search_by_keywords(self, tmdb_handler, mock_request):
        """Test search_by_keywords handler."""
        # First we need to get a keyword ID - using a known keyword ID for space opera
        req = mock_request({"keyword_ids": "825", "limit": "10", "page": "1"})
        response = await tmdb_handler.search_by_keywords(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = TMDBSearchGenreResponse.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate results contain valid media items
        # Results are already properly typed as MCMovieItem | MCTvItem
        for item in result.results:
            assert isinstance(item, (MCMovieItem, MCTvItem))
            if isinstance(item, MCMovieItem):
                assert item.mc_type == MCType.MOVIE
            elif isinstance(item, MCTvItem):
                assert item.mc_type == MCType.TV_SERIES
            # Verify required MCBaseItem fields
            item_name = item.title if isinstance(item, MCMovieItem) else item.name
            assert item.mc_id is not None, f"mc_id is missing for item: {item_name}"
            assert item.mc_type in [MCType.MOVIE, MCType.TV_SERIES], (
                f"mc_type is wrong for item: {item_name}"
            )
            assert item.source is not None, f"source is missing for item: {item_name}"
            assert item.source_id is not None, f"source_id is missing for item: {item_name}"

        write_snapshot(data, "search_by_keywords.json")

    @pytest.mark.asyncio
    async def test_get_media_details_movie(self, tmdb_handler, mock_request):
        """Test get_media_details handler for a movie."""
        req = mock_request({"tmdb_id": "550", "content_type": "movie"})
        response = await tmdb_handler.get_media_details(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Response is now the media item directly (not wrapped)
        # Validate the media item is a MCMovieItem
        item = MCMovieItem.model_validate(data)
        assert item.mc_type == MCType.MOVIE
        assert item.content_type == "movie"
        assert item.tmdb_id == 550

        write_snapshot(data, "get_media_details_movie.json")

    @pytest.mark.asyncio
    async def test_get_media_details_tv(self, tmdb_handler, mock_request):
        """Test get_media_details handler for a TV show."""
        req = mock_request({"tmdb_id": "1396", "content_type": "tv"})
        response = await tmdb_handler.get_media_details(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Response is now the media item directly (not wrapped)
        # Validate the media item is a MCTvItem
        item = MCTvItem.model_validate(data)
        assert item.mc_type == MCType.TV_SERIES
        assert item.content_type == "tv"
        assert item.tmdb_id == 1396

        write_snapshot(data, "get_media_details_tv.json")

    @pytest.mark.asyncio
    async def test_get_now_playing(self, tmdb_handler, mock_request):
        """Test get_now_playing handler."""
        req = mock_request({"region": "US", "limit": "10"})
        response = await tmdb_handler.get_now_playing(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure - results should be a list
        assert "results" in data
        assert len(data["results"]) > 0

        # Validate each result is a MCMovieItem
        # Results from JSON are dicts - validate directly
        for item_dict in data["results"]:
            item_validated = MCMovieItem.model_validate(item_dict)
            assert item_validated.mc_type == MCType.MOVIE
            assert item_validated.content_type == "movie"
            assert item_validated.tmdb_id is not None
            assert item_validated.title is not None
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for movie: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.MOVIE, (
                f"mc_type is wrong for movie: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for movie: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for movie: {item_validated.title}"
            )

        write_snapshot(data, "get_now_playing.json")

    @pytest.mark.asyncio
    async def test_get_popular_tv(self, tmdb_handler, mock_request):
        """Test get_popular_tv handler."""
        req = mock_request({"limit": "10"})
        response = await tmdb_handler.get_popular_tv(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure - results should be a list
        assert "results" in data
        assert len(data["results"]) > 0

        # Validate each result is a MCTvItem
        # Results from JSON are dicts - validate directly
        for item_dict in data["results"]:
            item_validated = MCTvItem.model_validate(item_dict)
            assert item_validated.mc_type == MCType.TV_SERIES
            assert item_validated.content_type == "tv"
            assert item_validated.tmdb_id is not None
            assert item_validated.name is not None
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for TV show: {item_validated.name}"
            )
            assert item_validated.mc_type == MCType.TV_SERIES, (
                f"mc_type is wrong for TV show: {item_validated.name}"
            )
            assert item_validated.source is not None, (
                f"source is missing for TV show: {item_validated.name}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for TV show: {item_validated.name}"
            )

        write_snapshot(data, "get_popular_tv.json")

    @pytest.mark.asyncio
    async def test_search_people(self, tmdb_handler, mock_request):
        """Test search_people handler."""
        req = mock_request({"query": "Brad Pitt", "limit": "10", "page": "1"})
        response = await tmdb_handler.search_people(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = MCSearchResponse.model_validate(data)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Validate each result is a MCPersonItem
        # Items in JSON data should be validated directly as MCPersonItem
        for item_data in data.get("results", []):
            item_validated = MCPersonItem.model_validate(item_data)
            assert item_validated.mc_type == MCType.PERSON
            assert item_validated.id is not None
            assert item_validated.name is not None
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for person: {item_validated.name}"
            )
            assert item_validated.mc_type == MCType.PERSON, (
                f"mc_type is wrong for person: {item_validated.name}"
            )
            assert item_validated.source is not None, (
                f"source is missing for person: {item_validated.name}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for person: {item_validated.name}"
            )

        write_snapshot(data, "search_people.json")

    @pytest.mark.asyncio
    async def test_get_person_details(self, tmdb_handler, mock_request):
        """Test get_person_details handler."""
        req = mock_request({"person_id": "287"})  # Brad Pitt
        response = await tmdb_handler.get_person_details(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = MCPersonDetailsResponse.model_validate(data)
        assert result.results is not None
        assert len(result.results) == 1

        # Validate person is a MCPersonItem
        person = result.results[0]
        assert isinstance(person, MCPersonItem)
        assert person.mc_type == MCType.PERSON
        assert person.id == 287
        assert person.name is not None
        # Verify required MCBaseItem fields
        assert person.mc_id is not None, f"mc_id is missing for person: {person.name}"
        assert person.mc_type == MCType.PERSON, f"mc_type is wrong for person: {person.name}"
        assert person.source is not None, f"source is missing for person: {person.name}"
        assert person.source_id is not None, f"source_id is missing for person: {person.name}"

        write_snapshot(data, "get_person_details.json")

    @pytest.mark.asyncio
    async def test_get_person_credits(self, tmdb_handler, mock_request):
        """Test get_person_credits handler."""
        req = mock_request({"person_id": "287", "limit": "50"})  # Brad Pitt
        response = await tmdb_handler.get_person_credits(req)

        assert response.status_code == 200
        data = json.loads(response.data)

        # Validate response structure using model
        result = MCPersonCreditsResponse.model_validate(data)
        assert result.results is not None
        assert len(result.results) > 0

        # Validate credits result structure
        credits = result.results[0]
        assert credits.person is not None
        assert len(credits.movies) > 0 or len(credits.tv_shows) > 0

        # Validate person is a MCPersonItem
        person = MCPersonItem.model_validate(credits.person)
        assert person.mc_type == MCType.PERSON
        # Verify required MCBaseItem fields for person
        assert person.mc_id is not None, f"mc_id is missing for person: {person.name}"
        assert person.mc_type == MCType.PERSON, f"mc_type is wrong for person: {person.name}"
        assert person.source is not None, f"source is missing for person: {person.name}"
        assert person.source_id is not None, f"source_id is missing for person: {person.name}"

        # Validate movies are MCMovieItem
        # Results from model validation are MCBaseMediaItem instances - convert to dict then validate as specific type
        for movie in credits.movies:
            movie_dict = movie.model_dump() if hasattr(movie, "model_dump") else dict(movie)
            item_validated = MCMovieItem.model_validate(movie_dict)
            assert item_validated.mc_type == MCType.MOVIE
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for movie: {item_validated.title}"
            )
            assert item_validated.mc_type == MCType.MOVIE, (
                f"mc_type is wrong for movie: {item_validated.title}"
            )
            assert item_validated.source is not None, (
                f"source is missing for movie: {item_validated.title}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for movie: {item_validated.title}"
            )

        # Validate TV shows are MCTvItem
        for tv in credits.tv_shows:
            tv_dict = tv.model_dump() if hasattr(tv, "model_dump") else dict(tv)
            item_validated = MCTvItem.model_validate(tv_dict)
            assert item_validated.mc_type == MCType.TV_SERIES
            # Verify required MCBaseItem fields
            assert item_validated.mc_id is not None, (
                f"mc_id is missing for TV show: {item_validated.name}"
            )
            assert item_validated.mc_type == MCType.TV_SERIES, (
                f"mc_type is wrong for TV show: {item_validated.name}"
            )
            assert item_validated.source is not None, (
                f"source is missing for TV show: {item_validated.name}"
            )
            assert item_validated.source_id is not None, (
                f"source_id is missing for TV show: {item_validated.name}"
            )

        write_snapshot(data, "get_person_credits.json")

    @pytest.mark.asyncio
    async def test_search_person_async(self):
        """Test search_person_async wrapper function."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request for Brad Pitt (TMDB ID: 287)
        person_request = MCPersonSearchRequest(
            source_id="287",
            source=MCSources.TMDB,
            mc_type=MCType.PERSON,
            mc_id="person_287",
            mc_subtype=MCSubType.ACTOR,
            name="Brad Pitt",
        )

        # Call the wrapper function
        result = await search_person_async(person_request, limit=20)

        # Validate response structure
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # Validate person details
        assert result.details is not None
        person = MCPersonItem.model_validate(result.details.model_dump())
        assert person.mc_type == MCType.PERSON
        assert person.id == 287
        assert person.name == "Brad Pitt"
        # Verify required MCBaseItem fields for person
        assert person.mc_id is not None, f"mc_id is missing for person: {person.name}"
        assert person.mc_type == MCType.PERSON, f"mc_type is wrong for person: {person.name}"
        assert person.source is not None, f"source is missing for person: {person.name}"
        assert person.source_id is not None, f"source_id is missing for person: {person.name}"
        assert person.source_id == "287", f"source_id is wrong for person: {person.name}"
        # Validate works array contains both movies and TV shows
        assert len(result.works) > 0, "works array should not be empty"

        movies_found = 0
        tv_shows_found = 0

        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)

            # Determine if it's a movie or TV show
            if work_dict.get("mc_type") == MCType.MOVIE.value:
                item_validated = MCMovieItem.model_validate(work_dict)
                assert item_validated.mc_type == MCType.MOVIE
                movies_found += 1
                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for movie: {item_validated.title}"
                )
                assert item_validated.mc_type == MCType.MOVIE, (
                    f"mc_type is wrong for movie: {item_validated.title}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for movie: {item_validated.title}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for movie: {item_validated.title}"
                )
            elif work_dict.get("mc_type") == MCType.TV_SERIES.value:
                item_validated = MCTvItem.model_validate(work_dict)
                assert item_validated.mc_type == MCType.TV_SERIES
                tv_shows_found += 1
                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for TV show: {item_validated.name}"
                )
                assert item_validated.mc_type == MCType.TV_SERIES, (
                    f"mc_type is wrong for TV show: {item_validated.name}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for TV show: {item_validated.name}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for TV show: {item_validated.name}"
                )

        # Verify we have at least some works
        assert movies_found > 0 or tv_shows_found > 0, (
            "works array should contain at least one movie or TV show"
        )

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works.json")

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source(self):
        """Test search_person_async with invalid source."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with invalid source (not TMDB)
        person_request = MCPersonSearchRequest(
            source_id="123",
            source=MCSources.OPENLIBRARY,  # Invalid for TMDB wrapper
            mc_type=MCType.PERSON,
            mc_id="person_123",
            mc_subtype=MCSubType.AUTHOR,
            name="Test Author",
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_person_id(self):
        """Test search_person_async with invalid person ID."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with invalid person ID
        person_request = MCPersonSearchRequest(
            source_id="-1",  # Invalid (must be positive)
            source=MCSources.TMDB,
            mc_type=MCType.PERSON,
            mc_id="person_-1",
            mc_subtype=MCSubType.ACTOR,
            name="Invalid Person",
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source_id" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_person_not_found(self):
        """Test search_person_async with non-existent person ID."""
        from contracts.models import MCPersonSearchRequest

        # Create a person search request with non-existent person ID
        person_request = MCPersonSearchRequest(
            source_id="999999999",  # Very unlikely to exist
            source=MCSources.TMDB,
            mc_type=MCType.PERSON,
            mc_id="person_999999999",
            mc_subtype=MCSubType.ACTOR,
            name="Non-existent Person",
        )

        # Call the wrapper function
        result = await search_person_async(person_request)

        # Validate error response
        assert result.status_code in [404, 500]  # Could be 404 or 500 depending on API response
        assert result.error is not None
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_get_providers_list_tv(self, tmdb_handler, mock_callable_request):
        """Test get_providers_list handler for TV providers."""
        req = mock_callable_request({"content_type": "tv", "region": "US"})
        result = await tmdb_handler.get_providers_list(req)

        # Validate response structure
        assert isinstance(result, dict)
        assert "providers" in result
        assert "content_type" in result
        assert "region" in result
        assert "count" in result

        assert result["content_type"] == "tv"
        assert result["region"] == "US"
        assert result["count"] > 0
        assert len(result["providers"]) > 0

        # Validate providers structure
        for provider in result["providers"]:
            assert "provider_id" in provider
            assert "provider_name" in provider
            assert "display_priority" in provider
            # Verify channels are filtered out
            assert "channel" not in provider["provider_name"].lower()

        write_snapshot(result, "get_providers_list_tv.json")

    @pytest.mark.asyncio
    async def test_get_providers_list_movie(self, tmdb_handler, mock_callable_request):
        """Test get_providers_list handler for movie providers."""
        req = mock_callable_request({"content_type": "movie", "region": "US"})
        result = await tmdb_handler.get_providers_list(req)

        # Validate response structure
        assert isinstance(result, dict)
        assert "providers" in result
        assert "content_type" in result
        assert "region" in result
        assert "count" in result

        assert result["content_type"] == "movie"
        assert result["region"] == "US"
        assert result["count"] > 0
        assert len(result["providers"]) > 0

        # Validate providers structure
        for provider in result["providers"]:
            assert "provider_id" in provider
            assert "provider_name" in provider
            assert "display_priority" in provider
            # Verify channels are filtered out
            assert "channel" not in provider["provider_name"].lower()

        write_snapshot(result, "get_providers_list_movie.json")

    @pytest.mark.asyncio
    async def test_get_providers_list_defaults(self, tmdb_handler, mock_callable_request):
        """Test get_providers_list handler with default parameters."""
        req = mock_callable_request({})  # No data, should use defaults
        result = await tmdb_handler.get_providers_list(req)

        # Validate defaults: content_type="movie", region="US"
        assert result["content_type"] == "movie"
        assert result["region"] == "US"
        assert result["count"] > 0

    @pytest.mark.asyncio
    async def test_get_providers_list_invalid_content_type(
        self, tmdb_handler, mock_callable_request
    ):
        """Test get_providers_list handler with invalid content_type."""
        req = mock_callable_request({"content_type": "invalid", "region": "US"})

        with pytest.raises(https_fn.HttpsError) as exc_info:
            await tmdb_handler.get_providers_list(req)

        assert exc_info.value.code == "invalid-argument"
        assert exc_info.value.message == "content_type must be 'tv' or 'movie'"
