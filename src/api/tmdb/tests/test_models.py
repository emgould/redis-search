"""
Unit tests for TMDB Pydantic models.
Tests model validation, field generation, and serialization.
"""

import pytest
from contracts.models import MCSearchResponse, MCSubType, MCType

from api.tmdb.models import (
    MCBaseMediaItem,
    MCDiscoverResponse,
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCMovieItem,
    MCNowPlayingResponse,
    MCPersonCreditsResponse,
    MCPersonCreditsResult,
    MCPersonItem,
    MCPopularTVResponse,
    MCTvItem,
)
from api.tmdb.tests.conftest import load_fixture
from api.tmdb.tmdb_models import (
    TMDBCastMember,
    TMDBKeyword,
    TMDBMovieDetailsResult,
    TMDBPersonDetailsResult,
    TMDBTvDetailsResult,
    TMDBVideo,
    TMDBWatchProvider,
)

pytestmark = pytest.mark.unit


class TestMCBaseMediaItem:
    """Tests for MCBaseMediaItem model."""

    def test_create_movie_item(self):
        """Test creating a movie item using from_movie_details."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        movie_item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert movie_item is not None
        assert isinstance(movie_item, MCMovieItem)
        assert movie_item.tmdb_id == mock_movie_details["id"]
        assert movie_item.mc_id is not None
        assert isinstance(movie_item.mc_id, str)
        assert len(movie_item.mc_id) > 0
        assert movie_item.mc_type == "movie"

    def test_create_tv_item(self):
        """Test creating a TV show item using from_tv_details."""
        mock_tv_details = load_fixture("make_requests/get_media_details_tv.json")
        tv_details = TMDBTvDetailsResult.model_validate(mock_tv_details)
        tv_item = MCTvItem.from_tv_details(tv_details, image_base_url="https://image.tmdb.org/t/p/")
        assert tv_item is not None
        assert isinstance(tv_item, MCTvItem)
        assert tv_item.tmdb_id == mock_tv_details["id"]
        assert tv_item.mc_id is not None
        assert isinstance(tv_item.mc_id, str)
        assert len(tv_item.mc_id) > 0
        assert tv_item.mc_type == MCType.TV_SERIES

    def test_auto_generate_mc_id(self):
        """Test mc_id is auto-generated if not provided."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )

        assert item.mc_id is not None
        assert "tmdb" in item.mc_id  # mc_id format is tmdb_{id}
        assert item.mc_type == "movie"

    def test_auto_generate_mc_type(self):
        """Test mc_type is auto-generated based on media_type."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        movie_item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert movie_item.mc_type == MCType.MOVIE

        mock_tv_details = load_fixture("make_requests/get_media_details_tv.json")
        tv_details = TMDBTvDetailsResult.model_validate(mock_tv_details)
        tv_item = MCTvItem.from_tv_details(tv_details, image_base_url="https://image.tmdb.org/t/p/")
        assert tv_item.mc_type == MCType.TV_SERIES

    def test_model_dump(self):
        """Test model serialization."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        # Convert to MCMovieItem which has mc_id and mc_type
        item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        data = item.model_dump()

        assert isinstance(data, dict)
        assert data["tmdb_id"] == 550
        assert data["title"] == "Fight Club"
        assert "mc_id" in data
        assert "mc_type" in data

    def test_default_values(self):
        """Test default values are set correctly."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )

        # Check that default values are set (some may be overridden by fixture data)
        assert isinstance(item.overview, str)
        assert isinstance(item.vote_average, float)
        assert isinstance(item.vote_count, int)
        assert isinstance(item.popularity, float)
        assert isinstance(item.genre_ids, list)
        assert isinstance(item.genres, list)
        assert isinstance(item.adult, bool)


class TestMCPersonItem:
    """Tests for MCPersonItem model."""

    def test_create_person(self):
        mock_person_details = load_fixture("make_requests/get_person_details.json")
        # Use from_person_details to properly set mc_subtype
        person_details = TMDBPersonDetailsResult.model_validate(mock_person_details)
        person = MCPersonItem.from_person_details(
            person_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert person.id is not None
        assert person.name is not None
        assert person.biography.startswith("William Bradley")
        assert person.birthday == "1963-12-18"
        assert person.place_of_birth == "Shawnee, Oklahoma, USA"
        assert person.known_for_department == "Acting"
        assert person.mc_id is not None
        assert person.mc_type == MCType.PERSON
        # Verify mc_subtype is set based on known_for_department
        assert person.mc_subtype is not None
        assert person.mc_subtype == MCSubType.ACTOR  # known_for_department is "Acting"

    def test_auto_generate_mc_fields(self):
        """Test mc_id and mc_type are auto-generated."""
        mock_person_details = load_fixture("make_requests/get_person_details.json")
        person_details = TMDBPersonDetailsResult.model_validate(mock_person_details)
        person = MCPersonItem.from_person_details(
            person_details, image_base_url="https://image.tmdb.org/t/p/"
        )

        assert person.mc_id is not None
        assert "person" in person.mc_id
        assert person.mc_type == MCType.PERSON

    def test_has_image_flag(self):
        """Test has_image flag is set correctly."""
        mock_person_details = load_fixture("make_requests/get_person_details.json")
        assert MCPersonItem.model_validate(mock_person_details) is not None
        person_with_image = MCPersonItem.model_validate(mock_person_details)
        assert person_with_image.has_image is False  # Default value

        # Without profile path
        data_no_image = mock_person_details.copy()
        data_no_image["profile_path"] = None
        person_no_image = MCPersonItem.model_validate(data_no_image)
        assert person_no_image.has_image is False


class TestTMDBCastMember:
    """Tests for TMDBCastMember model."""

    def test_create_cast_member(self):
        """Test creating a cast member."""
        cast_fixture = load_fixture("core/cast_and_crew_movie.json")
        # Get first cast member from fixture
        cast_data = cast_fixture["tmdb_cast"]["cast"][1]  # Brad Pitt is second in the list
        cast_member = TMDBCastMember.model_validate(cast_data)

        assert cast_member.id == 287
        assert cast_member.name == "Brad Pitt"
        assert cast_member.character == "Tyler Durden"
        assert cast_member.order == 1
        assert cast_member.has_image is True

    def test_default_order(self):
        """Test default order value."""
        cast_fixture = load_fixture("core/cast_and_crew_movie.json")
        # Get a cast member and remove order to test default
        cast_data = cast_fixture["tmdb_cast"]["cast"][0].copy()
        cast_data.pop("order", None)  # Remove order to test default
        cast_member = TMDBCastMember.model_validate(cast_data)
        assert cast_member.order == 999  # Default value


class TestTMDBVideo:
    """Tests for TMDBVideo model."""

    def test_create_video(self):
        """Test creating a video."""
        videos_fixture = load_fixture("core/videos_movie.json")
        # Get first trailer from fixture
        video_data = videos_fixture["tmdb_videos"]["trailers"][0]
        video = TMDBVideo.model_validate(video_data)

        assert video.id == "653b36ba5907de00c4953699"
        assert video.key == "6JnN1DmbqoU"
        assert video.name == "Theatrical Trailer (HD Fan Remaster)"
        assert video.site == "YouTube"
        assert video.type == "Trailer"
        assert video.official is False
        assert video.size == 1080
        assert video.iso_639_1 == "en"


class TestTMDBWatchProvider:
    """Tests for TMDBWatchProvider model."""

    def test_create_watch_provider(self):
        """Test creating a watch provider."""
        providers_fixture = load_fixture("core/watch_providers_movie.json")
        # Get first flatrate provider from fixture
        provider_data = providers_fixture["watch_providers"]["flatrate"][0]
        provider = TMDBWatchProvider.model_validate(provider_data)

        assert provider.provider_id == 257
        assert provider.provider_name == "fuboTV"
        assert provider.logo_path == "/9BgaNQRMDvVlji1JBZi6tcfxpKx.jpg"
        assert provider.display_priority == 10

    def test_default_display_priority(self):
        """Test default display_priority."""
        providers_fixture = load_fixture("core/watch_providers_movie.json")
        # Get a provider and remove display_priority to test default
        provider_data = providers_fixture["watch_providers"]["flatrate"][0].copy()
        provider_data.pop("display_priority", None)
        provider = TMDBWatchProvider.model_validate(provider_data)
        assert provider.display_priority == 999  # Default value


class TestTMDBKeyword:
    """Tests for TMDBKeyword model."""

    def test_create_keyword(self):
        """Test creating a keyword."""
        keywords_fixture = load_fixture("core/keywords_movie.json")
        # Get keyword with id 825 from fixture
        keyword_data = next(kw for kw in keywords_fixture["keywords"] if kw["id"] == 825)
        keyword = TMDBKeyword.model_validate(keyword_data)

        assert keyword.id == 825
        assert keyword.name == "support group"


class TestMCSearchResponse:
    """Tests for MCSearchResponse model."""

    def test_create_search_response(self):
        """Test creating a search response."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        mock_item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert mock_item.mc_id is not None
        assert isinstance(mock_item.mc_id, str)
        assert len(mock_item.mc_id) > 0
        assert mock_item.mc_type == MCType.MOVIE
        response = MCSearchResponse(
            results=[mock_item],
            total_results=1,
            page=1,
            query="test query",
        )

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.page == 1
        assert response.query == "test query"

    def test_optional_fields(self):
        """Test optional fields."""
        response = MCSearchResponse(results=[], total_results=0, page=1)

        assert response.query is None
        assert response.data_source is None
        assert response.data_type is None


class TestMCGetTrendingMovieResult:
    """Tests for MCGetTrendingMovieResult model."""

    def test_create_trending_movie_result(self):
        """Test creating a trending movie result."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        mock_item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert mock_item.mc_id is not None
        assert isinstance(mock_item.mc_id, str)
        assert len(mock_item.mc_id) > 0
        assert mock_item.mc_type == MCType.MOVIE

        response = MCGetTrendingMovieResult(
            results=[mock_item],
            total_results=1,
            query="limit:10",
            data_source="TMDB",
        )

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.data_type.value == "movie"
        assert isinstance(response.results[0], MCBaseMediaItem)


class TestMCGetTrendingShowResult:
    """Tests for MCGetTrendingShowResult model."""

    def test_create_trending_show_result(self):
        """Test creating a trending TV show result."""
        mock_tv_details = load_fixture("make_requests/get_media_details_tv.json")
        tv_details = TMDBTvDetailsResult.model_validate(mock_tv_details)
        mock_item = MCTvItem.from_tv_details(
            tv_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert mock_item.mc_id is not None
        assert isinstance(mock_item.mc_id, str)
        assert len(mock_item.mc_id) > 0
        assert mock_item.mc_type == MCType.TV_SERIES

        response = MCGetTrendingShowResult(
            results=[mock_item],
            total_results=1,
            query="limit:10",
            data_source="TMDB",
        )

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.data_type.value == "tv"
        assert isinstance(response.results[0], MCBaseMediaItem)


class TestMCNowPlayingResponse:
    """Tests for MCNowPlayingResponse model."""

    def test_create_now_playing_response(self):
        """Test creating a now playing response."""
        mock_movie_details = load_fixture("make_requests/get_media_details_movie.json")
        movie_details = TMDBMovieDetailsResult.model_validate(mock_movie_details)
        mock_item = MCMovieItem.from_movie_details(
            movie_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert mock_item.mc_id is not None
        assert isinstance(mock_item.mc_id, str)
        assert len(mock_item.mc_id) > 0
        assert mock_item.mc_type == MCType.MOVIE

        response = MCNowPlayingResponse(
            results=[mock_item],
            total_results=1,
            data_source="TMDB",
        )

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.data_source == "TMDB"
        assert response.data_type.value == "movie"
        assert isinstance(response.results[0], MCBaseMediaItem)


class TestMCPopularTVResponse:
    """Tests for MCPopularTVResponse model."""

    def test_create_popular_tv_response(self):
        """Test creating a popular TV response."""
        mock_tv_details = load_fixture("make_requests/get_media_details_tv.json")
        tv_details = TMDBTvDetailsResult.model_validate(mock_tv_details)
        mock_item = MCTvItem.from_tv_details(
            tv_details, image_base_url="https://image.tmdb.org/t/p/"
        )
        assert mock_item.mc_id is not None
        assert isinstance(mock_item.mc_id, str)
        assert len(mock_item.mc_id) > 0
        assert mock_item.mc_type == MCType.TV_SERIES

        response = MCPopularTVResponse(
            results=[mock_item],
            total_results=1,
            data_source="TMDB",
        )

        assert len(response.results) == 1
        assert response.total_results == 1
        assert response.data_source == "TMDB"
        assert response.data_type.value == "tv"
        assert isinstance(response.results[0], MCBaseMediaItem)


class TestMCDiscoverResponse:
    """Tests for MCDiscoverResponse model."""

    def test_create_discover_response(self):
        discover_movies_response = load_fixture("search/search_by_keywords.json")
        """Test creating a discover response."""
        # Create a movie item from mock data
        assert MCDiscoverResponse.model_validate(discover_movies_response) is not None
        response = MCDiscoverResponse.model_validate(discover_movies_response)
        assert len(response.results) == len(discover_movies_response["results"])
        # Fixture uses tmdb_id or source_id, not id
        first_result = discover_movies_response["results"][0]
        fixture_id = first_result.get("tmdb_id") or int(first_result.get("source_id", 0))
        assert response.results[0].tmdb_id == fixture_id
        assert response.results[0].title == first_result["title"]
        assert response.total_results == discover_movies_response["total_results"]


class TestMCPersonCreditsResponse:
    """Tests for MCPersonCreditsResponse model."""

    def test_create_person_credits_response(self):
        """Test creating a person credits response."""
        # Load fixture with proper structure (person, movies, tv_shows, metadata)
        fixture = load_fixture("person/cast_details.json")

        # Remove 'id' fields from movies and tv_shows as MCBaseMediaItem doesn't have 'id' field
        cleaned_fixture = fixture.copy()
        if "movies" in cleaned_fixture:
            cleaned_fixture["movies"] = [
                {k: v for k, v in movie.items() if k != "id"} for movie in cleaned_fixture["movies"]
            ]
        if "tv_shows" in cleaned_fixture:
            cleaned_fixture["tv_shows"] = [
                {k: v for k, v in tv.items() if k != "id"} for tv in cleaned_fixture["tv_shows"]
            ]

        # Create MCPersonCreditsResult from fixture
        result = MCPersonCreditsResult.model_validate(cleaned_fixture)

        # Calculate total_results from metadata (like wrappers do)
        total_results = result.metadata.get("total_results", 0)
        if total_results == 0:
            # Fallback: calculate from movies + tv_shows + person if metadata doesn't have it
            total_results = len(result.movies) + len(result.tv_shows) + (1 if result.person else 0)

        # Wrap it in a Response (as wrappers do)
        response = MCPersonCreditsResponse(
            results=[result],
            total_results=total_results,
            data_source="TMDB Person API",
        )

        assert response is not None
        assert isinstance(response, MCPersonCreditsResponse)
        assert response.total_results == total_results
        assert len(response.results) == 1
        assert response.results[0].metadata["total_movies"] == fixture["metadata"]["total_movies"]
        assert (
            response.results[0].metadata["total_tv_shows"] == fixture["metadata"]["total_tv_shows"]
        )


class TestMCPersonCreditsResult:
    """Tests for MCPersonCreditsResult model."""

    def test_create_person_credits_result(self):
        """Test creating a person credits result."""
        # Create person from mock data
        fixture = load_fixture("person/cast_details.json")
        # Remove 'id' fields from movies and tv_shows as MCBaseMediaItem doesn't have 'id' field
        cleaned_fixture = fixture.copy()
        if "movies" in cleaned_fixture:
            cleaned_fixture["movies"] = [
                {k: v for k, v in movie.items() if k != "id"} for movie in cleaned_fixture["movies"]
            ]
        if "tv_shows" in cleaned_fixture:
            cleaned_fixture["tv_shows"] = [
                {k: v for k, v in tv.items() if k != "id"} for tv in cleaned_fixture["tv_shows"]
            ]
        result = MCPersonCreditsResult.model_validate(cleaned_fixture)
        assert result is not None
        # Verify mc_id and mc_type are set for person if present
        if result.person:
            assert result.person.mc_id is not None
            assert isinstance(result.person.mc_id, str)
            assert len(result.person.mc_id) > 0
            assert result.person.mc_type == "person"
        # Verify mc_id and mc_type are set for all movies and tv_shows
        for movie in result.movies:
            assert movie.mc_id is not None
            assert isinstance(movie.mc_id, str)
            assert len(movie.mc_id) > 0
            assert movie.mc_type == MCType.MOVIE or movie.mc_type == "movie"
        for tv_show in result.tv_shows:
            assert tv_show.mc_id is not None
            assert isinstance(tv_show.mc_id, str)
            assert len(tv_show.mc_id) > 0
            assert tv_show.mc_type == MCType.TV_SERIES or tv_show.mc_type == "tv"
