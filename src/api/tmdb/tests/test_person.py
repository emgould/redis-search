"""
Unit tests for TMDB Person Service.
Tests TMDBPersonService functionality.
"""

from unittest.mock import AsyncMock, patch

import pytest

from api.tmdb.models import (
    MCMovieItem,
    MCPersonCreditsResult,
    MCPersonItem,
)
from api.tmdb.person import TMDBPersonService
from api.tmdb.tests.conftest import load_fixture
from api.tmdb.tmdb_models import (
    TMDBMovieDetailsResult,
    TMDBPersonMovieCreditsResponse,
)
from contracts.models import MCSearchResponse

pytestmark = pytest.mark.unit


class TestTMDBPersonService:
    """Tests for TMDBPersonService class."""

    @pytest.mark.asyncio
    async def test_get_person_details(self):
        """Test getting person details."""
        service = TMDBPersonService()
        mock_person_details = load_fixture("make_requests/get_person_details.json")
        with patch.object(
            service, "_make_request", new=AsyncMock(return_value=mock_person_details)
        ):
            result = await service.get_person_details(287)

            assert isinstance(result, MCPersonItem)
            assert result.id == 287
            assert result.name == "Brad Pitt"
            assert result.biography.startswith("William Bradley")
            assert result.mc_id is not None
            assert result.mc_type == "person"
            # Verify mc_subtype is set based on known_for_department
            assert result.mc_subtype is not None
            from contracts.models import MCSubType

            assert (
                result.mc_subtype == MCSubType.ACTOR
            )  # Brad Pitt's known_for_department is "Acting"

    @pytest.mark.asyncio
    async def test_get_person_details_not_found(self):
        """Test getting person details for non-existent person."""
        service = TMDBPersonService()

        with patch.object(service, "_make_request", new=AsyncMock(return_value=None)):
            result = await service.get_person_details(999999)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_person_details_with_profile_images(self):
        """Test person details includes profile images."""
        service = TMDBPersonService()
        mock_person_details = load_fixture("make_requests/get_person_details.json")

        with patch.object(
            service, "_make_request", new=AsyncMock(return_value=mock_person_details)
        ):
            result = await service.get_person_details(287)

            assert result.profile_images is not None
            assert "small" in result.profile_images
            assert "medium" in result.profile_images
            assert "large" in result.profile_images
            assert "original" in result.profile_images

    @pytest.mark.asyncio
    async def test_get_person_movie_credits(self):
        """Test getting person movie credits."""
        service = TMDBPersonService()
        mock_movie_credits = load_fixture("make_requests/get_person_movie_credits.json")

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_movie_credits)):
            result = await service.get_person_movie_credits(287)

            assert isinstance(result, MCPersonCreditsResult)
            assert len(result.movies) > 0
            # After filtering out movies with popularity < 0.5, we get 105 movies
            assert result.metadata["total_movies"] == 105

    @pytest.mark.asyncio
    async def test_get_person_movie_credits_filters_low_popularity(self):
        """Test movie credits filters out low popularity movies."""
        service = TMDBPersonService()

        credits_data = {
            "id": 287,  # Required by TMDBPersonMovieCreditsResponse
            "cast": [
                {
                    "id": 123,
                    "title": "Popular Movie",
                    "original_title": "Popular Movie",
                    "original_language": "en",
                    "popularity": 10.0,
                    "media_type": "movie",
                },
                {
                    "id": 456,
                    "title": "Obscure Movie",
                    "original_title": "Obscure Movie",
                    "original_language": "en",
                    "popularity": 0.1,
                    "media_type": "movie",
                },  # Should be filtered
            ],
            "crew": [],
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=credits_data)):
            result = await service.get_person_movie_credits(287)

            # Should only have the popular movie
            assert len(result.movies) == 1
            assert result.movies[0].title == "Popular Movie"

    @pytest.mark.asyncio
    async def test_get_person_tv_credits(self):
        """Test getting person TV credits."""
        service = TMDBPersonService()
        mock_tv_credits = load_fixture("make_requests/get_person_tv_credits.json")

        with patch.object(service, "_make_request", new=AsyncMock(return_value=mock_tv_credits)):
            result = await service.get_person_tv_credits(287, 50)

            assert isinstance(result, MCPersonCreditsResult)
            assert len(result.movies) == 0
            assert len(result.tv_shows) > 0
            assert result.person is None
            assert result.tv_shows[0].character == "Brad Pitt"
            assert result.tv_shows[0].episode_count > 0
            assert result.metadata["total_tv_shows"] > 0

    @pytest.mark.asyncio
    async def test_get_person_tv_credits_filters_no_episodes(self):
        """Test TV credits filters out shows with no episodes."""
        service = TMDBPersonService()

        credits_data = {
            "id": 287,
            "cast": [
                {
                    "adult": False,
                    "backdrop_path": "/6bSYn0NCdVqDuBdwqPvulsNstLA.jpg",
                    "genre_ids": [10759, 18],
                    "id": 3556,
                    "origin_country": ["US"],
                    "original_language": "en",
                    "original_name": "From the Earth to the Moon",
                    "overview": "The story of the United States' space program, from its beginnings in 1961 to the final moon mission in 1972.",
                    "popularity": 18.203,
                    "poster_path": "/sSnYvoVT2PYWJbF0aWdUFvLqKhR.jpg",
                    "first_air_date": "1998-04-05",
                    "name": "From the Earth to the Moon",
                    "vote_average": 8.247,
                    "vote_count": 83,
                    "credit_id": "6179a0ccd236e6002a825188",
                    "department": "Creator",
                    "job": "Creator",
                },
                {
                    "adult": False,
                    "backdrop_path": "/1FxfmcCniLlLNljdU4v9Xihe4IO.jpg",
                    "genre_ids": [],
                    "id": 214739,
                    "origin_country": ["US"],
                    "original_language": "en",
                    "original_name": "Tis the Season: The Holidays on Screen",
                    "overview": "A panoramic celebration of the beloved genre of holiday films and television specials, featuring decades of rich archival footage and lively interviews with notable celebrities, directors, producers, film critics, historians, and pop culture experts.",
                    "popularity": 0.989,
                    "poster_path": "/9HpmWqKAtZv9Yz2X2ktXVSJRnCC.jpg",
                    "first_air_date": "2022-11-27",
                    "name": "Tis the Season: The Holidays on Screen",
                    "vote_average": 0,
                    "vote_count": 0,
                    "credit_id": "6379c9b0976e480076835b85",
                    "department": "Creator",
                    "job": "Creator",
                },
                {
                    "adult": False,
                    "backdrop_path": "/cK5AbLFBY2JDoqEdVXk0697e2SV.jpg",
                    "genre_ids": [35],
                    "id": 4608,
                    "origin_country": ["US"],
                    "original_language": "en",
                    "original_name": "30 Rock",
                    "overview": "Liz Lemon, the head writer for a late-night TV variety show in New York, tries to juggle all the egos around her while chasing her own dream.",
                    "popularity": 40.171,
                    "poster_path": "/6wPINGH6SvYCJ9NHSvIvFAPnmWr.jpg",
                    "first_air_date": "2006-10-11",
                    "name": "30 Rock",
                    "vote_average": 7.425,
                    "vote_count": 549,
                    "character": "Tom Hanks",
                    "credit_id": "62661aad7fcab31167275841",
                    "episode_count": 1,
                },
                {
                    "adult": False,
                    "backdrop_path": "/eujU3vpBvZNOExepuL3ezTN9N5W.jpg",
                    "genre_ids": [99],
                    "id": 72757,
                    "origin_country": ["US"],
                    "original_language": "en",
                    "original_name": "The Nineties",
                    "overview": "Hit rewind and explore the most iconic moments and influential people of The Nineties, the decade that gave us the Internet, DVDs, and other cultural and political milestones.",
                    "popularity": 3.435,
                    "poster_path": "/c342XIOS93CIB5uafzOUDGIVqwe.jpg",
                    "first_air_date": "2017-07-09",
                    "name": "The Nineties",
                    "vote_average": 7.8,
                    "vote_count": 12,
                    "character": "Self",
                    "credit_id": "6290cdaedf86a87625b7c57f",
                    "episode_count": 1,
                },
            ],
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=credits_data)):
            result = await service.get_person_tv_credits(287, 50)
            assert result is not None
            assert isinstance(result, MCPersonCreditsResult)
            assert len(result.tv_shows) == 1
            assert result.tv_shows[0].name == "30 Rock"

    @pytest.mark.asyncio
    async def test_get_person_tv_credits_filters_talk_shows(self):
        """Test TV credits filters out talk show self appearances."""
        service = TMDBPersonService()

        credits_data = {
            "id": 287,
            "cast": [
                {
                    "adult": False,
                    "backdrop_path": "/eujU3vpBvZNOExepuL3ezTN9N5W.jpg",
                    "genre_ids": [99],
                    "id": 72757,
                    "origin_country": ["US"],
                    "original_language": "en",
                    "original_name": "The Nineties",
                    "overview": "Hit rewind and explore the most iconic moments and influential people of The Nineties, the decade that gave us the Internet, DVDs, and other cultural and political milestones.",
                    "popularity": 3.435,
                    "poster_path": "/c342XIOS93CIB5uafzOUDGIVqwe.jpg",
                    "first_air_date": "2017-07-09",
                    "name": "The Nineties",
                    "vote_average": 7.8,
                    "vote_count": 12,
                    "character": "Self",
                    "credit_id": "6290cdaedf86a87625b7c57f",
                    "episode_count": 1,
                }
            ],
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=credits_data)):
            result = await service.get_person_tv_credits(287, 50)

            # Should only have the acting role, not the talk show
            assert len(result.tv_shows) == 0

    @pytest.mark.asyncio
    async def test_get_cast_details_person_not_found(self):
        """Test getting cast details for non-existent person."""
        service = TMDBPersonService()

        with patch.object(service, "get_person_details") as mock_person:
            mock_person.return_value = None

            result = await service.get_person_credits(999999)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_cast_details_invalid_person_id(self):
        """Test getting cast details with invalid person ID."""
        service = TMDBPersonService()

        result = await service.get_person_credits(-1)
        assert result is None

        result = await service.get_person_credits(0)
        assert result is None

    @pytest.mark.asyncio
    async def test_search_people(self):
        """Test searching for people."""
        service = TMDBPersonService()
        mock_person_details = load_fixture("person/person_details.json")
        # Create a proper person search result from person details
        # Person search results have original_name, person details don't
        # The API returns raw dictionaries, not Pydantic models
        person_search_data = {
            "adult": mock_person_details["adult"],
            "id": mock_person_details["id"],
            "name": mock_person_details["name"],
            "original_name": mock_person_details["name"],  # Add required field
            "media_type": "person",
            "popularity": mock_person_details["popularity"],
            "gender": mock_person_details["gender"],
            "known_for_department": mock_person_details["known_for_department"],
            "profile_path": mock_person_details["profile_path"],
            "known_for": [],
        }
        search_data = {
            "results": [person_search_data],  # Return raw dict, not Pydantic model
            "total_results": 1,
            "total_pages": 1,
            "page": 1,
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=search_data)):
            result = await service.search_people("Brad Pitt", 1, 20)

            assert isinstance(result, MCSearchResponse)
            assert len(result.results) == 1
            assert result.results[0].name == "Brad Pitt"
            assert result.query == "Brad Pitt"

    @pytest.mark.asyncio
    async def test_search_people_empty_query(self):
        """Test searching with empty query."""
        service = TMDBPersonService()

        result = await service.search_people("", 1, 20)

        assert result.results == []
        assert result.total_results == 0

    @pytest.mark.asyncio
    async def test_search_people_with_profile_images(self):
        """Test search people includes profile images."""
        service = TMDBPersonService()
        mock_person_details = load_fixture("person/person_details.json")
        person_search_data = {
            "adult": mock_person_details["adult"],
            "id": mock_person_details["id"],
            "name": mock_person_details["name"],
            "original_name": mock_person_details["name"],  # Add required field
            "media_type": "person",
            "popularity": mock_person_details["popularity"],
            "gender": mock_person_details["gender"],
            "known_for_department": mock_person_details["known_for_department"],
            "profile_path": mock_person_details["profile_path"],
            "known_for": [],
            "profile_images": {
                "small": f"{service.image_base_url}w45{mock_person_details['profile_path']}",
                "medium": f"{service.image_base_url}w185{mock_person_details['profile_path']}",
                "large": f"{service.image_base_url}h632{mock_person_details['profile_path']}",
                "original": f"{service.image_base_url}original{mock_person_details['profile_path']}",
            },
        }
        search_data = {
            "results": [person_search_data],  # Return raw dict, not Pydantic model
            "total_results": 1,
            "total_pages": 1,
            "page": 1,
        }

        with patch.object(service, "_make_request", new=AsyncMock(return_value=search_data)):
            result = await service.search_people("Brad Pitt", 1, 20)

            assert result.results[0].profile_images is not None
            assert "small" in result.results[0].profile_images
