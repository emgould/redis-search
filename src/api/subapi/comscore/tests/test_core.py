"""
Unit tests for Comscore Core Service.
Tests ComscoreService class functionality.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from api.subapi.comscore.core import ComscoreService
from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking
from api.subapi.comscore.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestComscoreService:
    """Tests for ComscoreService class."""

    def test_service_initialization(self):
        """Test service initialization."""
        service = ComscoreService()

        assert service.base_url == "https://movies.comscore.com/api.html"

    @pytest.mark.skip(reason="Complex async context manager mocking - tested via integration tests")
    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request."""
        service = ComscoreService()
        mock_response_data = load_fixture("make_requests/domestic_rankings.json")

        with patch("aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)

            # Create async context managers
            mock_get = AsyncMock()
            mock_get.__aenter__.return_value = mock_response
            mock_get.__aexit__.return_value = AsyncMock(return_value=None)

            mock_session_instance = AsyncMock()
            mock_session_instance.get.return_value = mock_get
            mock_session_instance.__aenter__.return_value = mock_session_instance
            mock_session_instance.__aexit__.return_value = AsyncMock(return_value=None)

            mock_session.return_value = mock_session_instance

            result = await service._make_request()

            assert result is not None
            assert "rankings" in result
            assert len(result["rankings"]) == 5

    @pytest.mark.asyncio
    async def test_make_request_non_200_status(self):
        """Test API request with non-200 status code."""
        service = ComscoreService()

        with patch("aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 500

            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response

            result = await service._make_request()

            assert result is None

    @pytest.mark.asyncio
    async def test_make_request_exception(self):
        """Test API request that raises exception."""
        service = ComscoreService()

        with patch("aiohttp.ClientSession") as mock_session:
            mock_session.return_value.__aenter__.side_effect = Exception("Network error")

            result = await service._make_request()

            assert result is None

    def test_process_rankings_data_success(self):
        """Test successful processing of rankings data."""
        mock_comscore_response = load_fixture("make_requests/domestic_rankings.json")
        service = ComscoreService()
        result = service._process_rankings_data(mock_comscore_response)

        assert result is not None
        assert isinstance(result, BoxOfficeData)
        assert len(result.rankings) == 5
        assert result.exhibition_week == "2025-01-03"
        assert result.fetched_at is not None

    def test_process_rankings_data_invalid_format(self):
        """Test processing with invalid data format."""
        service = ComscoreService()
        result = service._process_rankings_data({"invalid": "data"})

        assert result is None

    def test_process_rankings_data_empty_rankings(self):
        """Test processing with empty rankings list."""
        service = ComscoreService()
        data = {"rankings": [], "exhibitionWeek": "2025-01-03"}
        result = service._process_rankings_data(data)

        assert result is not None
        assert len(result.rankings) == 0

    def test_process_rankings_data_malformed_ranking(self):
        """Test processing with malformed ranking item."""
        service = ComscoreService()
        data = {
            "rankings": [
                {"rank": 1, "titleName": "Valid Movie", "weekendEstimate": 1000000},
                {"rank": "invalid", "titleName": "Invalid Rank"},  # Invalid rank
                {"rank": 3, "titleName": "Another Valid", "weekendEstimate": 500000},
            ],
            "exhibitionWeek": "2025-01-03",
        }
        result = service._process_rankings_data(data)

        assert result is not None
        # Should skip the malformed item
        assert len(result.rankings) == 2

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_success(self):
        """Test successful fetching of domestic rankings."""
        mock_comscore_response = load_fixture("make_requests/domestic_rankings.json")
        service = ComscoreService()

        with patch.object(service, "_make_request", return_value=mock_comscore_response):
            result = await service.get_domestic_rankings()

            assert result is not None
            assert isinstance(result, BoxOfficeData)
            assert len(result.rankings) == 5

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_api_failure(self):
        """Test handling of API request failure."""
        service = ComscoreService()

        with patch.object(service, "_make_request", return_value=None):
            result = await service.get_domestic_rankings(no_cache=True)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_domestic_rankings_processing_failure(self):
        """Test handling of data processing failure."""
        service = ComscoreService()

        with patch.object(service, "_make_request", return_value={"invalid": "data"}):
            result = await service.get_domestic_rankings(no_cache=True)

            assert result is None


class TestLevenshteinDistance:
    """Tests for Levenshtein distance calculation."""

    def test_levenshtein_identical_strings(self):
        """Test distance between identical strings."""
        service = ComscoreService()
        distance = service._levenshtein_distance("test", "test")

        assert distance == 0

    def test_levenshtein_one_substitution(self):
        """Test distance with one substitution."""
        service = ComscoreService()
        distance = service._levenshtein_distance("test", "best")

        assert distance == 1

    def test_levenshtein_one_insertion(self):
        """Test distance with one insertion."""
        service = ComscoreService()
        distance = service._levenshtein_distance("test", "tests")

        assert distance == 1

    def test_levenshtein_one_deletion(self):
        """Test distance with one deletion."""
        service = ComscoreService()
        distance = service._levenshtein_distance("tests", "test")

        assert distance == 1

    def test_levenshtein_multiple_operations(self):
        """Test distance with multiple operations."""
        service = ComscoreService()
        distance = service._levenshtein_distance("kitten", "sitting")

        assert distance == 3  # k->s, e->i, insert g

    def test_levenshtein_empty_strings(self):
        """Test distance with empty strings."""
        service = ComscoreService()

        assert service._levenshtein_distance("", "") == 0
        assert service._levenshtein_distance("test", "") == 4
        assert service._levenshtein_distance("", "test") == 4


class TestMatchMovieToRanking:
    """Tests for movie title matching."""

    def test_match_movie_exact_match(self):
        """Test exact title match."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        result = service.match_movie_to_ranking("Wicked", data.rankings)

        assert result is not None
        assert result.rank == 1
        assert result.title_name == "Wicked"

    def test_match_movie_case_insensitive(self):
        """Test case-insensitive matching."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        result = service.match_movie_to_ranking("wicked", data.rankings)

        assert result is not None
        assert result.rank == 1

    def test_match_movie_fuzzy_match(self):
        """Test fuzzy matching with small differences."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        # Small typo should still match
        result = service.match_movie_to_ranking("Wickd", data.rankings)

        assert result is not None
        assert result.title_name == "Wicked"

    def test_match_movie_no_match(self):
        """Test no match for completely different title."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        result = service.match_movie_to_ranking("Completely Different Movie", data.rankings)

        assert result is not None
        assert result.error is not None
        assert result.status_code == 404
        assert "No match found" in result.error

    def test_match_movie_empty_title(self):
        """Test matching with empty title."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        result = service.match_movie_to_ranking("", data.rankings)

        assert result is not None
        assert result.error is not None
        assert result.status_code == 400
        assert "Invalid input" in result.error

    def test_match_movie_empty_rankings(self):
        """Test matching with empty rankings list."""
        service = ComscoreService()

        result = service.match_movie_to_ranking("Test Movie", [])

        assert result is not None
        assert result.error is not None
        assert result.status_code == 400
        assert "Invalid input" in result.error

    def test_match_movie_with_whitespace(self):
        """Test matching with extra whitespace."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        result = service.match_movie_to_ranking("  Wicked  ", data.rankings)

        assert result is not None
        assert result.rank == 1


class TestCreateRankingMap:
    """Tests for creating ranking map."""

    def test_create_ranking_map_success(self):
        """Test successful creation of ranking map."""
        mock_box_office_data = load_fixture("models/box_office_data.json")
        service = ComscoreService()
        data = BoxOfficeData.model_validate(mock_box_office_data)

        ranking_map = service.create_ranking_map(data.rankings)

        assert len(ranking_map) == 3
        assert ranking_map["wicked"] == 1
        assert ranking_map["moana 2"] == 2
        assert ranking_map["nosferatu"] == 3

    def test_create_ranking_map_empty_rankings(self):
        """Test creating map with empty rankings."""
        service = ComscoreService()

        ranking_map = service.create_ranking_map([])

        assert len(ranking_map) == 0

    def test_create_ranking_map_normalizes_titles(self):
        """Test that titles are normalized (lowercase, stripped)."""
        service = ComscoreService()
        rankings = [
            BoxOfficeRanking(rank=1, title_name="  Test Movie  ", weekend_estimate="1000000"),
            BoxOfficeRanking(rank=2, title_name="ANOTHER MOVIE", weekend_estimate="500000"),
        ]

        ranking_map = service.create_ranking_map(rankings)

        assert "test movie" in ranking_map
        assert "another movie" in ranking_map
