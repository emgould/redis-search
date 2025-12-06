"""
Unit tests for FlixPatrol Core Service.
Tests FlixPatrolService base class functionality.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.subapi.flixpatrol.core import FlixPatrolService
from api.subapi.flixpatrol.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestFlixPatrolService:
    """Tests for FlixPatrolService class."""

    def test_service_initialization(self):
        """Test service initialization."""
        service = FlixPatrolService()
        assert service is not None


class TestDetectPlatformAndType:
    """Tests for _detect_platform_and_type method."""

    def test_detect_netflix_tv_shows(self):
        """Test detection of Netflix TV shows."""
        service = FlixPatrolService()

        result = service._detect_platform_and_type(
            "Top 10 TV Shows on Netflix on January 1, 2024", "toc-netflix-tv-shows"
        )

        assert result == ("netflix", "shows")

    def test_detect_netflix_movies(self):
        """Test detection of Netflix movies."""
        service = FlixPatrolService()

        result = service._detect_platform_and_type(
            "Top 10 Movies on Netflix on January 1, 2024", "toc-netflix-movies"
        )

        assert result == ("netflix", "movies")

    def test_detect_hbo_tv_shows(self):
        """Test detection of HBO TV shows."""
        service = FlixPatrolService()

        result = service._detect_platform_and_type(
            "Top 10 TV Shows on HBO on January 1, 2024", "toc-hbo-tv-shows"
        )

        assert result == ("hbo", "shows")

    def test_detect_disney_plus_movies(self):
        """Test detection of Disney+ movies."""
        service = FlixPatrolService()

        result = service._detect_platform_and_type(
            "Top 10 Movies on Disney on January 1, 2024", "toc-disney-movies"
        )

        assert result == ("disney+", "movies")

    def test_detect_unknown_platform(self):
        """Test detection returns None for unknown platform."""
        service = FlixPatrolService()

        result = service._detect_platform_and_type("Some Random Content", "toc-unknown")

        assert result is None


class TestExtractTableEntries:
    """Tests for _extract_table_entries method."""

    def test_extract_table_entries_with_valid_data(self):
        """Test extraction of table entries with valid data."""
        from bs4 import BeautifulSoup

        service = FlixPatrolService()

        html = """
        <table>
            <tbody>
                <tr>
                    <td>1</td>
                    <td><a href="#">Test Show 1</a></td>
                    <td>1,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td><a href="#">Test Show 2</a></td>
                    <td>900</td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")

        entries = service._extract_table_entries(table)

        assert len(entries) == 2
        assert entries[0] == {"rank": 1, "title": "Test Show 1", "score": 1000}
        assert entries[1] == {"rank": 2, "title": "Test Show 2", "score": 900}

    def test_extract_table_entries_with_missing_cells(self):
        """Test extraction handles rows with missing cells."""
        from bs4 import BeautifulSoup

        service = FlixPatrolService()

        html = """
        <table>
            <tbody>
                <tr>
                    <td>1</td>
                    <td><a href="#">Test Show 1</a></td>
                </tr>
                <tr>
                    <td>2</td>
                    <td><a href="#">Test Show 2</a></td>
                    <td>900</td>
                </tr>
            </tbody>
        </table>
        """

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")

        entries = service._extract_table_entries(table)

        # Should only extract the second row with all cells
        assert len(entries) == 1
        assert entries[0] == {"rank": 2, "title": "Test Show 2", "score": 900}


class TestParseFlixPatrolHtml:
    """Tests for parse_flixpatrol_html method."""

    def test_parse_flixpatrol_html_with_valid_data(self, mock_flixpatrol_html):
        """Test parsing FlixPatrol HTML with valid data."""
        service = FlixPatrolService()

        result = service.parse_flixpatrol_html(mock_flixpatrol_html)

        assert result.date
        assert result.shows
        assert result.movies
        assert "netflix" in result.shows
        assert "netflix" in result.movies
        assert len(result.shows["netflix"]) == 2
        assert len(result.movies["netflix"]) == 2

    def test_parse_flixpatrol_html_extracts_correct_data(self, mock_flixpatrol_html):
        """Test that parsing extracts correct data values."""
        service = FlixPatrolService()

        result = service.parse_flixpatrol_html(mock_flixpatrol_html)

        # Check shows
        shows = result.shows["netflix"]
        assert shows[0]["rank"] == 1
        assert shows[0]["title"] == "Test Show 1"
        assert shows[0]["score"] == 1000

        # Check movies
        movies = result.movies["netflix"]
        assert movies[0]["rank"] == 1
        assert movies[0]["title"] == "Test Movie 1"
        assert movies[0]["score"] == 2000


class TestFetchFlixPatrolData:
    """Tests for fetch_flixpatrol_data method."""

    @pytest.mark.asyncio
    async def test_fetch_flixpatrol_data_success(self, mock_flixpatrol_html):
        """Test successful fetch of FlixPatrol data."""
        service = FlixPatrolService()

        # Mock the entire method instead of aiohttp internals
        with patch.object(service, "fetch_flixpatrol_data", return_value=mock_flixpatrol_html):
            result = await service.fetch_flixpatrol_data()
            assert result == mock_flixpatrol_html

    @pytest.mark.asyncio
    async def test_fetch_flixpatrol_data_timeout(self):
        """Test fetch handles timeout errors."""
        service = FlixPatrolService()

        # Mock to raise TimeoutError
        with (
            patch.object(
                service, "fetch_flixpatrol_data", side_effect=TimeoutError("Request timed out")
            ),
            pytest.raises(TimeoutError),
        ):
            await service.fetch_flixpatrol_data()


class TestGetFlixPatrolData:
    """Tests for get_flixpatrol_data method."""

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_basic(self, mock_flixpatrol_html):
        """Test getting FlixPatrol data."""
        service = FlixPatrolService()

        with patch.object(service, "fetch_flixpatrol_data", return_value=mock_flixpatrol_html):
            result = await service.get_flixpatrol_data()

            # Result is now a FlixPatrolResponse model
            assert result.date is not None
            assert result.shows is not None
            assert result.movies is not None
            assert result.top_trending_tv_shows is not None
            assert result.top_trending_movies is not None
            assert result.metadata is not None

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_adds_platform_info(self, mock_flixpatrol_html):
        """Test that platform info is added to items."""
        service = FlixPatrolService()

        with patch.object(service, "fetch_flixpatrol_data", return_value=mock_flixpatrol_html):
            result = await service.get_flixpatrol_data()

            if result.top_trending_tv_shows:
                first_show = result.top_trending_tv_shows[0]
                assert first_show.platform is not None
                assert first_show.content_type is not None
                assert first_show.content_type == "tv"

            if result.top_trending_movies:
                first_movie = result.top_trending_movies[0]
                assert first_movie.platform is not None
                assert first_movie.content_type is not None
                assert first_movie.content_type == "movie"

    @pytest.mark.asyncio
    async def test_get_flixpatrol_data_with_custom_providers(self, mock_flixpatrol_html):
        """Test getting FlixPatrol data with custom providers."""
        service = FlixPatrolService()

        with patch.object(service, "fetch_flixpatrol_data", return_value=mock_flixpatrol_html):
            result = await service.get_flixpatrol_data(providers=["netflix"])

            assert result.date is not None
            assert result.metadata is not None
            # All items should be from netflix
            for show in result.top_trending_tv_shows:
                assert show.platform == "netflix"
            for movie in result.top_trending_movies:
                assert movie.platform == "netflix"
