"""
Unit tests for LastFM Core Service.
Tests LastFMService base class functionality.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from api.lastfm.core import LastFMService
from api.lastfm.tests.conftest import load_fixture

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestLastFMService:
    """Tests for LastFMService class."""

    def test_service_initialization(self, mock_auth, mock_lastfm_api_key):
        """Test service initialization with API key."""
        service = LastFMService()

        assert service.lastfm_api_key == mock_lastfm_api_key
        assert service.base_url == "https://ws.audioscrobbler.com/2.0/"



class TestProcessAlbumItem:
    """Tests for _process_album_item method."""

    def test_process_album_item_with_full_data(self, mock_auth):
        """Test processing album item with complete data."""
        mock_album_data = load_fixture("make_requests/mock_album_data.json")

        service = LastFMService()
        result = service._process_album_item(mock_album_data)

        assert result["title"] == mock_album_data["name"]
        assert result["artist"] == mock_album_data["artist"]
        assert result["listeners"] == int(mock_album_data["listeners"])
        assert result["playcount"] == int(mock_album_data["playcount"])
        assert "image" in result
        assert result["mbid"] == mock_album_data["mbid"]
        assert "mc_id" in result
        assert result["mc_type"] == "music_album"

    def test_process_album_item_with_dict_artist(self, mock_auth):
        """Test processing album with artist as dictionary."""
        service = LastFMService()
        album_data = {
            "name": "Test Album",
            "artist": {"name": "Test Artist", "url": "https://example.com"},
            "listeners": "1000",
            "playcount": "5000",
        }

        result = service._process_album_item(album_data)

        assert result["artist"] == "Test Artist"
        assert result["artist_url"] == "https://example.com"

    def test_process_album_item_with_string_artist(self, mock_auth):
        """Test processing album with artist as string."""
        service = LastFMService()
        album_data = {
            "name": "Test Album",
            "artist": "Test Artist String",
            "listeners": "1000",
            "playcount": "5000",
        }

        result = service._process_album_item(album_data)

        assert result["artist"] == "Test Artist String"
        assert result["artist_url"] is None

    def test_process_album_item_selects_best_image(self, mock_auth):
        """Test that extralarge image is selected when available."""
        service = LastFMService()
        album_data = {
            "name": "Test Album",
            "artist": "Test Artist",
            "image": [
                {"#text": "small.jpg", "size": "small"},
                {"#text": "medium.jpg", "size": "medium"},
                {"#text": "large.jpg", "size": "large"},
                {"#text": "extralarge.jpg", "size": "extralarge"},
            ],
        }

        result = service._process_album_item(album_data)

        assert result["image"] == "extralarge.jpg"

    def test_process_album_item_fallback_to_any_image(self, mock_auth):
        """Test fallback to any available image if extralarge not found."""
        service = LastFMService()
        album_data = {
            "name": "Test Album",
            "artist": "Test Artist",
            "image": [
                {"#text": "", "size": "small"},
                {"#text": "medium.jpg", "size": "medium"},
            ],
        }

        result = service._process_album_item(album_data)

        assert result["image"] == "medium.jpg"

    def test_process_album_item_handles_missing_listeners(self, mock_auth):
        """Test handling of missing listeners field."""
        service = LastFMService()
        album_data = {"name": "Test Album", "artist": "Test Artist"}

        result = service._process_album_item(album_data)

        assert result["listeners"] == 0
        assert result["playcount"] == 0

    def test_process_album_item_handles_error(self, mock_auth):
        """Test error handling in album processing."""
        service = LastFMService()
        # Invalid data that will cause an error (non-dict will cause exception)
        album_data = {"listeners": "not_a_number_but_truthy"}

        result = service._process_album_item(album_data)

        assert "error" in result
        assert result["artist"] == "Unknown"


class TestLevenshteinDistance:
    """Tests for _levenshtein_distance method."""

    def test_levenshtein_identical_strings(self, mock_auth):
        """Test Levenshtein distance for identical strings."""
        service = LastFMService()
        distance = service._levenshtein_distance("hello", "hello")

        assert distance == 0

    def test_levenshtein_one_insertion(self, mock_auth):
        """Test Levenshtein distance for one insertion."""
        service = LastFMService()
        distance = service._levenshtein_distance("hello", "helo")

        assert distance == 1

    def test_levenshtein_one_deletion(self, mock_auth):
        """Test Levenshtein distance for one deletion."""
        service = LastFMService()
        distance = service._levenshtein_distance("helo", "hello")

        assert distance == 1

    def test_levenshtein_one_substitution(self, mock_auth):
        """Test Levenshtein distance for one substitution."""
        service = LastFMService()
        distance = service._levenshtein_distance("hello", "hallo")

        assert distance == 1

    def test_levenshtein_completely_different(self, mock_auth):
        """Test Levenshtein distance for completely different strings."""
        service = LastFMService()
        distance = service._levenshtein_distance("abc", "xyz")

        assert distance == 3

    def test_levenshtein_empty_strings(self, mock_auth):
        """Test Levenshtein distance with empty strings."""
        service = LastFMService()

        assert service._levenshtein_distance("", "") == 0
        assert service._levenshtein_distance("hello", "") == 5
        assert service._levenshtein_distance("", "hello") == 5


class TestProcessSpotifyResult:
    """Tests for _process_spotify_result method."""

    def test_process_spotify_result_artists(self, mock_auth):
        """Test processing Spotify artist results."""
        mock_artist_data = load_fixture("make_requests/mock_artist_data.json")

        service = LastFMService()
        results = service._process_spotify_result([mock_artist_data], "music_artist")

        assert len(results) == 1
        assert results[0]["id"] == mock_artist_data["id"]
        assert results[0]["name"] == mock_artist_data["name"]
        # mc_type is set by the Pydantic model, not by _process_spotify_result
        assert results[0]["popularity"] == mock_artist_data["popularity"]
        assert results[0]["artist"] == mock_artist_data["name"]
        assert results[0]["title"] == mock_artist_data["name"]

    def test_process_spotify_result_sorts_by_popularity(self, mock_auth):
        """Test that results are sorted by popularity descending."""
        service = LastFMService()
        data = [
            {"id": "1", "name": "Artist 1", "popularity": 50, "images": []},
            {"id": "2", "name": "Artist 2", "popularity": 90, "images": []},
            {"id": "3", "name": "Artist 3", "popularity": 70, "images": []},
        ]

        results = service._process_spotify_result(data, "music_artist")

        assert results[0]["popularity"] == 90
        assert results[1]["popularity"] == 70
        assert results[2]["popularity"] == 50

    def test_process_spotify_result_extracts_image_url(self, mock_auth):
        """Test that largest image URL is extracted."""
        service = LastFMService()
        data = [
            {
                "id": "1",
                "name": "Artist",
                "popularity": 80,
                "images": [
                    {"url": "large.jpg", "height": 640, "width": 640},
                    {"url": "small.jpg", "height": 160, "width": 160},
                ],
            }
        ]

        results = service._process_spotify_result(data, "music_artist")

        assert results[0]["image"] == "large.jpg"

    def test_process_spotify_result_handles_no_images(self, mock_auth):
        """Test handling of items with no images."""
        service = LastFMService()
        data = [{"id": "1", "name": "Artist", "popularity": 80, "images": []}]

        results = service._process_spotify_result(data, "music_artist")

        assert results[0]["image"] is None

    def test_process_spotify_result_includes_all_fields(self, mock_auth):
        """Test that all expected fields are included in results."""
        service = LastFMService()
        data = [
            {
                "id": "test-id",
                "name": "Test Name",
                "popularity": 75,
                "followers": {"total": 1000000},
                "genres": ["rock", "pop"],
                "images": [{"url": "test.jpg"}],
                "external_urls": {"spotify": "https://spotify.com/test"},
            }
        ]

        results = service._process_spotify_result(data, "music_artist")

        assert "id" in results[0]
        assert "name" in results[0]
        # mc_id and mc_type are set by the Pydantic model, not by _process_spotify_result
        assert "spotify_url" in results[0]
        assert "popularity" in results[0]
        assert "followers" in results[0]
        assert "genres" in results[0]
        assert "image" in results[0]
        assert "images" in results[0]
        assert "artist" in results[0]
        assert "title" in results[0]

    def test_process_spotify_result_handles_none_followers(self, mock_auth):
        """Test handling of None followers field (albums and playlists don't have followers)."""
        service = LastFMService()
        data = [
            {
                "id": "album-id",
                "name": "Test Album",
                "popularity": 80,
                "followers": None,  # Albums don't have followers
                "genres": [],
                "images": [{"url": "album.jpg"}],
                "external_urls": {"spotify": "https://spotify.com/album"},
            }
        ]

        results = service._process_spotify_result(data, "music_album")

        assert results[0]["followers"] == 0
        assert results[0]["spotify_url"] == "https://spotify.com/album"

    def test_process_spotify_result_handles_none_external_urls(self, mock_auth):
        """Test handling of None external_urls field."""
        service = LastFMService()
        data = [
            {
                "id": "test-id",
                "name": "Test Item",
                "popularity": 70,
                "followers": None,
                "genres": [],
                "images": [],
                "external_urls": None,  # Could be None in some cases
            }
        ]

        results = service._process_spotify_result(data, "music_playlist")

        assert results[0]["spotify_url"] is None
        assert results[0]["followers"] == 0
