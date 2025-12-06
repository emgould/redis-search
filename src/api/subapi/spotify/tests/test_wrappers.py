"""
Tests for Spotify async wrapper functions.
"""

from unittest.mock import patch

import pytest

from api.subapi.spotify.models import (
    SpotifyAlbum,
    SpotifyAlbumSearchResponse,
    SpotifyArtist,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyPlaylist,
    SpotifyTopTrackResponse,
    SpotifyTrack,
)
from api.subapi.spotify.wrappers import spotify_wrapper


class TestSearchAlbums:
    """Tests for search_albums wrapper."""

    @pytest.mark.asyncio
    async def test_search_albums_success(self, mock_auth):
        """Test successful album search wrapper."""
        mock_albums = [
            SpotifyAlbum(id="1", title="Dark Side", artist="Pink Floyd"),
            SpotifyAlbum(id="2", title="The Wall", artist="Pink Floyd"),
        ]
        mock_response = SpotifyAlbumSearchResponse(
            results=mock_albums, total_results=2, query="Pink Floyd"
        )

        with patch.object(spotify_wrapper.service, "search_albums", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.search_albums(
                query="Pink Floyd", limit=20
            )

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 2
            assert result_dict["results"][0]["title"] == "Dark Side"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_album"

    @pytest.mark.asyncio
    async def test_search_albums_handles_error(self, mock_auth):
        """Test error handling in album search wrapper."""
        with patch.object(
            spotify_wrapper.service, "search_albums", side_effect=Exception("Test error")
        ):
            result_dict, status_code = await spotify_wrapper.search_albums(query="test", limit=20)

            assert status_code == 500
            assert result_dict.get("error") == "Test error"
            assert result_dict.get("results") == []

    @pytest.mark.asyncio
    async def test_search_albums_with_track_type(self, mock_auth):
        """Test search_albums wrapper with type='track' - extracts albums from track results."""
        mock_albums = [
            SpotifyAlbum(
                id="0bCAjiUamIFqKJsekOYuRw",
                title="Wish You Were Here",
                artist="Pink Floyd",
            ),
        ]
        mock_response = SpotifyAlbumSearchResponse(
            results=mock_albums, total_results=1, query="track:wish you were here artist:pink floyd"
        )

        with patch.object(spotify_wrapper.service, "search_albums", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.search_albums(
                query="track:wish you were here artist:pink floyd", type="track", limit=20
            )

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 1
            assert result_dict["results"][0]["title"] == "Wish You Were Here"
            assert result_dict["results"][0]["artist"] == "Pink Floyd"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_album"


class TestSearchByGenre:
    """Tests for search_by_genre wrapper."""

    @pytest.mark.asyncio
    async def test_search_by_genre_success(self, mock_auth):
        """Test successful genre search wrapper."""
        mock_artists = [
            SpotifyArtist(id="1", name="Artist 1", genres=["rock"]),
            SpotifyArtist(id="2", name="Artist 2", genres=["rock"]),
        ]
        mock_response = SpotifyArtistSearchResponse(
            results=mock_artists, total_results=2, query="rock"
        )

        with patch.object(spotify_wrapper.service, "search_by_genre", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.search_by_genre(genre="rock", limit=50)

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 2
            assert result_dict["results"][0]["name"] == "Artist 1"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_artist"

    @pytest.mark.asyncio
    async def test_search_by_genre_handles_error(self, mock_auth):
        """Test error handling in genre search wrapper."""
        with patch.object(
            spotify_wrapper.service, "search_by_genre", side_effect=Exception("Test error")
        ):
            result_dict, status_code = await spotify_wrapper.search_by_genre(genre="rock", limit=50)

            assert status_code == 500
            assert result_dict.get("error") == "Test error"
            assert result_dict.get("results") == []


class TestSearchByKeyword:
    """Tests for search_by_keyword wrapper."""

    @pytest.mark.asyncio
    async def test_search_by_keyword_success(self, mock_auth):
        """Test successful keyword search wrapper."""
        mock_results = [
            SpotifyArtist(id="1", name="Result 1"),
            SpotifyAlbum(id="2", title="Result 2", artist="Artist 2"),
            SpotifyPlaylist(id="3", name="Result 3"),
        ]
        mock_response = SpotifyMultiSearchResponse(
            results=mock_results,
            total_results=3,
            artist_count=1,
            album_count=1,
            playlist_count=1,
            query="test",
        )

        with patch.object(spotify_wrapper.service, "search_by_keyword", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.search_by_keyword(
                keyword="test", limit=50
            )

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 3
            assert result_dict["results"][0]["name"] == "Result 1"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]

    @pytest.mark.asyncio
    async def test_search_by_keyword_handles_error(self, mock_auth):
        """Test error handling in keyword search wrapper."""
        with patch.object(
            spotify_wrapper.service, "search_by_keyword", side_effect=Exception("Test error")
        ):
            result_dict, status_code = await spotify_wrapper.search_by_keyword(
                keyword="test", limit=50
            )

            assert status_code == 500
            assert result_dict.get("error") == "Test error"
            assert result_dict.get("results") == []


class TestSearchArtists:
    """Tests for search_artists wrapper."""

    @pytest.mark.asyncio
    async def test_search_artists_success(self, mock_auth):
        """Test successful artist search wrapper."""
        mock_artists = [
            SpotifyArtist(id="1", name="The Weeknd", top_track_track="Blinding Lights"),
            SpotifyArtist(id="2", name="The Weekend", top_track_track="Other Song"),
        ]
        mock_response = SpotifyArtistSearchResponse(
            results=mock_artists, total_results=2, query="The Weeknd"
        )

        with patch.object(spotify_wrapper.service, "search_artists", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.search_artists(
                query="The Weeknd", limit=20
            )

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 2
            assert result_dict["results"][0]["name"] == "The Weeknd"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_artist"

    @pytest.mark.asyncio
    async def test_search_artists_handles_error(self, mock_auth):
        """Test error handling in artist search wrapper."""
        with patch.object(
            spotify_wrapper.service, "search_artists", side_effect=Exception("Test error")
        ):
            result_dict, status_code = await spotify_wrapper.search_artists(query="test", limit=20)

            assert status_code == 500
            assert result_dict.get("error") == "Test error"
            assert result_dict.get("results") == []


class TestWrapperReturnFormat:
    """Tests for wrapper return format consistency."""

    @pytest.mark.asyncio
    async def test_all_wrappers_return_tuple(self, mock_auth):
        """Test that all wrappers return ApiWrapperResponse tuple format."""
        mock_response_search = SpotifyAlbumSearchResponse(results=[], total_results=0, query="test")
        mock_response_artist = SpotifyArtistSearchResponse(
            results=[], total_results=0, query="test"
        )
        mock_response_multi = SpotifyMultiSearchResponse(results=[], total_results=0, query="test")
        mock_response_track = SpotifyTopTrackResponse(results=[], total_results=0, query="test")

        with (
            patch.object(
                spotify_wrapper.service, "search_albums", return_value=mock_response_search
            ),
            patch.object(
                spotify_wrapper.service, "search_by_genre", return_value=mock_response_artist
            ),
            patch.object(
                spotify_wrapper.service, "search_by_keyword", return_value=mock_response_multi
            ),
            patch.object(
                spotify_wrapper.service, "search_artists", return_value=mock_response_artist
            ),
            patch.object(
                spotify_wrapper.service, "get_top_track", return_value=mock_response_track
            ),
        ):
            # Test all wrappers
            result1 = await spotify_wrapper.search_albums(query="test")
            result2 = await spotify_wrapper.search_by_genre(genre="rock")
            result3 = await spotify_wrapper.search_by_keyword(keyword="test")
            result4 = await spotify_wrapper.search_artists(query="test")
            result5 = await spotify_wrapper.get_top_track(artist_id="test")

            # All should return tuple of (dict, int)
            assert isinstance(result1, tuple) and len(result1) == 2
            assert isinstance(result2, tuple) and len(result2) == 2
            assert isinstance(result3, tuple) and len(result3) == 2
            assert isinstance(result4, tuple) and len(result4) == 2
            assert isinstance(result5, tuple) and len(result5) == 2

            # Second element should be status code (200 or 500)
            assert isinstance(result1[1], int)
            assert isinstance(result2[1], int)
            assert isinstance(result3[1], int)
            assert isinstance(result4[1], int)
            assert isinstance(result5[1], int)

    @pytest.mark.asyncio
    async def test_wrappers_return_error_response_on_error(self, mock_auth):
        """Test that all wrappers return error response on error."""
        with (
            patch.object(spotify_wrapper.service, "search_albums", side_effect=Exception("Error")),
            patch.object(
                spotify_wrapper.service, "search_by_genre", side_effect=Exception("Error")
            ),
            patch.object(
                spotify_wrapper.service, "search_by_keyword", side_effect=Exception("Error")
            ),
            patch.object(spotify_wrapper.service, "search_artists", side_effect=Exception("Error")),
            patch.object(spotify_wrapper.service, "get_top_track", side_effect=Exception("Error")),
        ):
            # Test all wrappers
            result1_dict, status1 = await spotify_wrapper.search_albums(query="test")
            result2_dict, status2 = await spotify_wrapper.search_by_genre(genre="rock")
            result3_dict, status3 = await spotify_wrapper.search_by_keyword(keyword="test")
            result4_dict, status4 = await spotify_wrapper.search_artists(query="test")
            result5_dict, status5 = await spotify_wrapper.get_top_track(artist_id="test")

            # All should return error status and error message
            assert status1 == 500
            assert status2 == 500
            assert status3 == 500
            assert status4 == 500
            assert status5 == 500

            assert result1_dict.get("error") == "Error"
            assert result2_dict.get("error") == "Error"
            assert result3_dict.get("error") == "Error"
            assert result4_dict.get("error") == "Error"
            assert result5_dict.get("error") == "Error"


class TestGetTopTrack:
    """Tests for get_top_track wrapper."""

    @pytest.mark.asyncio
    async def test_get_top_track_success(self, mock_auth):
        """Test successful top track wrapper."""
        mock_track = SpotifyTrack(
            id="track123",
            name="Test Track",
            spotify_url="https://open.spotify.com/track/track123",
        )
        mock_response = SpotifyTopTrackResponse(
            results=[mock_track],
            total_results=1,
            query="artist123",
        )

        with patch.object(spotify_wrapper.service, "get_top_track", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.get_top_track(artist_id="artist123")

            assert status_code == 200
            assert result_dict.get("error") is None
            assert "results" in result_dict
            assert len(result_dict["results"]) == 1
            assert result_dict["results"][0]["name"] == "Test Track"
            # Verify MCBaseItem fields
            assert "mc_id" in result_dict["results"][0]
            assert "mc_type" in result_dict["results"][0]
            assert result_dict["results"][0]["mc_type"] == "music_track"

    @pytest.mark.asyncio
    async def test_get_top_track_handles_error(self, mock_auth):
        """Test error handling in top track wrapper."""
        with patch.object(
            spotify_wrapper.service, "get_top_track", side_effect=Exception("Test error")
        ):
            result_dict, status_code = await spotify_wrapper.get_top_track(artist_id="test123")

            assert status_code == 500
            assert result_dict.get("error") == "Test error"
            assert result_dict.get("results") == []

    @pytest.mark.asyncio
    async def test_get_top_track_handles_empty_response(self, mock_auth):
        """Test top track wrapper handles empty response."""
        mock_response = SpotifyTopTrackResponse(
            results=[],
            total_results=0,
            query="artist123",
            error="No tracks found",
        )

        with patch.object(spotify_wrapper.service, "get_top_track", return_value=mock_response):
            result_dict, status_code = await spotify_wrapper.get_top_track(artist_id="artist123")

            assert status_code == 500
            assert result_dict.get("error") == "No tracks found"
            assert result_dict.get("results") == []
