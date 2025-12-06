"""
Tests for Spotify search service.
"""

from unittest.mock import patch

import pytest

from api.subapi.spotify.models import (
    SpotifyAlbum,
    SpotifyAlbumSearchResponse,
    SpotifyArtist,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyTopTrackResponse,
    SpotifyTrack,
)
from api.subapi.spotify.search import SpotifySearchService
from api.subapi.spotify.tests.conftest import load_fixture


class TestSpotifySearchService:
    """Tests for SpotifySearchService."""

    @pytest.fixture
    def service(self):
        """Create SpotifySearchService instance."""
        return SpotifySearchService()

    @pytest.mark.asyncio
    async def test_search_albums_success(self, service, mock_auth):
        """Test successful album search."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/mock_spotify_album_search.json")

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_albums(query="Dark Side of the Moon", limit=20)

            assert isinstance(result, SpotifyAlbumSearchResponse)
            assert result.total_results > 0
            assert len(result.results) > 0
            # Verify MCBaseItem fields
            assert "mc_id" in result.results[0].model_dump()
            assert "mc_type" in result.results[0].model_dump()
            assert result.results[0].mc_type == "music_album"

    @pytest.mark.asyncio
    async def test_search_albums_no_results(self, service, mock_auth):
        """Test album search with no results."""
        mock_response_data = {"albums": {"items": []}}

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_albums(query="Nonexistent", limit=20)

            assert isinstance(result, SpotifyAlbumSearchResponse)
            assert result.total_results == 0
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_albums_with_track_type(self, service, mock_auth):
        """Test search_albums with type='track' - should extract albums from track results."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/mock_spotify_track_search.json")

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_albums(
                query="track:long distance artist:the who", type="track", limit=20
            )

            assert isinstance(result, SpotifyAlbumSearchResponse)
            assert result.total_results > 0
            assert len(result.results) > 0
            # Verify that albums were extracted from tracks
            assert result.results[0].title == "Wish You Were Here"
            assert result.results[0].artist == "Pink Floyd"
            # Verify MCBaseItem fields
            assert "mc_id" in result.results[0].model_dump()
            assert "mc_type" in result.results[0].model_dump()
            assert result.results[0].mc_type == "music_album"

    @pytest.mark.asyncio
    async def test_search_albums_with_track_type_no_results(self, service, mock_auth):
        """Test search_albums with type='track' when no tracks are found."""
        mock_response_data = {"tracks": {"items": []}}

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_albums(
                query="track:nonexistent track artist:fake artist", type="track", limit=20
            )

            assert isinstance(result, SpotifyAlbumSearchResponse)
            assert result.total_results == 0
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_artists_success(self, service, mock_auth):
        """Test successful artist search."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/mock_spotify_artist_search.json")

        with (
            patch.object(service, "_make_spotify_request", return_value=mock_response_data),
            patch.object(
                service,
                "get_top_track",
                return_value=SpotifyTopTrackResponse(
                    results=[],
                    total_results=0,
                    query="test",
                ),
            ),
        ):
            result = await service.search_artists(query="Pink Floyd", limit=20)

            assert isinstance(result, SpotifyArtistSearchResponse)
            assert result.total_results > 0
            assert len(result.results) > 0
            # Verify MCBaseItem fields
            assert "mc_id" in result.results[0].model_dump()
            assert "mc_type" in result.results[0].model_dump()
            assert result.results[0].mc_type == "music_artist"

    @pytest.mark.asyncio
    async def test_search_by_genre_success(self, service, mock_auth):
        """Test successful genre search."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/spotify_genre_search.json")

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_by_genre(genre="rock", limit=50)

            assert isinstance(result, SpotifyArtistSearchResponse)
            assert result.total_results > 0
            assert len(result.results) > 0
            # Verify MCBaseItem fields
            assert "mc_id" in result.results[0].model_dump()
            assert "mc_type" in result.results[0].model_dump()
            assert result.results[0].mc_type == "music_artist"

    @pytest.mark.asyncio
    async def test_search_by_keyword_success(self, service, mock_auth):
        """Test successful keyword search."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/spotify_keyword_search.json")

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.search_by_keyword(keyword="progressive", limit=50)

            assert isinstance(result, SpotifyMultiSearchResponse)
            assert result.total_results > 0
            # Verify MCBaseItem fields
            if result.results:
                assert "mc_id" in result.results[0].model_dump()
                assert "mc_type" in result.results[0].model_dump()

    @pytest.mark.asyncio
    async def test_search_albums_handles_api_error(self, service, mock_auth):
        """Test album search handles API errors."""
        with patch.object(service, "_make_spotify_request", return_value=None):
            result = await service.search_albums(query="test", limit=20)

            assert isinstance(result, SpotifyAlbumSearchResponse)
            assert result.total_results == 0
            assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_get_top_track_success(self, service, mock_auth):
        """Test successful top track retrieval."""
        # Load mock data from fixtures
        mock_response_data = load_fixture("make_requests/mock_spotify_top_tracks.json")

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.get_top_track(artist_id="0k17h0D3J5VfsdmQ1iZtE9")

            assert isinstance(result, SpotifyTopTrackResponse)
            assert result.total_results > 0
            assert len(result.results) > 0
            assert result.results[0].name == "Wish You Were Here"
            # Verify MCBaseItem fields
            assert "mc_id" in result.results[0].model_dump()
            assert "mc_type" in result.results[0].model_dump()
            assert result.results[0].mc_type == "music_track"

    @pytest.mark.asyncio
    async def test_get_top_track_no_results(self, service, mock_auth):
        """Test top track retrieval with no results."""
        mock_response_data = {"tracks": []}

        with patch.object(service, "_make_spotify_request", return_value=mock_response_data):
            result = await service.get_top_track(artist_id="test123")

            assert isinstance(result, SpotifyTopTrackResponse)
            assert result.total_results == 0
            assert len(result.results) == 0
            assert result.error == "No tracks found"

    @pytest.mark.asyncio
    async def test_get_top_track_handles_api_error(self, service, mock_auth):
        """Test top track retrieval handles API errors."""
        with patch.object(service, "_make_spotify_request", return_value=None):
            result = await service.get_top_track(artist_id="test123")

            assert isinstance(result, SpotifyTopTrackResponse)
            assert result.total_results == 0
            assert len(result.results) == 0
            assert result.error == "Failed to get top tracks"
