"""
Unit tests for LastFM Enrichment Service.
Tests LastFMEnrichmentService Spotify enrichment functionality.

Note: Odesli enrichment tests are deprecated but kept for reference.
Current enrichment uses direct Apple Music API integration.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.lastfm.enrichment import LastFMEnrichmentService
from api.lastfm.tests.conftest import load_fixture
from api.subapi.spotify.models import SpotifyAlbum, SpotifyAlbumSearchResponse
from api.subapi.spotify.wrappers import spotify_wrapper

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestOdesliExpansion:
    """
    DEPRECATED: Tests for Odesli platform link expansion.

    These tests are kept for reference but the functionality is no longer actively used.
    We have migrated to direct Apple Music API integration.
    """

    @pytest.mark.skip(reason="Odesli integration deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_expand_with_odesli_success(self, mock_auth):
        """Test successful Odesli expansion."""
        # The _odesli_make_request method creates its own session, so we need to mock it directly
        mock_odesli_response = load_fixture("make_requests/mock_odesli_response.json")
        expected_links = {
            "spotify": mock_odesli_response["linksByPlatform"]["spotify"]["url"],
            "appleMusic": mock_odesli_response["linksByPlatform"]["appleMusic"]["url"],
            "youtubeMusic": mock_odesli_response["linksByPlatform"]["youtubeMusic"]["url"],
        }

        service = LastFMEnrichmentService()

        # Mock _odesli_make_request directly (it creates its own session)
        mock_session = MagicMock()
        with patch.object(service, "_odesli_make_request", return_value=expected_links):
            result = await service._expand_with_odesli(
                mock_session, "https://open.spotify.com/album/123"
            )

        assert "spotify" in result
        assert "appleMusic" in result
        assert "youtubeMusic" in result
        assert result["spotify"] == mock_odesli_response["linksByPlatform"]["spotify"]["url"]

    @pytest.mark.skip(reason="Odesli integration deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_expand_with_odesli_empty_url(self, mock_auth):
        """Test Odesli expansion with empty URL."""
        service = LastFMEnrichmentService()

        mock_session = MagicMock()

        # Mock _odesli_make_request to return None for empty URL
        with patch.object(service, "_odesli_make_request", return_value=None):
            result = await service._expand_with_odesli(mock_session, "")

        assert result == {}

    @pytest.mark.skip(reason="Odesli integration deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_expand_with_odesli_handles_error(self, mock_auth):
        """Test error handling in Odesli expansion."""
        service = LastFMEnrichmentService()

        mock_session = MagicMock()

        # Mock _odesli_make_request to return None on error
        with patch.object(service, "_odesli_make_request", return_value=None):
            result = await service._expand_with_odesli(
                mock_session, "https://open.spotify.com/album/123"
            )

        assert result == {}

    @pytest.mark.skip(reason="Odesli integration deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_expand_with_odesli_nested_format(self, mock_auth):
        """Test Odesli expansion with nested entitiesByUniqueId format."""
        service = LastFMEnrichmentService()

        expected_links = {
            "spotify": "https://open.spotify.com/album/123",
            "apple_music": "https://music.apple.com/album/123",
        }

        mock_session = MagicMock()

        # Mock _odesli_make_request to return the extracted links dict
        with patch.object(service, "_odesli_make_request", return_value=expected_links):
            result = await service._expand_with_odesli(
                mock_session, "https://open.spotify.com/album/123"
            )

        assert "spotify" in result
        assert "apple_music" in result


class TestAlbumEnrichment:
    """
    DEPRECATED: Tests for complete album enrichment workflow.

    These tests are kept for reference but the functionality is no longer actively used.
    Enrichment now happens in LastFMSearchService using Apple Music API.
    """

    @pytest.mark.skip(reason="Odesli-based enrichment deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_enrich_with_spotify_success(self, mock_auth):
        """Test successful album enrichment."""
        mock_spotify_album_search = load_fixture("make_requests/mock_spotify_album_search.json")
        mock_odesli_response = load_fixture("make_requests/mock_odesli_response.json")

        service = LastFMEnrichmentService()

        album = {"title": "The Dark Side of the Moon", "artist": "Pink Floyd"}

        # Create mock SpotifyAlbum from fixture data
        spotify_album_data = mock_spotify_album_search["albums"]["items"][0]
        mock_album = SpotifyAlbum.from_spotify_albumdata(spotify_album_data)
        mock_search_response = SpotifyAlbumSearchResponse(
            results=[mock_album],
            total_results=1,
            query="album:The Dark Side of the Moon artist:Pink Floyd",
        )

        # Mock Odesli response (extract links dict format)
        mock_odesli_links = {
            "spotify": mock_odesli_response["linksByPlatform"]["spotify"]["url"],
            "applemusic": mock_odesli_response["linksByPlatform"]["appleMusic"]["url"],
            "youtubemusic": mock_odesli_response["linksByPlatform"]["youtubeMusic"]["url"],
        }

        mock_session = MagicMock()

        # Mock the wrapper method (creates its own session internally)
        with patch.object(spotify_wrapper, "search_albums", return_value=mock_search_response):
            # Mock _odesli_make_request directly (creates its own session internally)
            with patch.object(service, "_odesli_make_request", return_value=mock_odesli_links):
                result = await service._enrich_with_spotify(album, mock_session)

        assert "spotify_url" in result
        assert "apple_music_url" in result
        assert "release_date" in result
        assert result["total_tracks"] == spotify_album_data["total_tracks"]

    @pytest.mark.skip(reason="Odesli-based enrichment deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_enrich_with_spotify_missing_title(self, mock_auth):
        """Test enrichment with missing album title."""
        service = LastFMEnrichmentService()

        album = {"artist": "Pink Floyd"}

        mock_session = MagicMock()

        result = await service._enrich_with_spotify(album, mock_session)

        assert result == album
        mock_session.post.assert_not_called()

    @pytest.mark.skip(reason="Odesli-based enrichment deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_enrich_with_spotify_no_token(self, mock_auth):
        """Test enrichment when Spotify token cannot be obtained."""
        service = LastFMEnrichmentService()

        album = {"title": "Test Album", "artist": "Test Artist"}

        mock_session = MagicMock()

        # Mock wrapper to return error status
        error_response = SpotifyAlbumSearchResponse(
            results=[],
            total_results=0,
            query="album:Test Album artist:Test Artist",
            error="Failed to get token",
            status_code=500,
        )
        with patch.object(spotify_wrapper, "search_albums", return_value=error_response):
            result = await service._enrich_with_spotify(album, mock_session)

        assert result == album

    @pytest.mark.skip(reason="Odesli-based enrichment deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_enrich_with_spotify_album_not_found(self, mock_auth):
        """Test enrichment when album is not found on Spotify."""
        service = LastFMEnrichmentService()

        album = {"title": "Unknown Album", "artist": "Unknown Artist"}

        # Mock empty album search response
        mock_search_response = SpotifyAlbumSearchResponse(
            results=[],
            total_results=0,
            query="album:Unknown Album artist:Unknown Artist",
        )

        mock_session = MagicMock()

        # Mock wrapper to return empty results
        with patch.object(spotify_wrapper, "search_albums", return_value=mock_search_response):
            result = await service._enrich_with_spotify(album, mock_session)

        assert result == album
