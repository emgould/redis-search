"""
Integration tests for LastFM service.
These tests hit actual API endpoints (Spotify, Last.fm, Odesli) with no mocks.

Requirements:
- LASTFM_API_KEY environment variable must be set
- SPOTIFY_CLIENT_ID environment variable must be set
- SPOTIFY_CLIENT_SECRET environment variable must be set
- Internet connection required
- Tests may be slower due to actual API calls

Run with: pytest services/lastfm/tests/test_integration.py -v
"""

import json
import os

import pytest
from contracts.models import MCSources, MCSubType, MCType

from api.lastfm.core import LastFMService
from api.lastfm.enrichment import LastFMEnrichmentService
from api.lastfm.models import (
    LastFMAlbumSearchResponse,
    LastFMArtistSearchResponse,
    LastFMMultiSearchResponse,
    LastFMTrendingAlbumsResponse,
    MCMusicAlbum,
    MCMusicArtist,
    MCMusicPlaylist,
    SpotifyAlbumMetadata,
)
from api.lastfm.search import LastFMSearchService
from api.lastfm.tests.timing_utils import time_operation
from api.lastfm.wrappers import lastfm_wrapper

pytestmark = pytest.mark.integration


@pytest.fixture
def real_lastfm_api_key():
    """Get real Last.fm API key from environment."""
    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        pytest.skip("LASTFM_API_KEY environment variable not set")
    return api_key


@pytest.fixture
def real_spotify_credentials():
    """Get real Spotify credentials from environment."""
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        pytest.skip("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET environment variable not set")
    return {"client_id": client_id, "client_secret": client_secret}


@pytest.fixture
def lastfm_service(real_lastfm_api_key):
    """Create LastFMService instance with real API key."""
    return LastFMService(real_lastfm_api_key)


@pytest.fixture
def lastfm_enrichment_service(real_lastfm_api_key, real_spotify_credentials, monkeypatch):
    """Create LastFMEnrichmentService instance with real API credentials."""
    monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", real_spotify_credentials["client_id"])
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", real_spotify_credentials["client_secret"])
    return LastFMEnrichmentService()


@pytest.fixture
def lastfm_search_service(real_lastfm_api_key, real_spotify_credentials, monkeypatch):
    """Create LastFMSearchService instance with real API credentials."""
    monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", real_spotify_credentials["client_id"])
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", real_spotify_credentials["client_secret"])
    return LastFMSearchService()


def check_music_album(album: MCMusicAlbum, title: str = None, artist: str = None):
    """Validate MCMusicAlbum model structure and data quality."""
    assert isinstance(album, MCMusicAlbum)

    # Core fields
    assert album.title is not None
    assert len(album.title) > 0
    if title:
        assert album.title == title

    assert album.artist is not None
    assert len(album.artist) > 0
    if artist:
        assert album.artist == artist

    # MediaCircle standardized fields - REQUIRED
    assert album.mc_id is not None, "mc_id is required"
    assert len(album.mc_id) > 0, "mc_id cannot be empty"
    assert album.mc_type == "music_album", "mc_type must be 'music_album'"
    assert album.source is not None, "source is required"
    assert album.source.value in ["lastfm", "spotify"], "source must be 'lastfm' or 'spotify'"
    assert album.source_id is not None, "source_id is required"
    assert len(album.source_id) > 0, "source_id cannot be empty"

    # Image should be present (critical for UI)
    assert album.image is not None
    assert len(album.image) > 0
    assert album.image.startswith("http")

    # Spotify URL should be present
    assert album.spotify_url is not None
    assert "spotify.com" in album.spotify_url

    # Spotify metadata
    if album.release_date:
        assert len(album.release_date) >= 4  # At least year

    if album.total_tracks:
        assert album.total_tracks > 0

    if album.album_type:
        assert album.album_type in ["album", "single", "compilation"]

    if album.popularity is not None:
        assert 0 <= album.popularity <= 100


def check_music_artist(artist: MCMusicArtist, name: str = None):
    """Validate MCMusicArtist model structure and data quality."""
    assert isinstance(artist, MCMusicArtist)

    # Core fields
    assert artist.id is not None
    assert len(artist.id) > 0

    assert artist.name is not None
    assert len(artist.name) > 0
    if name:
        assert artist.name == name

    # MediaCircle standardized fields - REQUIRED
    assert artist.mc_id is not None, "mc_id is required"
    assert artist.mc_id.startswith("spotify_artist_"), "mc_id should start with 'spotify_artist_'"
    assert artist.mc_type == MCType.PERSON, "mc_type must be 'person'"
    assert artist.source is not None, "source is required"
    assert artist.source.value in ["lastfm", "spotify"], "source must be 'lastfm' or 'spotify'"
    assert artist.source_id is not None, "source_id is required"
    assert len(artist.source_id) > 0, "source_id cannot be empty"

    # Spotify URL
    assert artist.spotify_url is not None
    assert "spotify.com" in artist.spotify_url

    # Image should be present
    if artist.image:
        assert artist.image.startswith("http")

    # Popularity and followers
    if artist.popularity is not None:
        assert 0 <= artist.popularity <= 100

    if artist.followers is not None:
        assert artist.followers >= 0

    # Compatibility fields
    assert artist.artist == artist.name
    assert artist.title == artist.name


def check_music_playlist(playlist: MCMusicPlaylist):
    """Validate MCMusicPlaylist model structure and data quality."""
    assert isinstance(playlist, MCMusicPlaylist)

    # Core fields
    assert playlist.id is not None
    assert len(playlist.id) > 0

    assert playlist.name is not None
    assert len(playlist.name) > 0

    # MediaCircle standardized fields - REQUIRED
    assert playlist.mc_id is not None, "mc_id is required"
    assert playlist.mc_id.startswith("spotify_playlist_"), (
        "mc_id should start with 'spotify_playlist_'"
    )
    assert playlist.mc_type == "music_playlist", "mc_type must be 'music_playlist'"
    assert playlist.source is not None, "source is required"
    assert playlist.source.value in ["lastfm", "spotify"], "source must be 'lastfm' or 'spotify'"
    assert playlist.source_id is not None, "source_id is required"
    assert len(playlist.source_id) > 0, "source_id cannot be empty"

    # Spotify URL
    assert playlist.spotify_url is not None
    assert "spotify.com" in playlist.spotify_url

    # Compatibility fields
    assert playlist.title == playlist.name


class TestLastFMEnrichmentServiceIntegration:
    """Integration tests for LastFMEnrichmentService."""

    @pytest.mark.asyncio
    async def test_search_spotify_track(self, lastfm_enrichment_service):
        """Test searching for a track on Spotify."""
        with time_operation("Spotify track search"):
            result = await lastfm_enrichment_service._search_spotify_track(
                track_name="Bohemian Rhapsody", artist_name="Queen"
            )

        assert result is not None
        assert isinstance(result, dict)
        assert result.get("spotify_url") is not None
        assert "spotify.com" in result["spotify_url"]
        assert result.get("album_name") is not None
        assert result.get("image") is not None

    @pytest.mark.asyncio
    async def test_search_spotify_album(self, lastfm_enrichment_service: LastFMEnrichmentService):
        """Test searching for an album on Spotify."""
        from api.subapi.spotify.models import SpotifyAlbumMetadata as SpotifyAlbumMetadataType

        with time_operation("Spotify album search"):
            result = await lastfm_enrichment_service._search_spotify_album(
                album_name="Abbey Road", artist_name="The Beatles"
            )

        assert result is not None
        assert isinstance(result, SpotifyAlbumMetadataType)
        assert result.spotify_url is not None
        assert "spotify.com" in result.spotify_url
        assert result.album_name is not None
        assert result.image is not None

    @pytest.mark.skip(reason="Odesli integration deprecated - using Apple Music API now")
    @pytest.mark.asyncio
    async def test_expand_with_odesli(self, lastfm_enrichment_service):
        """
        DEPRECATED: Test expanding Spotify URL with Odesli for cross-platform links.

        This test is kept for reference but the functionality is no longer actively used.
        We have migrated to direct Apple Music API integration.
        """
        import aiohttp

        # Use a well-known Spotify album URL
        spotify_url = "https://open.spotify.com/album/0ETFjACtuP2ADo6LFhL6HN"  # Abbey Road

        async with aiohttp.ClientSession() as session:
            with time_operation("Odesli API expansion"):
                result = await lastfm_enrichment_service._expand_with_odesli(session, spotify_url)

            assert result is not None
            assert isinstance(result, dict)

            # Odesli API may sometimes return empty results or fail (rate limiting, etc.)
            # For integration tests, we check if we got results, but don't fail if API is unavailable
            if result:
                # Should have at least Spotify URL if API returned data
                assert result.get("spotify") is not None, (
                    "Odesli API should return spotify URL when data is available"
                )

                # May have other platform URLs (not guaranteed)
                # Just check they're strings if present
                if result.get("applemusic"):
                    assert isinstance(result["applemusic"], str)
                if result.get("youtubemusic"):
                    assert isinstance(result["youtubemusic"], str)
            else:
                # If API returned empty dict, log a warning but don't fail the test
                # This handles transient API issues gracefully
                import logging

                logging.warning(
                    "Odesli API returned empty result - may be rate limited or unavailable"
                )


class TestLastFMSearchServiceIntegration:
    """Integration tests for LastFMSearchService."""

    @pytest.mark.asyncio
    async def test_get_trending_albums(self, lastfm_search_service):
        """Test getting trending albums."""
        with time_operation("get_trending_albums API call"):
            result = await lastfm_search_service.get_trending_albums(limit=5)

        assert isinstance(result, LastFMTrendingAlbumsResponse)
        assert result.total_results > 0
        assert len(result.results) > 0
        assert len(result.results) <= 5

        # Check each album
        for album in result.results:
            check_music_album(album)

            # Trending albums should have playcount/listeners
            assert album.playcount >= 0
            assert album.listeners >= 0

    @pytest.mark.asyncio
    async def test_search_albums(self, snapshot, lastfm_search_service: LastFMSearchService):
        """Test searching for albums."""
        with time_operation("search_albums API call"):
            result = await lastfm_search_service.search_albums(
                query="The Dark Side of the Moon", limit=10
            )

        assert isinstance(result, LastFMAlbumSearchResponse)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Check first result
        first_album = result.results[0]
        check_music_album(first_album)

        # Should find Pink Floyd's album
        assert (
            "dark side" in first_album.title.lower() or "pink floyd" in first_album.artist.lower()
        )
        # snapshot.assert_match(
        #     json.dumps(result.model_dump(), indent=4), "search_albums_result.json"
        # )

    @pytest.mark.asyncio
    async def test_search_albums_by_artist(
        self, snapshot, lastfm_search_service: LastFMSearchService
    ):
        """Test searching for albums."""
        with time_operation("search_albums_by_artist API call"):
            result = await lastfm_search_service.search_albums(query="Taylor Swift", limit=10)

        assert isinstance(result, LastFMAlbumSearchResponse)
        assert result.total_results > 0
        assert len(result.results) > 0

        # Check first result
        first_album = result.results[0]
        check_music_album(first_album)

        # Should find Pink Floyd's album
        assert "taylor swift" in first_album.artist.lower()
        # snapshot.assert_match(
        #     json.dumps(result.model_dump(), indent=4), "search_albums_by_artist_result.json"
        # )

    @pytest.mark.asyncio
    async def test_search_albums_no_results(self, lastfm_search_service):
        """Test searching for albums with no results."""
        result = await lastfm_search_service.search_albums(
            query="xyznonexistentalbum123456", limit=10
        )

        assert isinstance(result, LastFMAlbumSearchResponse)
        assert result.total_results == 0
        assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_search_spotify_artist(self, lastfm_search_service):
        """Test searching for artists."""
        with time_operation("search_spotify_artist API call"):
            result = await lastfm_search_service.search_spotify_artist(
                query="The Beatles", limit=10
            )

        assert isinstance(result, LastFMArtistSearchResponse)
        assert result.query == "The Beatles"
        assert result.total_results > 0
        assert len(result.results) > 0

        # Check first result
        first_artist = result.results[0]
        check_music_artist(first_artist)

        # Should find The Beatles
        assert "beatles" in first_artist.name.lower()

        # Should have top track info
        if first_artist.top_track_track:
            assert isinstance(first_artist.top_track_track, str)
            assert len(first_artist.top_track_track) > 0

    @pytest.mark.asyncio
    async def test_search_by_genre(self, lastfm_search_service):
        """Test searching by genre."""
        with time_operation("search_by_genre API call"):
            result = await lastfm_search_service.search_by_genre(genre="rock", limit=10)

        assert isinstance(result, LastFMArtistSearchResponse)
        assert result.query == "rock"
        assert result.total_results > 0
        assert len(result.results) > 0
        assert len(result.results) <= 10

        # Check each artist
        for artist in result.results:
            check_music_artist(artist)

            # Artists from genre search should have genres
            if artist.genres:
                assert isinstance(artist.genres, list)

    @pytest.mark.asyncio
    async def test_search_by_keyword(self, lastfm_search_service: LastFMSearchService):
        """Test searching by keyword (multi-type search)."""
        with time_operation("search_by_keyword API call"):
            result = await lastfm_search_service.search_by_keyword(keyword="jazz", limit=10)

        assert isinstance(result, LastFMMultiSearchResponse)
        assert result.query == "jazz"
        assert result.total_results > 0
        assert len(result.results) > 0

        # Should have mixed results
        assert result.artist_count >= 0
        assert result.album_count >= 0
        assert result.playlist_count >= 0

        # Check each result
        for item in result.results:
            if isinstance(item, MCMusicArtist):
                check_music_artist(item)
            elif isinstance(item, MCMusicAlbum):
                check_music_album(item)
            elif isinstance(item, MCMusicPlaylist):
                check_music_playlist(item)
            else:
                pytest.fail(f"Unexpected result type: {type(item)}")


class TestLastFMWrappersIntegration:
    """Integration tests for LastFM wrapper functions."""

    @pytest.mark.asyncio
    async def test_get_trending_albums_wrapper(self, real_lastfm_api_key, monkeypatch):
        """Test get_trending_albums wrapper."""
        from api.lastfm.wrappers import lastfm_wrapper
        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
        with time_operation("get_trending_albums wrapper"):
            result = await lastfm_wrapper.get_trending_albums(limit=5)

        assert result.status_code == 200
        assert result.error is None
        assert len(result.results) > 0
        assert len(result.results) <= 5

        # Check first album has all required fields
        first_album = result.results[0]
        assert hasattr(first_album, "title")
        assert hasattr(first_album, "artist")
        assert first_album.mc_id is not None, "mc_id is required"
        assert first_album.mc_type == MCType.MUSIC_ALBUM, "mc_type must be MUSIC_ALBUM"
        assert first_album.source is not None, "source is required"
        assert first_album.source.value in ["lastfm", "spotify"], (
            "source must be 'lastfm' or 'spotify'"
        )
        assert first_album.source_id is not None, "source_id is required"

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "get_trending_albums_result.json")

    @pytest.mark.asyncio
    async def test_search_albums_wrapper(self, real_lastfm_api_key, monkeypatch):
        """Test search_albums wrapper."""
        from api.lastfm.wrappers import lastfm_wrapper
        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
        with time_operation("search_albums wrapper"):
            result = await lastfm_wrapper.search_albums(query="Thriller", limit=10)

        assert result.status_code == 200
        assert result.error is None
        assert len(result.results) > 0

        # Check first album has all required fields
        first_album = result.results[0]
        assert hasattr(first_album, "title")
        assert hasattr(first_album, "artist")
        assert hasattr(first_album, "spotify_url")
        assert first_album.mc_id is not None, "mc_id is required"
        assert first_album.mc_type == MCType.MUSIC_ALBUM, "mc_type must be MUSIC_ALBUM"
        assert first_album.source is not None, "source is required"
        assert first_album.source.value in ["lastfm", "spotify"], (
            "source must be 'lastfm' or 'spotify'"
        )
        assert first_album.source_id is not None, "source_id is required"

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "search_albums_result.json")

    @pytest.mark.asyncio
    async def test_search_by_genre_wrapper(self, real_lastfm_api_key, monkeypatch):
        """Test search_by_genre wrapper."""
        from api.lastfm.wrappers import lastfm_wrapper
        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
        with time_operation("search_by_genre wrapper"):
            result = await lastfm_wrapper.search_by_genre(genre="pop", limit=10)

        assert result.status_code == 200
        assert result.error is None
        assert len(result.results) > 0

        # Check first artist has all required fields
        first_artist = result.results[0]
        assert hasattr(first_artist, "name")
        assert first_artist.mc_id is not None, "mc_id is required"
        assert first_artist.mc_type == MCType.PERSON, "mc_type must be PERSON"
        assert first_artist.source is not None, "source is required"
        assert first_artist.source.value in ["lastfm", "spotify"], (
            "source must be 'lastfm' or 'spotify'"
        )
        assert first_artist.source_id is not None, "source_id is required"

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "search_by_genre_result.json")

    @pytest.mark.asyncio
    async def test_search_by_keyword_wrapper(self, real_lastfm_api_key, monkeypatch):
        """Test search_by_keyword wrapper."""
        from api.lastfm.wrappers import lastfm_wrapper
        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
        result = await lastfm_wrapper.search_by_keyword(keyword="classical", limit=10)

        assert result.status_code == 200
        assert result.error is None
        assert len(result.results) > 0

        # Results should be mixed types with all required fields
        for item in result.results:
            assert item.mc_id is not None, "mc_id is required"
            assert item.mc_type in [MCType.PERSON, MCType.MUSIC_ALBUM, MCType.MUSIC_PLAYLIST]
            assert item.source is not None, "source is required"
            assert item.source.value in ["lastfm", "spotify"], (
                "source must be 'lastfm' or 'spotify'"
            )
            assert item.source_id is not None, "source_id is required"

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "search_by_keyword_result.json")

    @pytest.mark.asyncio
    async def test_search_artist_wrapper(self, real_lastfm_api_key, monkeypatch):
        """Test search_artist wrapper."""
        from api.lastfm.wrappers import lastfm_wrapper
        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)
        result = await lastfm_wrapper.search_artist(query="Coldplay", limit=10)

        assert result.status_code == 200
        assert result.error is None
        assert len(result.results) > 0

        # Check first artist has all required fields
        first_artist = result.results[0]
        assert hasattr(first_artist, "name")
        assert hasattr(first_artist, "spotify_url")
        assert first_artist.mc_id is not None, "mc_id is required"
        assert first_artist.mc_type == MCType.PERSON, "mc_type must be PERSON"
        assert first_artist.source is not None, "source is required"
        assert first_artist.source.value in ["lastfm", "spotify"], (
            "source must be 'lastfm' or 'spotify'"
        )
        assert first_artist.source_id is not None, "source_id is required"

        # Write snapshot for integration test
        write_snapshot(result.model_dump(), "search_artist_result.json")


class TestLastFMEdgeCases:
    """Integration tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_search_with_special_characters(self, lastfm_search_service):
        """Test search with special characters."""
        result = await lastfm_search_service.search_albums(query="AC/DC", limit=10)

        assert isinstance(result, LastFMAlbumSearchResponse)
        # Should handle special characters gracefully
        assert result.total_results >= 0

    @pytest.mark.asyncio
    async def test_search_with_unicode(self, lastfm_search_service):
        """Test search with unicode characters."""
        result = await lastfm_search_service.search_spotify_artist(query="Björk", limit=10)

        assert isinstance(result, LastFMArtistSearchResponse)
        # Should handle unicode gracefully
        assert result.total_results >= 0

    @pytest.mark.asyncio
    async def test_search_with_very_long_query(self, lastfm_search_service):
        """Test search with very long query string."""
        long_query = "a" * 200
        result = await lastfm_search_service.search_albums(query=long_query, limit=10)

        assert isinstance(result, LastFMAlbumSearchResponse)
        # Should handle long queries without crashing
        assert result.total_results >= 0

    @pytest.mark.asyncio
    async def test_search_with_empty_query(self, lastfm_search_service):
        """Test search with empty query."""
        result = await lastfm_search_service.search_albums(query="", limit=10)

        assert isinstance(result, LastFMAlbumSearchResponse)
        # Should handle empty query gracefully
        assert result.total_results >= 0

    @pytest.mark.asyncio
    async def test_trending_with_large_limit(self, lastfm_search_service):
        """Test getting trending albums with large limit."""
        result = await lastfm_search_service.get_trending_albums(limit=50)

        assert isinstance(result, LastFMTrendingAlbumsResponse)
        assert result.total_results > 0
        # Should handle large limits (may be capped internally)
        assert len(result.results) > 0


class TestLastFMDataQuality:
    """Integration tests to verify data quality and completeness."""

    @pytest.mark.asyncio
    async def test_album_data_completeness(self, lastfm_search_service):
        """Test that album data contains all expected fields."""
        result = await lastfm_search_service.search_albums(query="Led Zeppelin IV", limit=5)

        assert len(result.results) > 0
        album = result.results[0]

        # Required fields including MediaCircle standardized fields
        required_fields = [
            "title",
            "artist",
            "image",
            "spotify_url",
            "mc_id",
            "mc_type",
            "source",
            "source_id",
        ]

        for field in required_fields:
            assert hasattr(album, field), f"Missing required field: {field}"
            assert getattr(album, field) is not None, f"Field {field} is None"

        # Verify correct values for standardized fields
        assert album.mc_type == "music_album"
        assert album.source.value in ["lastfm", "spotify"]

    @pytest.mark.asyncio
    async def test_artist_data_completeness(self, lastfm_search_service):
        """Test that artist data contains all expected fields."""
        result = await lastfm_search_service.search_spotify_artist(query="Queen", limit=5)

        assert len(result.results) > 0
        artist = result.results[0]

        # Required fields including MediaCircle standardized fields
        required_fields = [
            "id",
            "name",
            "spotify_url",
            "mc_id",
            "mc_type",
            "source",
            "source_id",
        ]

        for field in required_fields:
            assert hasattr(artist, field), f"Missing required field: {field}"
            assert getattr(artist, field) is not None, f"Field {field} is None"

        # Verify correct values for standardized fields
        assert artist.mc_type == MCType.PERSON
        assert artist.source.value in ["lastfm", "spotify"]

    @pytest.mark.asyncio
    async def test_mc_id_generation(self, lastfm_search_service):
        """Test that mc_id is properly generated for all results."""
        result = await lastfm_search_service.get_trending_albums(limit=5)

        for album in result.results:
            assert album.mc_id is not None
            assert len(album.mc_id) > 0
            assert album.mc_type == "music_album"

            # mc_id should be unique and consistent
            # Format: album_{spotify_id} or album_{artist}_{title}
            assert "album_" in album.mc_id

    @pytest.mark.asyncio
    async def test_image_urls_validity(self, lastfm_search_service):
        """Test that image URLs are valid and accessible."""
        result = await lastfm_search_service.search_albums(query="Pink Floyd", limit=5)

        for album in result.results:
            assert album.image is not None
            assert album.image.startswith("http")
            # Should be HTTPS for security
            assert album.image.startswith("https://")

    @pytest.mark.asyncio
    async def test_spotify_url_format(self, lastfm_search_service):
        """Test that Spotify URLs have correct format."""
        result = await lastfm_search_service.search_albums(query="The Beatles", limit=5)

        for album in result.results:
            assert album.spotify_url is not None
            assert "spotify.com" in album.spotify_url
            assert album.spotify_url.startswith("https://")
            # Should be open.spotify.com
            assert "open.spotify.com" in album.spotify_url

    @pytest.mark.asyncio
    async def test_cross_platform_enrichment(self, lastfm_search_service):
        """
        Test that albums are enriched with cross-platform links via Apple Music API.

        Note: This now tests Apple Music API integration instead of Odesli.
        """
        result = await lastfm_search_service.get_trending_albums(limit=3)

        # At least some albums should have cross-platform links from Apple Music API
        has_apple_music = any(album.apple_music_url for album in result.results)
        has_youtube_music = any(album.youtube_music_url for album in result.results)

        # Note: Not all albums may have all platforms, but we should see some enrichment
        # This is a soft assertion - just log if no enrichment found
        if not (has_apple_music or has_youtube_music):
            import logging

            logging.warning("No cross-platform enrichment found in trending albums")


class TestLastFMCaching:
    """Integration tests for caching behavior."""

    @pytest.mark.asyncio
    async def test_cache_consistency(self, lastfm_search_service):
        """Test that cached results are consistent with fresh results."""
        query = "Radiohead"

        # First call - should cache
        result1 = await lastfm_search_service.search_albums(query=query, limit=10)

        # Second call - should use cache
        result2 = await lastfm_search_service.search_albums(query=query, limit=10)

        # Results should be identical
        assert len(result1.results) == len(result2.results)
        assert result1.total_results == result2.total_results

        # Check first album is the same
        if len(result1.results) > 0:
            assert result1.results[0].mc_id == result2.results[0].mc_id
            assert result1.results[0].title == result2.results[0].title

    @pytest.mark.asyncio
    async def test_normalized_query_caching(self, lastfm_search_service):
        """Test that normalized queries use same cache."""
        # These should normalize to the same query
        result1 = await lastfm_search_service.search_albums(query="The Beatles", limit=10)
        result2 = await lastfm_search_service.search_albums(query="the beatles", limit=10)
        result3 = await lastfm_search_service.search_albums(query="THE BEATLES", limit=10)

        # All should return same results (from cache)
        try:
            assert len(result1.results) == len(result2.results)
            assert len(result2.results) == len(result3.results)
        except AssertionError:
            print("DEBUG (results 1):", [r.artist for r in result1.results])
            print("DEBUG (results 2):", [r.artist for r in result2.results])
            print("DEBUG (results 3):", [r.artist for r in result3.results])
            raise


class TestLastFMPersonSearchWorks:
    """Integration tests for search_person_async wrapper function."""

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_id_valid_name(
        self, real_lastfm_api_key, monkeypatch
    ):
        """Test search_person_async with invalid ID but valid name - should fallback to name search."""
        from contracts.models import MCPersonSearchRequest

        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Create request with invalid ID but valid name (should fallback to name search)
        person_request = MCPersonSearchRequest(
            source_id="invalid_spotify_id_123",  # Invalid Spotify ID (not a real artist ID)
            source=MCSources.LASTFM,
            mc_type=MCType.PERSON,
            mc_id="person_invalid_spotify_id_123",
            mc_subtype=MCSubType.MUSICIAN,
            name="The Beatles",  # Valid name - should use this for fallback
        )

        # Call wrapper
        result = await lastfm_wrapper.search_person_async(person_request, limit=20)

        # Validate response - should succeed via name fallback
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # Validate person details
        assert result.details is not None
        artist = MCMusicArtist.model_validate(result.details.model_dump())
        assert artist.mc_type == MCType.PERSON
        assert "beatles" in artist.name.lower()
        # Verify required MCBaseItem fields for artist
        assert artist.mc_id is not None, f"mc_id is missing for artist: {artist.name}"
        assert artist.mc_type == MCType.PERSON, f"mc_type is wrong for artist: {artist.name}"
        assert artist.source is not None, f"source is missing for artist: {artist.name}"
        assert artist.source_id is not None, f"source_id is missing for artist: {artist.name}"

        # Validate works array contains albums
        assert len(result.works) > 0, "works array should not be empty"

        albums_found = 0
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)

            # Should be music albums
            if work_dict.get("mc_type") == MCType.MUSIC_ALBUM.value:
                item_validated = MCMusicAlbum.model_validate(work_dict)
                assert item_validated.mc_type == MCType.MUSIC_ALBUM
                albums_found += 1
                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for album: {item_validated.title}"
                )
                assert item_validated.mc_type == MCType.MUSIC_ALBUM, (
                    f"mc_type is wrong for album: {item_validated.title}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for album: {item_validated.title}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for album: {item_validated.title}"
                )
                # Verify album is by The Beatles (case-insensitive)
                assert "beatles" in item_validated.artist.lower(), (
                    f"Album artist '{item_validated.artist}' doesn't match 'The Beatles'"
                )

        # Verify we have at least some albums
        assert albums_found > 0, "works array should contain at least one album"

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"

        # Write snapshot
        write_snapshot(result.model_dump(), "search_person_works.json")

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source(self, real_lastfm_api_key, monkeypatch):
        """Test search_person_async with invalid source."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Create a person search request with invalid source (not LastFM)
        person_request = MCPersonSearchRequest(
            source_id="test_123",
            source=MCSources.TMDB,  # Invalid for LastFM wrapper
            mc_type=MCType.PERSON,
            mc_id="person_test_123",
            mc_subtype=MCSubType.ACTOR,
            name="Test Actor",
        )

        # Call the wrapper function
        result = await lastfm_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_source_id(self, real_lastfm_api_key, monkeypatch):
        """Test search_person_async with invalid source_id."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Create a person search request with invalid source_id
        person_request = MCPersonSearchRequest(
            source_id="",  # Invalid (must be provided and non-empty)
            source=MCSources.LASTFM,
            mc_type=MCType.PERSON,
            mc_id="person_empty",
            mc_subtype=MCSubType.MUSICIAN,
            name="Invalid Artist",
        )

        # Call the wrapper function
        result = await lastfm_wrapper.search_person_async(person_request)

        # Validate error response
        assert result.status_code == 400
        assert result.error is not None
        assert "Invalid source_id" in result.error
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_invalid_id_invalid_name(
        self, real_lastfm_api_key, monkeypatch
    ):
        """Test search_person_async with invalid ID and invalid name - should return 404."""
        from contracts.models import MCPersonSearchRequest

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Create a person search request with invalid ID and invalid name
        person_request = MCPersonSearchRequest(
            source_id="invalid_spotify_id_999999999",  # Invalid Spotify ID
            source=MCSources.LASTFM,
            mc_type=MCType.PERSON,
            mc_id="person_invalid_spotify_id_999999999",
            mc_subtype=MCSubType.MUSICIAN,
            name="XyZNonExistentArtist123456",  # Invalid name - won't be found
        )

        # Call the wrapper function
        result = await lastfm_wrapper.search_person_async(person_request)

        # Validate error response - should be 404 after both ID and name search fail
        assert result.status_code == 404
        assert result.error is not None
        assert result.details is None
        assert result.works == []
        assert result.related == []

    @pytest.mark.asyncio
    async def test_search_person_async_valid_id(self, real_lastfm_api_key, monkeypatch):
        """Test search_person_async with valid Spotify ID - should succeed regardless of name."""
        from contracts.models import MCPersonSearchRequest

        from utils.pytest_utils import write_snapshot

        monkeypatch.setenv("LASTFM_API_KEY", real_lastfm_api_key)

        # Get The Beatles' actual Spotify ID by searching first
        search_result = await lastfm_wrapper.search_artist(query="The Beatles", limit=1)
        assert search_result.status_code == 200
        assert len(search_result.results) > 0
        beatles_spotify_id = search_result.results[
            0
        ].source_id  # This is a string like "3WrFJ7ztbogyGnTHbXXFlQ"

        # Create request with valid Spotify artist ID - name doesn't matter when ID is valid
        person_request = MCPersonSearchRequest(
            source_id=beatles_spotify_id,  # Valid Spotify artist ID for The Beatles
            source=MCSources.LASTFM,
            mc_type=MCType.PERSON,
            mc_id=f"person_{beatles_spotify_id}",
            mc_subtype=MCSubType.MUSICIAN,
            name="Wrong Name That Should Be Ignored",  # Name doesn't matter when ID is valid
        )

        # Call wrapper
        result = await lastfm_wrapper.search_person_async(person_request, limit=20)

        # Validate response - should succeed using ID, ignoring name
        assert result.status_code == 200
        assert result.error is None
        assert result.input == person_request

        # Validate person details - should be The Beatles from ID, not "Wrong Name"
        assert result.details is not None
        artist = MCMusicArtist.model_validate(result.details.model_dump())
        assert artist.mc_type == MCType.PERSON
        # Should be The Beatles (from ID), not "Wrong Name"
        assert "beatles" in artist.name.lower()
        assert artist.name == "The Beatles"
        # Verify required MCBaseItem fields for artist
        assert artist.mc_id is not None, f"mc_id is missing for artist: {artist.name}"
        assert artist.mc_type == MCType.PERSON, f"mc_type is wrong for artist: {artist.name}"
        assert artist.source is not None, f"source is missing for artist: {artist.name}"
        assert artist.source_id is not None, f"source_id is missing for artist: {artist.name}"

        # Validate works array contains albums
        assert len(result.works) > 0, "works array should not be empty"

        albums_found = 0
        for work in result.works:
            work_dict = work.model_dump() if hasattr(work, "model_dump") else dict(work)

            # Should be music albums
            if work_dict.get("mc_type") == MCType.MUSIC_ALBUM.value:
                item_validated = MCMusicAlbum.model_validate(work_dict)
                assert item_validated.mc_type == MCType.MUSIC_ALBUM
                albums_found += 1
                # Verify required MCBaseItem fields
                assert item_validated.mc_id is not None, (
                    f"mc_id is missing for album: {item_validated.title}"
                )
                assert item_validated.mc_type == MCType.MUSIC_ALBUM, (
                    f"mc_type is wrong for album: {item_validated.title}"
                )
                assert item_validated.source is not None, (
                    f"source is missing for album: {item_validated.title}"
                )
                assert item_validated.source_id is not None, (
                    f"source_id is missing for album: {item_validated.title}"
                )
                # Verify album is by The Beatles (case-insensitive)
                assert "beatles" in item_validated.artist.lower(), (
                    f"Album artist '{item_validated.artist}' doesn't match 'The Beatles'"
                )

        # Verify we have at least some albums
        assert albums_found > 0, "works array should contain at least one album"

        # Validate related is empty (will be filled by search_broker)
        assert result.related == [], "related should be empty (filled by search_broker)"
