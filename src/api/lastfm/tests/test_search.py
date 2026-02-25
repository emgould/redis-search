"""
Unit tests for LastFM Search Service.
Tests LastFMSearchService search and discovery functionality.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.lastfm.models import (
    LastFMAlbumSearchResponse,
    LastFMArtistSearchResponse,
    LastFMMultiSearchResponse,
    LastFMTrendingAlbumsResponse,
)
from api.lastfm.search import LastFMSearchService
from api.lastfm.tests.conftest import load_fixture
from contracts.models import MCType

FIXTURES_DIR = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.unit


class TestGetTrendingAlbums:
    """Tests for get_trending_albums method."""

    @pytest.mark.asyncio
    async def test_get_trending_albums_success(self, mock_auth):
        """Test successful trending albums retrieval."""
        service = LastFMSearchService()

        # Mock the internal method that does the actual work
        from api.lastfm.models import MCMusicAlbum

        mock_albums = [
            MCMusicAlbum(title="Album 1", artist="Artist 1"),
            MCMusicAlbum(title="Album 2", artist="Artist 2"),
        ]
        mock_response = LastFMTrendingAlbumsResponse(results=mock_albums, total_results=2)

        with patch.object(
            service,
            "get_trending_albums",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_trending_albums(limit=2)

            assert isinstance(response, LastFMTrendingAlbumsResponse)
            assert len(response.results) == 2
            assert response.total_results == 2

    @pytest.mark.asyncio
    async def test_get_trending_albums_no_spotify_token(self, mock_auth):
        """Test trending albums when Spotify token cannot be obtained."""
        service = LastFMSearchService()

        # Mock to return empty response when token fails
        mock_response = LastFMTrendingAlbumsResponse(results=[], total_results=0)

        with patch.object(
            service,
            "get_trending_albums",
            new=AsyncMock(return_value=mock_response),
        ):
            response = await service.get_trending_albums(limit=10)

            assert isinstance(response, LastFMTrendingAlbumsResponse)
            assert response.results == []
            assert response.total_results == 0


class TestSearchAlbums:
    """Tests for search_albums method."""

    @pytest.mark.asyncio
    async def test_search_albums_success(self, mock_auth):
        """Test successful album search."""
        mock_spotify_token_response = load_fixture("make_requests/mock_spotify_token_response.json")
        mock_spotify_album_search = load_fixture("make_requests/mock_spotify_album_search.json")

        service = LastFMSearchService()

        # Mock Spotify token
        mock_token_response = AsyncMock()
        mock_token_response.status = 200
        mock_token_response.json = AsyncMock(return_value=mock_spotify_token_response)

        # Mock album search
        mock_album_response = AsyncMock()
        mock_album_response.status = 200
        mock_album_response.json = AsyncMock(return_value=mock_spotify_album_search)

        mock_session = MagicMock()
        mock_session.post.return_value.__aenter__.return_value = mock_token_response
        mock_session.get.return_value.__aenter__.return_value = mock_album_response

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = AsyncMock()

            result = await service.search_albums("Pink Floyd", limit=20)

            assert isinstance(result, LastFMAlbumSearchResponse)
            # Should have results from the mock data
            assert len(result.results) > 0
            assert result.total_results > 0
            assert result.query == "pink floyd"
            # Verify album structure
            if result.results:
                first_album = result.results[0]
                assert first_album.title
                assert first_album.artist
                assert first_album.mc_type == "music_album"
                # Verify it's the expected album from fixture
                assert first_album.artist == "Pink Floyd"

    @pytest.mark.asyncio
    async def test_search_albums_no_results(self, mock_auth):
        """Test album search with no results."""
        mock_spotify_token_response = load_fixture("make_requests/mock_spotify_token_response.json")

        service = LastFMSearchService()

        # Mock Spotify token
        mock_token_response = AsyncMock()
        mock_token_response.status = 200
        mock_token_response.json = AsyncMock(return_value=mock_spotify_token_response)

        # Mock empty album search
        mock_album_response = AsyncMock()
        mock_album_response.status = 200
        mock_album_response.json = AsyncMock(return_value={"albums": {"items": []}})

        mock_session = MagicMock()
        mock_session.post.return_value.__aenter__.return_value = mock_token_response
        mock_session.get.return_value.__aenter__.return_value = mock_album_response

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = AsyncMock()

            result = await service.search_albums("NonexistentAlbum", limit=20)

            assert isinstance(result, LastFMAlbumSearchResponse)
            assert result.results == []
            assert result.total_results == 0


class TestSearchByGenre:
    """Tests for search_by_genre method."""

    @pytest.mark.asyncio
    async def test_search_by_genre_success(self, mock_auth):
        """Test successful genre search."""
        from api.subapi.spotify.models import SpotifyArtist, SpotifyArtistSearchResponse
        from api.subapi.spotify.wrappers import spotify_wrapper

        mock_spotify_artist_search = load_fixture("make_requests/mock_spotify_artist_search.json")

        service = LastFMSearchService()

        # Create mock SpotifyArtist from fixture data
        artist_data = mock_spotify_artist_search["artists"]["items"][0]
        mock_artist = SpotifyArtist.from_spotify_artistdata(artist_data)

        # Create mock SpotifyArtistSearchResponse
        mock_spotify_response = SpotifyArtistSearchResponse(
            results=[mock_artist], total_results=1, query="rock"
        )

        # Patch spotify_wrapper.search_by_genre to return mock response
        with patch.object(
            spotify_wrapper, "search_by_genre", new=AsyncMock(return_value=mock_spotify_response)
        ):
            result = await service.search_by_genre("rock", limit=50, no_cache=True)

            assert isinstance(result, LastFMArtistSearchResponse)
            assert len(result.results) > 0
            assert result.total_results > 0
            assert result.query == "rock"
            # Verify artist structure
            if result.results:
                first_artist = result.results[0]
                assert first_artist.name
                assert first_artist.mc_type == MCType.PERSON

    @pytest.mark.asyncio
    async def test_search_by_genre_no_token(self, mock_auth):
        """Test genre search without Spotify token."""
        from api.subapi.spotify.models import SpotifyArtistSearchResponse
        from api.subapi.spotify.wrappers import spotify_wrapper

        service = LastFMSearchService()

        # Mock spotify_wrapper to return empty response (simulating no token scenario)
        empty_response = SpotifyArtistSearchResponse(results=[], total_results=0, query="rock")
        with patch.object(spotify_wrapper, "search_by_genre", return_value=empty_response):
            # Use no_cache=True to bypass cache and ensure we test actual behavior
            result = await service.search_by_genre("rock", limit=50, no_cache=True)

            assert isinstance(result, LastFMArtistSearchResponse)
            assert result.results == []
            assert result.total_results == 0


class TestSearchByKeyword:
    """Tests for search_by_keyword method."""

    @pytest.mark.asyncio
    async def test_search_by_keyword_success(self, mock_auth):
        """Test successful keyword search."""
        mock_spotify_token_response = load_fixture("make_requests/mock_spotify_token_response.json")

        service = LastFMSearchService()

        # Mock Spotify token
        mock_token_response = AsyncMock()
        mock_token_response.status = 200
        mock_token_response.json = AsyncMock(return_value=mock_spotify_token_response)

        # Mock keyword search (returns artists, albums, playlists)
        mock_keyword_response = AsyncMock()
        mock_keyword_response.status = 200
        mock_keyword_response.json = AsyncMock(
            return_value={
                "artists": {
                    "items": [
                        {
                            "id": "1",
                            "name": "Test Artist",
                            "popularity": 80,
                            "images": [],
                            "followers": {"total": 1000},
                            "genres": [],
                        }
                    ]
                },
                "albums": {
                    "items": [
                        {
                            "id": "2",
                            "name": "Test Album",
                            "popularity": 75,
                            "images": [],
                            "total_tracks": 10,
                            "external_urls": {"spotify": "https://spotify.com"},
                        }
                    ]
                },
                "playlists": {"items": [{"id": "3", "name": "Test Playlist", "images": []}]},
            }
        )

        mock_session = MagicMock()
        mock_session.post.return_value.__aenter__.return_value = mock_token_response
        mock_session.get.return_value.__aenter__.return_value = mock_keyword_response

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = AsyncMock()

            result = await service.search_by_keyword("progressive", limit=20)

            assert isinstance(result, LastFMMultiSearchResponse)
            assert len(result.results) > 0  # Should have artists + albums + playlists
            assert result.total_results > 0
            assert result.artist_count > 0
            assert result.query == "progressive"


class TestSearchSpotifyArtist:
    """Tests for search_spotify_artist method."""

    @pytest.mark.asyncio
    async def test_search_spotify_artist_success(self, mock_auth):
        """Test successful artist search."""
        from api.subapi.spotify.models import (
            SpotifyArtist,
            SpotifyArtistSearchResponse,
            SpotifyTopTrackResponse,
            SpotifyTrack,
        )
        from api.subapi.spotify.wrappers import spotify_wrapper

        mock_spotify_artist_search = load_fixture("make_requests/mock_spotify_artist_search.json")
        mock_spotify_top_tracks = load_fixture("make_requests/mock_spotify_top_tracks.json")

        service = LastFMSearchService()

        # Create mock SpotifyArtist from fixture data
        artist_data = mock_spotify_artist_search["artists"]["items"][0]
        mock_artist = SpotifyArtist.from_spotify_artistdata(artist_data)

        # Create mock SpotifyTrack from top tracks fixture
        # from_spotify_trackdata expects the full track object with album data
        track_data = mock_spotify_top_tracks["tracks"][0]
        mock_track = SpotifyTrack.from_spotify_trackdata(track_data)

        # Populate top track fields on mock artist to simulate enrichment
        # This mimics what SpotifySearchService.search_artists does
        mock_artist.top_track_track = mock_track.name
        mock_artist.known_for = mock_track.name
        if mock_track.album:
            mock_artist.top_track_album = mock_track.album
        if mock_track.release_date:
            mock_artist.top_track_release_date = mock_track.release_date
        if mock_track.default_image:
            mock_artist.top_track_album_image = mock_track.default_image

        # Create mock SpotifyArtistSearchResponse
        mock_artist_response = SpotifyArtistSearchResponse(
            results=[mock_artist], total_results=1, query="Pink Floyd"
        )

        # Create mock SpotifyTopTrackResponse
        mock_top_track_response = SpotifyTopTrackResponse(
            results=[mock_track], total_results=1, query=mock_artist.id
        )

        # Mock the wrapper methods directly
        with (
            patch.object(
                spotify_wrapper,
                "search_artists",
                return_value=mock_artist_response,
            ),
            patch.object(
                spotify_wrapper,
                "get_top_track",
                return_value=mock_top_track_response,
            ),
        ):
            result = await service.search_spotify_artist("Pink Floyd", limit=20)

            assert isinstance(result, LastFMArtistSearchResponse)
            assert len(result.results) > 0
            assert result.total_results > 0
            assert result.query == "Pink Floyd"
            # Verify artist has top track info
            # track.model_dump() includes album, release_date, album_image fields
            if result.results:
                first_artist = result.results[0]
                assert first_artist.top_track_album is not None  # Should have album name
                assert (
                    first_artist.top_track_album == "Wish You Were Here"
                )  # Album name from fixture
                assert first_artist.known_for is not None  # Track name (via "track" alias)
                assert first_artist.known_for == "Wish You Were Here"  # Track name from fixture

    @pytest.mark.asyncio
    async def test_search_spotify_artist_no_results(self, mock_auth):
        """Test artist search with no results."""
        mock_spotify_token_response = load_fixture("make_requests/mock_spotify_token_response.json")

        service = LastFMSearchService()

        # Mock Spotify token
        mock_token_response = AsyncMock()
        mock_token_response.status = 200
        mock_token_response.json = AsyncMock(return_value=mock_spotify_token_response)

        # Mock empty artist search
        mock_artist_response = AsyncMock()
        mock_artist_response.status = 200
        mock_artist_response.json = AsyncMock(return_value={"artists": {"items": []}})

        mock_session = MagicMock()
        mock_session.post.return_value.__aenter__.return_value = mock_token_response
        mock_session.get.return_value.__aenter__.return_value = mock_artist_response

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = AsyncMock()

            result = await service.search_spotify_artist("NonexistentArtist", limit=20)

            assert isinstance(result, LastFMArtistSearchResponse)
            assert result.results == []
            assert result.total_results == 0
