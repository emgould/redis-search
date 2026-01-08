"""
Tests for Spotify models.
"""

import pytest

from api.subapi.spotify.models import (
    SpotifyAlbum,
    SpotifyAlbumMetadata,
    SpotifyAlbumSearchResponse,
    SpotifyArtist,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyPlaylist,
)
from contracts.models import MCSources, MCType


class TestSpotifyArtist:
    """Tests for SpotifyArtist model."""

    def test_spotify_artist_creation(self):
        """Test creating a SpotifyArtist with required fields."""
        artist = SpotifyArtist(
            id="artist123",
            name="Test Artist",
            spotify_url="https://open.spotify.com/artist/artist123",
            popularity=80,
            followers=1000,
        )

        assert artist.id == "artist123"
        assert artist.name == "Test Artist"
        assert artist.mc_type == MCType.MUSIC_ARTIST
        assert artist.source == MCSources.SPOTIFY
        assert artist.mc_id == "spotify_artist_artist123"
        # artist and title fields are set by from_spotify_artistdata, not direct instantiation
        assert artist.artist is None  # Not set in direct instantiation
        assert artist.title is None  # Not set in direct instantiation

    def test_spotify_artist_with_mc_fields(self):
        """Test SpotifyArtist auto-generates mc_id and mc_type."""
        artist_data = {
            "id": "artist456",
            "name": "Another Artist",
        }
        artist = SpotifyArtist.model_validate(artist_data)

        assert artist.mc_id == "spotify_artist_artist456"
        assert artist.mc_type == MCType.MUSIC_ARTIST.value
        assert artist.source == MCSources.SPOTIFY.value

    def test_spotify_artist_with_top_track(self):
        """Test SpotifyArtist with top track metadata."""
        artist = SpotifyArtist(
            id="artist789",
            name="Top Track Artist",
            top_track_track="Blinding Lights",
            top_track_album="After Hours",
            top_track_release_date="2020-01-01",
        )

        assert artist.top_track_track == "Blinding Lights"
        assert artist.top_track_album == "After Hours"


class TestSpotifyAlbum:
    """Tests for SpotifyAlbum model."""

    def test_spotify_album_creation(self):
        """Test creating a SpotifyAlbum with required fields."""
        album = SpotifyAlbum(
            id="album123",
            title="Test Album",
            artist="Test Artist",
            spotify_url="https://open.spotify.com/album/album123",
            release_date="2023-01-01",
            total_tracks=12,
        )

        assert album.id == "album123"
        assert album.title == "Test Album"
        # SpotifyAlbum doesn't have an 'artist' field, only 'owner' for playlists
        assert album.mc_type == MCType.MUSIC_ALBUM
        assert album.source == MCSources.SPOTIFY
        assert album.mc_id == "album_album123"

    def test_spotify_album_with_mc_fields(self):
        """Test SpotifyAlbum auto-generates mc_id and mc_type."""
        album_data = {
            "id": "album456",
            "title": "Another Album",
            "artist": "Another Artist",
        }
        album = SpotifyAlbum.model_validate(album_data)

        assert album.mc_id == "album_album456"
        assert album.mc_type == MCType.MUSIC_ALBUM.value
        assert album.source == MCSources.SPOTIFY.value


class TestSpotifyPlaylist:
    """Tests for SpotifyPlaylist model."""

    def test_spotify_playlist_creation(self):
        """Test creating a SpotifyPlaylist with required fields."""
        playlist = SpotifyPlaylist(
            id="playlist123",
            name="Test Playlist",
            spotify_url="https://open.spotify.com/playlist/playlist123",
            popularity=50,
        )

        assert playlist.id == "playlist123"
        assert playlist.name == "Test Playlist"
        assert playlist.mc_type == MCType.MUSIC_PLAYLIST
        assert playlist.source == MCSources.SPOTIFY
        assert playlist.mc_id == "spotify_playlist_playlist123"
        # SpotifyPlaylist doesn't have 'artist' or 'title' fields in direct instantiation

    def test_spotify_playlist_with_mc_fields(self):
        """Test SpotifyPlaylist auto-generates mc_id and mc_type."""
        playlist_data = {
            "id": "playlist456",
            "name": "Another Playlist",
        }
        playlist = SpotifyPlaylist.model_validate(playlist_data)

        assert playlist.mc_id == "spotify_playlist_playlist456"
        assert playlist.mc_type == MCType.MUSIC_PLAYLIST.value
        assert playlist.source == MCSources.SPOTIFY.value


class TestSpotifyAlbumMetadata:
    """Tests for SpotifyAlbumMetadata model."""

    def test_spotify_album_metadata_creation(self):
        """Test creating SpotifyAlbumMetadata."""
        metadata = SpotifyAlbumMetadata(
            album_name="Test Album",
            spotify_url="https://open.spotify.com/album/test",
            release_date="2023-01-01",
            total_tracks=10,
        )

        assert metadata.album_name == "Test Album"
        assert metadata.release_date == "2023-01-01"
        assert metadata.total_tracks == 10


class TestSpotifyResponseModels:
    """Tests for Spotify response models."""

    def test_spotify_artist_search_response(self):
        """Test SpotifyArtistSearchResponse."""
        artists = [
            SpotifyArtist(id="1", name="Artist 1"),
            SpotifyArtist(id="2", name="Artist 2"),
        ]
        response = SpotifyArtistSearchResponse(results=artists, total_results=2, query="test")

        assert len(response.results) == 2
        assert response.total_results == 2
        assert response.query == "test"
        assert response.error is None

    def test_spotify_album_search_response(self):
        """Test SpotifyAlbumSearchResponse."""
        albums = [
            SpotifyAlbum(id="1", title="Album 1", artist="Artist 1"),
            SpotifyAlbum(id="2", title="Album 2", artist="Artist 2"),
        ]
        response = SpotifyAlbumSearchResponse(results=albums, total_results=2, query="test")

        assert len(response.results) == 2
        assert response.total_results == 2
        assert response.query == "test"
        assert response.error is None

    def test_spotify_multi_search_response(self):
        """Test SpotifyMultiSearchResponse."""
        results = [
            SpotifyArtist(id="1", name="Artist 1"),
            SpotifyAlbum(id="2", title="Album 1", artist="Artist 1"),
            SpotifyPlaylist(id="3", name="Playlist 1"),
        ]
        response = SpotifyMultiSearchResponse(
            results=results,
            total_results=3,
            artist_count=1,
            album_count=1,
            playlist_count=1,
            query="test",
        )

        assert len(response.results) == 3
        assert response.total_results == 3
        assert response.artist_count == 1
        assert response.album_count == 1
        assert response.playlist_count == 1
        assert response.query == "test"
        assert response.error is None

    def test_response_with_error(self):
        """Test response models with error field."""
        response = SpotifyArtistSearchResponse(
            results=[], total_results=0, query="test", error="Test error"
        )

        assert response.error == "Test error"
        assert len(response.results) == 0


class TestFromSpotifyMethods:
    """Tests for from_spotify_* class methods."""

    def test_from_spotify_artistdata(self):
        """Test from_spotify_artistdata with real data structure."""
        artist_data = {
            "id": "test123",
            "name": "Test Artist",
            "popularity": 50,
            "followers": {"total": 1000},
            "genres": ["rock", "pop"],
            "images": [
                {"url": "https://example.com/image1.jpg", "width": 640, "height": 640},
                {"url": "https://example.com/image2.jpg", "width": 320, "height": 320},
            ],
            "external_urls": {"spotify": "https://open.spotify.com/artist/test123"},
            "href": "https://api.spotify.com/v1/artists/test123",
        }

        artist = SpotifyArtist.from_spotify_artistdata(artist_data)

        assert artist.id == "test123"
        assert artist.name == "Test Artist"
        assert artist.popularity == 50
        assert artist.followers == 1000
        assert artist.genres == ["rock", "pop"]
        assert artist.spotify_url == "https://open.spotify.com/artist/test123"
        assert len(artist.images) == 2
        assert artist.default_image is not None

    def test_from_spotify_albumdata(self):
        """Test from_spotify_albumdata with real data structure."""
        album_data = {
            "id": "album123",
            "name": "Test Album",
            "type": "album",
            "artists": [{"id": "artist1", "name": "Test Artist"}],
            "images": [{"url": "https://example.com/cover.jpg", "width": 640, "height": 640}],
            "external_urls": {"spotify": "https://open.spotify.com/album/album123"},
            "href": "https://api.spotify.com/v1/albums/album123",
            "available_markets": ["US", "CA"],
        }

        album = SpotifyAlbum.from_spotify_albumdata(album_data)

        assert album.id == "album123"
        assert album.title == "Test Album"
        assert album.spotify_url == "https://open.spotify.com/album/album123"
        assert len(album.images) == 1
        assert album.default_image is not None

    def test_from_spotify_playlistdata(self):
        """Test from_spotify_playlistdata with real data structure."""
        playlist_data = {
            "id": "playlist123",
            "name": "Test Playlist",
            "type": "playlist",
            "images": [{"url": "https://example.com/playlist.jpg", "width": 640, "height": 640}],
            "external_urls": {"spotify": "https://open.spotify.com/playlist/playlist123"},
            "href": "https://api.spotify.com/v1/playlists/playlist123",
            "collaborative": False,
            "description": "A test playlist",
        }

        playlist = SpotifyPlaylist.from_spotify_playlistdata(playlist_data)

        assert playlist.id == "playlist123"
        assert playlist.name == "Test Playlist"
        assert playlist.spotify_url == "https://open.spotify.com/playlist/playlist123"
        assert len(playlist.images) == 1
        assert playlist.default_image is not None

    def test_from_spotify_artistdata_with_empty_images(self):
        """Test from_spotify_artistdata handles empty images."""
        artist_data = {
            "id": "test123",
            "name": "Test Artist",
            "popularity": 50,
            "followers": {"total": 1000},
            "genres": [],
            "images": [],
            "external_urls": {},
            "href": "https://api.spotify.com/v1/artists/test123",
        }

        artist = SpotifyArtist.from_spotify_artistdata(artist_data)

        assert artist.id == "test123"
        assert artist.name == "Test Artist"
        assert len(artist.images) == 0
        assert artist.default_image is None
