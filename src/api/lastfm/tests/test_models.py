"""
Tests for LastFM Pydantic models.
"""

import pytest
from pydantic import ValidationError

from api.lastfm._models import OdesliPlatformLinks
from api.lastfm.models import (
    LastFMTrack,
    MCMusicAlbum,
    MCMusicArtist,
    MCMusicPlaylist,
    SpotifyAlbumMetadata,
    SpotifyTokenResponse,
)
from contracts.models import MCType


class TestMCMusicAlbum:
    """Tests for MCMusicAlbum model."""

    def test_music_album_creation(self):
        """Test creating a MCMusicAlbum with all fields."""
        album = MCMusicAlbum(
            title="The Dark Side of the Moon",
            artist="Pink Floyd",
            listeners=2500000,
            playcount=50000000,
            image="https://example.com/image.jpg",
            url="https://www.last.fm/music/album",
            mbid="test-mbid-123",
            spotify_url="https://open.spotify.com/album/123",
            release_date="1973-03-01",
            total_tracks=10,
        )

        assert album.title == "The Dark Side of the Moon"
        assert album.artist == "Pink Floyd"
        assert album.listeners == 2500000
        assert album.playcount == 50000000
        assert album.mc_type == "music_album"

    def test_music_album_auto_generates_mc_id(self):
        """Test that mc_id is auto-generated if not provided."""
        album = MCMusicAlbum(
            title="Test Album",
            artist="Test Artist",
            mbid="test-mbid-456",
        )

        assert album.mc_id is not None
        assert "album_" in album.mc_id

    def test_music_album_auto_generates_mc_type(self):
        """Test that mc_type is auto-generated if not provided."""
        album = MCMusicAlbum(
            title="Test Album",
            artist="Test Artist",
        )

        assert album.mc_type == "music_album"

    def test_music_album_with_minimal_fields(self):
        """Test creating a MCMusicAlbum with only required fields."""
        album = MCMusicAlbum(
            title="Minimal Album",
            artist="Minimal Artist",
        )

        assert album.title == "Minimal Album"
        assert album.artist == "Minimal Artist"
        assert album.listeners == 0
        assert album.playcount == 0
        assert album.spotify_url is None

    def test_music_album_with_streaming_urls(self):
        """Test MCMusicAlbum with streaming platform URLs."""
        album = MCMusicAlbum(
            title="Streaming Album",
            artist="Streaming Artist",
            spotify_url="https://open.spotify.com/album/123",
            apple_music_url="https://music.apple.com/album/123",
            youtube_music_url="https://music.youtube.com/playlist/123",
        )

        assert album.spotify_url == "https://open.spotify.com/album/123"
        assert album.apple_music_url == "https://music.apple.com/album/123"
        assert album.youtube_music_url == "https://music.youtube.com/playlist/123"


class TestMCMusicArtist:
    """Tests for MCMusicArtist model."""

    def test_music_artist_creation(self):
        """Test creating a MCMusicArtist with all fields."""
        artist = MCMusicArtist(
            id="spotify-artist-123",
            name="Pink Floyd",
            mc_id="spotify_artist_spotify-artist-123",
            spotify_url="https://open.spotify.com/artist/123",
            popularity=85,
            followers=5000000,
            genres=["progressive rock", "psychedelic rock"],
            image="https://example.com/artist.jpg",
        )

        assert artist.id == "spotify-artist-123"
        assert artist.name == "Pink Floyd"
        assert artist.mc_type == MCType.PERSON
        assert artist.popularity == 85
        assert artist.followers == 5000000
        assert len(artist.genres) == 2

    def test_music_artist_auto_sets_artist_field(self):
        """Test that artist field is auto-set from name."""
        artist = MCMusicArtist(
            id="test-123",
            name="Test Artist",
            mc_id="test_mc_id",
        )

        assert artist.artist == "Test Artist"

    def test_music_artist_auto_sets_title_field(self):
        """Test that title field is auto-set from name."""
        artist = MCMusicArtist(
            id="test-123",
            name="Test Artist",
            mc_id="test_mc_id",
        )

        assert artist.title == "Test Artist"

    def test_music_artist_with_top_track_info(self):
        """Test MCMusicArtist with top track metadata."""
        artist = MCMusicArtist(
            id="test-123",
            name="Test Artist",
            mc_id="test_mc_id",
            top_track_album="Top Album",
            top_track_track="Top Track",
            top_track_release_date="2023-01-01",
            content_type="musician",
            media_type="person",
            known_for="Top Track",
            known_for_department="Music",
        )

        assert artist.top_track_album == "Top Album"
        assert artist.top_track_track == "Top Track"
        assert artist.content_type == "musician"
        assert artist.known_for == "Top Track"


class TestMCMusicPlaylist:
    """Tests for MCMusicPlaylist model."""

    def test_music_playlist_creation(self):
        """Test creating a MCMusicPlaylist."""
        playlist = MCMusicPlaylist(
            id="playlist-123",
            name="Best of Rock",
            mc_id="spotify_playlist_playlist-123",
            spotify_url="https://open.spotify.com/playlist/123",
            popularity=75,
            followers=100000,
            image="https://example.com/playlist.jpg",
        )

        assert playlist.id == "playlist-123"
        assert playlist.name == "Best of Rock"
        assert playlist.mc_type == "music_playlist"
        assert playlist.popularity == 75

    def test_music_playlist_auto_sets_fields(self):
        """Test that artist and title fields are auto-set from name."""
        playlist = MCMusicPlaylist(
            id="test-123",
            name="Test Playlist",
            mc_id="test_mc_id",
        )

        assert playlist.artist == "Test Playlist"
        assert playlist.title == "Test Playlist"


class TestSpotifyTokenResponse:
    """Tests for SpotifyTokenResponse model."""

    def test_spotify_token_response_creation(self):
        """Test creating a SpotifyTokenResponse."""
        token = SpotifyTokenResponse(
            access_token="test_token_xyz123",
            token_type="Bearer",
            expires_in=3600,
        )

        assert token.access_token == "test_token_xyz123"
        assert token.token_type == "Bearer"
        assert token.expires_in == 3600

    def test_spotify_token_response_defaults(self):
        """Test SpotifyTokenResponse with default values."""
        token = SpotifyTokenResponse(access_token="test_token")

        assert token.token_type == "Bearer"
        assert token.expires_in == 3600


class TestSpotifyAlbumMetadata:
    """Tests for SpotifyAlbumMetadata model."""

    def test_spotify_album_metadata_creation(self):
        """Test creating SpotifyAlbumMetadata."""
        metadata = SpotifyAlbumMetadata(
            album_name="Test Album",
            spotify_url="https://open.spotify.com/album/123",
            release_date="2023-01-01",
            release_date_precision="day",
            total_tracks=12,
            album_type="album",
            popularity=80,
            image="https://example.com/album.jpg",
        )

        assert metadata.album_name == "Test Album"
        assert metadata.spotify_url == "https://open.spotify.com/album/123"
        assert metadata.total_tracks == 12
        assert metadata.popularity == 80

    def test_spotify_album_metadata_all_optional(self):
        """Test SpotifyAlbumMetadata with all optional fields."""
        metadata = SpotifyAlbumMetadata()

        assert metadata.album_name is None
        assert metadata.spotify_url is None
        assert metadata.total_tracks is None


class TestOdesliPlatformLinks:
    """Tests for OdesliPlatformLinks model."""

    def test_odesli_platform_links_creation(self):
        """Test creating OdesliPlatformLinks."""
        links = OdesliPlatformLinks(
            spotify="https://open.spotify.com/album/123",
            apple_music="https://music.apple.com/album/123",
            youtube_music="https://music.youtube.com/playlist/123",
            amazon_music="https://music.amazon.com/albums/123",
            tidal="https://tidal.com/album/123",
            deezer="https://www.deezer.com/album/123",
        )

        assert links.spotify == "https://open.spotify.com/album/123"
        assert links.apple_music == "https://music.apple.com/album/123"
        assert links.youtube_music == "https://music.youtube.com/playlist/123"
        assert links.amazon_music == "https://music.amazon.com/albums/123"
        assert links.tidal == "https://tidal.com/album/123"
        assert links.deezer == "https://www.deezer.com/album/123"

    def test_odesli_platform_links_all_optional(self):
        """Test OdesliPlatformLinks with all optional fields."""
        links = OdesliPlatformLinks()

        assert links.spotify is None
        assert links.apple_music is None
        assert links.youtube_music is None


class TestLastFMTrack:
    """Tests for LastFMTrack model."""

    def test_lastfm_track_creation(self):
        """Test creating a LastFMTrack."""
        track = LastFMTrack(
            name="Comfortably Numb",
            listeners=1500000,
            playcount=25000000,
            artist={"name": "Pink Floyd", "url": "https://www.last.fm/music/Pink+Floyd"},
            url="https://www.last.fm/music/track",
            mbid="track-mbid-123",
        )

        assert track.name == "Comfortably Numb"
        assert track.listeners == 1500000
        assert track.playcount == 25000000
        assert track.artist["name"] == "Pink Floyd"

    def test_lastfm_track_with_defaults(self):
        """Test LastFMTrack with default values."""
        track = LastFMTrack(name="Test Track")

        assert track.name == "Test Track"
        assert track.listeners == 0
        assert track.playcount == 0
        assert track.artist == {}
        assert track.image == []
