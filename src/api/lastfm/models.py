"""
LastFM Models - Pydantic models for LastFM music data structures
Follows Pydantic 2.0 patterns with full type safety.
"""

from typing import Any

from contracts.models import MCBaseItem, MCSearchResponse, MCSources, MCSubType, MCType
from pydantic import BaseModel, Field, model_validator


class MCMusicAlbum(MCBaseItem):
    """Model for music album data."""

    # Core fields
    id: str = ""  # Required for MCBaseItem mc_id generation
    title: str
    artist: str
    listeners: int = 0
    playcount: int = 0
    image: str | None = None
    url: str = ""
    mbid: str = ""
    artist_url: str | None = None
    streamable: bool | None = None

    # External music service metadata
    spotify_url: str | None = None
    release_date: str | None = None
    release_date_precision: str | None = None
    total_tracks: int | None = None
    album_type: str | None = None
    popularity: int | None = None

    # Streaming platform URLs
    apple_music_url: str | None = None
    youtube_music_url_ios: str | None = None
    youtube_music_url_android: str | None = None
    youtube_music_url: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.MUSIC_ALBUM
    source: MCSources = MCSources.LASTFM

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        """Set default values for mc_id and mc_type if not provided."""
        # If data is already a BaseModel instance, return it as-is
        if isinstance(data, BaseModel):
            return data
        # Ensure data is a dict for processing
        if not isinstance(data, dict):
            return data
        # Set id if not present (use mbid as fallback)
        if not data.get("id") and data.get("mbid"):
            data["id"] = data.get("mbid")
        return data


class MCMusicArtist(MCBaseItem):
    """Model for music artist data."""

    # Core fields
    id: str
    name: str

    # External music service fields
    spotify_url: str | None = None
    popularity: int = 0
    followers: int = 0
    genres: list[str] = Field(default_factory=list)

    # Images
    image: str | None = None
    raw_images: list[dict[str, Any]] = Field(
        default_factory=list
    )  # Raw image data from music source
    # Note: 'images' field inherited from MCBaseItem (list[MCImage])

    # Compatibility fields
    artist: str | None = None  # For cards that expect 'artist' field
    title: str | None = None  # For cards that expect 'title' field

    # Additional metadata
    top_track_album: str | None = None
    top_track_release_date: str | None = None
    top_track_album_image: str | None = None
    top_track_track: str | None = None
    content_type: str | None = None
    media_type: str | None = None
    known_for: str | None = None
    known_for_department: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.MUSIC_ARTIST
    source: MCSources = MCSources.LASTFM

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        """Set default values for mc_id, mc_type, artist and title if not provided."""
        # If data is already a BaseModel instance, return it as-is
        if isinstance(data, BaseModel):
            return data
        # Ensure data is a dict for processing
        if not isinstance(data, dict):
            return data
        if not data.get("artist"):
            data["artist"] = data.get("name", "")
        if not data.get("title"):
            data["title"] = data.get("name", "")
        return data


class MCMusicPlaylist(MCBaseItem):
    """Model for music playlist data."""

    # Core fields
    id: str
    name: str

    # External music service fields
    spotify_url: str | None = None
    popularity: int = 0
    followers: int = 0

    # Images
    image: str | None = None
    raw_images: list[dict[str, Any]] = Field(
        default_factory=list
    )  # Raw image data from music source
    # Note: 'images' field inherited from MCBaseItem (list[MCImage])

    # Compatibility fields
    artist: str | None = None
    title: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.MUSIC_PLAYLIST
    source: MCSources = MCSources.LASTFM

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        """Set default values for mc_id, mc_type, artist and title if not provided."""
        # If data is already a BaseModel instance, return it as-is
        if isinstance(data, BaseModel):
            return data
        # Ensure data is a dict for processing
        if not isinstance(data, dict):
            return data
        if not data.get("artist"):
            data["artist"] = data.get("name", "")
        if not data.get("title"):
            data["title"] = data.get("name", "")
        return data


class SpotifyTokenResponse(BaseModel):
    """Model for Spotify token response."""

    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600


class SpotifyAlbumMetadata(BaseModel):
    """Model for Spotify album metadata."""

    album_name: str | None = None
    spotify_url: str | None = None
    release_date: str | None = None
    release_date_precision: str | None = None
    total_tracks: int | None = None
    album_type: str | None = None
    popularity: int | None = None
    image: str | None = None


# OdesliPlatformLinks has been moved to _models.py (deprecated models)
# Import from there if needed for backwards compatibility
# from api.lastfm._models import OdesliPlatformLinks


class LastFMTrack(BaseModel):
    """Model for Last.fm track data."""

    name: str
    listeners: int = 0
    playcount: int = 0
    artist: dict[str, Any] = Field(default_factory=dict)
    image: list[dict[str, str]] = Field(default_factory=list)
    url: str = ""
    mbid: str = ""


# ============================================================================
# Search Response Models
# These models represent responses from Last.fm search operations
# ============================================================================


class LastFMTrendingAlbumsResponse(MCSearchResponse):
    """Model for Last.fm trending albums response."""

    results: list[MCMusicAlbum] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    data_source: str = "Last.fm Top Tracks"
    data_type: MCType = MCType.MUSIC_ALBUM


class LastFMAlbumSearchResponse(MCSearchResponse):
    """Model for Last.fm album search response."""

    results: list[MCMusicAlbum] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str = ""
    data_source: str = "LastFM Album Search"
    data_type: MCType = MCType.MUSIC_ALBUM


class LastFMArtistSearchResponse(MCSearchResponse):
    """Model for Last.fm artist search response."""

    results: list[MCMusicArtist] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "LastFM Artist Search"
    data_type: MCType = MCType.PERSON


class LastFMMultiSearchResponse(MCSearchResponse):
    """Model for Last.fm multi-type search response (artists, albums, playlists)."""

    results: list[MCMusicArtist | MCMusicAlbum | MCMusicPlaylist] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    artist_count: int = 0
    album_count: int = 0
    playlist_count: int = 0
    query: str = ""
    data_source: str = "LastFM Multi-Type Search"
    data_type: MCType = MCType.MIXED
