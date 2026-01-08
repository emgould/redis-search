"""
Spotify Models - Pydantic models for Spotify music data structures
Follows Pydantic 2.0 patterns with full type safety and MCBaseItem integration.
"""

from pydantic import Field, model_validator

from api.subapi.spotify.core import process_spotify_images, process_spotify_links
from contracts.models import MCBaseItem, MCLink, MCSources, MCSubType, MCType


class SpotifyArtist(MCBaseItem):
    """Model for Spotify artist data."""

    # Core fields
    id: str
    name: str

    # Spotify specific
    spotify_url: str | None = None
    artist_link: MCLink | None = None

    popularity: int = 0
    followers: int = 0
    genres: list[str] = Field(default_factory=list)

    # Images
    default_image: str | None = None

    # Compatibility fields
    artist: str | None = None  # For cards that expect 'artist' field
    title: str | None = None  # For cards that expect 'title' field

    # Known for
    known_for: str | None = None

    # Additional metadata
    top_track_album: str | None = None
    top_track_release_date: str | None = None
    top_track_album_image: str | None = None
    top_track_track: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.MUSIC_ARTIST
    source: MCSources = MCSources.SPOTIFY

    @classmethod
    def from_spotify_artistdata(cls, item: dict) -> "SpotifyArtist":
        """
        Process Spotify artist data and format for MediaCircle.

        Args:
            item: Dictionary containing Spotify artist data

        Returns:
            SpotifyArtist instance with standardized fields
        """
        images, default_image = process_spotify_images(item.get("images", []))
        links, url = process_spotify_links(item)

        artist_name = item.get("name")
        artist_id = item.get("id")
        if not artist_id or not artist_name:
            return SpotifyArtist(
                id="",
                name="",
                error="SpotifyArtist: Missing required fields (id or name)",
            )
        artist_link = (
            MCLink(
                url=url,
                key="artist",
                description=f"{artist_name}",
            )
            if url
            else None
        )
        metrics = {
            "popularity": item.get("popularity", 0),
            "followers": item.get("followers", {}).get("total", 0),
        }
        return SpotifyArtist(
            id=artist_id,
            name=artist_name,
            artist=artist_name,
            artist_link=artist_link,
            spotify_url=url or None,
            popularity=metrics.get("popularity", 0),
            followers=metrics.get("followers", 0),
            genres=item.get("genres", []),
            default_image=default_image,
            images=images,
            links=links,
            metrics=metrics,
            source_id=artist_id,
        )


class SpotifyAlbum(MCBaseItem):
    """Model for Spotify album data."""

    # Core fields
    id: str
    title: str

    album_type: str | None = None
    artist: str | None = None
    artist_id: str | None = None

    # Spotify specific
    spotify_url: str | None = None

    # Images
    default_image: str | None = None

    # Additional metadata
    mbid: str = ""
    artist_url: str | None = None

    release_date: str | None = None
    release_date_precision: str | None = None
    total_tracks: int | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.MUSIC_ALBUM
    source: MCSources = MCSources.SPOTIFY

    # link urls
    apple_music_url: str | None = None
    youtube_music_url_ios: str | None = None
    youtube_music_url_android: str | None = None
    youtube_music_url_web: str | None = None

    @classmethod
    def from_spotify_albumdata(cls, item: dict) -> "SpotifyAlbum":
        """
        Process Spotify album data and format for MediaCircle.

        Args:
            item: Dictionary containing Spotify album data

        Returns:
            SpotifyAlbum instance with standardized fields
        """
        if item.get("type") != "album":
            return SpotifyAlbum(
                id="",
                title="",
                error="SpotifyAlbum: Item is not an album",
            )

        artists = item.get("artists")
        if not artists or not isinstance(artists, list) or len(artists) == 0:
            return SpotifyAlbum(
                id="",
                title="",
                error="SpotifyAlbum: No artists found",
            )

        images, default_image = process_spotify_images(item.get("images", []))
        links, spotify_url = process_spotify_links(item)

        album_id = item.get("id")
        album_title = item.get("name")
        if not album_id or not album_title:
            return SpotifyAlbum(
                id="",
                title="",
                error="SpotifyAlbum: Missing required fields (id or name)",
            )

        metrics = {"num_available_markets": len(item.get("available_markets", []))}

        # artists is verified above to be a non-empty list
        first_artist = artists[0]
        artist_name = first_artist.get("name") if isinstance(first_artist, dict) else None
        artist_id = first_artist.get("id") if isinstance(first_artist, dict) else None

        return SpotifyAlbum(
            id=album_id,
            title=album_title,
            artist=artist_name,
            artist_id=artist_id,
            spotify_url=spotify_url,
            release_date=item.get("release_date"),
            release_date_precision=item.get("release_date_precision"),
            total_tracks=item.get("total_tracks"),
            album_type=item.get("type"),
            default_image=default_image,
            images=images,
            links=links,
            metrics=metrics,
            source_id=album_id,
        )


class SpotifyPlaylist(MCBaseItem):
    """Model for Spotify playlist data."""

    # Core fields
    id: str
    name: str

    # Spotify specific
    spotify_url: str | None = None
    collaborative: bool | None = None
    description: str | None = None
    owner: str | None = None

    # Images
    default_image: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.MUSIC_PLAYLIST
    source: MCSources = MCSources.SPOTIFY

    @classmethod
    def from_spotify_playlistdata(cls, item: dict) -> "SpotifyPlaylist":
        """
        Process Spotify playlist data and format for MediaCircle.

        Args:
            item: Dictionary containing Spotify playlist data

        Returns:
            SpotifyPlaylist instance with standardized fields
        """
        if item.get("type") != "playlist":
            return SpotifyPlaylist(
                id="",
                name="",
                error="SpotifyPlaylist: Item is not a playlist",
            )

        images, default_image = process_spotify_images(item.get("images", []))
        links, spotify_url = process_spotify_links(item)

        playlist_id = item.get("id")
        playlist_name = item.get("name")
        if not playlist_id or not playlist_name:
            return SpotifyPlaylist(
                id="",
                name="",
                error="SpotifyPlaylist: Missing required fields (id or name)",
            )

        # Extract owner name if owner is a dict
        owner = item.get("owner")
        if isinstance(owner, dict):
            owner = owner.get("display_name") or owner.get("id")

        return SpotifyPlaylist(
            id=playlist_id,
            name=playlist_name,
            collaborative=item.get("collaborative", False),
            description=item.get("description"),
            owner=owner,
            spotify_url=spotify_url,
            default_image=default_image,
            images=images,
            links=links,
            source_id=playlist_id,
        )


class SpotifyAlbumMetadata(MCBaseItem):
    """Model for Spotify album metadata (used internally)."""

    mc_type: MCType = MCType.MUSIC_ALBUM_METADATA
    source: MCSources = MCSources.SPOTIFY

    album_name: str | None = None
    spotify_url: str | None = None
    release_date: str | None = None
    release_date_precision: str | None = None
    total_tracks: int | None = None
    album_type: str | None = None
    popularity: int | None = None
    image: str | None = None


# ============================================================================
# Search Response Models
# These models represent responses from Spotify search operations
# ============================================================================


class SpotifyArtistSearchResponse(MCBaseItem):
    """Model for Spotify artist search response."""

    results: list[SpotifyArtist] = Field(default_factory=list)
    total_results: int = 0
    query: str | None = None
    data_source: str = "Spotify Artist Search"

    mc_type: MCType = MCType.PERSON
    source: MCSources = MCSources.SPOTIFY

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "SpotifyArtistSearchResponse":
        """Auto-generate mc_id if not provided."""
        if not self.mc_id and self.query:
            self.mc_id = f"spotify_artist_search_{self.query}"
        return self


class SpotifyAlbumSearchResponse(MCBaseItem):
    """Model for Spotify album search response."""

    results: list[SpotifyAlbum] = Field(default_factory=list)
    total_results: int = 0
    query: str = ""
    data_source: str = "Spotify Album Search"

    mc_type: MCType = MCType.MUSIC_ALBUM
    source: MCSources = MCSources.SPOTIFY

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "SpotifyAlbumSearchResponse":
        """Auto-generate mc_id if not provided."""
        if not self.mc_id and self.query:
            self.mc_id = f"spotify_album_search_{self.query}"
        return self


class SpotifyMultiSearchResponse(MCBaseItem):
    """Model for Spotify multi-type search response (artists, albums, playlists)."""

    results: list[SpotifyArtist | SpotifyAlbum | SpotifyPlaylist] = Field(default_factory=list)
    total_results: int = 0
    artist_count: int = 0
    album_count: int = 0
    playlist_count: int = 0
    query: str = ""
    data_source: str = "Spotify Multi-Type Search"

    mc_type: MCType = MCType.MIXED
    source: MCSources = MCSources.SPOTIFY

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "SpotifyMultiSearchResponse":
        """Auto-generate mc_id if not provided."""
        if not self.mc_id and self.query:
            self.mc_id = f"spotify_multi_search_{self.query}"
        return self


class SpotifyTrack(MCBaseItem):
    """Model for Spotify track data."""

    # Core fields
    id: str
    name: str

    # Spotify specific
    spotify_url: str | None = None

    # Images (from album)
    default_image: str | None = None

    # Album metadata (tracks come with album info)
    album: str | None = None  # Album name
    release_date: str | None = None  # Album release date
    album_image: str | None = None  # Album image URL (alternative to default_image)

    # MCBaseItem fields
    mc_type: MCType = MCType.MUSIC_TRACK
    source: MCSources = MCSources.SPOTIFY

    @classmethod
    def from_spotify_trackdata(cls, item: dict) -> "SpotifyTrack":
        """
        Process Spotify track data and format for MediaCircle.
        Tracks from Spotify API include album data which is extracted here.
        """
        # Tracks don't have images directly - images come from the album
        album_data = item.get("album", {})
        album_images = album_data.get("images", []) if album_data else []
        images, default_image = process_spotify_images(album_images)

        links, spotify_url = process_spotify_links(item)

        track_id = item.get("id")
        track_name = item.get("name")
        if not track_id or not track_name:
            return SpotifyTrack(
                id="",
                name="",
                error="SpotifyTrack: Missing required fields (id or name)",
            )

        # Extract album metadata if available
        album_name = album_data.get("name") if album_data else None
        album_release_date = album_data.get("release_date") if album_data else None
        # Get album image URL (first/largest image)
        album_image_url = album_images[0].get("url") if album_images else None

        return SpotifyTrack(
            id=track_id,
            name=track_name,
            spotify_url=spotify_url,
            default_image=default_image,
            album=album_name,
            release_date=album_release_date,
            album_image=album_image_url,
            images=images,
            links=links,
            source_id=track_id,
        )


class SpotifyTopTrackResponse(MCBaseItem):
    """Model for Spotify top track response."""

    results: list[SpotifyTrack] = Field(default_factory=list)
    total_results: int = 0
    query: str = ""
    data_source: str = "Spotify Top Track"

    mc_type: MCType = MCType.MUSIC_TRACK
    source: MCSources = MCSources.SPOTIFY

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "SpotifyTopTrackResponse":
        """Auto-generate mc_id if not provided."""
        if not self.mc_id and self.query:
            self.mc_id = f"spotify_top_track_{self.query}"
        return self
