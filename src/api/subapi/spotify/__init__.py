"""
Spotify Service Package - Modular Spotify service for music search and discovery.

This package provides:
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.subapi.spotify.models import (
    SpotifyAlbum,
    SpotifyAlbumMetadata,
    SpotifyAlbumSearchResponse,
    SpotifyArtist,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyPlaylist,
    SpotifyPodcastEpisode,
    SpotifyPodcastSearchResponse,
    SpotifyPodcastShow,
    SpotifyTopTrackResponse,
    parse_spotify_show_id,
)
from api.subapi.spotify.wrappers import SpotifyWrapper, spotify_wrapper

__all__ = [
    # Models
    "SpotifyArtist",
    "SpotifyAlbum",
    "SpotifyPlaylist",
    "SpotifyPodcastShow",
    "SpotifyPodcastEpisode",
    "SpotifyAlbumMetadata",
    "SpotifyArtistSearchResponse",
    "SpotifyAlbumSearchResponse",
    "SpotifyMultiSearchResponse",
    "SpotifyPodcastSearchResponse",
    "SpotifyTopTrackResponse",
    # Helpers
    "parse_spotify_show_id",
    # Wrappers
    "SpotifyWrapper",
    "spotify_wrapper",
]
