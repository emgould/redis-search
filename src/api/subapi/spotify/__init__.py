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
    SpotifyTopTrackResponse,
)
from api.subapi.spotify.wrappers import SpotifyWrapper, spotify_wrapper

__all__ = [
    # Models
    "SpotifyArtist",
    "SpotifyAlbum",
    "SpotifyPlaylist",
    "SpotifyAlbumMetadata",
    "SpotifyArtistSearchResponse",
    "SpotifyAlbumSearchResponse",
    "SpotifyMultiSearchResponse",
    "SpotifyTopTrackResponse",
    # Wrappers
    "SpotifyWrapper",
    "spotify_wrapper",
]
