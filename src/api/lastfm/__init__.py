"""
LastFM Service Package - Modular music service for Last.fm and Spotify integration.

This package provides:
- LastFMService: Base service class with core utilities
- LastFMEnrichmentService: Spotify enrichment (Odesli methods deprecated)
- LastFMSearchService: All search operations with Apple Music integration
- LastFMHandler: Firebase function handlers
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
- MusicService: Backward compatibility singleton (deprecated)

Note: Odesli integration has been replaced with direct Apple Music API.
Deprecated Odesli models and methods are preserved in _models.py for future reference.
"""

import os

from api.lastfm.handlers import LastFMHandler, lastfm_handler
from api.lastfm.models import (
    LastFMTrack,
    MCMusicAlbum,
    MCMusicArtist,
    MCMusicPlaylist,
    SpotifyAlbumMetadata,
    SpotifyTokenResponse,
)

# OdesliPlatformLinks moved to _models.py (deprecated)
# from api.lastfm._models import OdesliPlatformLinks
from api.lastfm.wrappers import lastfm_wrapper

__all__ = [
    "LastFMSearchService",
    # Handlers
    "LastFMHandler",
    "lastfm_handler",
    "lastfm_search_service",
    # Models
    "MCMusicAlbum",
    "MCMusicArtist",
    "MCMusicPlaylist",
    "SpotifyAlbumMetadata",
    "SpotifyTokenResponse",
    "LastFMTrack",
    # Wrappers
    "lastfm_wrapper",
    # Note: OdesliPlatformLinks moved to _models.py (deprecated)
]
