"""
Spotify Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides backward-compatible async wrappers for Firebase Functions integration.
"""

import logging

from api.subapi.spotify.models import (
    SpotifyAlbumSearchResponse,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyTopTrackResponse,
)
from api.subapi.spotify.search import spotify_search_service
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__, level=logging.WARNING)

# Cache for standalone async functions (not class methods)
SpotifyWrapperCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="spotify_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="1.1.1",  # Version bump for Redis migration
)


class SpotifyWrapper:
    def __init__(self):
        self.service = spotify_search_service

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="search_albums_wrapper")
    async def search_albums(
        self, query: str, type: str = "album", limit: int = 20, **kwargs
    ) -> SpotifyAlbumSearchResponse:
        """
        Async wrapper function to search albums.

        Args:
            query: Search query string
            type: Type of search (default=album) (album, artist, playlist, track)
            limit: Number of results to return (default=20)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyAlbumSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: SpotifyAlbumSearchResponse = await self.service.search_albums(
                query=query, type=type, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_albums: {e}")
            return SpotifyAlbumSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="search_by_genre_wrapper")
    async def search_by_genre(
        self, genre: str, limit: int = 50, **kwargs
    ) -> SpotifyArtistSearchResponse:
        """
        Async wrapper function to search artists by genre.

        Args:
            genre: Music genre to search for (e.g., "rock", "pop", "jazz")
            limit: Number of results to return (default=50)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyArtistSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: SpotifyArtistSearchResponse = await self.service.search_by_genre(
                genre=genre, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_by_genre: {e}")
            return SpotifyArtistSearchResponse(
                results=[],
                total_results=0,
                query=genre,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="search_by_keyword_wrapper")
    async def search_by_keyword(
        self, keyword: str, limit: int = 50, **kwargs
    ) -> SpotifyMultiSearchResponse:
        """
        Async wrapper function to search by keyword (artists, albums, playlists).

        Args:
            keyword: Keyword to search for
            limit: Number of results to return per type (default=50)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyMultiSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: SpotifyMultiSearchResponse = await self.service.search_by_keyword(
                keyword=keyword, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_by_keyword: {e}")
            return SpotifyMultiSearchResponse(
                results=[],
                total_results=0,
                query=keyword,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="search_artists_wrapper")
    async def search_artists(
        self, query: str, limit: int = 20, **kwargs
    ) -> SpotifyArtistSearchResponse:
        """
        Async wrapper function to search artists by name.

        Args:
            query: Search query string (artist name)
            limit: Number of results to return (default=20)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyArtistSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: SpotifyArtistSearchResponse = await self.service.search_artists(
                query=query, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_artists: {e}")
            return SpotifyArtistSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="get_artist_wrapper")
    async def get_artist(self, artist_id: str, **kwargs) -> SpotifyArtistSearchResponse:
        """
        Async wrapper function to get an artist by ID.

        Args:
            artist_id: Spotify artist ID
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyArtistSearchResponse: MCBaseItem derivative containing artist or error information
        """
        try:
            data: SpotifyArtistSearchResponse = await self.service.get_artist(artist_id=artist_id)

            if data.error:
                data.status_code = 404 if "not found" in data.error.lower() else 500
                return data

            data.status_code = 200
            return data

        except Exception as e:
            logger.error(f"Error in get_artist: {e}")
            return SpotifyArtistSearchResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="get_top_track_wrapper")
    async def get_top_track(self, artist_id: str, **kwargs) -> SpotifyTopTrackResponse:
        """
        Async wrapper function to get top track for an artist.

        Args:
            artist_id: Spotify artist ID
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyTopTrackResponse: MCBaseItem derivative containing top track or error information
        """
        try:
            data: SpotifyTopTrackResponse = await self.service.get_top_track(artist_id=artist_id)

            if data.error:
                data.status_code = 500
                return data

            return data

        except Exception as e:
            logger.error(f"Error in get_top_track: {e}")
            return SpotifyTopTrackResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(SpotifyWrapperCache, prefix="get_artist_albums_wrapper")
    async def get_artist_albums(
        self, artist_id: str, limit: int = 30, include_groups: str = "album", **kwargs
    ) -> SpotifyAlbumSearchResponse:
        """
        Async wrapper function to get albums by artist ID.

        Args:
            artist_id: Spotify artist ID
            limit: Number of results to return (default=30)
            include_groups: Album types to include (default="album")
            **kwargs: Additional arguments (for compatibility)

        Returns:
            SpotifyAlbumSearchResponse: MCBaseItem derivative containing albums or error information
        """
        try:
            data: SpotifyAlbumSearchResponse = await self.service.get_artist_albums(
                artist_id=artist_id, limit=limit, include_groups=include_groups
            )
            return data

        except Exception as e:
            logger.error(f"Error in get_artist_albums: {e}")
            return SpotifyAlbumSearchResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
                status_code=500,
            )


spotify_wrapper = SpotifyWrapper()
