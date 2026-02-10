"""
LastFM Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides backward-compatible async wrappers for Firebase Functions integration.
"""

import asyncio
from typing import cast

from api.lastfm.models import (
    LastFMAlbumSearchResponse,
    LastFMArtistSearchResponse,
    LastFMMultiSearchResponse,
    LastFMTrendingAlbumsResponse,
)
from api.subapi.apple.wrapper import apple_wrapper
from api.subapi.spotify.models import SpotifyAlbum, SpotifyAlbumSearchResponse
from api.subapi.spotify.wrappers import spotify_wrapper
from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSources,
    MCSubType,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache
from utils.soft_comparison import soft_compare

logger = get_logger(__name__)

# Cache for standalone async functions (not class methods)
LastFMWrapperCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="lastfm_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
)


class LastFMWrapper:
    def __init__(self):
        # Use singleton service instance to ensure shared rate limiter
        # Import here to avoid circular dependency
        from api.lastfm.search import lastfm_search_service

        self.service = lastfm_search_service
        self.apple_wrapper = apple_wrapper

    @RedisCache.use_cache(LastFMWrapperCache, prefix="get_trending_albums_wrapper")
    async def get_trending_albums(self, limit: int = 10, **kwargs) -> LastFMTrendingAlbumsResponse:
        """
        Async wrapper function to get trending albums.

        Args:
            limit: Number of albums to return (default=10)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            LastFMTrendingAlbumsResponse: MCBaseItem derivative containing trending albums or error information
        """
        try:
            data: LastFMTrendingAlbumsResponse = await self.service.get_trending_albums(limit=limit)
            return data

        except Exception as e:
            logger.error(f"Error in get_trending_albums: {e}")
            return LastFMTrendingAlbumsResponse(
                results=[],
                total_results=0,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(LastFMWrapperCache, prefix="search_albums_wrapper")
    async def search_albums(
        self, query: str, limit: int = 20, **kwargs
    ) -> LastFMAlbumSearchResponse:
        """
        Async wrapper function to search albums.

        Args:
            query: Search query string
            limit: Number of results to return (default=20)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            LastFMAlbumSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: LastFMAlbumSearchResponse = await self.service.search_albums(
                query=query, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_albums: {e}")
            return LastFMAlbumSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(LastFMWrapperCache, prefix="search_by_genre_wrapper")
    async def search_by_genre(
        self, genre: str, limit: int = 50, **kwargs
    ) -> LastFMArtistSearchResponse:
        """
        Async wrapper function to search artists by genre.

        Args:
            genre: Music genre to search for (e.g., "rock", "pop", "jazz")
            limit: Number of results to return (default=50)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            LastFMArtistSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: LastFMArtistSearchResponse = await self.service.search_by_genre(
                genre=genre, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_by_genre: {e}")
            return LastFMArtistSearchResponse(
                results=[],
                total_results=0,
                query=genre,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(LastFMWrapperCache, prefix="search_by_keyword_wrapper")
    async def search_by_keyword(
        self, keyword: str, limit: int = 50, **kwargs
    ) -> LastFMMultiSearchResponse:
        """
        Async wrapper function to search by keyword (artists, albums, playlists).

        Args:
            keyword: Keyword to search for
            limit: Number of results to return per type (default=50)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            LastFMMultiSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: LastFMMultiSearchResponse = await self.service.search_by_keyword(
                keyword=keyword, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_by_keyword: {e}")
            return LastFMMultiSearchResponse(
                results=[],
                total_results=0,
                query=keyword,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(LastFMWrapperCache, prefix="search_artist_wrapper")
    async def search_artist(
        self, query: str, limit: int = 20, **kwargs
    ) -> LastFMArtistSearchResponse:
        """
        Async wrapper function to search artists by name.

        Args:
            query: Search query string (artist name)
            limit: Number of results to return (default=20)
            **kwargs: Additional arguments (for compatibility)

        Returns:
            LastFMArtistSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            data: LastFMArtistSearchResponse = await self.service.search_spotify_artist(
                query=query, limit=limit
            )
            return data

        except Exception as e:
            logger.error(f"Error in search_artist: {e}")
            return LastFMArtistSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(LastFMWrapperCache, prefix="search_person_works")
    async def search_person_async(
        self,
        request: "MCPersonSearchRequest",
        limit: int | None = None,
    ) -> "MCPersonSearchResponse":
        """Search for artist works (albums) based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

        Args:
            request: MCPersonSearchRequest with artist identification details
            limit: Maximum number of albums to return (default: 50)

        Returns:
            MCPersonSearchResponse with artist details and works
            - details: MCMusicArtist (artist details)
            - works: list[MCMusicAlbum] (albums by the artist)
            - related: [] (empty, will be filled by search_broker)
        """

        # Note: Cache key automatically includes request.source and request.source_id
        # via the function arguments, so validation errors get their own cache entries
        try:
            # Validate that this is a music artist source (LastFM or Spotify)
            # The wrapper can handle both since it uses Spotify internally
            if request.source not in (MCSources.LASTFM, MCSources.SPOTIFY):
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source for music artist search: {request.source} (expected LASTFM or SPOTIFY)",
                    status_code=400,
                )

            # The wrapper can handle both since it uses Spotify internally
            if request.mc_subtype not in (
                MCSubType.MUSIC_ARTIST,
                MCSubType.ARTIST,
                MCSubType.MUSICIAN,
            ):
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid subtype for music artist search: {request.mc_subtype} (expected MUSIC_ARTIST, ARTIST, or MUSICIAN)",
                    status_code=400,
                )

            # Validate source_id (must be provided and non-empty)
            # source_id is a string (Spotify artist ID)
            if not request.source_id or len(request.source_id.strip()) == 0:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source_id for LastFM musician: {request.source_id} (must be provided)",
                    status_code=400,
                )
            # If we have a source_id, try to fetch the artist directly by ID first
            # source_id is a string (Spotify artist ID)
            artist_id = request.source_id.strip()
            artist = None

            # Try to fetch artist by ID using Spotify wrapper
            artist_response = await spotify_wrapper.get_artist(artist_id=artist_id)

            if artist_response.status_code == 200 and artist_response.results:
                # Successfully found artist by ID
                artist = artist_response.results[0]
            else:
                # ID lookup failed - fall back to name search if name is provided
                artist_name = request.name
                if not artist_name:
                    # No name to fall back to, return error
                    status_code = (
                        404 if not artist_response.results else (artist_response.status_code or 404)
                    )
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error=artist_response.error or "Artist not found",
                        status_code=status_code,
                    )

                # Fall back to name search
                artist_limit = 1  # We only need the first match
                artists_response = await self.search_artist(query=artist_name, limit=artist_limit)

                if artists_response.status_code != 200 or not artists_response.results:
                    # If no results found, return 404; otherwise use the response status code
                    status_code = (
                        404
                        if not artists_response.results
                        else (artists_response.status_code or 404)
                    )
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error=artists_response.error or "Artist not found",
                        status_code=status_code,
                    )

                # Get the artist details (first result)
                found_artist = artists_response.results[0]

                # Verify the found artist actually matches the requested name
                # Spotify's fuzzy search can return unrelated results, so we need to validate
                found_artist_name = found_artist.name if found_artist.name else ""
                names_match, _ = soft_compare(artist_name, found_artist_name)

                if not names_match:
                    # The found artist doesn't match the requested name - treat as not found
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error="Artist not found",
                        status_code=404,
                    )

                artist = found_artist
                # Update artist_id to use the found artist's ID (fallback case)
                artist_id = found_artist.source_id if found_artist.source_id else artist_id

            # Use artist name for logging
            artist_name = artist.name if artist.name else request.name

            # Get albums directly by artist ID using Spotify's artist albums endpoint
            # This bypasses generic album search filtering and returns all albums by this artist
            album_limit = limit if limit is not None else 30
            albums_response: SpotifyAlbumSearchResponse = await spotify_wrapper.get_artist_albums(
                artist_id=artist_id, limit=album_limit
            )

            # Convert albums to works list
            works: list[SpotifyAlbum] = []
            if albums_response.status_code == 200 and albums_response.results:
                works = albums_response.results
                logger.info(
                    f"Found {len(works)} albums for artist '{artist_name}' (ID: {artist_id})"
                )

                # Enrich albums with Apple Music and YouTube Music links
                # Search using both artist and album title for better accuracy
                enrichment_tasks = []
                album_indices = []  # Track which albums have enrichment tasks
                for i, album in enumerate[SpotifyAlbum](works):
                    # Build search term with artist and album title
                    album_title = getattr(album, "title", None) or getattr(album, "name", None)
                    if album_title:
                        search_term = f"{artist_name} {album_title}"
                        enrichment_tasks.append(self.apple_wrapper.search_album(search_term))
                        album_indices.append(i)
                # Execute enrichment tasks in parallel
                if enrichment_tasks:
                    enrichment_results = await asyncio.gather(
                        *enrichment_tasks, return_exceptions=True
                    )

                    # Apply enrichment results to albums
                    for album_idx, result in zip(album_indices, enrichment_results, strict=False):
                        if isinstance(result, Exception):
                            logger.debug(f"Apple Music enrichment failed for album: {result}")
                            continue

                        if result and isinstance(result, list) and len(result) > 0:
                            # Update album with Apple Music and YouTube Music URLs
                            works[album_idx].apple_music_url = result[0].deeplink
                            works[album_idx].youtube_music_url_ios = result[0].youtube_ios_link
                            works[album_idx].youtube_music_url_android = result[
                                0
                            ].youtube_android_link
                            works[album_idx].youtube_music_url_web = result[0].youtube_web_fallback

                    logger.info(f"Enriched {len(works)} albums with Apple Music metadata")

            # Return response with artist details and works
            # related will be filled by search_broker
            # Cast works to list[MCBaseItem] for type compatibility
            works_base: list[MCBaseItem] = [cast(MCBaseItem, album) for album in works]
            return MCPersonSearchResponse(
                input=request,
                details=artist,  # MCMusicArtist
                works=works_base,  # list[MCBaseItem]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching artist works for {request.name}: {e}")
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )


lastfm_wrapper = LastFMWrapper()
