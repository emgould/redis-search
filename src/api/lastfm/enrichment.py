"""
LastFM Enrichment Service - Spotify enrichment operations.
Extends core service with streaming platform link enrichment.

Note: Odesli enrichment methods are deprecated but kept for potential future use.
Current enrichment uses direct Apple Music API integration.
"""

import aiohttp

from api.lastfm.core import LastFMService
from api.subapi.apple.wrapper import apple_wrapper
from api.subapi.spotify.models import SpotifyAlbumMetadata
from api.subapi.spotify.wrappers import spotify_wrapper
from utils.get_logger import get_logger

logger = get_logger(__name__)


class LastFMEnrichmentService(LastFMService):
    """
    Handles Spotify enrichment for music data.
    Extends LastFMService with streaming platform link enrichment.

    Note: Odesli enrichment methods (_expand_with_odesli, _enrich_with_spotify)
    are deprecated but kept for potential future use.
    """

    def __init__(self):
        """Initialize LastFM enrichment service."""
        super().__init__()
        self.apple_wrapper = apple_wrapper

    async def _search_spotify_track(
        self,
        track_name: str,
        artist_name: str,
    ) -> dict | None:
        """
        Search Spotify for a track and return its album metadata.
        Uses Spotify wrapper to search for albums matching track name and artist.

        Args:
            track_name: Track name
            artist_name: Artist name

        Returns:
            Dict with album metadata including spotify_url, release_date, etc. or None if not found
        """
        try:
            # Use wrapper to search albums with track and artist name
            # This finds albums that match the track, allowing us to extract album metadata
            query = f"track:{track_name} artist:{artist_name}"
            spotify_response = await spotify_wrapper.search_albums(
                query=query, type="track", limit=1
            )

            if spotify_response.status_code != 200 or spotify_response.error:
                return None

            albums = spotify_response.results
            if not albums:
                return None

            # Take the first album result and extract metadata
            # The wrapper returns SpotifyAlbum objects
            album = albums[0].model_dump() if hasattr(albums[0], "model_dump") else albums[0]

            return {
                "album_name": album.get("title") or album.get("name"),
                "spotify_url": album.get("spotify_url"),
                "release_date": album.get("release_date"),
                "release_date_precision": album.get("release_date_precision"),
                "total_tracks": album.get("total_tracks"),
                "album_type": album.get("album_type"),
                "popularity": album.get("popularity", 0),
                "image": album.get("default_image") or album.get("image"),
            }
        except Exception as e:
            logger.debug(
                f"Error searching Spotify for track '{track_name}' by '{artist_name}': {e}"
            )
            return None

    async def _search_spotify_album(
        self,
        album_name: str,
        artist_name: str,
    ) -> SpotifyAlbumMetadata | None:
        """
        Search Spotify for an album and return album metadata.
        Uses Spotify wrapper to search for albums.

        Args:
            session: aiohttp ClientSession (kept for compatibility, not used)
            album_name: Album name
            artist_name: Artist name
            max_retries: Maximum number of retry attempts (default: 3, kept for compatibility)

        Returns:
            SpotifyAlbumMetadata instance or None if not found
        """
        try:
            # Use wrapper to search albums
            query = f"album:{album_name} artist:{artist_name}"
            spotify_response = await spotify_wrapper.search_albums(query=query, limit=10)

            if spotify_response.status_code != 200 or spotify_response.error:
                return None

            albums = spotify_response.results
            if len(albums) == 0:
                return None

            # Find the best matching album by artist name
            best_match = None
            artist_lower = artist_name.lower()
            for album in albums:
                # Check if artist matches (may be in album artist field or title)
                if album.artist is None:
                    continue
                album_artist = album.artist.lower()
                if artist_lower in album_artist or album_artist in artist_lower:
                    best_match = album
                    break

            # If no exact match, use first result
            if not best_match:
                best_match = albums[0]

            # Convert to SpotifyAlbumMetadata format
            return SpotifyAlbumMetadata(
                album_name=best_match.title,
                spotify_url=best_match.spotify_url,
                release_date=best_match.release_date,
                release_date_precision=best_match.release_date_precision,
                total_tracks=best_match.total_tracks,
                album_type=best_match.album_type,
                image=best_match.default_image,
            )
        except Exception as e:
            logger.debug(f"Error searching Spotify for '{album_name}' by '{artist_name}': {e}")
            return None

    async def _expand_with_odesli(self, session: aiohttp.ClientSession, spotify_url: str) -> dict:
        """
        DEPRECATED: Use Odesli (Songlink) to expand a Spotify URL to all streaming platforms.

        This method is no longer actively used. We have migrated to direct API integrations:
        - Apple Music: api.subapi.apple.wrapper.AppleMusicAPI
        - YouTube: Scraping via AppleMusicAPI.get_youtube_video_id()

        Kept for potential future use if we need to reintegrate Odesli or similar service.
        Uses core _odesli_make_request() method.

        Args:
            session: aiohttp ClientSession (kept for compatibility, not used)
            spotify_url: Spotify URL to expand

        Returns:
            Dictionary of platform URLs (spotify, applemusic, youtube, etc.)
        """
        result = await self._odesli_make_request(spotify_url)
        return result if result else {}

    async def _enrich_with_spotify(self, album: dict, session: aiohttp.ClientSession) -> dict:
        """
        DEPRECATED: Enrich album with streaming links using Spotify API + Odesli.

        This method is no longer actively used. We have migrated to direct API integrations
        in the search service (see LastFMSearchService).

        Kept for potential future use if we need Odesli-based enrichment.

        Process:
        1. Search Spotify for the album by artist + title
        2. Get Spotify URL
        3. Use Odesli to expand to all streaming platforms

        Args:
            album: Processed album dictionary
            session: aiohttp ClientSession for making requests

        Returns:
            Album enriched with Spotify, Apple Music, and other streaming URLs
        """
        album_title = album.get("title")
        artist_name = album.get("artist")

        if not album_title or not artist_name:
            return album

        try:
            # Search Spotify for the album (token is generated internally)
            spotify_data = await self._search_spotify_album(album_title, artist_name)
            if not spotify_data:
                return album

            # Add Spotify metadata to album
            spotify_url = spotify_data.spotify_url
            album["release_date"] = spotify_data.release_date
            album["release_date_precision"] = spotify_data.release_date_precision
            album["total_tracks"] = spotify_data.total_tracks
            album["album_type"] = spotify_data.album_type
            album["popularity"] = spotify_data.popularity

            if not spotify_url:
                return album

            # Expand with Odesli to get all platform links
            apple_album = await self.apple_wrapper.search_album(album_title)

            # Extract specific platform URLs
            if apple_album and isinstance(apple_album, list):
                album["apple_music_url"] = apple_album[0].deeplink
                # Use youtube_ios_link as the primary YouTube link (or fallback to web)
                album["youtube_music_url"] = (
                    apple_album[0].youtube_ios_link or apple_album[0].youtube_web_fallback or ""
                )
            else:
                # Fallback to just Spotify URL if Odesli fails
                album["spotify_url"] = spotify_url

        except Exception as e:
            logger.debug(f"Failed to enrich album '{album.get('title')}' with Spotify data: {e}")

        return album
