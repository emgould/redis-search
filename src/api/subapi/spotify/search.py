"""
Spotify Search Service - All search operations for music discovery.
Extends core service with search functionality.
"""

import asyncio
import logging
from collections import Counter
from datetime import UTC, datetime

import aiohttp

from api.subapi.spotify.core import SpotifyService
from api.subapi.spotify.models import (
    SpotifyAlbum,
    SpotifyAlbumSearchResponse,
    SpotifyArtist,
    SpotifyArtistSearchResponse,
    SpotifyMultiSearchResponse,
    SpotifyPlaylist,
    SpotifyTopTrackResponse,
    SpotifyTrack,
)
from utils.get_logger import get_logger

logger = get_logger(__name__, level=logging.WARNING)


class SpotifySearchService(SpotifyService):
    """
    Handles all search operations for music discovery.
    Extends core service with search functionality.
    """

    async def search_spotify_album(
        self,
        session: aiohttp.ClientSession,
        album_name: str,
        artist_name: str,
        max_retries: int = 3,
    ) -> SpotifyAlbum:
        """
        Search Spotify for an album and return album metadata.

        Args:
            session: aiohttp ClientSession
            album_name: Album name
            artist_name: Artist name
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            SpotifyAlbumMetadata instance or None if not found
        """
        search_url = "https://api.spotify.com/v1/search"
        params = {
            "q": f"album:{album_name} artist:{artist_name}",
            "type": "album",
            "limit": "1",
        }

        # Use the shared request method which handles rate limiting and retries
        data = await self._make_spotify_request(
            session, search_url, params, max_retries=max_retries
        )
        if data:
            items = data.get("albums", {}).get("items", [])
            albums = [SpotifyAlbum.from_spotify_albumdata(album) for album in items]
            return albums[0]
        else:
            return SpotifyAlbum(
                id="",
                title="",
                error="Failed to search for album",
            )

    async def search_albums(
        self, query: str, type: str = "album", limit: int = 20
    ) -> SpotifyAlbumSearchResponse:
        """
        Search for albums by name or artist using Spotify.
        Also supports searching for tracks and returning album information from track results.

        Args:
            query: Search query string (album name, artist name, or track search query)
            type: Type of search (default=album) (album, artist, playlist, track)
            limit: Number of results to return (default=20)

        Returns:
            SpotifyAlbumSearchResponse with validated album models
        """
        try:
            logger.info(f"Searching {type} on Spotify for query: '{query}' (limit={limit})")

            async with aiohttp.ClientSession() as session:
                # Search Spotify using the specified type
                search_url = "https://api.spotify.com/v1/search"
                params = {"q": query, "type": type, "limit": str(limit)}

                data = await self._make_spotify_request(session, search_url, params)
                if not data:
                    return SpotifyAlbumSearchResponse(results=[], total_results=0, query=query)

                # Handle track searches by extracting album data from tracks
                if type == "track":
                    spotify_tracks = data.get("tracks", {}).get("items", [])
                    if not spotify_tracks:
                        logger.warning(f"No tracks found for query: '{query}'")
                        return SpotifyAlbumSearchResponse(results=[], total_results=0, query=query)

                    # Extract album data from each track's album field
                    albums = []
                    for track in spotify_tracks:
                        album_data = track.get("album")
                        if album_data:
                            albums.append(SpotifyAlbum.from_spotify_albumdata(album_data))
                else:
                    # Handle album searches (default)
                    spotify_albums = data.get("albums", {}).get("items", [])

                    if not spotify_albums:
                        logger.warning(f"No albums found for query: '{query}'")
                        return SpotifyAlbumSearchResponse(results=[], total_results=0, query=query)

                    # Process Spotify albums
                    albums = [
                        SpotifyAlbum.from_spotify_albumdata(album) for album in spotify_albums
                    ]

                # Results are already sorted by relevance from Spotify API

                return SpotifyAlbumSearchResponse(
                    results=albums,
                    total_results=len(albums),
                    query=query,
                )

        except aiohttp.ClientError as e:
            logger.error(f"Network error searching albums: {e}")
            return SpotifyAlbumSearchResponse(results=[], total_results=0, query=query)
        except Exception as e:
            logger.error(f"Unexpected error searching albums: {e}")
            return SpotifyAlbumSearchResponse(results=[], total_results=0, query=query)

    async def search_by_genre(self, genre: str, limit: int = 50) -> SpotifyArtistSearchResponse:
        """
        Search for artists by genre using Spotify API.

        Args:
            genre: Music genre to search for (e.g., "rock", "pop", "jazz")
            limit: Number of results to return (default=50)

        Returns:
            SpotifyArtistSearchResponse with validated artist models
        """
        try:
            logger.info(f"Searching Spotify for genre: '{genre}' (limit={limit})")

            async with aiohttp.ClientSession() as session:
                # Search Spotify for artists by genre
                search_url = "https://api.spotify.com/v1/search"
                params = {
                    "q": genre,
                    "type": "artist",
                    "limit": str(min(limit, 50)),  # Spotify max is 50 per request
                }

                data = await self._make_spotify_request(session, search_url, params)
                if not data:
                    return SpotifyArtistSearchResponse(results=[], total_results=0, query=genre)

                artists = data.get("artists", {}).get("items", [])

                if not artists:
                    logger.info(f"No artists found for genre: {genre}")
                    return SpotifyArtistSearchResponse(results=[], total_results=0, query=genre)

                artists = [SpotifyArtist.from_spotify_artistdata(artist) for artist in artists]

                # Sort by popularity (descending)
                artists.sort(key=lambda x: x.popularity, reverse=True)

                return SpotifyArtistSearchResponse(
                    results=artists, total_results=len(artists), query=genre
                )

        except Exception as e:
            logger.error(f"Error searching by genre '{genre}': {e}")
            return SpotifyArtistSearchResponse(results=[], total_results=0, query=genre)

    async def search_by_keyword(self, keyword: str, limit: int = 50) -> SpotifyMultiSearchResponse:
        """
        Search for artists, albums, and playlists by keyword using Spotify API.

        Args:
            keyword: Music keyword to search
            limit: Number of results to return per type (default=50)

        Returns:
            SpotifyMultiSearchResponse with validated mixed results
        """
        try:
            logger.info(f"Searching Spotify for keyword: '{keyword}' (limit={limit})")

            async with aiohttp.ClientSession() as session:
                # Search Spotify for multiple types
                search_url = "https://api.spotify.com/v1/search"
                params = {
                    "q": keyword,
                    "type": "artist,album,playlist",
                    "limit": str(min(limit, 20)),  # Spotify max is 50 per request
                }

                data = await self._make_spotify_request(session, search_url, params)
                if not data:
                    return SpotifyMultiSearchResponse(results=[], total_results=0, query=keyword)

                results: list[SpotifyArtist | SpotifyAlbum | SpotifyPlaylist] = []
                artists_count = 0
                albums_count = 0
                playlists_count = 0

                # Process artists
                for item in data.get("artists", {}).get("items", []):
                    if item:
                        results.append(SpotifyArtist.from_spotify_artistdata(item))
                        artists_count += 1

                # Process albums
                for item in data.get("albums", {}).get("items", []):
                    if item:
                        results.append(SpotifyAlbum.from_spotify_albumdata(item))
                        albums_count += 1

                # Process playlists
                for item in data.get("playlists", {}).get("items", []):
                    if item:
                        results.append(SpotifyPlaylist.from_spotify_playlistdata(item))
                        playlists_count += 1

                logger.info(f"Found {len(results)} results for keyword '{keyword}'")
                return SpotifyMultiSearchResponse(
                    results=results,
                    total_results=len(results),
                    artist_count=artists_count,
                    album_count=albums_count,
                    playlist_count=playlists_count,
                    query=keyword,
                )

        except Exception as e:
            logger.error(f"Error searching by keyword '{keyword}': {e}")
            return SpotifyMultiSearchResponse(results=[], total_results=0, query=keyword)

    async def get_artist(self, artist_id: str) -> SpotifyArtistSearchResponse:
        """
        Get an artist by ID using Spotify API.

        Args:
            artist_id: Spotify artist ID

        Returns:
            SpotifyArtistSearchResponse with artist information
        """
        try:
            async with aiohttp.ClientSession() as session:
                artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
                data = await self._make_spotify_request(session, artist_url)
                if not data:
                    return SpotifyArtistSearchResponse(
                        results=[],
                        total_results=0,
                        query=artist_id,
                        error="Failed to get artist",
                    )
                # Convert to SpotifyArtist model
                artist = SpotifyArtist.from_spotify_artistdata(data)
                # Get top track for enrichment
                top_track_response = await self.get_top_track(artist_id=artist_id)
                if top_track_response.results:
                    track = top_track_response.results[0]
                    artist.top_track_track = track.name
                    artist.known_for = track.name
                    if track.album:
                        artist.top_track_album = track.album
                    if track.release_date:
                        artist.top_track_release_date = track.release_date
                    if track.default_image:
                        artist.top_track_album_image = track.default_image
                return SpotifyArtistSearchResponse(
                    results=[artist],
                    total_results=1,
                    query=artist_id,
                )
        except Exception as e:
            logger.error(f"Error getting artist '{artist_id}': {e}")
            return SpotifyArtistSearchResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
            )

    async def get_top_track(self, artist_id: str) -> SpotifyTopTrackResponse:
        """
        Get top tracks for an artist using Spotify API.

        Args:
            artist_id: Spotify artist ID

        Returns:
            SpotifyTopTrackResponse with top track information
        """
        try:
            async with aiohttp.ClientSession() as session:
                top_tracks_url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
                data = await self._make_spotify_request(
                    session, top_tracks_url, params={"market": "US"}
                )
                if not data:
                    return SpotifyTopTrackResponse(
                        results=[],
                        total_results=0,
                        query=artist_id,
                        error="Failed to get top tracks",
                    )
                tracks = data.get("tracks", [])
                if not tracks:
                    logger.warning(f"No tracks found for artist: '{artist_id}'")
                    return SpotifyTopTrackResponse(
                        results=[],
                        total_results=0,
                        query=artist_id,
                        error="No tracks found",
                    )
                return SpotifyTopTrackResponse(
                    results=[SpotifyTrack.from_spotify_trackdata(tracks[0])],
                    total_results=len(tracks),
                    query=artist_id,
                )
        except Exception as e:
            logger.error(f"Error getting top tracks for artist: '{artist_id}': {e}")
            return SpotifyTopTrackResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
            )

    async def get_artist_albums(
        self, artist_id: str, limit: int = 30, include_groups: str = "album"
    ) -> SpotifyAlbumSearchResponse:
        """
        Get all albums by a specific artist using Spotify API.

        This method uses Spotify's dedicated artist albums endpoint which returns
        albums directly without needing title-based filtering.

        Args:
            artist_id: Spotify artist ID
            limit: Maximum number of albums to return (default=30, Spotify max per request is 50)
            include_groups: Album types to include (default="album")
                           Options: album, single, appears_on, compilation

        Returns:
            SpotifyAlbumSearchResponse with albums by this artist
        """
        try:
            logger.info(f"Getting albums for artist ID: '{artist_id}' (limit={limit})")

            async with aiohttp.ClientSession() as session:
                albums_url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
                params = {
                    "include_groups": include_groups,
                    "market": "US",
                    "limit": str(min(limit, 50)),  # Spotify max is 50 per request
                }

                data = await self._make_spotify_request(session, albums_url, params)
                if not data:
                    return SpotifyAlbumSearchResponse(
                        results=[],
                        total_results=0,
                        query=artist_id,
                        error="Failed to get artist albums",
                    )

                albums_data = data.get("items", [])
                if not albums_data:
                    logger.warning(f"No albums found for artist: '{artist_id}'")
                    return SpotifyAlbumSearchResponse(
                        results=[],
                        total_results=0,
                        query=artist_id,
                    )

                # Convert to SpotifyAlbum models
                albums = [SpotifyAlbum.from_spotify_albumdata(album) for album in albums_data]

                # Sort by release date (newest first)
                albums.sort(key=lambda x: x.release_date or "", reverse=True)

                logger.info(f"Found {len(albums)} albums for artist '{artist_id}'")
                return SpotifyAlbumSearchResponse(
                    results=albums,
                    total_results=len(albums),
                    query=artist_id,
                )

        except Exception as e:
            logger.error(f"Error getting albums for artist '{artist_id}': {e}")
            return SpotifyAlbumSearchResponse(
                results=[],
                total_results=0,
                query=artist_id,
                error=str(e),
            )

    async def search_artists(
        self, query: str, limit: int = 20, enrich_with_top_tracks: bool = True
    ) -> SpotifyArtistSearchResponse:
        """
        Search for artists by name using Spotify.

        Args:
            query: Search query string (artist name)
            limit: Number of results to return (default=20)
            enrich_with_top_tracks: If True, fetch top track for each artist (slower but richer data).
                                    Set to False for autocomplete/fast searches.

        Returns:
            SpotifyArtistSearchResponse with validated artist models including top track information
        """
        try:
            logger.info(f"Searching artists on Spotify for query: '{query}' (limit={limit}, enrich={enrich_with_top_tracks})")
            async with aiohttp.ClientSession() as session:
                search_url = "https://api.spotify.com/v1/search"
                params = {"q": query, "type": "artist", "limit": str(limit)}

                data = await self._make_spotify_request(session, search_url, params)
                if not data:
                    return SpotifyArtistSearchResponse(results=[], total_results=0, query=query)

                artists = data.get("artists", {}).get("items", [])
                if not artists:
                    logger.warning(f"No artists found for query: '{query}'")
                    return SpotifyArtistSearchResponse(results=[], total_results=0, query=query)

                # Fast path: skip top track enrichment for autocomplete
                if not enrich_with_top_tracks:
                    final_artists = [SpotifyArtist.from_spotify_artistdata(a) for a in artists]
                    return SpotifyArtistSearchResponse(
                        results=final_artists, total_results=len(final_artists), query=query
                    )

                # Slow path: Get top tracks for each artist to help identify
                top_tracks_tasks = [self.get_top_track(artist.get("id")) for artist in artists]
                top_tracks_results = await asyncio.gather(*top_tracks_tasks, return_exceptions=True)
                final_artists = []
                for artist, track_response in zip(artists, top_tracks_results, strict=False):
                    artist = SpotifyArtist.from_spotify_artistdata(artist)

                    if isinstance(track_response, Exception):
                        final_artists.append(artist)
                        continue

                    if (
                        isinstance(track_response, SpotifyTopTrackResponse)
                        and track_response.results
                    ):
                        track = track_response.results[0]
                        artist.known_for = track.name
                        artist.top_track_track = track.name
                        if track.default_image:
                            artist.top_track_album_image = track.default_image

                    final_artists.append(artist)

                return SpotifyArtistSearchResponse(
                    results=final_artists, total_results=len(final_artists), query=query
                )
        except aiohttp.ClientError as e:
            logger.error(f"Network error searching artists: {e}")
            return SpotifyArtistSearchResponse(results=[], total_results=0, query=query)
        except Exception as e:
            logger.error(f"Unexpected error searching artists: {e}")
            return SpotifyArtistSearchResponse(results=[], total_results=0, query=query)

    async def get_playlist_tracks(
        self, session: aiohttp.ClientSession, playlist_id: str
    ) -> list[dict]:
        """
        Fetch all tracks from a Spotify playlist.

        Args:
            session: aiohttp ClientSession
            playlist_id: Spotify playlist ID

        Returns:
            List of track dictionaries from the playlist
        """
        tracks = []
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        limit = 100  # Spotify max per request
        offset = 0

        while True:
            params = {"limit": limit, "offset": offset}
            data = await self._make_spotify_request(session, url, params)
            if not data:
                break

            items = data.get("items", [])
            if not items:
                break

            tracks.extend(items)
            offset += limit

            # Check if there are more pages
            if len(items) < limit:
                break

        return tracks

    async def get_album_details(self, session: aiohttp.ClientSession, album_id: str) -> dict:
        """
        Get detailed information for a Spotify album.

        Args:
            session: aiohttp ClientSession
            album_id: Spotify album ID

        Returns:
            Dictionary containing full album data from Spotify API
        """
        url = f"https://api.spotify.com/v1/albums/{album_id}"
        data = await self._make_spotify_request(session, url)
        return data if data else {}

    def _calculate_recency_score(self, release_date: str | None) -> float:
        """
        Calculate a recency score for an album based on its release date.
        Newer albums get higher scores to boost them in trending rankings.

        Args:
            release_date: Release date string (YYYY, YYYY-MM, or YYYY-MM-DD format)

        Returns:
            Recency score (higher = newer). Range: 0.0 (very old) to 100.0 (brand new)
        """
        if not release_date:
            return 0.0

        try:
            # Parse release date (Spotify formats: YYYY, YYYY-MM, or YYYY-MM-DD)
            parts = release_date.split("-")
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1

            release_dt = datetime(year, month, day, tzinfo=UTC)
            now = datetime.now(UTC)
            days_old = (now - release_dt).days

            # Calculate recency score
            # Albums released in last 7 days: 100-93 points
            # Albums released in last 30 days: 93-80 points
            # Albums released in last 90 days: 80-60 points
            # Albums released in last 365 days: 60-20 points
            # Older albums: 20-0 points (decay slowly)

            if days_old <= 7:
                # Brand new: 100 - (days * 1)
                return max(93.0, 100.0 - days_old)
            elif days_old <= 30:
                # Very recent: 93 - ((days - 7) * 0.57)
                return 93.0 - ((days_old - 7) * 0.57)
            elif days_old <= 90:
                # Recent: 80 - ((days - 30) * 0.33)
                return 80.0 - ((days_old - 30) * 0.33)
            elif days_old <= 365:
                # Within a year: 60 - ((days - 90) * 0.15)
                return max(20.0, 60.0 - ((days_old - 90) * 0.15))
            else:
                # Older: decay slowly, minimum 0
                years_old = days_old / 365.0
                return max(0.0, 20.0 - (years_old - 1) * 2.0)

        except (ValueError, IndexError) as e:
            logger.debug(f"Error parsing release date '{release_date}': {e}")
            return 0.0

    async def get_trending_albums(
        self, playlist_id: str = "37i9dQZEVXbMDoHDwVN2tF", limit: int = 50
    ) -> SpotifyAlbumSearchResponse:
        """
        Get trending albums based on playlist track frequency, popularity, and recency.

        Fetches tracks from a Spotify playlist (default: Top 50 Global), extracts unique
        albums, counts their frequency, and ranks them by frequency, recency (new releases
        boosted), and popularity. New releases are strongly weighted to increase volatility.

        Args:
            playlist_id: Spotify playlist ID (default: Top 50 Global)
            limit: Maximum number of albums to return (default: 50)

        Returns:
            SpotifyAlbumSearchResponse with ranked albums (new releases boosted)
        """
        try:
            logger.info(f"Fetching trending albums from playlist {playlist_id}")

            async with aiohttp.ClientSession() as session:
                # Fetch playlist tracks
                logger.info("Fetching playlist tracks...")
                tracks = await self.get_playlist_tracks(session, playlist_id)

                # Extract album IDs from tracks
                album_ids = [
                    t["track"]["album"]["id"]
                    for t in tracks
                    if t.get("track") and t["track"].get("album", {}).get("id")
                ]

                if not album_ids:
                    logger.warning("No album IDs found in playlist tracks")
                    return SpotifyAlbumSearchResponse(
                        results=[], total_results=0, query=f"playlist:{playlist_id}"
                    )

                # Count album frequency
                counts = Counter(album_ids)

                logger.info(f"Found {len(album_ids)} tracks across {len(counts)} unique albums.")

                # Get album details for all unique albums in parallel
                albums_data = await asyncio.gather(
                    *(self.get_album_details(session, album_id) for album_id in counts),
                    return_exceptions=True,
                )

                # Filter out errors and empty results, extract ranking info
                valid_albums = []
                for album_data in albums_data:
                    if isinstance(album_data, Exception):
                        logger.debug(f"Error fetching album: {album_data}")
                        continue
                    if isinstance(album_data, dict) and album_data.get("id"):
                        # Extract artist name for ranking
                        artists = album_data.get("artists", [])
                        artist_name = artists[0].get("name") if artists else "Unknown Artist"
                        release_date = album_data.get("release_date")

                        # Calculate recency score (boost for new releases)
                        recency_score = self._calculate_recency_score(release_date)

                        valid_albums.append(
                            {
                                "data": album_data,
                                "id": album_data.get("id"),
                                "popularity": album_data.get("popularity", 0),
                                "recency_score": recency_score,
                                "name": album_data.get("name"),
                                "artist": artist_name,
                                "release_date": release_date,
                            }
                        )

                # Rank by frequency, recency (new releases boosted), and popularity
                # Recency is weighted strongly to increase volatility
                ranked = sorted(
                    valid_albums,
                    key=lambda a: (
                        counts[a["id"]],  # Frequency first
                        a["recency_score"],  # Then recency (new releases boosted)
                        a["popularity"],  # Finally popularity
                    ),
                    reverse=True,
                )

                # Limit results
                ranked = ranked[:limit]

                logger.info(f"\n🎧  Top Albums Feed ({len(ranked)} albums)\n")
                for i, album in enumerate(ranked[:10], 1):  # Log top 10
                    logger.info(
                        f"{i:2d}. {album['name']} – {album['artist']} "
                        f"(freq: {counts[album['id']]}, recency: {album['recency_score']:.1f}, "
                        f"pop: {album['popularity']}, released: {album.get('release_date', 'N/A')})"
                    )

                # Convert to SpotifyAlbum models
                album_models = []
                for album in ranked:
                    album_model = SpotifyAlbum.from_spotify_albumdata(album["data"])
                    if not album_model.error:
                        album_models.append(album_model)

                return SpotifyAlbumSearchResponse(
                    results=album_models,
                    total_results=len(album_models),
                    query=f"playlist:{playlist_id}",
                )

        except Exception as e:
            logger.error(f"Error getting trending albums: {e}")
            return SpotifyAlbumSearchResponse(
                results=[],
                total_results=0,
                query=f"playlist:{playlist_id}",
                error=str(e),
            )


# Create Spotify search service instance
spotify_search_service = SpotifySearchService()
