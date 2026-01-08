"""
LastFM Search Service - All search operations for music discovery.
Extends enrichment service with trending, search, and discovery features.
"""

import asyncio
import math
from typing import Any
from urllib.parse import urlencode

import aiohttp

from api.lastfm.enrichment import LastFMEnrichmentService
from api.lastfm.models import (
    LastFMAlbumSearchResponse,
    LastFMArtistSearchResponse,
    LastFMMultiSearchResponse,
    LastFMTrendingAlbumsResponse,
    MCMusicAlbum,
    MCMusicArtist,
    MCMusicPlaylist,
)
from api.subapi.apple.wrapper import apple_wrapper
from api.subapi.spotify.wrappers import spotify_wrapper
from utils.get_logger import get_logger
from utils.normalize import normalize
from utils.redis_cache import RedisCache
from utils.soft_comparison import _levenshtein_distance, soft_compare

from .core import LastFMCache

logger = get_logger(__name__)


class LastFMSearchService(LastFMEnrichmentService):
    """
    Handles all search operations for music discovery.
    Extends enrichment service with trending, search, and discovery features.
    """

    def __init__(self):
        """Initialize LastFM search service."""
        super().__init__()
        self.apple_wrapper = apple_wrapper

    @RedisCache.use_cache(LastFMCache, prefix="spotify_top50")
    async def get_trending_albums(self, limit: int = 10) -> LastFMTrendingAlbumsResponse:
        """
        Get trending albums by extracting unique albums from Last.fm's top trending tracks.

        Process:
        1. Fetch Last.fm's chart.gettoptracks (current hot tracks)
        2. Extract unique albums from tracks
        3. Search Spotify for each album to get metadata
        4. Enrich with Odesli for all streaming platforms

        Args:
            limit: Number of albums to return (default=10)

        Returns:
            LastFMTrendingAlbumsResponse with validated album models
        """
        try:
            logger.info(f"Fetching trending albums from Last.fm top tracks (limit={limit})")

            # Fetch Last.fm's top tracks chart using core request method
            params = {
                "method": "chart.gettoptracks",
                "limit": "50",  # Fetch more to ensure we get enough unique albums
            }

            data, status_code = await self._make_request(params=params)
            if status_code != 200 or not data:
                logger.error(f"Last.fm API returned status {status_code}")
                return LastFMTrendingAlbumsResponse(results=[], total_results=0)

            tracks = data.get("tracks", {}).get("track", [])

            if not tracks:
                logger.warning("No tracks found in Last.fm chart")
                return LastFMTrendingAlbumsResponse(results=[], total_results=0)

            # Create Spotify search tasks for ALL tracks in parallel
            # Token is generated internally by _search_spotify_track
            track_data_list = []
            for track in tracks:
                artist_name = track.get("artist", {}).get("name", "")
                track_name = track.get("name", "")
                if artist_name and track_name:
                    track_data_list.append((track, track_name, artist_name))

            # Search Spotify for all tracks concurrently
            spotify_search_tasks = [
                self._search_spotify_track(track_name, artist_name)
                for _, track_name, artist_name in track_data_list
            ]

            logger.info(f"Searching Spotify for {len(spotify_search_tasks)} tracks in parallel...")
            spotify_results = await asyncio.gather(*spotify_search_tasks, return_exceptions=True)

            # Extract unique albums from Spotify results
            seen_albums = set()
            albums = []

            for (track, track_name, artist_name), spotify_data in zip(
                track_data_list, spotify_results, strict=False
            ):
                # Skip failed searches
                if (
                    isinstance(spotify_data, Exception)
                    or not spotify_data
                    or not isinstance(spotify_data, dict)
                ):
                    continue

                if not spotify_data.get("spotify_url"):
                    continue

                # Deduplicate by Spotify URL
                album_url = spotify_data["spotify_url"]
                if album_url in seen_albums:
                    continue

                seen_albums.add(album_url)

                # Extract Spotify album ID from URL for unique identification
                # URL format: https://open.spotify.com/album/{album_id}
                spotify_album_id = (
                    album_url.split("/album/")[-1].split("?")[0] if "/album/" in album_url else ""
                )

                # Create album object with Spotify album data
                # Must include 'id' field for MCBaseItem mc_id generation
                album = {
                    "id": spotify_album_id,  # Required for MCBaseItem
                    "title": spotify_data.get("album_name", track_name),
                    "artist": artist_name,
                    "listeners": int(track.get("listeners", 0)),
                    "playcount": int(track.get("playcount", 0)),
                    "image": spotify_data.get("image") or track.get("image", [{}])[-1].get("#text"),
                    "url": spotify_data.get("spotify_url", ""),
                    "mbid": spotify_album_id,  # Use Spotify album ID as mbid for unique mc_id generation
                    "artist_url": track.get("artist", {}).get("url"),
                    "streamable": True,
                    "spotify_url": spotify_data.get("spotify_url"),
                    "release_date": spotify_data.get("release_date"),
                    "release_date_precision": spotify_data.get("release_date_precision"),
                    "total_tracks": spotify_data.get("total_tracks"),
                    "album_type": spotify_data.get("album_type"),
                    "popularity": spotify_data.get("popularity"),
                    "apple_music_url": None,
                    "youtube_music_url": None,
                    "source_id": spotify_album_id,  # Required for MCBaseItem
                }

                albums.append(album)

                if len(albums) >= limit:
                    break

            if not albums:
                logger.warning("No unique albums extracted from top tracks")
                return LastFMTrendingAlbumsResponse(results=[], total_results=0)

            logger.info(f"Extracted {len(albums)} unique albums from Spotify track searches")

            # Enrich with Odesli for cross-platform links
            enrichment_tasks = [
                self.apple_wrapper.search_album(album.get("title", track_name)) for album in albums
            ]

            enrichment_results = await asyncio.gather(*enrichment_tasks, return_exceptions=True)

            # Apply Odesli enrichment
            for album, result in zip(albums, enrichment_results, strict=False):
                if isinstance(result, Exception):
                    logger.debug(f"apple enrichment failed: {result}")
                    continue
                if result and isinstance(result, list):
                    album["apple_music_url"] = result[0].deeplink
                    album["youtube_music_url_ios"] = result[0].youtube_ios_link
                    album["youtube_music_url_android"] = result[0].youtube_android_link
                    album["youtube_music_url_web"] = result[0].youtube_web_fallback

            # Convert to Pydantic models
            validated_albums = [MCMusicAlbum.model_validate(album) for album in albums]

            logger.info(
                f"Successfully fetched {len(validated_albums)} trending albums from top tracks"
            )
            return LastFMTrendingAlbumsResponse(
                results=validated_albums, total_results=len(validated_albums)
            )

        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching trending albums: {e}")
            return LastFMTrendingAlbumsResponse(results=[], total_results=0)
        except Exception as e:
            logger.error(f"Unexpected error fetching trending albums: {e}")
            return LastFMTrendingAlbumsResponse(results=[], total_results=0)

    @RedisCache.use_cache(LastFMCache, prefix="search_albums")
    async def _search_albums_impl(self, query: str, limit: int = 20) -> LastFMAlbumSearchResponse:
        """
        Internal implementation of search_albums (cached).
        Query should already be normalized when calling this method.
        This is the actual cached function - cache key is based on normalized query.
        """
        # Query is already normalized when this is called
        normalized_query = query
        try:
            logger.info(f"Searching albums for query: '{query}' (limit={limit})")
            max_distance_threshold = max(3, int(len(query) * 0.3))
            if len(query) <= 3:
                max_distance_threshold = 0

            # Use Spotify wrapper to search albums
            spotify_response = await spotify_wrapper.search_albums(
                query=normalized_query, limit=limit
            )

            if spotify_response.status_code != 200 or spotify_response.error:
                logger.warning(f"No albums found for query: '{normalized_query}'")
                return LastFMAlbumSearchResponse(
                    results=[], total_results=0, query=normalized_query
                )

            # Extract albums from wrapper response
            spotify_albums_raw = spotify_response.results
            if not spotify_albums_raw:
                logger.warning(f"No albums found for query: '{normalized_query}'")
                return LastFMAlbumSearchResponse(
                    results=[], total_results=0, query=normalized_query
                )

            # Convert SpotifyAlbum models to dicts for processing
            spotify_albums = []
            for album in spotify_albums_raw:
                if isinstance(album, dict):
                    spotify_albums.append(album)
                else:
                    # If it's a model instance, convert to dict
                    spotify_albums.append(
                        album.model_dump() if hasattr(album, "model_dump") else album
                    )

            if not spotify_albums:
                logger.warning(f"No albums found for query: '{normalized_query}'")
                return LastFMAlbumSearchResponse(
                    results=[], total_results=0, query=normalized_query
                )

            # Process Spotify albums with deduplication
            # Preserve Spotify's relevance ordering - don't sort
            # Use album IDs for deduplication to ensure deterministic behavior
            # even if Spotify returns results in different orders
            seen_album_ids = set()
            albums: list[dict[str, Any]] = []
            filters_albums: list[tuple[dict[str, Any], int]] = []
            logger.info(f"Processing {len(spotify_albums)} Spotify albums")

            for spotify_album in spotify_albums:
                # Get Spotify album ID directly from the album object
                spotify_album_id = spotify_album.get("id", "")
                spotify_album_name = spotify_album.get("title") or spotify_album.get("name", "")
                album_url = spotify_album.get("spotify_url", "")

                # Skip if no ID or name
                if not spotify_album_id or not spotify_album_name:
                    # logger.warning(
                    #     f"Skipping album without ID or name: {spotify_album.get('name')}"
                    # )
                    continue

                # Skip duplicates based on album ID (more reliable than name)
                if spotify_album_id in seen_album_ids:
                    # logger.info(f"DUPLICATE SKIPPED: {spotify_album_name} (ID: {spotify_album_id})")
                    continue

                seen_album_ids.add(spotify_album_id)
                # logger.info(f"Added album: {spotify_album_name} (ID: {spotify_album_id})")

                # Get artist info (already processed by wrapper)
                # Handle None explicitly - .get() with default only works if key doesn't exist
                artist_name_raw = spotify_album.get("artist")
                artist_name = artist_name_raw if artist_name_raw is not None else "Unknown"
                has_artist = artist_name_raw is not None

                # Get best quality image - prefer default_image, fallback to last image in images array
                image_url = spotify_album.get("default_image")
                if not image_url:
                    # Fallback to first image in images array (largest in Spotify's order)
                    images_list = spotify_album.get("images", [])
                    if isinstance(images_list, list) and len(images_list) > 0:
                        image_obj = images_list[0]
                        if isinstance(image_obj, dict):
                            image_url = image_obj.get("url")
                        else:
                            # If it's an MCImage object, access url attribute
                            image_url = getattr(image_obj, "url", None)

                # Skip albums without images (critical - prevents gray boxes)
                if not image_url:
                    # logger.debug(f"Skipping album {spotify_album_name} - no image")
                    continue

                # Create album object (use data from wrapper response)
                # Must include 'id' field for MCBaseItem mc_id generation
                album = {
                    "id": spotify_album_id,  # Required for MCBaseItem
                    "title": spotify_album_name,
                    "artist": artist_name,
                    "listeners": 0,
                    "playcount": 0,
                    "image": image_url,
                    "url": album_url,
                    "mbid": spotify_album_id,  # Spotify album ID for unique mc_id
                    "artist_url": None,
                    "streamable": True,
                    "spotify_url": album_url,
                    "release_date": spotify_album.get("release_date"),
                    "release_date_precision": spotify_album.get("release_date_precision"),
                    "total_tracks": spotify_album.get("total_tracks"),
                    "album_type": spotify_album.get("album_type"),
                    "popularity": spotify_album.get("popularity"),
                    "apple_music_url": None,
                    "youtube_music_url": None,
                    "source_id": spotify_album_id,
                }

                # Add MediaCircle standardized fields
                # If artist is missing, only check title match (trust Spotify's ranking)
                album_title = album.get("title", "")
                title_distance = self._levenshtein_distance(
                    normalize(album_title), normalized_query
                )

                if has_artist:
                    # If artist is present, check both artist and title
                    distance = self._levenshtein_distance(normalize(artist_name), normalized_query)
                    matches = (
                        title_distance <= max_distance_threshold
                        or distance <= max_distance_threshold
                    )
                else:
                    # If artist is missing, only check title (Spotify already ranked it)
                    matches = title_distance <= max_distance_threshold

                if matches:
                    albums.append(album)
                else:
                    # Store albums that don't match but came from Spotify
                    # Track the minimum distance for fallback evaluation
                    min_distance = title_distance
                    if has_artist:
                        min_distance = min(title_distance, distance)
                    # Store with distance for fallback evaluation
                    filters_albums.append((album, min_distance))

            if not albums:
                if filters_albums:
                    # Only use fallback if at least one album has a reasonable distance
                    # Maximum fallback threshold: 50% of query length (stricter than match threshold)
                    max_fallback_threshold = max(5, int(len(normalized_query) * 0.5))
                    relevant_fallback: list[dict[str, Any]] = [
                        album_dict
                        for album_dict, album_distance in filters_albums
                        if album_distance <= max_fallback_threshold
                    ]
                    if relevant_fallback:
                        albums = relevant_fallback
                        logger.info(
                            f"No strict matches found, using {len(relevant_fallback)} Spotify-ranked albums"
                        )
                    else:
                        logger.warning(
                            f"No matches found (all filtered albums exceed fallback threshold of {max_fallback_threshold})"
                        )
                        return LastFMAlbumSearchResponse(
                            results=[], total_results=0, query=normalized_query
                        )
                else:
                    logger.warning("No matches based upon artist or title")
                    return LastFMAlbumSearchResponse(
                        results=[], total_results=0, query=normalized_query
                    )

            # Apply weighted sorting with heavy emphasis on exact title matches
            try:
                query_normalized = normalized_query.lower().strip() if normalized_query else ""

                # Helper function to normalize title (remove leading articles)
                def normalize_title(text: str) -> str:
                    if not text:
                        return ""
                    text_lower = text.lower().strip()
                    for article in ["the ", "a ", "an "]:
                        if text_lower.startswith(article):
                            return text_lower[len(article) :].strip()
                    return text_lower

                def calculate_weighted_score(album: dict[str, Any]) -> float:
                    """Calculate weighted score combining title similarity, popularity, and recency."""
                    try:
                        # Title similarity using Levenshtein edit distance
                        title_similarity = 0.0
                        if query_normalized:
                            album_title = album.get("title", "") or album.get("name", "")
                            if album_title:
                                title_normalized = normalize_title(album_title)
                                query_norm = normalize_title(query_normalized)

                                if query_norm and title_normalized:
                                    # Calculate Levenshtein distance
                                    distance = _levenshtein_distance(query_norm, title_normalized)

                                    # Convert distance to similarity score (0.0 to 1.0)
                                    # Lower distance = higher similarity
                                    max_len = max(len(query_norm), len(title_normalized))
                                    if max_len > 0:
                                        # Normalize: 0 distance = 1.0 similarity, max distance = 0.0 similarity
                                        title_similarity = 1.0 - (distance / max_len)
                                    else:
                                        title_similarity = 0.0

                                    # HEAVY boost for exact matches
                                    if query_norm == title_normalized:
                                        title_similarity = 1.0
                                    # Boost for substring matches (query in title or title in query)
                                    elif (
                                        query_norm in title_normalized
                                        or title_normalized in query_norm
                                    ):
                                        title_similarity = max(title_similarity, 0.9)

                                # Also check artist name for matches
                                artist_name = album.get("artist", "")
                                if artist_name and query_norm:
                                    artist_normalized = normalize_title(artist_name)
                                    if query_norm == artist_normalized:
                                        # Exact artist match gets high score too
                                        title_similarity = max(title_similarity, 0.95)
                                    elif (
                                        query_norm in artist_normalized
                                        or artist_normalized in query_norm
                                    ):
                                        title_similarity = max(title_similarity, 0.85)

                        # Popularity weight (0.0 to 1.0) - normalize Spotify popularity (0-100)
                        popularity = float(album.get("popularity", 0) or 0)
                        popularity_weight = min(1.0, popularity / 100.0)

                        # Recency weight based on release_date
                        recency_weight = 0.5  # Default neutral weight
                        release_date_str = album.get("release_date", "")
                        if release_date_str:
                            try:
                                # Parse release date (format varies: YYYY-MM-DD, YYYY-MM, or YYYY)
                                from datetime import UTC, datetime

                                if len(release_date_str) == 4:
                                    # Year only
                                    release_year = int(release_date_str)
                                    today = datetime.now(UTC).date()
                                    years_old = today.year - release_year
                                    # Use exponential decay with 10-year half-life
                                    recency_weight = max(0.0, math.exp(-years_old / 10.0))
                                elif len(release_date_str) >= 7:
                                    # Has month (YYYY-MM or YYYY-MM-DD)
                                    release_date = datetime.strptime(
                                        release_date_str[:7], "%Y-%m"
                                    ).date()
                                    today = datetime.now(UTC).date()
                                    days_since_release = (today - release_date).days
                                    # Use exponential decay with 2-year half-life (730 days)
                                    recency_weight = math.exp(-days_since_release / 730.0)
                            except (ValueError, TypeError, AttributeError) as e:
                                logger.debug(
                                    f"Could not parse release_date '{release_date_str}': {e}"
                                )
                                recency_weight = 0.5

                        # Combined weight: 60% title similarity (heavily weighted), 30% popularity, 10% recency
                        weighted_score = (
                            (title_similarity * 0.6)
                            + (popularity_weight * 0.3)
                            + (recency_weight * 0.1)
                        )

                        return weighted_score
                    except Exception as e:
                        logger.warning(
                            f"Error calculating weighted score for album {album.get('id')}: {e}"
                        )
                        return 0.0  # Default sort to end

                # Sort by weighted score (descending)
                # Calculate scores for all items first for debugging
                scored_albums = [(album, calculate_weighted_score(album)) for album in albums]
                # Log top 5 scores for debugging
                scored_albums.sort(key=lambda x: x[1], reverse=True)
                top_5 = scored_albums[:5]
                logger.info(
                    f"Top 5 weighted scores for query '{normalized_query}': "
                    + ", ".join(
                        [f"{album.get('title', '')[:30]}: {score:.3f}" for album, score in top_5]
                    )
                )
                # Sort the actual results
                albums.sort(key=calculate_weighted_score, reverse=True)
                logger.info(
                    f"Sorted {len(albums)} albums using weighted sort "
                    f"(60% title similarity, 30% popularity, 10% recency) for query '{normalized_query}'"
                )
            except Exception as e:
                logger.error(f"Error sorting album results: {e}")
                # Fall back to release date sorting if weighted sort fails
                albums.sort(key=lambda x: x.get("release_date", ""), reverse=True)
                logger.info(f"Fell back to release date sorting for {len(albums)} albums")

            # Enrich with Apple Music metadata (in parallel)
            # Search using both artist and album title for better accuracy
            enrichment_tasks = [
                self.apple_wrapper.search_album(
                    f"{album.get('artist', '')} {album.get('title', '')}".strip()
                )
                for album in albums
                if album.get("title")
            ]

            enrichment_results = await asyncio.gather(*enrichment_tasks, return_exceptions=True)

            # Apply Apple Music enrichment
            for album, result in zip(albums, enrichment_results, strict=False):
                if isinstance(result, Exception):
                    logger.debug(f"apple enrichment failed: {result}")
                    continue
                if result and isinstance(result, list):
                    album["apple_music_url"] = result[0].deeplink
                    album["youtube_music_url"] = result[0].youtube_web_fallback
                    album["youtube_music_url_ios"] = result[0].youtube_ios_link
                    album["youtube_music_url_android"] = result[0].youtube_android_link

            # Convert to Pydantic models
            validated_albums = [MCMusicAlbum.model_validate(album) for album in albums]

            logger.info(
                f"Successfully found {len(validated_albums)} albums for query: '{normalized_query}'"
            )
            return LastFMAlbumSearchResponse(
                results=validated_albums,
                total_results=len(validated_albums),
                query=normalized_query,
            )

        except aiohttp.ClientError as e:
            logger.error(f"Network error searching albums: {e}")
            return LastFMAlbumSearchResponse(results=[], total_results=0, query=normalized_query)
        except Exception as e:
            logger.error(f"Unexpected error searching albums: {e}")
            return LastFMAlbumSearchResponse(results=[], total_results=0, query=normalized_query)

    async def search_albums(self, query: str, limit: int = 20) -> LastFMAlbumSearchResponse:
        """
        Search for albums by name or artist using Spotify.

        Args:
            query: Search query string (album name or artist name)
            limit: Number of results to return (default=20)

        Returns:
            LastFMAlbumSearchResponse with validated album models

        Note:
            - Uses Spotify album search for comprehensive results
            - Enriched with Odesli for all streaming platforms
            - Returns albums with full metadata
            - Cache key is based on normalized query for consistency
            - Query is normalized before cache lookup to ensure case-insensitive caching
            - This wrapper normalizes the query, then calls the cached implementation
        """
        # Normalize query BEFORE cache lookup to ensure consistent cache keys
        # This ensures "The Beatles", "the beatles", and "THE BEATLES" all use the same cache
        # The cache decorator on _search_albums_impl will use the normalized query for the key
        normalized_query = normalize(query)
        result: LastFMAlbumSearchResponse = await self._search_albums_impl(normalized_query, limit)
        return result

    @RedisCache.use_cache(LastFMCache, prefix="search_by_genre")
    async def search_by_genre(self, genre: str, limit: int = 50) -> LastFMArtistSearchResponse:
        """
        Search for artists by genre using Spotify API.

        Args:
            genre: Music genre to search for (e.g., "rock", "pop", "jazz", "alternative rock")
            limit: Number of results to return (default=50)

        Returns:
            LastFMArtistSearchResponse with validated artist models

        Note:
            - Uses Spotify wrapper for genre search
            - Returns artists that match the specified genre query
        """
        try:
            logger.info(f"Searching for genre: '{genre}' (limit={limit})")

            # Use Spotify wrapper to search by genre
            spotify_response = await spotify_wrapper.search_by_genre(
                genre=genre, limit=min(limit, 50)
            )

            if spotify_response.status_code != 200 or spotify_response.error:
                logger.info(f"No artists found for genre: {genre}")
                return LastFMArtistSearchResponse(results=[], total_results=0, query=genre)

            # Extract artists from wrapper response
            artists_raw = spotify_response.results
            if not artists_raw:
                logger.info(f"No artists found for genre: {genre}")
                return LastFMArtistSearchResponse(results=[], total_results=0, query=genre)

            # Convert SpotifyArtist models to dicts for processing
            artists = []
            for artist in artists_raw:
                if isinstance(artist, dict):
                    artists.append(artist)
                else:
                    # If it's a model instance, convert to dict
                    artists.append(artist.model_dump() if hasattr(artist, "model_dump") else artist)

            # Format artist results to match app's expected structure
            results = []
            for artist in artists:
                # Use processed data from wrapper
                artist_data = {
                    # Core fields
                    "id": artist.get("id"),
                    "name": artist.get("name"),
                    # Spotify specific
                    "spotify_url": artist.get("spotify_url"),
                    "popularity": artist.get("popularity", 0),
                    "followers": artist.get("followers", 0),
                    "genres": artist.get("genres", []),
                    # Image
                    "image": artist.get("image"),
                    "raw_images": artist.get("raw_images", []),  # Raw image data from music source
                    # For compatibility with existing music display
                    "artist": artist.get("artist") or artist.get("name"),
                    "title": artist.get("title") or artist.get("name"),
                    # MediaCircle standardized fields
                    "source_id": artist.get("source_id")
                    or artist.get("id"),  # Required for MCBaseItem
                }
                results.append(artist_data)

            # Convert to Pydantic models
            validated_artists = [MCMusicArtist.model_validate(artist) for artist in results]

            logger.info(f"Found {len(validated_artists)} artists for genre '{genre}'")
            return LastFMArtistSearchResponse(
                results=validated_artists, total_results=len(validated_artists), query=genre
            )

        except Exception as e:
            logger.error(f"Error searching by genre '{genre}': {e}")
            return LastFMArtistSearchResponse(results=[], total_results=0, query=genre)

    @RedisCache.use_cache(LastFMCache, prefix="search_by_keyword")
    async def search_by_keyword(self, keyword: str, limit: int = 50) -> LastFMMultiSearchResponse:
        """
        Search for artists, albums, and playlists by keyword using Spotify API.

        Args:
            keyword: Music keyword to search
            limit: Number of results to return per type (default=50)

        Returns:
            LastFMMultiSearchResponse with validated mixed results

        Note:
            - Uses Spotify wrapper for keyword search
            - Returns mixed results that match the specified keyword
        """
        try:
            logger.info(f"Searching for keyword: '{keyword}' (limit={limit})")

            # Use Spotify wrapper to search by keyword
            spotify_response = await spotify_wrapper.search_by_keyword(
                keyword=keyword, limit=min(limit, 20)
            )

            if spotify_response.status_code != 200 or spotify_response.error:
                return LastFMMultiSearchResponse(results=[], total_results=0, query=keyword)

            # Extract results from wrapper response
            results_raw = spotify_response.results
            if not results_raw:
                return LastFMMultiSearchResponse(results=[], total_results=0, query=keyword)

            # Convert Spotify models to dicts and separate by type
            artists = []
            albums = []
            playlists = []

            for item in results_raw:
                if isinstance(item, dict):
                    item_dict = item
                else:
                    # If it's a model instance, convert to dict
                    item_dict = item.model_dump() if hasattr(item, "model_dump") else item

                mc_type = item_dict.get("mc_type", "")
                mc_subtype = item_dict.get("mc_subtype", "")
                # Music artists are now MCType.PERSON with MCSubType.MUSIC_ARTIST
                if mc_type == "person" and mc_subtype == "music_artist":
                    artists.append(item_dict)
                elif mc_type == "music_album":
                    albums.append(item_dict)
                elif mc_type == "music_playlist":
                    playlists.append(item_dict)

            # Ensure albums have required fields before validation
            # MCMusicAlbum requires artist: str, not str | None
            # Also ensure image field is set from default_image or images array
            for album in albums:
                if album.get("artist") is None:
                    album["artist"] = "Unknown"
                # Set image field if not present (from default_image or images array)
                if not album.get("image"):
                    image_url = album.get("default_image")
                    if not image_url:
                        # Fallback to first image in images array (largest in Spotify's order)
                        images_list = album.get("images", [])
                        if isinstance(images_list, list) and len(images_list) > 0:
                            image_obj = images_list[0]
                            if isinstance(image_obj, dict):
                                image_url = image_obj.get("url")
                            else:
                                # If it's an MCImage object, access url attribute
                                image_url = getattr(image_obj, "url", None)
                    if image_url:
                        album["image"] = image_url

            # Convert to Pydantic models
            validated_artists = [MCMusicArtist.model_validate(a) for a in artists]
            validated_albums = [MCMusicAlbum.model_validate(a) for a in albums]
            validated_playlists = [MCMusicPlaylist.model_validate(p) for p in playlists]

            # Combine results
            results = validated_artists + validated_albums + validated_playlists

            logger.info(f"Found {len(results)} results for keyword '{keyword}'")
            return LastFMMultiSearchResponse(
                results=results,
                total_results=len(results),
                artist_count=len(validated_artists),
                album_count=len(validated_albums),
                playlist_count=len(validated_playlists),
                query=keyword,
            )

        except Exception as e:
            logger.error(f"Error searching by keyword '{keyword}': {e}")
            return LastFMMultiSearchResponse(results=[], total_results=0, query=keyword)

    @RedisCache.use_cache(LastFMCache, prefix="get_top_tracks")
    async def get_top_track(self, session: aiohttp.ClientSession, artist_id: str) -> dict[str, Any]:
        """
        Get top tracks for an artist using Spotify wrapper.

        Args:
            session: aiohttp ClientSession (kept for compatibility, not used)
            artist_id: Spotify artist ID

        Returns:
            Dictionary with top track information
        """
        try:
            logger.info(f"Getting top tracks for artist: '{artist_id}'")
            # Use Spotify wrapper to get top track
            spotify_response = await spotify_wrapper.get_top_track(artist_id=artist_id)

            if spotify_response.status_code != 200 or spotify_response.error:
                return {}

            # The wrapper returns SpotifyTopTrackResponse directly
            results = spotify_response.results
            if results:
                # Return first track as dict with expected fields
                track_dict = results[0].model_dump()
                # Add "track" field as alias for "name" (used by known_for field at line 716)
                if "name" in track_dict:
                    track_dict["track"] = track_dict["name"]
                return {
                    "tracks": [track_dict],
                    "total": len(results),
                }
            return {}
        except Exception as e:
            logger.error(f"Error getting top tracks for artist: '{artist_id}': {e}")
            return {}

    @RedisCache.use_cache(LastFMCache, prefix="search_artists")
    async def search_spotify_artist(
        self, query: str, limit: int = 20
    ) -> LastFMArtistSearchResponse:
        """
        Search for artists by name using Spotify.

        Args:
            query: Search query string (artist name)
            limit: Number of results to return (default=20)

        Returns:
            LastFMArtistSearchResponse with validated artist models including top track information
        """
        try:
            logger.info(f"Searching artists for query: '{query}' (limit={limit})")

            # Use Spotify wrapper to search artists
            spotify_response = await spotify_wrapper.search_artists(query=query, limit=limit)

            if spotify_response.status_code != 200 or spotify_response.error:
                logger.warning(f"No artists found for query: '{query}'")
                return LastFMArtistSearchResponse(results=[], total_results=0, query=query)

            # Extract artists from wrapper response
            artists_raw = spotify_response.results
            if not artists_raw:
                logger.warning(f"No artists found for query: '{query}'")
                return LastFMArtistSearchResponse(results=[], total_results=0, query=query)

            # Filter artists using soft_compare to ensure they match the query
            # This prevents unrelated results (e.g., "U2 Tribute Band" when searching for "U2")
            # For short queries (<=3 chars), soft_compare requires exact matches
            # Also filter out low-popularity artists (popularity < 10) to remove obscure/tribute bands
            filtered_artists = []
            for artist in artists_raw:
                artist_name = artist.name if hasattr(artist, "name") else ""
                artist_popularity = getattr(artist, "popularity", 0)

                if artist_name:
                    names_match, _ = soft_compare(query, artist_name)
                    if names_match:
                        # Additional filter: remove artists with very low popularity (< 10)
                        if artist_popularity < 10:
                            logger.debug(
                                f"Filtered out artist '{artist_name}' - popularity too low ({artist_popularity})"
                            )
                        else:
                            filtered_artists.append(artist)
                    else:
                        logger.debug(
                            f"Filtered out artist '{artist_name}' - does not match query '{query}'"
                        )

            if not filtered_artists:
                logger.warning(f"No artists matched query '{query}' after soft_compare filtering")
                return LastFMArtistSearchResponse(results=[], total_results=0, query=query)

            # Convert SpotifyArtist models to dicts for processing
            # Note: Spotify search already enriches artists with top track data,
            # so we can use that directly without making redundant API calls
            final_artists = []
            for artist in filtered_artists:
                artist_dict = artist.model_dump()

                # Set content_type and media_type if top track data exists
                if artist_dict.get("top_track_track") or artist_dict.get("known_for"):
                    artist_dict["content_type"] = "musician"
                    artist_dict["media_type"] = "person"
                    artist_dict["known_for_department"] = "Music"
                    # Ensure known_for is set if top_track_track exists
                    if not artist_dict.get("known_for") and artist_dict.get("top_track_track"):
                        artist_dict["known_for"] = artist_dict.get("top_track_track")

                # Format artist data (use processed data from wrapper)
                artist_data = {
                    "id": artist_dict.get("id"),
                    "name": artist_dict.get("name"),
                    "spotify_url": artist_dict.get("spotify_url"),
                    "popularity": artist_dict.get("popularity", 0),
                    "followers": artist_dict.get("followers", 0),
                    "genres": artist_dict.get("genres", []),
                    "image": artist_dict.get("image"),
                    "raw_images": artist_dict.get(
                        "raw_images", []
                    ),  # Raw image data from music source
                    "artist": artist_dict.get("artist") or artist_dict.get("name"),
                    "title": artist_dict.get("title") or artist_dict.get("name"),
                    "top_track_album": artist_dict.get("top_track_album"),
                    "top_track_release_date": artist_dict.get("top_track_release_date"),
                    "top_track_album_image": artist_dict.get("top_track_album_image"),
                    "top_track_track": artist_dict.get("top_track_track"),
                    "content_type": artist_dict.get("content_type"),
                    "media_type": artist_dict.get("media_type"),
                    "known_for": artist_dict.get("known_for"),
                    "known_for_department": artist_dict.get("known_for_department"),
                    "source_id": artist_dict.get("source_id"),
                    "source": artist_dict.get("source"),
                }
                final_artists.append(artist_data)

            # Convert to Pydantic models
            validated_artists = [MCMusicArtist.model_validate(a) for a in final_artists]

            return LastFMArtistSearchResponse(
                results=validated_artists, total_results=len(validated_artists), query=query
            )
        except aiohttp.ClientError as e:
            logger.error(f"Network error searching artists: {e}")
            return LastFMArtistSearchResponse(results=[], total_results=0, query=query)
        except Exception as e:
            logger.error(f"Unexpected error searching artists: {e}")
            return LastFMArtistSearchResponse(results=[], total_results=0, query=query)

    @RedisCache.use_cache(LastFMCache, prefix="search_mb_artist")
    async def _search_mb_artist(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        """
        Search for music artists by name.

        Query Parameters:
        - query: Search query string (artist name) (required)
        - limit: Number of results to return (1-50, default: 20)
        """
        # URL-encode the query string to safely include user text in API endpoint
        query_encoded = urlencode({"artist": query})
        endpoint = f"artist/?{query_encoded}&fmt=json"

        data, status_code = await self._mb_make_request(endpoint=endpoint)
        if status_code != 200 or not data:
            logger.error(f"Error searching music artists: status_code={status_code}")
            return []

        artists = data.get("artists", [])

        # Return as dict to match return type
        if isinstance(artists, list):
            return artists
        return []


# Create LastFM handler instance
lastfm_search_service = LastFMSearchService()
