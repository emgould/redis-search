"""
LastFM Music Firebase Functions Handlers
Handles trending music albums and other music-related functionality.
"""

import json
import logging
from datetime import datetime

from firebase_functions import https_fn

# Import the LastFM wrapper
from api.lastfm.wrappers import lastfm_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class LastFMHandler:
    """Class containing all LastFM Music-focused Firebase Functions."""

    def __init__(self):
        """Initialize LastFM handler. Uses wrapper instance for all API calls."""
        logger.info("LastFMHandler initialized - using lastfm_wrapper")

    async def get_trending_music_albums(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending music albums from Last.fm with streaming service links.

        Query Parameters:
        - limit: Number of albums to return (1-50, default: 10)

        Returns:
            JSON response with trending albums data including:
            - Album details (title, artist, image, playcount, listeners)
            - MediaCircle standardized fields (mc_id, mc_type)
            - Streaming service links (Spotify, Apple Music, YouTube Music, etc.) via Spotify API + Odesli
            - Metadata (generated timestamp, source, count)
        """
        try:
            # Parse query parameters
            limit = int(req.args.get("limit", 10))

            # Validate parameters
            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Getting trending music albums (limit={limit})")

            # Call wrapper - returns MCBaseItem derivative directly
            result = await lastfm_wrapper.get_trending_albums(limit=limit)

            # Check for errors
            if result.status_code != 200 or result.error:
                error_msg = result.error or "Unknown error"
                logger.error(f"Failed to fetch trending albums: {error_msg}")
                return https_fn.Response(
                    json.dumps({"error": "Failed to fetch trending albums", "message": error_msg}),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Extract albums from response - convert to dicts for JSON serialization
            albums_dict = [album.model_dump() for album in result.results]

            # Check for empty results
            if not albums_dict:
                logger.warning("No albums returned from Last.fm")
                return https_fn.Response(
                    json.dumps(
                        {
                            "items": [],
                            "count": 0,
                            "metadata": {
                                "generated_at": datetime.now().isoformat(),
                                "data_source": "Last.fm Chart API (Cached - 24hr TTL)",
                                "limit": limit,
                                "message": "No albums available",
                            },
                        }
                    ),
                    status=200,
                    headers={"Content-Type": "application/json"},
                )

            # Format response following backend standards
            response_data = {
                "items": albums_dict,
                "count": len(albums_dict),
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "data_source": "Last.fm Chart API + Spotify + Odesli (Cached - 24hr TTL)",
                    "limit": limit,
                    "cache_ttl": "24 hours",
                    "enrichment": {
                        "spotify_links": sum(1 for a in albums_dict if a.get("spotify_url")),
                        "apple_music_links": sum(
                            1 for a in albums_dict if a.get("apple_music_url")
                        ),
                        "youtube_music_links": sum(
                            1 for a in albums_dict if a.get("youtube_music_url")
                        ),
                    },
                },
            }

            logger.info(f"Successfully fetched {len(albums_dict)} trending music albums")

            return https_fn.Response(
                json.dumps(response_data),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
                },
            )

        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_trending_music_albums: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_music_albums(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for music albums by name or artist.

        Query Parameters:
        - query: Search query string (album name or artist name) (required)
        - limit: Number of results to return (1-50, default: 20)

        Returns:
            JSON response with search results including:
            - Album details (title, artist, image, playcount, listeners)
            - MediaCircle standardized fields (mc_id, mc_type)
            - Streaming service links (Spotify, Apple Music, YouTube Music, etc.) via Spotify API + Odesli
            - Metadata (generated timestamp, source, count)
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            limit = int(req.args.get("limit", 20))

            # Validate parameters
            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Searching music albums for query: '{query}' (limit={limit})")

            # Call wrapper - returns MCBaseItem derivative directly
            result = await lastfm_wrapper.search_albums(query=query, limit=limit)

            # Check for errors
            if result.status_code != 200 or result.error:
                error_msg = result.error or "Unknown error"
                logger.error(f"Failed to search albums: {error_msg}")
                return https_fn.Response(
                    json.dumps({"error": "Failed to search albums", "message": error_msg}),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Extract albums from response - convert to dicts for JSON serialization
            albums_list_of_dicts = [album.model_dump() for album in result.results]

            # Check for empty results
            if not albums_list_of_dicts:
                logger.warning(f"No albums found for query: '{query}'")
                return https_fn.Response(
                    json.dumps(
                        {
                            "items": [],
                            "count": 0,
                            "metadata": {
                                "generated_at": datetime.now().isoformat(),
                                "data_source": "Last.fm Search API (Cached - 24hr TTL)",
                                "query": query,
                                "limit": limit,
                                "message": "No albums found",
                            },
                        }
                    ),
                    status=200,
                    headers={"Content-Type": "application/json"},
                )

            # Format response following backend standards
            response_data = {
                "items": albums_list_of_dicts,
                "count": len(albums_list_of_dicts),
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "data_source": "Last.fm Search API + Spotify + Odesli (Cached - 24hr TTL)",
                    "query": query,
                    "limit": limit,
                    "cache_ttl": "24 hours",
                    "enrichment": {
                        "spotify_links": sum(
                            1 for a in albums_list_of_dicts if a.get("spotify_url")
                        ),
                        "apple_music_links": sum(
                            1 for a in albums_list_of_dicts if a.get("apple_music_url")
                        ),
                        "youtube_music_links": sum(
                            1 for a in albums_list_of_dicts if a.get("youtube_music_url")
                        ),
                    },
                },
            }

            logger.info(
                f"Successfully found {len(albums_list_of_dicts)} albums for query: '{query}'"
            )

            return https_fn.Response(
                json.dumps(response_data),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
                },
            )

        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in search_music_albums: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create LastFM handler instance
lastfm_handler = LastFMHandler()
