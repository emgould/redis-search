"""
YouTube-focused Firebase Functions
Handles trending videos, search, and other YouTube-related functionality.
"""

import json
import logging

from firebase_functions import https_fn

# Import the YouTube wrapper and auth
from api.youtube.auth import Auth
from api.youtube.wrappers import youtube_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class YouTubeHandler(Auth):
    """Class containing all YouTube-focused Firebase Functions."""

    def __init__(self):
        """Initialize YouTube handler. API keys are accessed from secrets at runtime."""
        super().__init__()
        logger.info("YouTubeHandler initialized - API keys will be resolved at runtime")

    async def get_trending_videos(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending YouTube videos.

        Query Parameters:
        - region_code: ISO 3166-1 alpha-2 country code (default: US)
        - language: Language code for results (default: en)
        - max_results: Maximum number of results (1-50, default: 50)
        - category_id: Optional category ID to filter by
        - query: Optional search query to filter trending videos

        Returns:
            JSON response with trending videos data
        """
        try:
            # Parse query parameters
            region_code = req.args.get("region_code", "US")
            language = req.args.get("language", "en")
            max_results = int(req.args.get("max_results", 50))
            category_id = req.args.get("category_id")
            query = req.args.get("query")

            logger.info(f"Getting trending videos for region: {region_code}, language: {language}")

            # Validate parameters
            if max_results < 1 or max_results > 50:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Call wrapper method
            result = await youtube_wrapper.get_trending_videos(
                region_code=region_code,
                language=language,
                max_results=max_results,
                category_id=category_id,
                query=query,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                error_status = result.status_code
                # Handle specific error status codes
                if result.error and result.error.startswith("YouTube API error"):
                    if "403" in result.error:
                        error_status = 403
                    elif "400" in result.error:
                        error_status = 400

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=error_status,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched {result.total_results} trending videos")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_trending_videos: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_videos(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search YouTube videos.

        Query Parameters:
        - query: Search query string (required)
        - max_results: Maximum number of results (1-50, default: 25)
        - order: Sort order (relevance, date, rating, viewCount, title, default: relevance)
        - published_after: RFC 3339 formatted date-time value
        - region_code: ISO 3166-1 alpha-2 country code (default: US)
        - language: Language code for results (default: en)

        Returns:
            JSON response with search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            max_results = int(req.args.get("max_results", 25))
            order = req.args.get("order", "relevance")
            published_after = req.args.get("published_after")
            region_code = req.args.get("region_code", "US")
            language = req.args.get("language", "en")

            # Validate parameters
            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if max_results < 1 or max_results > 50:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            valid_orders = ["relevance", "date", "rating", "viewCount", "title"]
            if order not in valid_orders:
                return https_fn.Response(
                    json.dumps({"error": f"order must be one of: {', '.join(valid_orders)}"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Searching YouTube videos for: {query}, region: {region_code}, language: {language}"
            )

            # Call wrapper method
            result = await youtube_wrapper.search_videos(
                query=query,
                max_results=max_results,
                order=order,
                published_after=published_after,
                region_code=region_code,
                language=language,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully found {result.total_results} videos for query: {query}")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in search_videos: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_video_categories(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get available video categories for a region.

        Query Parameters:
        - region_code: ISO 3166-1 alpha-2 country code (default: US)
        - language: Language code for results (default: en)

        Returns:
            JSON response with video categories
        """
        try:
            # Parse query parameters
            region_code = req.args.get("region_code", "US")
            language = req.args.get("language", "en")

            logger.info(f"Getting video categories for region: {region_code}, language: {language}")

            # Call wrapper method
            result = await youtube_wrapper.get_video_categories(
                region_code=region_code,
                language=language,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched {len(result.categories)} categories")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_video_categories: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_popular_videos(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get popular YouTube videos using search API (alternative to trending).

        Query Parameters:
        - query: Optional search query (defaults to 'trending')
        - region_code: ISO 3166-1 alpha-2 country code (default: US)
        - language: Language code for results (default: en)
        - max_results: Maximum number of results (1-50, default: 50)
        - published_after: RFC 3339 formatted date-time value

        Returns:
            JSON response with popular videos data
        """
        try:
            # Parse query parameters
            query = req.args.get("query")  # Optional
            region_code = req.args.get("region_code", "US")
            language = req.args.get("language", "en")
            max_results = int(req.args.get("max_results", 50))
            published_after = req.args.get("published_after")

            # Validate parameters
            if max_results < 1 or max_results > 50:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Getting popular videos for region: {region_code}, language: {language}, query: {query}"
            )

            # Call wrapper method
            result = await youtube_wrapper.get_popular_videos(
                query=query,
                region_code=region_code,
                language=language,
                max_results=max_results,
                published_after=published_after,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched {result.total_results} popular videos")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_popular_videos: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create YouTube handler instance
youtube_handler = YouTubeHandler()
