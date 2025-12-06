"""
RottenTomatoes Firebase Functions handlers.
Handles search functionality for movies, TV shows, and people via Algolia.
"""

import json
import logging

from contracts.models import MCType
from firebase_functions import https_fn

from api.rottentomatoes.wrappers import get_rt_metrics, rottentomatoes_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class RottenTomatoesHandler:
    """Class containing all RottenTomatoes-focused Firebase Functions."""

    def __init__(self):
        """Initialize RottenTomatoes handler."""
        logger.info("RottenTomatoesHandler initialized - No API Keys needed (public Algolia)")

    async def search_content(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for movies and TV shows on RottenTomatoes.

        Query Parameters:
        - query: Search query string (required)
        - limit: Number of results (1-50, default: 10)
        - media_type: Filter by type ('movie' or 'tv', optional)

        Returns:
            JSON response with content search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            limit = int(req.args.get("limit", 10))
            media_type_str = req.args.get("media_type")

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

            # Parse media_type filter
            media_type: MCType | None = None
            if media_type_str:
                if media_type_str.lower() == "movie":
                    media_type = MCType.MOVIE
                elif media_type_str.lower() in ("tv", "series"):
                    media_type = MCType.TV_SERIES
                else:
                    return https_fn.Response(
                        json.dumps({"error": "media_type must be 'movie' or 'tv'"}),
                        status=400,
                        headers={"Content-Type": "application/json"},
                    )

            logger.info(f"Searching RottenTomatoes content: query={query}, limit={limit}")

            # Call wrapper
            result = await rottentomatoes_wrapper.search_content(
                query=query,
                limit=limit,
                media_type=media_type,
            )

            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully found {len(result.results)} RottenTomatoes content items"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
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
            logger.error(f"Unexpected error in search_content: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_people(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for people (actors, directors, etc.) on RottenTomatoes.

        Query Parameters:
        - query: Search query string (required)
        - limit: Number of results (1-50, default: 10)

        Returns:
            JSON response with people search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            limit = int(req.args.get("limit", 10))

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

            logger.info(f"Searching RottenTomatoes people: query={query}, limit={limit}")

            # Call wrapper
            result = await rottentomatoes_wrapper.search_people(
                query=query,
                limit=limit,
            )

            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully found {len(result.results)} RottenTomatoes people")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
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
            logger.error(f"Unexpected error in search_people: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for both content (movies/TV) and people on RottenTomatoes.

        Query Parameters:
        - query: Search query string (required)
        - limit: Number of results per type (1-50, default: 10)

        Returns:
            JSON response with all search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            limit = int(req.args.get("limit", 10))

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

            logger.info(f"Searching RottenTomatoes: query={query}, limit={limit}")

            # Call wrapper
            result = await rottentomatoes_wrapper.search_all(
                query=query,
                limit=limit,
            )

            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully found {len(result.results)} RottenTomatoes items")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
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
            logger.error(f"Unexpected error in search: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


    async def get_metrics(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get RottenTomatoes critic and audience scores for a title.

        Simple lookup that returns just the scores. Useful for quick score checks.

        Query Parameters:
        - title: The movie or TV show title (required)
        - year: Release year to filter by (optional)
        - star: Cast member name to filter by (optional)

        Returns:
            JSON response with critics and audience scores, or null if not found
            Example: {"critics": 83, "audience": 85}
        """
        try:
            # Parse query parameters
            title = req.args.get("title")
            year_str = req.args.get("year")
            star = req.args.get("star")

            # Validate parameters
            if not title:
                return https_fn.Response(
                    json.dumps({"error": "title parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Parse year if provided
            year: int | None = None
            if year_str:
                try:
                    year = int(year_str)
                except ValueError:
                    return https_fn.Response(
                        json.dumps({"error": "year must be a valid integer"}),
                        status=400,
                        headers={"Content-Type": "application/json"},
                    )

            logger.info(f"Getting RT metrics: title={title}, year={year}, star={star}")

            # Call wrapper
            result = await get_rt_metrics(title=title, year=year, star=star)

            if result is None:
                return https_fn.Response(
                    json.dumps(None),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                    },
                )

            logger.info(f"Found RT metrics for '{title}': {result}")

            return https_fn.Response(
                json.dumps(result),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_metrics: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create global handler instance
rottentomatoes_handler = RottenTomatoesHandler()

