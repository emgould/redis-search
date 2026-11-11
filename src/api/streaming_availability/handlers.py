"""
Streaming Availability Firebase Functions Handlers

This module contains all Streaming Availability-related Firebase function handlers.
Handlers exclusively call wrapper methods - no auth or business logic here.
"""

import json
import logging
from typing import Literal

from firebase_functions import https_fn

from api.streaming_availability.wrappers import streaming_availability_wrapper
from utils.async_runner import run_async

# Configure logging
logger = logging.getLogger(__name__)


class StreamingAvailabilityHandler:
    """Class containing all Streaming Availability-related Firebase Functions."""

    def __init__(self) -> None:
        """Initialize Streaming Availability handler. All auth handled by wrapper."""
        logger.info("StreamingAvailabilityHandler initialized")

    def get_streaming_availability(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get streaming availability for a movie or TV show by TMDB ID.

        This function returns streaming options for all available countries
        unless filtered by the optional country parameter.

        Usage: GET /get_streaming_availability?tmdb_id=238&type=movie&country=us

        Args (query params):
            tmdb_id: Required. TMDB ID of the movie or TV show
            type: Optional. "movie" (default) or "tv"
            country: Optional. 2-letter country code to filter results (e.g., "us", "gb")

        Returns:
            JSON response with streaming availability data
        """
        try:
            # Parse request parameters
            tmdb_id_str = req.args.get("tmdb_id")
            content_type = req.args.get("type", "movie")
            country = req.args.get("country")

            # Validate tmdb_id
            if not tmdb_id_str:
                return https_fn.Response(
                    json.dumps({"error": "tmdb_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                tmdb_id = int(tmdb_id_str)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "tmdb_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate content_type
            if content_type not in ("movie", "tv"):
                return https_fn.Response(
                    json.dumps({"error": "type must be 'movie' or 'tv'"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate country if provided
            if country and len(country) != 2:
                return https_fn.Response(
                    json.dumps({"error": "country must be a 2-letter country code"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Cast content_type to Literal for type safety
            typed_content_type: Literal["movie", "tv"] = (
                "movie" if content_type == "movie" else "tv"
            )

            # Create and run the async function
            async def run_get_streaming():
                return await streaming_availability_wrapper.get_streaming_availability_by_tmdb_id(
                    tmdb_id=tmdb_id,
                    content_type=typed_content_type,
                    country=country,
                )

            # Run the async function safely
            result = run_async(run_get_streaming())

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Log success
            streaming_count = sum(
                len(options) for options in result.streaming_options.values()
            )
            countries_count = len(result.streaming_options)
            logger.info(
                f"Successfully fetched streaming availability for TMDB ID {tmdb_id} "
                f"({content_type}): {streaming_count} options across {countries_count} countries"
            )

            return https_fn.Response(
                json.dumps(result.model_dump(), default=str),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as ve:
            logger.error(f"Validation error in get_streaming_availability: {str(ve)}")
            return https_fn.Response(
                json.dumps({"error": str(ve)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Error in get_streaming_availability function: {str(e)}", exc_info=True)
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create a global instance of the handler
streaming_availability_handler = StreamingAvailabilityHandler()
