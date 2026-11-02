"""
Streaming Availability Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using MCBaseItem response pattern.
"""

from typing import Literal

from api.streaming_availability.auth import streaming_availability_auth
from api.streaming_availability.core import StreamingAvailabilityService
from api.streaming_availability.models import (
    SAShowResult,
    StreamingAvailabilityResponse,
)
from api.streaming_availability.rapidapi_key_pool import RapidAPIQuotaExhaustedError
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
StreamingAvailabilityWrapperCache = RedisCache(
    defaultTTL=4 * 60 * 60,  # 4 hours
    prefix="sa_wrapper",
    verbose=False,
    isClassMethod=True,
)


class StreamingAvailabilityWrapper:
    """Wrapper class for Streaming Availability API operations."""

    def __init__(self) -> None:
        """Initialize Streaming Availability wrapper. Services are created per-request."""

    @RedisCache.use_cache(StreamingAvailabilityWrapperCache, prefix="get_by_tmdb")
    async def get_streaming_availability_by_tmdb_id(
        self,
        tmdb_id: int,
        content_type: Literal["movie", "tv"] = "movie",
        country: str | None = None,
        **kwargs: object,
    ) -> StreamingAvailabilityResponse:
        """
        Async wrapper function to get streaming availability by TMDB ID.

        Args:
            tmdb_id: TMDB ID of the movie or TV show
            content_type: "movie" or "tv" (default: "movie")
            country: Optional 2-letter country code to filter results (e.g., "us", "gb")

        Returns:
            StreamingAvailabilityResponse: MCBaseItem derivative containing streaming data
                or error information
        """
        try:
            if not streaming_availability_auth.has_rapidapi_keys():
                return StreamingAvailabilityResponse(
                    tmdb_id=tmdb_id,
                    tmdb_type=content_type,
                    title="",
                    show_type=content_type,
                    error="RapidAPI key is required",
                    status_code=400,
                )

            service = StreamingAvailabilityService()

            # Get streaming availability
            result = await service.get_show_by_tmdb_id(
                tmdb_id=tmdb_id,
                content_type=content_type,
                country=country,
            )

            if not result:
                return StreamingAvailabilityResponse(
                    tmdb_id=tmdb_id,
                    tmdb_type=content_type,
                    title="",
                    show_type=content_type,
                    error=f"Title with TMDB ID {tmdb_id} ({content_type}) not found",
                    status_code=404,
                )

            # Parse the result into our model
            try:
                sa_result = SAShowResult.model_validate(result)
                response = StreamingAvailabilityResponse.from_sa_result(sa_result)
                response.status_code = 200
                return response
            except Exception as parse_error:
                logger.error(f"Error parsing streaming availability result: {parse_error}")
                # Return raw data if parsing fails
                return StreamingAvailabilityResponse(
                    sa_id=result.get("id", ""),
                    tmdb_id=tmdb_id,
                    tmdb_type=content_type,
                    title=result.get("title", ""),
                    show_type=result.get("showType", content_type),
                    overview=result.get("overview"),
                    streaming_options=result.get("streamingOptions", {}),
                    error=f"Partial parse: {parse_error}",
                    status_code=200,
                )

        except RapidAPIQuotaExhaustedError as quota_error:
            logger.warning("Streaming Availability quota exhausted: %s", quota_error)
            return StreamingAvailabilityResponse(
                tmdb_id=tmdb_id,
                tmdb_type=content_type,
                title="",
                show_type=content_type,
                error=str(quota_error),
                status_code=429,
            )
        except Exception as e:
            logger.error(f"Error in get_streaming_availability_by_tmdb_id: {e}")
            return StreamingAvailabilityResponse(
                tmdb_id=tmdb_id,
                tmdb_type=content_type,
                title="",
                show_type=content_type,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(StreamingAvailabilityWrapperCache, prefix="get_by_imdb")
    async def get_streaming_availability_by_imdb_id(
        self,
        imdb_id: str,
        country: str | None = None,
        **kwargs: object,
    ) -> StreamingAvailabilityResponse:
        """
        Async wrapper function to get streaming availability by IMDB ID.

        Args:
            imdb_id: IMDB ID of the movie or TV show (e.g., "tt0068646")
            country: Optional 2-letter country code to filter results

        Returns:
            StreamingAvailabilityResponse: MCBaseItem derivative containing streaming data
                or error information
        """
        try:
            if not streaming_availability_auth.has_rapidapi_keys():
                return StreamingAvailabilityResponse(
                    imdb_id=imdb_id,
                    title="",
                    show_type="",
                    error="RapidAPI key is required",
                    status_code=400,
                )

            service = StreamingAvailabilityService()

            # Get streaming availability
            result = await service.get_show_by_imdb_id(
                imdb_id=imdb_id,
                country=country,
            )

            if not result:
                return StreamingAvailabilityResponse(
                    imdb_id=imdb_id,
                    title="",
                    show_type="",
                    error=f"Title with IMDB ID {imdb_id} not found",
                    status_code=404,
                )

            # Parse the result into our model
            try:
                sa_result = SAShowResult.model_validate(result)
                response = StreamingAvailabilityResponse.from_sa_result(sa_result)
                response.status_code = 200
                return response
            except Exception as parse_error:
                logger.error(f"Error parsing streaming availability result: {parse_error}")
                return StreamingAvailabilityResponse(
                    sa_id=result.get("id", ""),
                    imdb_id=imdb_id,
                    title=result.get("title", ""),
                    show_type=result.get("showType", ""),
                    overview=result.get("overview"),
                    streaming_options=result.get("streamingOptions", {}),
                    error=f"Partial parse: {parse_error}",
                    status_code=200,
                )

        except RapidAPIQuotaExhaustedError as quota_error:
            logger.warning("Streaming Availability quota exhausted: %s", quota_error)
            return StreamingAvailabilityResponse(
                imdb_id=imdb_id,
                title="",
                show_type="",
                error=str(quota_error),
                status_code=429,
            )
        except Exception as e:
            logger.error(f"Error in get_streaming_availability_by_imdb_id: {e}")
            return StreamingAvailabilityResponse(
                imdb_id=imdb_id,
                title="",
                show_type="",
                error=str(e),
                status_code=500,
            )


# Singleton instance for use across the application
streaming_availability_wrapper = StreamingAvailabilityWrapper()
