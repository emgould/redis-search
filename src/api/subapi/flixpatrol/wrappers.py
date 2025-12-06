"""
FlixPatrol Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides backward-compatible async wrappers for Firebase Functions integration.
"""

from datetime import UTC, datetime
from typing import cast

from api.subapi.flixpatrol.core import flixpatrol_service
from api.subapi.flixpatrol.models import FlixPatrolResponse
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for standalone async functions (not class methods)
FlixPatrolCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="flixpatrol_func",
    verbose=False,
    isClassMethod=True,  # For standalone functions
    version="2.0.1",  # Version bump for Redis migration
)


class FlixPatrolWrapper:
    def __init__(self):
        self.service = flixpatrol_service

    @RedisCache.use_cache(FlixPatrolCache, prefix="get_flixpatrol_data_wrapper")
    async def get_flixpatrol_data(
        self, providers: list[str] | None = None, **kwargs
    ) -> FlixPatrolResponse:
        """
        Async wrapper function to get FlixPatrol data.

        Args:
            providers: List of providers to include
            **kwargs: Additional arguments (for compatibility)

        Returns:
            FlixPatrolResponse: MCBaseItem derivative containing FlixPatrol data or error information
        """
        try:
            data = await self.service.get_flixpatrol_data(providers=providers)

            if data is None:
                return FlixPatrolResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    error="Failed to fetch FlixPatrol data",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(FlixPatrolResponse, data)

        except Exception as e:
            logger.error(f"Error in get_flixpatrol_data: {e}")
            return FlixPatrolResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                error=str(e),
                status_code=500,
            )


flixpatrol_wrapper = FlixPatrolWrapper()
