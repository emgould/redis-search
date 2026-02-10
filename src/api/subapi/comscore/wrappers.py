"""
Comscore Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides backward-compatible async wrappers for Firebase Functions integration.
"""

from datetime import datetime
from typing import cast

from api.subapi.comscore.core import comscore_service
from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for standalone async functions (not class methods)
ComscoreCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="comscore_func",
    verbose=False,
    isClassMethod=True,  # For standalone functions
)


class ComscoreWrapper:
    def __init__(self):
        self.service = comscore_service

    @RedisCache.use_cache(ComscoreCache, prefix="domestic_rankings_wrapper")
    async def get_domestic_rankings(self) -> BoxOfficeData:
        """
        Async wrapper function to get domestic box office rankings.

        Returns:
            BoxOfficeData: MCBaseItem derivative containing rankings data or error information
        """
        try:
            data = await self.service.get_domestic_rankings()

            if data is None:
                return BoxOfficeData(
                    rankings=[],
                    exhibition_week="",
                    fetched_at=datetime.now().isoformat(),
                    error="Failed to fetch box office rankings",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(BoxOfficeData, data)

        except Exception as e:
            logger.error(f"Error in get_domestic_rankings_async: {e}")
            return BoxOfficeData(
                rankings=[],
                exhibition_week="",
                fetched_at=datetime.now().isoformat(),
                error=str(e),
                status_code=500,
            )

    def match_movie_to_ranking(
        self, movie_title: str, rankings: list[BoxOfficeRanking]
    ) -> BoxOfficeRanking:
        """
        Async wrapper function to match a movie title to a box office ranking.

        Returns:
            BoxOfficeRanking with match data or error information
        """
        return self.service.match_movie_to_ranking(movie_title, rankings)


comscore_wrapper = ComscoreWrapper()
