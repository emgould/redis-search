"""
Podcast Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

from datetime import UTC, datetime
from typing import cast

from api.podcast.models import (
    EpisodeListResponse,
    MCPodcastItem,
    PersonSearchResponse,
    PodcasterSearchResponse,
    PodcastSearchResponse,
    PodcastTrendingResponse,
    PodcastWithLatestEpisode,
)
from api.podcast.search import PodcastSearchService
from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSources,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
PodcastWrapperCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="podcast_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="4.0.5",  # Updated podcast search sorting to use weighted title matching with Levenshtein distance
)


class PodcastWrapper:
    def __init__(self):
        self.service = PodcastSearchService()

    @RedisCache.use_cache(PodcastWrapperCache, prefix="get_trending_podcasts")
    async def get_trending_podcasts(
        self, max_results: int = 25, lang: str = "en"
    ) -> PodcastTrendingResponse:
        """
        Async wrapper function to get trending podcasts.

        Args:
            max_results: Maximum number of results to return (default=25)
            lang: Language filter (default='en')

        Returns:
            PodcastTrendingResponse: MCSearchResponse derivative containing trending podcasts or error information
        """
        try:
            data = await self.service.get_trending_podcasts(max_results=max_results, lang=lang)

            if data is None or data.error:
                return PodcastTrendingResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    error=data.error if data else "Failed to fetch trending podcasts",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(PodcastTrendingResponse, data)

        except Exception as e:
            logger.error(f"Error in get_trending_podcasts: {e}")
            return PodcastTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(PodcastWrapperCache, prefix="search_podcasts")
    async def search_podcasts(self, query: str, max_results: int = 20) -> PodcastSearchResponse:
        """
        Async wrapper function to search podcasts.

        Args:
            query: Search query string
            max_results: Maximum number of results to return (default=20)

        Returns:
            PodcastSearchResponse: MCSearchResponse derivative containing search results or error information
        """
        try:
            response = await self.service.search_podcasts(query=query, max_results=max_results)

            if response is None or response.error:
                return PodcastSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    query=query,
                    error=response.error if response else "Failed to fetch podcast search results",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(PodcastSearchResponse, response)

        except Exception as e:
            logger.error(f"Error in search_podcasts: {e}")
            return PodcastSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )

    async def get_podcast_by_id(self, feed_id: int) -> MCPodcastItem:
        """
        Async wrapper function to get a podcast by ID.

        Args:
            feed_id: PodcastIndex feed ID

        Returns:
            MCPodcastItem: MCBaseItem derivative containing podcast data or error information
        """
        try:
            data = await self.service.get_podcast_by_id(feed_id=feed_id)

            if data is None or data.status_code != 200:
                # Return error MCPodcastItem for not found
                return MCPodcastItem(error="Podcast not found", status_code=404)

            # Type assertion for mypy
            return cast(MCPodcastItem, data)

        except Exception as e:
            logger.error(f"Error in get_podcast_by_id: {e}")
            return MCPodcastItem(error=str(e), status_code=500)

    @RedisCache.use_cache(PodcastWrapperCache, prefix="get_podcast_episodes")
    async def get_podcast_episodes(
        self, feed_id: int, max_results: int = 25, since: int | None = None
    ) -> EpisodeListResponse:
        """
        Async wrapper function to get podcast episodes.

        Args:
            feed_id: PodcastIndex feed ID
            max_results: Maximum number of episodes to return (default=25)
            since: Optional timestamp to get episodes since this date

        Returns:
            EpisodeListResponse: MCSearchResponse derivative containing episodes or error information
        """
        try:
            data = await self.service.get_podcast_episodes(
                feed_id=feed_id, max_results=max_results, since=since
            )

            if data is None or data.error:
                return EpisodeListResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    feed_id=feed_id,
                    error=data.error if data else "Failed to fetch podcast episodes",
                    status_code=500,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(EpisodeListResponse, data)

        except Exception as e:
            logger.error(f"Error in get_podcast_episodes: {e}")
            return EpisodeListResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                feed_id=feed_id,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(PodcastWrapperCache, prefix="get_podcast_with_latest_episode")
    async def get_podcast_with_latest_episode(self, feed_id: int) -> PodcastWithLatestEpisode:
        """
        Async wrapper function to get a podcast with its latest episode.

        Args:
            feed_id: PodcastIndex feed ID

        Returns:
            PodcastWithLatestEpisode: MCBaseItem derivative containing podcast with latest episode or error information
        """
        try:
            data = await self.service.get_podcast_with_latest_episode(feed_id=feed_id)

            if data is None or data.status_code != 200:
                # Return error PodcastWithLatestEpisode for not found
                return PodcastWithLatestEpisode(
                    error="Podcast not found",
                    status_code=404,
                )

            # Type assertion for mypy - we've already checked for None above
            return cast(PodcastWithLatestEpisode, data)

        except Exception as e:
            logger.error(f"Error in get_podcast_with_latest_episode: {e}")
            # Return error PodcastWithLatestEpisode
            return PodcastWithLatestEpisode(
                id=feed_id,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(PodcastWrapperCache, prefix="search_by_person")
    async def search_by_person(
        self, person_name: str, max_results: int = 20
    ) -> PersonSearchResponse:
        """
        Async wrapper function to search podcasts and episodes by person name.

        Args:
            person_name: Name of the person to search for (host, guest, creator)
            max_results: Maximum number of results to return (default=20)

        Returns:
            PersonSearchResponse: Separated podcasts (hosts) and episodes (guests)
        """
        try:
            response = await self.service.search_by_person(
                person_name=person_name, max_results=max_results
            )

            if response is None or response.error:
                return PersonSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    podcasts=[],
                    episodes=[],
                    total_podcasts=0,
                    total_episodes=0,
                    person_name=person_name,
                    error=response.error if response else "Failed to fetch person search results",
                    status_code=500,
                )

            return cast(PersonSearchResponse, response)

        except Exception as e:
            logger.error(f"Error in search_by_person: {e}")
            return PersonSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                podcasts=[],
                episodes=[],
                total_podcasts=0,
                total_episodes=0,
                person_name=person_name,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(PodcastWrapperCache, prefix="search_person")
    async def search_person(
        self, person_name: str, max_results: int = 20
    ) -> PodcasterSearchResponse:
        """
        Async wrapper function to search for podcasters by person name.
        Returns MCPodcaster items that aggregate all podcasts where the person is a host/creator.

        Args:
            person_name: Name of the person to search for (host, creator)
            max_results: Maximum number of podcasters to return (default=20)

        Returns:
            PodcasterSearchResponse: MCPodcaster items with aggregated podcast data
        """
        try:
            print(
                f"[DEBUG WRAPPER search_person] Calling service.search_person('{person_name}', max_results={max_results})"
            )
            logger.info(
                f"Wrapper search_person calling service.search_person('{person_name}', max_results={max_results})"
            )
            response = await self.service.search_person(
                person_name=person_name, max_results=max_results
            )
            print(
                f"[DEBUG WRAPPER search_person] service.search_person returned: status_code={response.status_code if response else 'None'}, "
                f"error={response.error if response else 'None'}, results_count={len(response.results) if response and hasattr(response, 'results') else 0}"
            )
            logger.info(
                f"Wrapper search_person received response: status_code={response.status_code if response else 'None'}, "
                f"error={response.error if response else 'None'}, results_count={len(response.results) if response and hasattr(response, 'results') else 0}"
            )

            if response is None or response.error:
                return PodcasterSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    query=person_name,
                    error=response.error
                    if response
                    else "Failed to fetch podcaster search results",
                    status_code=500,
                )

            return cast(PodcasterSearchResponse, response)

        except Exception as e:
            logger.error(f"Error in search_person: {e}")
            return PodcasterSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=person_name,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(PodcastWrapperCache, prefix="search_person_works")
    async def search_person_async(
        self,
        request: "MCPersonSearchRequest",
        limit: int | None = None,
    ) -> "MCPersonSearchResponse":
        """Search for podcaster works (podcasts) based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.
        The source_id contains feed IDs (comma-delimited) that should be fetched directly.

        Args:
            request: MCPersonSearchRequest with podcaster identification details
            limit: Maximum number of podcasts to return (default: 50, ignored if source_id provided)

        Returns:
            MCPersonSearchResponse with podcaster details and works
            - details: None (no podcaster details available when using feed IDs)
            - works: list[MCPodcastItem] (podcasts fetched by feed IDs)
            - related: [] (empty, will be filled by search_broker)
        """
        try:
            # Validate source - must be PodcastIndex for this wrapper
            if request.source != MCSources.PODCASTINDEX:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source: {request.source}. This wrapper only accepts PODCASTINDEX sources.",
                    status_code=400,
                )

            # Parse source_id which contains feed IDs (may be comma-delimited)
            source_id = request.source_id
            if not source_id:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="source_id is required (must contain feed IDs)",
                    status_code=400,
                )

            # Split source_id by comma and convert to integers
            feed_id_strings = [s.strip() for s in source_id.split(",") if s.strip()]
            if not feed_id_strings:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="No valid feed IDs found in source_id",
                    status_code=400,
                )

            # Convert to integers, filtering out invalid values
            feed_ids: list[int] = []
            for feed_id_str in feed_id_strings:
                try:
                    feed_id = int(feed_id_str)
                    if feed_id > 0:
                        feed_ids.append(feed_id)
                    else:
                        logger.warning(f"Invalid feed ID (must be positive): {feed_id_str}")
                except ValueError:
                    logger.warning(f"Invalid feed ID (not a number): {feed_id_str}")

            if not feed_ids:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="No valid feed IDs found in source_id",
                    status_code=400,
                )

            # Apply limit if provided
            if limit is not None and limit > 0:
                feed_ids = feed_ids[:limit]

            logger.info(
                f"Fetching {len(feed_ids)} podcasts by feed IDs: {feed_ids[:5]}{'...' if len(feed_ids) > 5 else ''}"
            )

            # Fetch podcasts by feed IDs
            podcasts = await self.service.get_podcasts_by_ids(feed_ids)

            if not podcasts:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="No podcasts found for the provided feed IDs",
                    status_code=404,
                )

            # Convert to list[MCBaseItem] for works
            works: list[MCBaseItem] = list(podcasts)

            logger.info(f"Successfully fetched {len(works)} podcasts by feed IDs")

            # Return response with works (no podcaster details available when using feed IDs)
            return MCPersonSearchResponse(
                input=request,
                details=None,  # No podcaster details when using feed IDs directly
                works=works,  # list[MCPodcastItem]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error fetching podcasts by feed IDs for {request.source_id}: {e}")
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )


podcast_wrapper = PodcastWrapper()
