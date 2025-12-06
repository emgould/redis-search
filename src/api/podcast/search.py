"""
Podcast Search Service - All search operations for podcast discovery.
Extends core service with trending, search, and episode retrieval features.
"""

import asyncio
import math
from datetime import UTC, datetime
from typing import Any

from api.podcast.core import PodcastFunctionCache, PodcastService
from api.podcast.models import (
    EpisodeListResponse,
    MCEpisodeItem,
    MCPodcaster,
    MCPodcastItem,
    PersonSearchResponse,
    PodcasterSearchResponse,
    PodcastSearchResponse,
    PodcastTrendingResponse,
    PodcastWithLatestEpisode,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache
from utils.soft_comparison import _levenshtein_distance

logger = get_logger(__name__)


class PodcastSearchService(PodcastService):
    """
    Handles all search operations for podcast discovery.
    Extends PodcastService with trending, search, and episode retrieval features.
    """

    @RedisCache.use_cache(PodcastFunctionCache, prefix="trending")
    async def get_trending_podcasts(
        self, max_results: int = 25, lang: str = "en"
    ) -> PodcastTrendingResponse:
        """
        Get trending podcasts from PodcastIndex.

        Args:
            max_results: Maximum number of results to return (default=25)
            lang: Language filter (e.g., 'en', 'es', etc.)

        Returns:
            PodcastTrendingResponse with validated podcast models
        """
        try:
            async with await self.get_client() as client:
                trending_feeds = await client.trending_podcasts(max_results=max_results, lang=lang)

                # Fetch latest episodes for all podcasts in parallel to get dates
                async def get_latest_episode_date(feed_id: int) -> str | None:
                    """Helper to fetch the latest episode date for a podcast."""
                    try:
                        episodes = await client.episodes_by_feedid(feed_id=feed_id, max_results=1)
                        if episodes and len(episodes) > 0:
                            return (
                                episodes[0].date_published.isoformat()
                                if episodes[0].date_published
                                else None
                            )
                    except Exception as e:
                        logger.debug(f"Failed to fetch latest episode for feed {feed_id}: {e}")
                    return None

                # Fetch latest episode dates in parallel (limit to avoid too many concurrent requests)
                episode_date_tasks = [
                    get_latest_episode_date(feed.id) for feed in trending_feeds[:max_results]
                ]
                latest_episode_dates = await asyncio.gather(
                    *episode_date_tasks, return_exceptions=True
                )

                # Process the PodcastFeed objects into dictionaries
                results = []
                for idx, feed in enumerate(trending_feeds):
                    # Get latest episode date (handle exceptions)
                    latest_episode_date: str | None = None
                    if idx < len(latest_episode_dates):
                        date_result = latest_episode_dates[idx]
                        if isinstance(date_result, str):
                            latest_episode_date = date_result
                        elif isinstance(date_result, (Exception, BaseException)):
                            # Skip exceptions
                            pass
                        else:
                            # date_result is None or some other non-exception type
                            # Since get_latest_episode_date returns str | None, this must be None
                            latest_episode_date = None

                    # Debug logging for timestamp fields
                    if __debug__:
                        logger.debug(
                            f"Podcast '{feed.title}': "
                            f"last_update_time={feed.last_update_time}, "
                            f"last_crawl_time={feed.last_crawl_time}, "
                            f"last_parse_time={feed.last_parse_time}, "
                            f"latest_episode_date={latest_episode_date}"
                        )

                    # Convert PodcastFeed to dict format
                    # Prefer latest episode date, then fallback to feed timestamps
                    last_update_time_value = (
                        latest_episode_date
                        if latest_episode_date
                        else (
                            feed.last_update_time.isoformat()
                            if feed.last_update_time
                            else (
                                feed.last_crawl_time.isoformat()
                                if feed.last_crawl_time
                                else (
                                    feed.last_parse_time.isoformat()
                                    if feed.last_parse_time
                                    else None
                                )
                            )
                        )
                    )

                    podcast_dict = {
                        "id": feed.id,
                        "title": feed.title,
                        "url": feed.url,
                        "site": feed.site,
                        "description": feed.description,
                        "author": feed.author,
                        "owner_name": feed.owner_name,
                        "image": feed.image,
                        "artwork": feed.artwork,
                        "last_update_time": last_update_time_value,
                        "trend_score": feed.trend_score,
                        "language": feed.language,
                        "categories": feed.categories,
                        "episode_count": self._safe_episode_count(feed.episode_count),
                        "itunes_id": feed.itunes_id,
                        "podcast_guid": feed.podcast_guid,
                    }

                    processed_item = self._process_podcast_item(podcast_dict)

                    # Filter out podcasts without valid images
                    if self._has_valid_image(processed_item):
                        results.append(processed_item)
                    else:
                        logger.debug(
                            f"Filtered out podcast '{processed_item.title}' - no valid image"
                        )

                logger.info(f"Successfully fetched {len(results)} trending podcasts")
                return PodcastTrendingResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=results,
                    total_results=len(results),
                    error=None,
                )

        except Exception as e:
            logger.error(f"Error fetching trending podcasts: {e}")
            return PodcastTrendingResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                error=str(e),
            )

    @RedisCache.use_cache(PodcastFunctionCache, prefix="trending")
    async def search_podcasts(self, query: str, max_results: int = 20) -> PodcastSearchResponse:
        """
        Search for podcasts by term.

        Args:
            query: Search query
            max_results: Maximum number of results to return (default=20)

        Returns:
            PodcastSearchResponse with validated podcast models
        """
        try:
            async with await self.get_client() as client:
                cleansed_query = (
                    query.lower()
                    .replace(" podcast", "")
                    .replace(" pod", "")
                    .replace(" podcasts", "")
                    .replace(" pods", "")
                    .rstrip()
                )
                search_feeds = await client.search_podcasts(
                    query=cleansed_query, max_results=max_results
                )

                # Process the PodcastFeed objects into dictionaries WITH relevancy calculation
                temp_results = []
                for feed in search_feeds:
                    # Get values for relevancy calculation
                    trend_score = float(feed.trend_score or 0)
                    # Safely get episode count, ensuring it's always a valid integer
                    episode_count = self._safe_episode_count(feed.episode_count)

                    # Relevancy formula parameters
                    alpha = 0.7  # Weight for popularity (0.6-0.8 as suggested)
                    e_max = 200  # Expected max episodes (200 as suggested)

                    # Calculate relevancy score
                    # Relevance = α · Popularity + (1 - α) · [100 × log(1 + Episodes) / log(1 + E_max)]
                    popularity = trend_score  # Use trend_score as popularity

                    if episode_count > 0:
                        episode_score = 100 * (math.log(1 + episode_count) / math.log(1 + e_max))
                    else:
                        episode_score = 0

                    relevancy_score = alpha * popularity + (1 - alpha) * episode_score

                    # Convert PodcastFeed to dict format WITH relevancy score
                    podcast_dict = {
                        "id": feed.id,
                        "title": feed.title,
                        "url": feed.url,
                        "site": feed.site,
                        "description": feed.description,
                        "author": feed.author,
                        "owner_name": feed.owner_name,
                        "image": feed.image,
                        "artwork": feed.artwork,
                        "last_update_time": feed.last_update_time.isoformat()
                        if feed.last_update_time
                        else None,
                        "trend_score": feed.trend_score,
                        "language": feed.language,
                        "categories": feed.categories,
                        "episode_count": episode_count,
                        "itunes_id": feed.itunes_id,
                        "podcast_guid": feed.podcast_guid,
                        "relevancy_score": relevancy_score,  # Include relevancy score
                    }

                    processed_item = self._process_podcast_item(podcast_dict)

                    # Filter out podcasts without valid images
                    if self._has_valid_image(processed_item):
                        temp_results.append(processed_item)
                    else:
                        logger.debug(
                            f"Filtered out podcast '{processed_item.title}' from search - no valid image"
                        )

                results = temp_results

                # Apply weighted sorting with heavy emphasis on exact title matches
                try:
                    query_normalized = query.lower().strip() if query else ""

                    # Helper function to normalize title (remove leading articles)
                    def normalize_title(text: str) -> str:
                        if not text:
                            return ""
                        text_lower = text.lower().strip()
                        for article in ["the ", "a ", "an "]:
                            if text_lower.startswith(article):
                                return text_lower[len(article) :].strip()
                        return text_lower

                    def calculate_weighted_score(item: Any) -> float:
                        """Calculate weighted score combining title similarity, relevancy, and recency."""
                        try:
                            # Title similarity using Levenshtein edit distance
                            title_similarity = 0.0
                            if query_normalized and item.title:
                                title_normalized = normalize_title(item.title)
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

                            # Get existing relevancy score (trend_score + episode_count)
                            relevancy_score = float(item.relevancy_score or 0)
                            # Normalize relevancy score to 0-1 range (assuming max around 200)
                            relevancy_weight = min(1.0, relevancy_score / 200.0)

                            # Recency weight based on last_update_time
                            recency_weight = 0.5  # Default neutral weight
                            if item.last_update_time:
                                try:
                                    # Parse ISO format date string
                                    date_str = str(item.last_update_time).strip()
                                    # Handle various ISO formats (with/without timezone, with/without Z)
                                    if date_str.endswith("Z"):
                                        date_str = date_str[:-1] + "+00:00"
                                    elif (
                                        "+" not in date_str
                                        and "-" in date_str
                                        and len(date_str) > 10
                                        and "T" in date_str
                                    ):
                                        # Has time but no timezone, assume UTC
                                        date_str = date_str + "+00:00"

                                    # Parse the date
                                    if "T" in date_str:
                                        # Has time component
                                        update_date = datetime.fromisoformat(date_str).date()
                                    else:
                                        # Date only (YYYY-MM-DD)
                                        update_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                                    today = datetime.now(UTC).date()
                                    days_since_update = (today - update_date).days

                                    # More recent updates get higher weight
                                    # Use exponential decay with 2-year half-life (730 days)
                                    recency_weight = math.exp(-days_since_update / 730.0)
                                except (ValueError, TypeError, AttributeError) as e:
                                    # If date parsing fails, use neutral weight
                                    logger.debug(
                                        f"Could not parse last_update_time '{item.last_update_time}': {e}"
                                    )
                                    recency_weight = 0.5
                            else:
                                # No date available, use neutral weight
                                recency_weight = 0.5

                            # Combined weight: 60% title similarity (heavily weighted), 30% relevancy, 10% recency
                            weighted_score = (
                                (title_similarity * 0.6)
                                + (relevancy_weight * 0.3)
                                + (recency_weight * 0.1)
                            )

                            return weighted_score
                        except Exception as e:
                            logger.warning(
                                f"Error calculating weighted score for podcast {item.id}: {e}"
                            )
                            return 0.0  # Default sort to end

                    # Sort by weighted score (descending)
                    # Calculate scores for all items first for debugging
                    scored_results = [(item, calculate_weighted_score(item)) for item in results]
                    # Log top 5 scores for debugging
                    scored_results.sort(key=lambda x: x[1], reverse=True)
                    top_5 = scored_results[:5]
                    logger.info(
                        f"Top 5 weighted scores for query '{query}': "
                        + ", ".join([f"{item.title[:30]}: {score:.3f}" for item, score in top_5])
                    )
                    # Sort the actual results
                    results.sort(key=calculate_weighted_score, reverse=True)
                    logger.info(
                        f"Sorted {len(results)} podcast results using weighted sort "
                        f"(60% title similarity, 30% relevancy, 10% recency) for query '{query}'"
                    )
                except Exception as e:
                    logger.error(f"Error sorting podcast results: {e}")
                    # If sorting fails, fall back to relevancy score sorting
                    try:

                        def safe_sort_key(item: Any) -> float:
                            try:
                                relevancy_score = float(item.relevancy_score or 0)
                                return -relevancy_score  # Negative for descending order
                            except Exception as e:
                                logger.warning(f"Error in sort key for podcast {item.id}: {e}")
                                return 0.0  # Default sort to end

                        results.sort(key=safe_sort_key)
                        logger.info("Fell back to relevancy score sorting")
                    except Exception as e2:
                        logger.error(f"Error in fallback sorting: {e2}")
                        # If sorting fails completely, still return results but without custom sorting

                logger.info(f"Successfully searched podcasts for '{query}': {len(results)} results")
                return PodcastSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=results,
                    total_results=len(results),
                    query=query,
                )

        except Exception as e:
            logger.error(f"Error searching podcasts for '{query}': {e}")
            return PodcastSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                query=query,
                error=f"Error searching podcasts for '{query}': {e}",
                status_code=500,
            )

    @RedisCache.use_cache(PodcastFunctionCache, prefix="podcast_by_id")
    async def get_podcast_by_id(self, feed_id: int) -> MCPodcastItem:
        """
        Get a specific podcast by its feed ID.

        Args:
            feed_id: PodcastIndex feed ID

        Returns:
            MCPodcastItem object or None if not found
        """
        try:
            async with await self.get_client() as client:
                feed = await client.podcast_by_feedid(feed_id)

                if not feed:
                    return MCPodcastItem(error="Podcast not found", status_code=404)

                # Convert PodcastFeed to dict format
                podcast_dict = {
                    "id": feed.id,
                    "title": feed.title,
                    "url": feed.url,
                    "site": feed.site,
                    "description": feed.description,
                    "author": feed.author,
                    "owner_name": feed.owner_name,
                    "image": feed.image,
                    "artwork": feed.artwork,
                    "last_update_time": feed.last_update_time.isoformat()
                    if feed.last_update_time
                    else None,
                    "trend_score": feed.trend_score,
                    "language": feed.language,
                    "categories": feed.categories,
                    "episode_count": self._safe_episode_count(feed.episode_count),
                    "itunes_id": feed.itunes_id,
                    "podcast_guid": feed.podcast_guid,
                }

                # Try to get Spotify URL
                try:
                    if feed.title:
                        spotify_url = await client.get_podcast_link(feed.title)
                        podcast_dict["spotify_url"] = spotify_url
                        if spotify_url:
                            logger.info(f"Found Spotify URL for {feed.title}: {spotify_url}")
                        else:
                            logger.debug(f"No Spotify URL found for {feed.title}")
                except Exception as e:
                    logger.warning(f"Failed to fetch Spotify URL for {feed.title}: {e}")
                    podcast_dict["spotify_url"] = None

                processed_item = self._process_podcast_item(podcast_dict)
                logger.info(f"Successfully fetched podcast {feed_id}: {processed_item.title}")
                return processed_item

        except Exception as e:
            logger.error(f"Error fetching podcast {feed_id}: {e}")
            return MCPodcastItem(error=f"Error fetching podcast {feed_id}: {e}", status_code=500)

    @RedisCache.use_cache(PodcastFunctionCache, prefix="episodes")
    async def get_podcast_episodes(
        self, feed_id: int, max_results: int = 25, since: int | None = None
    ) -> EpisodeListResponse:
        """
        Get episodes for a specific podcast by feed ID.

        Args:
            feed_id: PodcastIndex feed ID
            max_results: Maximum number of episodes to return (default=25)
            since: Optional timestamp to get episodes since this date

        Returns:
            EpisodeListResponse with validated episode models
        """
        try:
            async with await self.get_client() as client:
                episodes = await client.episodes_by_feedid(
                    feed_id=feed_id, max_results=max_results, since=since
                )

                # Process the EpisodeItem objects into dictionaries
                results = []
                for episode in episodes:
                    # Convert EpisodeItem to dict format
                    episode_dict = {
                        "id": episode.id,
                        "title": episode.title,
                        "description": episode.description,
                        "link": episode.link,
                        "guid": episode.guid,
                        "date_published": episode.date_published.isoformat()
                        if episode.date_published
                        else None,
                        "enclosure_url": episode.enclosure_url,  # This is the playback URL
                        "enclosure_type": episode.enclosure_type,
                        "enclosure_length": episode.enclosure_length,
                        "duration_seconds": episode.duration_seconds,
                        "explicit": episode.explicit,
                        "episode_type": episode.episode_type,
                        "season": episode.season,
                        "episode": episode.episode,
                        "feed_id": episode.feed_id,
                        "feed_title": episode.feed_title,
                        "image": episode.image,
                    }

                    processed_item = self._process_episode_item(episode_dict)
                    results.append(processed_item)

                logger.info(f"Successfully fetched {len(results)} episodes for podcast {feed_id}")
                return EpisodeListResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=results,
                    total_results=len(results),
                    feed_id=feed_id,
                    error=None,
                )

        except Exception as e:
            logger.error(f"Error fetching episodes for podcast {feed_id}: {e}")
            return EpisodeListResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                feed_id=feed_id,
                error=str(e),
            )

    @RedisCache.use_cache(PodcastFunctionCache, prefix="podcast_with_latest_episode")
    async def get_podcast_with_latest_episode(self, feed_id: int) -> PodcastWithLatestEpisode:
        """
        Get a podcast with its most recent episode included.

        Args:
            feed_id: PodcastIndex feed ID

        Returns:
            PodcastWithLatestEpisode object with latest episode info, or None if not found
        """
        try:
            # Get podcast details and latest episode in parallel
            async with await self.get_client() as client:
                # Get podcast details
                feed = await client.podcast_by_feedid(feed_id)

                if not feed:
                    return PodcastWithLatestEpisode(error="Podcast not found", status_code=404)

                # Get the most recent episode (limit to 1)
                episodes = await client.episodes_by_feedid(feed_id=feed_id, max_results=1)

                # Convert PodcastFeed to dict format
                podcast_dict = {
                    "id": feed.id,
                    "title": feed.title,
                    "url": feed.url,
                    "site": feed.site,
                    "description": feed.description,
                    "author": feed.author,
                    "owner_name": feed.owner_name,
                    "image": feed.image,
                    "artwork": feed.artwork,
                    "last_update_time": feed.last_update_time.isoformat()
                    if feed.last_update_time
                    else None,
                    "trend_score": feed.trend_score,
                    "language": feed.language,
                    "categories": feed.categories,
                    "episode_count": self._safe_episode_count(feed.episode_count),
                    "itunes_id": feed.itunes_id,
                    "podcast_guid": feed.podcast_guid,
                }

                # Try to get Spotify URL
                try:
                    if feed.title:
                        spotify_url = await client.get_podcast_link(feed.title)
                        podcast_dict["spotify_url"] = spotify_url
                        if spotify_url:
                            logger.info(f"Found Spotify URL for {feed.title}: {spotify_url}")
                        else:
                            logger.debug(f"No Spotify URL found for {feed.title}")
                except Exception as e:
                    logger.warning(f"Failed to fetch Spotify URL for {feed.title}: {e}")
                    podcast_dict["spotify_url"] = None

                processed_podcast = self._process_podcast_item(podcast_dict)

                # Add latest episode if available
                latest_episode_result = None
                if episodes:
                    latest_episode = episodes[0]
                    latest_episode_dict = {
                        "id": latest_episode.id,
                        "title": latest_episode.title,
                        "description": latest_episode.description,
                        "date_published": latest_episode.date_published.isoformat()
                        if latest_episode.date_published
                        else None,
                        "enclosure_url": latest_episode.enclosure_url,  # Playback URL
                        "enclosure_type": latest_episode.enclosure_type,
                        "duration_seconds": latest_episode.duration_seconds,
                        "image": latest_episode.image,
                        "link": latest_episode.link,
                        "guid": latest_episode.guid,
                        "enclosure_length": latest_episode.enclosure_length,
                        "explicit": latest_episode.explicit,
                        "episode_type": latest_episode.episode_type,
                        "season": latest_episode.season,
                        "episode": latest_episode.episode,
                        "feed_id": latest_episode.feed_id,
                        "feed_title": latest_episode.feed_title,
                    }
                    latest_episode_result = self._process_episode_item(latest_episode_dict)

                # Create PodcastWithLatestEpisode object
                podcast_dict = processed_podcast.model_dump()
                podcast_dict["latest_episode"] = latest_episode_result
                result = PodcastWithLatestEpisode.model_validate(podcast_dict)

                logger.info(
                    f"Successfully fetched podcast {feed_id} with latest episode: {result.title}"
                )
                return result

        except Exception as e:
            logger.error(f"Error fetching podcast with latest episode {feed_id}: {e}")
            return PodcastWithLatestEpisode(error=str(e), status_code=404)

    @RedisCache.use_cache(PodcastFunctionCache, prefix="search_by_person")
    async def search_by_person(
        self, person_name: str, max_results: int = 20
    ) -> PersonSearchResponse:
        """
        Search for podcasts and episodes by person name.
        Separates results into:
        - Podcasts: Where person is host/creator (matches feed author/owner)
        - Episodes: Where person is guest (doesn't match feed author/owner)

        Args:
            person_name: Name of the person to search for
            max_results: Maximum number of results to return (default=20)

        Returns:
            PersonSearchResponse with separated podcasts and episodes
        """
        try:
            async with await self.get_client() as client:
                # Get episodes from person search
                episodes = await client.search_episodes_by_person(
                    person_name=person_name, max_results=max_results
                )

                # Normalize person name for comparison (lowercase, strip)
                person_name_normalized = person_name.lower().strip()

                # Track unique feed IDs we've already fetched
                feed_cache: dict[int, Any] = {}
                podcasts_dict: dict[int, MCPodcastItem] = {}  # Use dict to deduplicate
                guest_episodes: list[MCEpisodeItem] = []

                # Process each episode
                for episode in episodes:
                    feed_id = episode.feed_id
                    if not feed_id:
                        # No feed_id, treat as guest episode
                        episode_dict = {
                            "id": episode.id,
                            "title": episode.title,
                            "description": episode.description,
                            "link": episode.link,
                            "guid": episode.guid,
                            "date_published": episode.date_published.isoformat()
                            if episode.date_published
                            else None,
                            "enclosure_url": episode.enclosure_url,
                            "enclosure_type": episode.enclosure_type,
                            "enclosure_length": episode.enclosure_length,
                            "duration_seconds": episode.duration_seconds,
                            "explicit": episode.explicit,
                            "episode_type": episode.episode_type,
                            "season": episode.season,
                            "episode": episode.episode,
                            "feed_id": episode.feed_id,
                            "feed_title": episode.feed_title,
                            "image": episode.image,
                        }
                        processed_item = self._process_episode_item(episode_dict)
                        guest_episodes.append(processed_item)
                        continue

                    # Fetch feed if not cached
                    if feed_id not in feed_cache:
                        feed = await client.podcast_by_feedid(feed_id)
                        if feed:
                            feed_cache[feed_id] = feed

                    feed = feed_cache.get(feed_id)
                    if not feed:
                        # Feed not found, treat as guest episode
                        episode_dict = {
                            "id": episode.id,
                            "title": episode.title,
                            "description": episode.description,
                            "link": episode.link,
                            "guid": episode.guid,
                            "date_published": episode.date_published.isoformat()
                            if episode.date_published
                            else None,
                            "enclosure_url": episode.enclosure_url,
                            "enclosure_type": episode.enclosure_type,
                            "enclosure_length": episode.enclosure_length,
                            "duration_seconds": episode.duration_seconds,
                            "explicit": episode.explicit,
                            "episode_type": episode.episode_type,
                            "season": episode.season,
                            "episode": episode.episode,
                            "feed_id": episode.feed_id,
                            "feed_title": episode.feed_title,
                            "image": episode.image,
                        }
                        processed_item = self._process_episode_item(episode_dict)
                        guest_episodes.append(processed_item)
                        continue

                    # Check if person matches feed author or owner (host/creator)
                    is_host = False
                    if feed.author:
                        author_normalized = feed.author.lower().strip()
                        if (
                            person_name_normalized in author_normalized
                            or author_normalized in person_name_normalized
                        ):
                            is_host = True

                    if not is_host and feed.owner_name:
                        owner_normalized = feed.owner_name.lower().strip()
                        if (
                            person_name_normalized in owner_normalized
                            or owner_normalized in person_name_normalized
                        ):
                            is_host = True

                    if is_host:
                        # Person is host/creator - add podcast (deduplicate by feed_id)
                        if feed_id not in podcasts_dict:
                            podcast_dict = {
                                "id": feed.id,
                                "title": feed.title,
                                "url": feed.url,
                                "site": feed.site,
                                "description": feed.description,
                                "author": feed.author,
                                "owner_name": feed.owner_name,
                                "image": feed.image,
                                "artwork": feed.artwork,
                                "last_update_time": feed.last_update_time.isoformat()
                                if feed.last_update_time
                                else None,
                                "trend_score": feed.trend_score,
                                "language": feed.language,
                                "categories": feed.categories,
                                "episode_count": self._safe_episode_count(feed.episode_count),
                                "itunes_id": feed.itunes_id,
                                "podcast_guid": feed.podcast_guid,
                            }
                            processed_podcast = self._process_podcast_item(podcast_dict)
                            podcasts_dict[feed_id] = processed_podcast
                    else:
                        # Person is guest - add episode
                        episode_dict = {
                            "id": episode.id,
                            "title": episode.title,
                            "description": episode.description,
                            "link": episode.link,
                            "guid": episode.guid,
                            "date_published": episode.date_published.isoformat()
                            if episode.date_published
                            else None,
                            "enclosure_url": episode.enclosure_url,
                            "enclosure_type": episode.enclosure_type,
                            "enclosure_length": episode.enclosure_length,
                            "duration_seconds": episode.duration_seconds,
                            "explicit": episode.explicit,
                            "episode_type": episode.episode_type,
                            "season": episode.season,
                            "episode": episode.episode,
                            "feed_id": episode.feed_id,
                            "feed_title": episode.feed_title,
                            "image": episode.image,
                        }
                        processed_item = self._process_episode_item(episode_dict)
                        guest_episodes.append(processed_item)

                podcasts_list = list(podcasts_dict.values())

                logger.info(
                    f"Person search '{person_name}': {len(podcasts_list)} podcasts (hosts), "
                    f"{len(guest_episodes)} episodes (guests)"
                )

                return PersonSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    podcasts=podcasts_list,
                    episodes=guest_episodes,
                    total_podcasts=len(podcasts_list),
                    total_episodes=len(guest_episodes),
                    person_name=person_name,
                )

        except Exception as e:
            logger.error(f"Error searching by person '{person_name}': {e}")
            return PersonSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                podcasts=[],
                episodes=[],
                total_podcasts=0,
                total_episodes=0,
                person_name=person_name,
                error=f"Error searching by person '{person_name}': {e}",
                status_code=500,
            )

    @RedisCache.use_cache(PodcastFunctionCache, prefix="search_person")
    async def search_person(
        self, person_name: str, max_results: int = 20
    ) -> PodcasterSearchResponse:
        """
        Search for podcasters by person name.
        Returns MCPodcaster items that aggregate all podcasts where the person is a host/creator.

        IMPORTANT: For person searches, requires EXACT name matching (case-insensitive).
        This prevents fuzzy matches like "Sal Pacinoski" when searching for "Al Pacino".

        Args:
            person_name: Name of the person to search for
            max_results: Maximum number of podcasters to return (default=20)

        Returns:
            PodcasterSearchResponse with MCPodcaster items
        """
        try:
            print(
                f"[DEBUG] Starting search_person for '{person_name}' with max_results={max_results}"
            )
            logger.info(
                f"Starting search_person for '{person_name}' with max_results={max_results}"
            )
            person_name_normalized = person_name.lower().strip()

            # Search for podcasts directly by person name (more reliable than searching episodes)
            # This finds podcasts where the person might be mentioned in title/author
            print(f"[DEBUG] Calling search_podcasts('{person_name}')...")
            logger.info(f"Calling search_podcasts('{person_name}')...")
            podcast_search_result = await self.search_podcasts(
                query=person_name,
                max_results=max_results * 2,  # Get more results to filter
            )
            results_count = (
                len(podcast_search_result.results)
                if podcast_search_result and hasattr(podcast_search_result, "results")
                else 0
            )
            print(
                f"[DEBUG] search_podcasts returned: error={podcast_search_result.error if podcast_search_result else 'None'}, "
                f"status_code={podcast_search_result.status_code if podcast_search_result else 'None'}, "
                f"results_count={results_count}"
            )
            logger.info(
                f"search_podcasts returned: error={podcast_search_result.error if podcast_search_result else 'None'}, "
                f"status_code={podcast_search_result.status_code if podcast_search_result else 'None'}, "
                f"results_count={results_count}"
            )

            # Also use search_by_person to get podcasts from episode matching
            print(f"[DEBUG] Calling search_by_person('{person_name}')...")
            logger.info(f"Calling search_by_person('{person_name}')...")
            person_search_result = await self.search_by_person(
                person_name=person_name, max_results=max_results
            )
            podcasts_count = (
                len(person_search_result.podcasts)
                if person_search_result and hasattr(person_search_result, "podcasts")
                else 0
            )
            episodes_count = (
                person_search_result.total_episodes
                if person_search_result and hasattr(person_search_result, "total_episodes")
                else 0
            )
            print(
                f"[DEBUG] search_by_person returned: error={person_search_result.error if person_search_result else 'None'}, "
                f"status_code={person_search_result.status_code if person_search_result else 'None'}, "
                f"podcasts_count={podcasts_count}, episodes_count={episodes_count}"
            )
            logger.info(
                f"search_by_person returned: error={person_search_result.error if person_search_result else 'None'}, "
                f"status_code={person_search_result.status_code if person_search_result else 'None'}, "
                f"podcasts_count={podcasts_count}, episodes_count={episodes_count}"
            )

            # Combine podcasts from both searches, deduplicate by feed ID
            all_podcasts_dict: dict[int, MCPodcastItem] = {}

            # Add podcasts from direct search
            if podcast_search_result and not podcast_search_result.error:
                logger.info(
                    f"Direct search_podcasts('{person_name}') returned {len(podcast_search_result.results)} results"
                )
                for idx, podcast in enumerate(podcast_search_result.results[:5]):  # Log first 5
                    logger.info(
                        f"  [{idx + 1}] ID={podcast.id}, Title='{podcast.title}', "
                        f"Author='{podcast.author}', Owner='{podcast.owner_name}'"
                    )
                for podcast in podcast_search_result.results:
                    if podcast.id and podcast.id not in all_podcasts_dict:
                        all_podcasts_dict[podcast.id] = podcast

            # Add podcasts from person search (episode-based)
            if person_search_result and not person_search_result.error:
                logger.info(
                    f"search_by_person('{person_name}') returned {len(person_search_result.podcasts)} podcasts, "
                    f"{person_search_result.total_episodes} episodes"
                )
                for idx, podcast in enumerate(person_search_result.podcasts[:5]):  # Log first 5
                    logger.info(
                        f"  [{idx + 1}] ID={podcast.id}, Title='{podcast.title}', "
                        f"Author='{podcast.author}', Owner='{podcast.owner_name}'"
                    )
                for podcast in person_search_result.podcasts:
                    if podcast.id and podcast.id not in all_podcasts_dict:
                        all_podcasts_dict[podcast.id] = podcast

            logger.info(
                f"Found {len(all_podcasts_dict)} unique podcasts from search for '{person_name}' "
                f"({len(podcast_search_result.results) if podcast_search_result else 0} from direct search, "
                f"{len(person_search_result.podcasts) if person_search_result else 0} from person search)"
            )

            # Filter podcasts to ensure they match the person name (author/owner/title)
            matched_podcasts = []

            logger.info(
                f"Filtering {len(all_podcasts_dict)} podcasts for matches to '{person_name}'"
            )
            for podcast in all_podcasts_dict.values():
                # Check if author or owner_name matches (exact or contains person name)
                is_match = False
                match_reason = None

                if podcast.author:
                    author_normalized = podcast.author.lower().strip()
                    # Exact match or person name is contained in author (substring match)
                    if author_normalized == person_name_normalized:
                        is_match = True
                        match_reason = f"author exact match: '{podcast.author}'"
                    elif person_name_normalized in author_normalized:
                        is_match = True
                        match_reason = f"author contains name: '{podcast.author}'"

                if not is_match and podcast.owner_name:
                    owner_normalized = podcast.owner_name.lower().strip()
                    # Exact match or person name is contained in owner_name (substring match)
                    if owner_normalized == person_name_normalized:
                        is_match = True
                        match_reason = f"owner exact match: '{podcast.owner_name}'"
                    elif person_name_normalized in owner_normalized:
                        is_match = True
                        match_reason = f"owner contains name: '{podcast.owner_name}'"

                # Fallback: check title if author/owner don't match or are missing
                # This handles cases where search_by_person found the podcast via episode matching
                # but author/owner fields aren't populated or don't match exactly
                if not is_match and podcast.title:
                    title_normalized = podcast.title.lower().strip()
                    # Check if person name is in title (e.g., "Joe Rogan" in "The Joe Rogan Experience")
                    if person_name_normalized in title_normalized:
                        is_match = True
                        match_reason = f"title contains name: '{podcast.title}'"

                if is_match:
                    logger.info(f"✓ MATCHED: '{podcast.title}' (ID={podcast.id}) - {match_reason}")
                    matched_podcasts.append(podcast)
                else:
                    logger.info(
                        f"✗ FILTERED: '{podcast.title}' (ID={podcast.id}) - "
                        f"Author='{podcast.author}', Owner='{podcast.owner_name}', Title='{podcast.title}'"
                    )

            # Group podcasts by person name (normalize for matching)
            # Since we're searching for a specific person, we'll create one MCPodcaster per unique name match
            podcasters_dict: dict[str, MCPodcaster] = {}

            # Only create podcaster if there are podcasts
            if not matched_podcasts:
                # No podcasts found for this person
                logger.info(
                    f"No podcasts with name match found for '{person_name}' "
                    f"(filtered from {len(all_podcasts_dict)} total podcasts)"
                )
                return PodcasterSearchResponse(
                    date=datetime.now(UTC).strftime("%Y-%m-%d"),
                    results=[],
                    total_results=0,
                    query=person_name,
                )

            # Use the person name from search (normalized)
            podcaster_name = person_name

            # Check if we already have a podcaster for this name
            if podcaster_name not in podcasters_dict:
                # Determine primary podcast (use first one with image, or first one)
                primary_podcast = None
                primary_podcast_id = None
                primary_podcast_title = None
                image = None
                bio = None
                website = None

                # Find best podcast for profile info (prefer one with image)
                for p in matched_podcasts:
                    if p.image:
                        primary_podcast = p
                        primary_podcast_id = p.id
                        primary_podcast_title = p.title
                        image = p.image
                        bio = p.description
                        website = p.site
                        break

                # If no podcast with image, use first one
                if not primary_podcast and matched_podcasts:
                    primary_podcast = matched_podcasts[0]
                    primary_podcast_id = primary_podcast.id
                    primary_podcast_title = primary_podcast.title
                    image = primary_podcast.image
                    bio = primary_podcast.description
                    website = primary_podcast.site

                # Calculate total episodes across all podcasts
                total_episodes = sum(p.episode_count for p in matched_podcasts)

                # Create MCPodcaster
                # Note: podcast_count will be set automatically by model_validator
                # Build composite source_id from all matched podcasts' source_ids
                source_id_parts = [p.source_id for p in matched_podcasts if p.source_id is not None]
                source_id = ",".join(source_id_parts) if source_id_parts else None
                podcaster = MCPodcaster(
                    name=podcaster_name,
                    id=str(primary_podcast_id),
                    podcasts=list(matched_podcasts),
                    total_episodes=total_episodes,
                    image=image,
                    bio=bio,
                    website=website,
                    primary_podcast_title=primary_podcast_title,
                    primary_podcast_id=primary_podcast_id,
                    source_id=source_id,
                )

                podcasters_dict[podcaster_name] = podcaster
            # Note: We process all podcasts for the person into a single MCPodcaster

            podcasters_list = list(podcasters_dict.values())

            logger.info(
                f"Podcaster search '{person_name}': {len(podcasters_list)} podcasters found"
            )

            return PodcasterSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=podcasters_list,
                total_results=len(podcasters_list),
                query=person_name,
            )

        except Exception as e:
            import traceback

            logger.error(f"Error searching for podcaster '{person_name}': {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return PodcasterSearchResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                results=[],
                total_results=0,
                query=person_name,
                error=f"Error searching for podcaster '{person_name}': {e}",
                status_code=500,
            )

    async def get_podcasts_by_ids(self, feed_ids: list[int]) -> list[MCPodcastItem]:
        """
        Get multiple podcasts by their feed IDs.

        Args:
            feed_ids: List of PodcastIndex feed IDs

        Returns:
            List of MCPodcastItem objects (may include error items for failed lookups)
        """
        try:
            # Fetch all podcasts in parallel
            tasks = [self.get_podcast_by_id(feed_id) for feed_id in feed_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            podcasts: list[MCPodcastItem] = []
            for i, result in enumerate(results):
                # Handle exceptions from gather
                if isinstance(result, (Exception, BaseException)):
                    logger.error(f"Error fetching podcast {feed_ids[i]}: {result}")
                    continue

                # At this point, result should be MCPodcastItem
                # Type guard: check if it's actually an MCPodcastItem
                if not isinstance(result, MCPodcastItem):
                    logger.warning(
                        f"Unexpected result type for podcast {feed_ids[i]}: {type(result)}"
                    )
                    continue

                # Check if podcast was successfully fetched
                if result.status_code == 200 and not result.error:
                    podcasts.append(result)
                else:
                    logger.warning(
                        f"Podcast {feed_ids[i]} not found or error: {result.error if result.error else 'Unknown error'}"
                    )

            logger.info(f"Successfully fetched {len(podcasts)}/{len(feed_ids)} podcasts by IDs")
            return podcasts

        except Exception as e:
            logger.error(f"Error fetching podcasts by IDs: {e}")
            return []


podcast_search_service = PodcastSearchService()

if __name__ == "__main__":
    import asyncio

    asyncio.run(podcast_search_service.search_person(person_name="Eric Gould"))
