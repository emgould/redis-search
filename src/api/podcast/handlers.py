"""
Podcast-focused Firebase Functions handlers.
Handles trending podcasts, search, and other podcast-related functionality.
"""

import json
import logging
from datetime import datetime

from firebase_functions import https_fn

from api.podcast.wrappers import podcast_wrapper
from utils.cache import EnhancedJSONEncoder

# Configure logging
logger = logging.getLogger(__name__)


class PodcastHandler:
    """Class containing all podcast-focused Firebase Functions."""

    def __init__(self):
        """Initialize Podcast handler. API keys are accessed from secrets at runtime."""
        logger.info("PodcastHandler initialized - API keys will be resolved at runtime")

    async def get_trending_podcasts(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending podcasts using PodcastIndex API.

        Usage:
        GET /get_trending_podcasts                    # Default: top 25 podcasts
        GET /get_trending_podcasts?max_results=10    # Limit results
        GET /get_trending_podcasts?lang=es           # Filter by language
        """
        try:
            # Parse request parameters
            max_results = int(req.args.get("max_results", 25))
            lang = req.args.get("lang", "en")

            # Validate limits
            if max_results < 1 or max_results > 100:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Fetch trending podcasts
            try:
                result = await podcast_wrapper.get_trending_podcasts(
                    max_results=max_results,
                    lang=lang,
                )
                if result.status_code != 200 or result.error:
                    error_msg = result.error or "Unknown error"
                    logger.error(f"Error fetching trending podcasts: {error_msg}")
                    podcast_data = {
                        "results": [],
                        "total_results": 0,
                        "language": lang,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }
                else:
                    podcast_data = {
                        "results": [p.model_dump() for p in result.results],
                        "total_results": result.total_results,
                        "language": lang,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }

            except Exception as e:
                logger.error(f"Error fetching trending podcasts: {e}")
                raise e

            # Log success
            trending_podcasts = podcast_data.get("results", [])
            logger.info(
                f"Successfully fetched {len(trending_podcasts) if isinstance(trending_podcasts, list) else 0} trending podcasts"
            )

            return https_fn.Response(
                json.dumps(podcast_data, cls=EnhancedJSONEncoder),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_trending_podcasts function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch trending podcasts",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_podcasts(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for podcasts using PodcastIndex API.

        Usage:
        GET /search_podcasts?query=true%20crime&max_results=20
        """
        try:
            # Parse request parameters
            query = req.args.get("query")
            max_results = int(req.args.get("max_results", 20))

            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if max_results < 1 or max_results > 100:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Search for podcasts
            try:
                result = await podcast_wrapper.search_podcasts(query=query, max_results=max_results)
                if result.status_code != 200 or result.error:
                    error_msg = result.error or "Unknown error"
                    logger.error(f"Error searching podcasts for '{query}': {error_msg}")
                    search_data = {
                        "results": [],
                        "total_results": 0,
                        "query": query,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }
                else:
                    search_data = {
                        "results": [p.model_dump() for p in result.results],
                        "total_results": result.total_results,
                        "query": query,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }

            except Exception as e:
                logger.error(f"Error searching podcasts for '{query}': {e}")
                raise e

            # Log success
            results = search_data.get("results", [])
            logger.info(
                f"Successfully searched podcasts for '{query}': {len(results) if isinstance(results, list) else 0} results"
            )

            return https_fn.Response(
                json.dumps(search_data, cls=EnhancedJSONEncoder),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in search_podcasts function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {"error": "Internal server error", "message": "Failed to search podcasts"}
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_podcast_details(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get detailed information for a specific podcast by feed ID.

        Usage:
        GET /get_podcast_details?feed_id=12345
        """
        try:
            # Parse request parameters
            feed_id_str = req.args.get("feed_id")

            if not feed_id_str:
                return https_fn.Response(
                    json.dumps({"error": "feed_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                feed_id = int(feed_id_str)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "feed_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Get podcast details with latest episode
            try:
                # Use the new method that includes latest episode
                result = await podcast_wrapper.get_podcast_with_latest_episode(feed_id)
                if result.status_code != 200 or result.error:
                    error_msg = result.error or "Unknown error"
                    if result.status_code == 404:
                        return https_fn.Response(
                            json.dumps({"error": "Podcast not found"}),
                            status=404,
                            headers={"Content-Type": "application/json"},
                        )
                    logger.error(f"Error fetching podcast details for {feed_id}: {error_msg}")
                    podcast_data = {
                        "podcast": None,
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {"generated_at": datetime.now().isoformat()},
                    }
                else:
                    podcast_data = {
                        "podcast": result.model_dump(),
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {"generated_at": datetime.now().isoformat()},
                    }

            except Exception as e:
                logger.error(f"Error fetching podcast details for {feed_id}: {e}")
                raise e

            # Log success
            podcast_title = None
            if podcast_data["podcast"] and isinstance(podcast_data["podcast"], dict):
                podcast_title = podcast_data["podcast"].get("title")
            logger.info(f"Successfully fetched podcast details for {feed_id}: {podcast_title}")

            return https_fn.Response(
                json.dumps(podcast_data, cls=EnhancedJSONEncoder),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_podcast_details function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {"error": "Internal server error", "message": "Failed to fetch podcast details"}
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_podcast_episodes(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get episodes for a specific podcast with playback URLs.

        Usage:
        GET /get_podcast_episodes?feed_id=12345&max_results=20&since=1640995200
        """
        try:
            # Parse request parameters
            feed_id_str = req.args.get("feed_id")
            max_results = int(req.args.get("max_results", 25))
            since_str = req.args.get("since")

            if not feed_id_str:
                return https_fn.Response(
                    json.dumps({"error": "feed_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                feed_id = int(feed_id_str)
                since = int(since_str) if since_str else None
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "feed_id and since must be valid integers"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if max_results < 1 or max_results > 100:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Get podcast episodes
            try:
                result = await podcast_wrapper.get_podcast_episodes(
                    feed_id=feed_id, max_results=max_results, since=since
                )
                if result.status_code != 200 or result.error:
                    error_msg = result.error or "Unknown error"
                    logger.error(f"Error fetching episodes for podcast {feed_id}: {error_msg}")
                    episodes_data = {
                        "results": [],
                        "total_results": 0,
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "since": since,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }
                else:
                    episodes_data = {
                        "results": [e.model_dump() for e in result.results],
                        "total_results": result.total_results,
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {
                            "max_results": max_results,
                            "since": since,
                            "generated_at": datetime.now().isoformat(),
                        },
                    }

            except Exception as e:
                logger.error(f"Error fetching episodes for podcast {feed_id}: {e}")
                raise e

            # Log success
            episodes = episodes_data.get("results", [])
            logger.info(
                f"Successfully fetched {len(episodes) if isinstance(episodes, list) else 0} episodes for podcast {feed_id}"
            )

            return https_fn.Response(
                json.dumps(episodes_data, cls=EnhancedJSONEncoder),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in get_podcast_episodes function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch podcast episodes",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_podcast_details_with_latest_episode(
        self, req: https_fn.Request
    ) -> https_fn.Response:
        """
        Get detailed information for a specific podcast including the most recent episode with playback URL.

        Usage:
        GET /get_podcast_details_with_latest_episode?feed_id=12345
        """
        try:
            # Parse request parameters
            feed_id_str = req.args.get("feed_id")

            if not feed_id_str:
                return https_fn.Response(
                    json.dumps({"error": "feed_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                feed_id = int(feed_id_str)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "feed_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Get podcast details with latest episode
            try:
                result = await podcast_wrapper.get_podcast_with_latest_episode(feed_id)
                if result.status_code != 200 or result.error:
                    error_msg = result.error or "Unknown error"
                    if result.status_code == 404:
                        return https_fn.Response(
                            json.dumps({"error": "Podcast not found"}),
                            status=404,
                            headers={"Content-Type": "application/json"},
                        )
                    logger.error(
                        f"Error fetching podcast with latest episode for {feed_id}: {error_msg}"
                    )
                    podcast_data = {
                        "podcast": None,
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {"generated_at": datetime.now().isoformat()},
                    }
                else:
                    podcast_data = {
                        "podcast": result.model_dump(),
                        "feed_id": feed_id,
                        "data_source": "PodcastIndex.org",
                        "metadata": {"generated_at": datetime.now().isoformat()},
                    }

            except Exception as e:
                logger.error(f"Error fetching podcast with latest episode for {feed_id}: {e}")
                raise e

            # Log success
            podcast_title = None
            if podcast_data["podcast"] and isinstance(podcast_data["podcast"], dict):
                podcast_title = podcast_data["podcast"].get("title")
            logger.info(
                f"Successfully fetched podcast with latest episode for {feed_id}: {podcast_title}"
            )

            return https_fn.Response(
                json.dumps(podcast_data, cls=EnhancedJSONEncoder),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in get_podcast_details_with_latest_episode function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch podcast details with latest episode",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create global handler instance
podcast_handler = PodcastHandler()

# Export bound methods for direct import
search_podcasts = podcast_handler.search_podcasts
