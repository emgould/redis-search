"""
NewsAI-focused Firebase Functions handlers.
Handles trending news, search, and other news-related functionality using Event Registry API.
Drop-in replacement for news API handlers.
"""

import json
import logging

from firebase_functions import https_fn

from api.newsai.wrappers import newsai_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class NewsAIHandler:
    """Class containing all NewsAI-focused Firebase Functions."""

    def __init__(self):
        """Initialize NewsAI handler. API keys are accessed from secrets at runtime."""
        logger.info("NewsAIHandler initialized - API keys will be resolved at runtime")

    async def get_trending_events(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending news events (clusters of related articles) using Event Registry.

        This is the PREFERRED endpoint for trending news as it returns events (news stories)
        rather than individual articles.

        Query Parameters:
        - country: 2-letter ISO 3166-1 country code (default: us)
        - query: Optional search query to filter events
        - category: Optional category (business, entertainment, general, health, science, sports, technology)
        - page_size: Number of events to return (1-50, default: 20)
        - page: Page number for pagination (default: 1)

        Returns:
            JSON response with trending events data
        """
        try:
            # Parse query parameters
            country = req.args.get("country", "us").lower()
            query = req.args.get("query")
            category = req.args.get("category")

            # Parse pagination parameters
            try:
                page_size = int(req.args.get("page_size", "20"))
            except (ValueError, TypeError):
                page_size = 20

            try:
                page = int(req.args.get("page", "1"))
            except (ValueError, TypeError):
                page = 1

            # Validate page_size (events max is 50)
            if page_size < 1 or page_size > 50:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate page
            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be >= 1"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate category if provided
            valid_categories = [
                "business",
                "entertainment",
                "general",
                "health",
                "science",
                "sports",
                "technology",
            ]
            if category and category.lower() not in valid_categories:
                return https_fn.Response(
                    json.dumps(
                        {"error": f"category must be one of: {', '.join(valid_categories)}"}
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Fetching trending events for country: {country}")

            # Call wrapper
            result = await newsai_wrapper.get_trending_events(
                country=country,
                query=query,
                category=category,
                page_size=page_size,
                page=page,
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                # Check for specific error types
                error_msg = result.error or ""
                if (
                    "API key" in str(error_msg).lower()
                    or "authentication" in str(error_msg).lower()
                ):
                    status_code = 401
                elif "rate limit" in str(error_msg).lower():
                    status_code = 429
                result.status_code = status_code

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully fetched {result.total_results} trending events for country: {country}"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Validation error in get_trending_events: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid request parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Unexpected error in get_trending_events: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_trending_news(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending news articles using Event Registry.

        Query Parameters:
        - country: 2-letter ISO 3166-1 country code (default: us)
        - query: Optional search query to filter articles
        - category: Optional category (business, entertainment, general, health, science, sports, technology)
        - page_size: Number of articles to return (1-100, default: 20)
        - page: Page number for pagination (default: 1)

        Returns:
            JSON response with trending news data
        """
        try:
            # Parse query parameters
            country = req.args.get("country", "us").lower()
            query = req.args.get("query")
            category = req.args.get("category")

            # Parse pagination parameters
            try:
                page_size = int(req.args.get("page_size", "20"))
            except (ValueError, TypeError):
                page_size = 20

            try:
                page = int(req.args.get("page", "1"))
            except (ValueError, TypeError):
                page = 1

            # Validate page_size
            if page_size < 1 or page_size > 100:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate page
            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be >= 1"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate category if provided
            valid_categories = [
                "business",
                "entertainment",
                "general",
                "health",
                "science",
                "sports",
                "technology",
            ]
            if category and category.lower() not in valid_categories:
                return https_fn.Response(
                    json.dumps(
                        {"error": f"category must be one of: {', '.join(valid_categories)}"}
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Fetching trending news for country: {country}")

            # Call wrapper
            result = await newsai_wrapper.get_trending_news(
                country=country,
                query=query,
                category=category,
                page_size=page_size,
                page=page,
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                # Check for specific error types
                error_msg = result.error or ""
                if (
                    "API key" in str(error_msg).lower()
                    or "authentication" in str(error_msg).lower()
                ):
                    status_code = 401
                elif "rate limit" in str(error_msg).lower():
                    status_code = 429
                result.status_code = status_code

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully fetched {result.total_results} trending news articles for country: {country}"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Validation error in get_trending_news: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid request parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Unexpected error in get_trending_news: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_news(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for news articles using Event Registry.

        Query Parameters:
        - query: Search query string (required)
        - from_date: Oldest article date (YYYY-MM-DD format)
        - to_date: Newest article date (YYYY-MM-DD format)
        - language: Language code (en, es, fr, de, it, etc., default: en)
        - sort_by: Sort order (relevancy, popularity, publishedAt, default: publishedAt)
        - page_size: Number of articles to return (1-100, default: 20)
        - page: Page number for pagination (default: 1)

        Returns:
            JSON response with search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            from_date = req.args.get("from_date") or req.args.get("from")
            to_date = req.args.get("to_date") or req.args.get("to")
            language = req.args.get("language", "en")
            sort_by = req.args.get("sort_by", "publishedAt")

            # Parse pagination parameters
            try:
                page_size = int(req.args.get("page_size", "20"))
            except (ValueError, TypeError):
                page_size = 20

            try:
                page = int(req.args.get("page", "1"))
            except (ValueError, TypeError):
                page = 1

            # Validate page_size
            if page_size < 1 or page_size > 100:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate page
            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be >= 1"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            valid_sort_orders = ["relevancy", "popularity", "publishedAt"]
            if sort_by not in valid_sort_orders:
                return https_fn.Response(
                    json.dumps(
                        {"error": f"sort_by must be one of: {', '.join(valid_sort_orders)}"}
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Searching news for: {query}")

            # Call wrapper
            result = await newsai_wrapper.search_news(
                query=query,
                from_date=from_date,
                to_date=to_date,
                language=language,
                sort_by=sort_by,
                page_size=page_size,
                page=page,
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                # Check for specific error types
                error_msg = result.error or ""
                if (
                    "API key" in str(error_msg).lower()
                    or "authentication" in str(error_msg).lower()
                ):
                    status_code = 401
                elif "rate limit" in str(error_msg).lower():
                    status_code = 429
                result.status_code = status_code

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully found {result.total_results} news articles for query: {query}"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Validation error in search_news: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid request parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Unexpected error in search_news: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_news_sources(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get available news sources using Event Registry.

        Query Parameters:
        - category: Optional category filter
        - language: Optional language filter
        - country: Optional country filter

        Returns:
            JSON response with news sources
        """
        try:
            # Parse query parameters
            category = req.args.get("category")
            language = req.args.get("language")
            country = req.args.get("country")

            logger.info(
                f"Getting news sources for category: {category}, language: {language}, country: {country}"
            )

            # Call wrapper
            result = await newsai_wrapper.get_news_sources(
                category=category, language=language, country=country
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched {len(result.results)} news sources")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except Exception as e:
            logger.error(f"Unexpected error in get_news_sources: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_media_reviews(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get recent critic reviews for a TV show or movie from top review sources.

        Query Parameters:
        - title: Title of the TV show or movie (required)
        - media_type: Type of media - 'movie' or 'tv' (required)
        - page_size: Number of articles to return (1-100, default: 20)
        - page: Page number for pagination (default: 1)

        Returns:
            JSON response with review articles
        """
        try:
            # Parse query parameters
            title = req.args.get("title")
            media_type = req.args.get("media_type")

            # Validate required parameters
            if not title:
                return https_fn.Response(
                    json.dumps({"error": "title parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if not media_type:
                return https_fn.Response(
                    json.dumps({"error": "media_type parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate media_type
            if media_type.lower() not in ["movie", "tv", "tv_series"]:
                return https_fn.Response(
                    json.dumps({"error": "media_type must be 'movie' or 'tv'"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Parse pagination parameters
            try:
                page_size = int(req.args.get("page_size", "20"))
            except (ValueError, TypeError):
                page_size = 20

            try:
                page = int(req.args.get("page", "1"))
            except (ValueError, TypeError):
                page = 1

            # Validate page_size
            if page_size < 1 or page_size > 100:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate page
            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be >= 1"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Fetching media reviews for {media_type}: {title}")

            # Call wrapper
            result = await newsai_wrapper.get_media_reviews(
                title=title,
                media_type=media_type,
                page_size=page_size,
                page=page,
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                # Check for specific error types
                error_msg = result.error or ""
                if (
                    "API key" in str(error_msg).lower()
                    or "authentication" in str(error_msg).lower()
                ):
                    status_code = 401
                elif "rate limit" in str(error_msg).lower():
                    status_code = 429
                result.status_code = status_code

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully fetched {result.total_results} review articles for {media_type}: {title}"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )

        except ValueError as e:
            logger.error(f"Validation error in get_media_reviews: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid request parameters", "message": str(e)}),
                status=400,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Unexpected error in get_media_reviews: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create NewsAI handler instance
newsai_handler = NewsAIHandler()
