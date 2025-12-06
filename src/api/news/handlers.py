"""
News-focused Firebase Functions handlers.
Handles trending news, search, and other news-related functionality.
"""

import json
import logging

from firebase_functions import https_fn

from api.news.wrappers import news_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class NewsHandler:
    """Class containing all News-focused Firebase Functions."""

    def __init__(self):
        """Initialize News handler. API keys are accessed from secrets at runtime."""
        logger.info("NewsHandler initialized - API keys will be resolved at runtime")

    async def get_trending_news(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending news articles.

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
            page_size = int(req.args.get("page_size", 20))
            page = int(req.args.get("page", 1))

            # Validate parameters
            if page_size < 1 or page_size > 100:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be 1 or greater"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate country code (basic check for 2 letters)
            if len(country) != 2 or not country.isalpha():
                return https_fn.Response(
                    json.dumps({"error": "country must be a 2-letter ISO 3166-1 country code"}),
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
            if category and category not in valid_categories:
                return https_fn.Response(
                    json.dumps(
                        {"error": f"category must be one of: {', '.join(valid_categories)}"}
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Getting trending news for country: {country}, query: {query}")

            # Call wrapper
            result = await news_wrapper.get_trending_news(
                country=country, query=query, category=category, page_size=page_size, page=page
            )

            # Check for errors
            status_code = result.status_code
            if status_code != 200 or result.error:
                # Check for specific error types
                error_msg = result.error or ""
                if "API key" in str(error_msg).lower():
                    status_code = 401
                elif "rate limit" in str(error_msg).lower():
                    status_code = 429
                result.status_code = status_code

                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched {result.total_results} trending news articles")

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
            logger.error(f"Unexpected error in get_trending_news: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_news(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search news articles.

        Query Parameters:
        - query: Search query string (required)
        - from_date: Oldest article date (YYYY-MM-DD format)
        - to_date: Newest article date (YYYY-MM-DD format)
        - language: Language code (default: en)
        - sort_by: Sort order (relevancy, popularity, publishedAt, default: publishedAt)
        - page_size: Number of articles to return (1-100, default: 20)
        - page: Page number for pagination (default: 1)

        Returns:
            JSON response with search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            from_date = req.args.get("from_date")
            to_date = req.args.get("to_date")
            language = req.args.get("language", "en")
            sort_by = req.args.get("sort_by", "publishedAt")
            page_size = int(req.args.get("page_size", 20))
            page = int(req.args.get("page", 1))

            # Validate parameters
            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page_size < 1 or page_size > 100:
                return https_fn.Response(
                    json.dumps({"error": "page_size must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be 1 or greater"}),
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
            result = await news_wrapper.search_news(
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
                if "API key" in str(error_msg).lower():
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
            logger.error(f"Parameter validation error: {e}")
            return https_fn.Response(
                json.dumps({"error": "Invalid parameters", "message": str(e)}),
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
        Get available news sources.

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
            result = await news_wrapper.get_news_sources(
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


# Create News handler instance
news_handler = NewsHandler()
