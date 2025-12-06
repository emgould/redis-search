"""
Google Books Firebase Functions handlers.
Handles book search and volume lookup functionality.
"""

import json
import logging
from datetime import datetime

from firebase_functions import https_fn

from api.subapi._google.auth import Auth
from api.subapi._google.wrappers import google_books_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class GoogleBooksHandler(Auth):
    """Class containing all Google Books-focused Firebase Functions."""

    def __init__(self):
        """Initialize Google Books handler. API keys are accessed from secrets at runtime."""
        super().__init__()
        logger.info("GoogleBooksHandler initialized - API keys will be resolved at runtime")

    async def search_books(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for books using Google Books API.

        Query Parameters:
        - query: General search query
        - title: Book title
        - author: Author name
        - isbn: ISBN (10 or 13 digit)
        - max_results: Number of results (1-40, default: 10)
        - start_index: Starting index for pagination (default: 0)
        - order_by: Sort order ('relevance' or 'newest', default: 'relevance')

        Returns:
            JSON response with book search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            title = req.args.get("title")
            author = req.args.get("author")
            isbn = req.args.get("isbn")
            max_results = int(req.args.get("max_results", 10))
            start_index = int(req.args.get("start_index", 0))
            order_by = req.args.get("order_by", "relevance")

            # Validate parameters
            if not any([query, title, author, isbn]):
                return https_fn.Response(
                    json.dumps({"error": "At least one search parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if max_results < 1 or max_results > 40:
                return https_fn.Response(
                    json.dumps({"error": "max_results must be between 1 and 40"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if order_by not in ["relevance", "newest"]:
                return https_fn.Response(
                    json.dumps({"error": "order_by must be 'relevance' or 'newest'"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Searching Google Books: query={query}, title={title}, author={author}, isbn={isbn}"
            )

            # Get API key
            api_key = self.google_books_api_key

            # Call wrapper
            result = await google_books_wrapper.search_books(
                query=query,
                title=title,
                author=author,
                isbn=isbn,
                max_results=max_results,
                start_index=start_index,
                order_by=order_by,
                api_key=api_key,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Add metadata
            result_dict = result.model_dump()
            result_dict["metadata"] = {
                "search_params": {
                    "query": query,
                    "title": title,
                    "author": author,
                    "isbn": isbn,
                    "max_results": max_results,
                    "start_index": start_index,
                    "order_by": order_by,
                },
                "generated_at": datetime.now().isoformat(),
                "source": "Google Books API",
            }

            logger.info(
                f"Successfully found {result_dict.get('totalItems', 0)} books "
                f"(returned {len(result_dict.get('items', []))} items)"
            )

            return https_fn.Response(
                json.dumps(result_dict),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=604800",  # Cache for 7 days
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
            logger.error(f"Unexpected error in search_books: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_volume(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get a specific volume by Google Books ID.

        Query Parameters:
        - volume_id: Google Books volume ID (required)

        Returns:
            JSON response with volume details
        """
        try:
            # Parse request parameters
            volume_id = req.args.get("volume_id")

            if not volume_id:
                return https_fn.Response(
                    json.dumps({"error": "volume_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Getting Google Books volume: {volume_id}")

            # Get API key
            api_key = self.google_books_api_key

            # Call wrapper
            result = await google_books_wrapper.get_volume_by_id(
                volume_id=volume_id,
                api_key=api_key,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Add metadata
            result_dict = result.model_dump()
            response_data = {
                "data": result_dict,
                "metadata": {
                    "volume_id": volume_id,
                    "generated_at": datetime.now().isoformat(),
                    "source": "Google Books API",
                },
            }

            logger.info(f"Successfully fetched volume: {volume_id}")

            return https_fn.Response(
                json.dumps(response_data),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=604800",  # Cache for 7 days
                },
            )

        except Exception as e:
            logger.error(f"Error in get_volume: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_volume_by_isbn(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get a volume by ISBN.

        Query Parameters:
        - isbn: ISBN-10 or ISBN-13 (required)

        Returns:
            JSON response with volume details
        """
        try:
            # Parse request parameters
            isbn = req.args.get("isbn")

            if not isbn:
                return https_fn.Response(
                    json.dumps({"error": "isbn parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Getting Google Books volume by ISBN: {isbn}")

            # Get API key
            api_key = self.google_books_api_key

            # Call wrapper
            result = await google_books_wrapper.get_volume_by_isbn(
                isbn=isbn,
                api_key=api_key,
            )

            # Check for errors
            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            # Add metadata
            result_dict = result.model_dump()
            response_data = {
                "data": result_dict,
                "metadata": {
                    "isbn": isbn,
                    "generated_at": datetime.now().isoformat(),
                    "source": "Google Books API",
                },
            }

            logger.info(f"Successfully fetched volume by ISBN: {isbn}")

            return https_fn.Response(
                json.dumps(response_data),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=604800",  # Cache for 7 days
                },
            )

        except Exception as e:
            logger.error(f"Error in get_volume_by_isbn: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create global handler instance
google_books_handler = GoogleBooksHandler()
