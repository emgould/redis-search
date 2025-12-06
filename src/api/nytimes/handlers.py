"""
NYTimes Books API Firebase Functions Handlers

This module contains Firebase function handlers for the NYTimes Books API.
Handlers wrap the core service and provide HTTP endpoints for Firebase Functions.
"""

import asyncio
import json

from firebase_functions import https_fn

# Import from nytimes service modules directly (avoid circular import)
from api.nytimes.wrappers import nytimes_wrapper
from utils.get_logger import get_logger

# Configure logging
logger = get_logger(__name__)


class NYTimesHandler:
    """
    Firebase Functions handler for NYTimes Books API.

    Creates HTTP endpoints that exclusively use the nytimes_wrapper instance.
    All handlers call wrapper methods, never the core service directly.
    """

    def __init__(self):
        """Initialize NYTimes handler using auth service for API key."""

    def create_bestseller_lists_function(self):
        """Create the get_bestseller_lists Firebase function."""

        @https_fn.on_request()
        def get_bestseller_lists(req: https_fn.Request) -> https_fn.Response:
            """
            Get NYTimes bestseller lists.

            Usage:
            GET /get_bestseller_lists                    # Get all current lists
            GET /get_bestseller_lists?list=hardcover-fiction  # Get specific list
            GET /get_bestseller_lists?date=2024-01-15   # Get lists for specific date
            GET /get_bestseller_lists?published_date=latest  # Get latest lists
            """
            try:
                # Parse request parameters
                list_name = req.args.get("list")
                date = req.args.get("date")
                published_date = req.args.get("published_date", "current")

                # Use the wrapper (exclusively calls wrappers per migration guide)
                result = asyncio.run(
                    nytimes_wrapper.get_bestseller_lists(
                        list_name=list_name, date=date, published_date=published_date
                    )
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                # Convert model to dict
                result_dict = result.model_dump()

                return https_fn.Response(
                    json.dumps(
                        {
                            "data": result_dict,
                            "metadata": {
                                "list_name": list_name,
                                "date": date,
                                "published_date": published_date,
                                "generated_at": None,
                                "source": "NYTimes Books API",
                            },
                        }
                    ),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=3600",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_bestseller_lists: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_bestseller_lists

    def create_list_names_function(self):
        """Create the get_list_names Firebase function."""

        @https_fn.on_request()
        def get_list_names(req: https_fn.Request) -> https_fn.Response:
            """
            Get all available NYTimes bestseller list names.

            Usage:
            GET /get_list_names
            """
            try:
                # Use the wrapper (exclusively calls wrappers per migration guide)
                result = asyncio.run(nytimes_wrapper.get_list_names())

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                # Convert model to dict
                result_dict = result.model_dump()

                return https_fn.Response(
                    json.dumps(
                        {
                            "data": result_dict,
                            "metadata": {
                                "total_lists": result_dict.get("num_results", 0),
                                "generated_at": None,
                                "source": "NYTimes Books API",
                            },
                        }
                    ),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=86400",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_list_names: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_list_names

    def create_book_reviews_function(self):
        """Create the get_book_reviews Firebase function."""

        @https_fn.on_request()
        def get_book_reviews(req: https_fn.Request) -> https_fn.Response:
            """
            Get NYTimes book reviews.

            WARNING: This endpoint has been deprecated by NYTimes and returns 404.

            Usage:
            GET /get_book_reviews?author=Stephen+King
            GET /get_book_reviews?title=The+Great+Gatsby
            GET /get_book_reviews?isbn=9780743273565
            """
            try:
                # Parse request parameters
                author = req.args.get("author")
                title = req.args.get("title")
                isbn = req.args.get("isbn")

                if not any([author, title, isbn]):
                    return https_fn.Response(
                        json.dumps(
                            {
                                "error": "At least one of author, title, or isbn parameters is required"
                            }
                        ),
                        status=400,
                        headers={"Content-Type": "application/json"},
                    )

                # Use the wrapper (exclusively calls wrappers per migration guide)
                result = asyncio.run(
                    nytimes_wrapper.get_book_reviews(author=author, title=title, isbn=isbn)
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                # Convert model to dict
                result_dict = result.model_dump()

                return https_fn.Response(
                    json.dumps(
                        {
                            "data": result_dict,
                            "metadata": {
                                "search_params": {"author": author, "title": title, "isbn": isbn},
                                "total_reviews": result_dict.get("num_results", 0),
                                "generated_at": None,
                                "source": "NYTimes Books API",
                            },
                        }
                    ),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=7200",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_book_reviews: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_book_reviews

    def create_historical_bestsellers_function(self):
        """Create the get_historical_bestsellers Firebase function."""

        @https_fn.on_request()
        def get_historical_bestsellers(req: https_fn.Request) -> https_fn.Response:
            """
            Get historical NYTimes bestseller data for a specific list over a date range.

            Usage:
            GET /get_historical_bestsellers?list=hardcover-fiction&weeks_back=4
            GET /get_historical_bestsellers?list=combined-print-and-e-book-fiction&start_date=2024-01-01&end_date=2024-01-31
            """
            try:
                # Parse request parameters
                list_name = req.args.get("list")
                weeks_back_str = req.args.get("weeks_back", "4")
                start_date = req.args.get("start_date")
                end_date = req.args.get("end_date")

                if not list_name:
                    return https_fn.Response(
                        json.dumps({"error": "list parameter is required"}),
                        status=400,
                        headers={"Content-Type": "application/json"},
                    )

                try:
                    weeks_back = int(weeks_back_str)
                except ValueError:
                    weeks_back = 4

                # Use the refactored wrapper (uses auth service)
                result = asyncio.run(
                    nytimes_wrapper.get_historical_bestsellers(
                        list_name=list_name,
                        weeks_back=weeks_back,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                result_dict = result.model_dump()
                return https_fn.Response(
                    json.dumps(
                        {
                            "data": result_dict,
                            "metadata": {
                                "list_name": list_name,
                                "weeks_back": weeks_back,
                                "total_weeks": len(result_dict.get("historical_data", [])),
                                "generated_at": None,
                                "source": "NYTimes Books API",
                            },
                        }
                    ),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=14400",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_historical_bestsellers: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_historical_bestsellers

    def create_enhanced_bestseller_lists_function(self):
        """Create the get_bestseller_lists_with_covers Firebase function."""

        @https_fn.on_request()
        def get_bestseller_lists_with_covers(req: https_fn.Request) -> https_fn.Response:
            """
            Get NYTimes bestseller lists enriched with OpenLibrary cover images.

            Usage:
            GET /get_bestseller_lists_with_covers                    # Get all current lists with covers
            GET /get_bestseller_lists_with_covers?list=hardcover-fiction  # Get specific list with covers
            GET /get_bestseller_lists_with_covers?date=2024-01-15   # Get lists for specific date with covers
            """
            try:
                # Parse request parameters
                list_name = req.args.get("list")
                date = req.args.get("date")
                published_date = req.args.get("published_date", "current")

                # Use the refactored wrapper (uses auth service)
                result = asyncio.run(
                    nytimes_wrapper.get_bestseller_lists_with_covers(
                        list_name=list_name,
                        date=date,
                        published_date=published_date,
                    )
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                result_dict = result.model_dump()
                # Extract books_enriched_count from result metrics if present
                books_enriched_count = result_dict.get("metrics", {}).get("books_enriched_count", 0)

                return https_fn.Response(
                    json.dumps(
                        {
                            "data": result_dict,
                            "metadata": {
                                "list_name": list_name,
                                "date": date,
                                "published_date": published_date,
                                "books_enriched_count": books_enriched_count,
                                "generated_at": None,
                                "source": "NYTimes Books API + OpenLibrary",
                            },
                        }
                    ),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=3600",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_bestseller_lists_with_covers: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_bestseller_lists_with_covers

    def create_fiction_bestsellers_function(self):
        """Create the get_fiction_bestsellers Firebase function."""

        @https_fn.on_request()
        def get_fiction_bestsellers(req: https_fn.Request) -> https_fn.Response:
            """
            Get NYTimes Fiction bestsellers with covers.

            Usage:
            GET /get_fiction_bestsellers                    # Get current fiction bestsellers
            GET /get_fiction_bestsellers?date=2024-01-15   # Get fiction bestsellers for specific date
            """
            try:
                # Parse request parameters
                date = req.args.get("date")
                published_date = req.args.get("published_date", "current")

                # Use the refactored wrapper (uses auth service)
                result = asyncio.run(
                    nytimes_wrapper.get_fiction_bestsellers(
                        date=date, published_date=published_date
                    )
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                result_dict = result.model_dump()
                return https_fn.Response(
                    json.dumps(result_dict),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=3600",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_fiction_bestsellers: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_fiction_bestsellers

    def create_nonfiction_bestsellers_function(self):
        """Create the get_nonfiction_bestsellers Firebase function."""

        @https_fn.on_request()
        def get_nonfiction_bestsellers(req: https_fn.Request) -> https_fn.Response:
            """
            Get NYTimes Non-Fiction bestsellers with covers.

            Usage:
            GET /get_nonfiction_bestsellers                    # Get current non-fiction bestsellers
            GET /get_nonfiction_bestsellers?date=2024-01-15   # Get non-fiction bestsellers for specific date
            """
            try:
                # Parse request parameters
                date = req.args.get("date")
                published_date = req.args.get("published_date", "current")

                # Use the refactored wrapper (uses auth service)
                result = asyncio.run(
                    nytimes_wrapper.get_nonfiction_bestsellers(
                        date=date, published_date=published_date
                    )
                )

                if result.status_code != 200 or result.error:
                    return https_fn.Response(
                        json.dumps(result.model_dump()),
                        status=result.status_code,
                        headers={"Content-Type": "application/json"},
                    )

                result_dict = result.model_dump()
                return https_fn.Response(
                    json.dumps(result_dict),
                    status=200,
                    headers={
                        "Content-Type": "application/json",
                        "Cache-Control": "public, max-age=3600",
                    },
                )

            except Exception as e:
                logger.error(f"Error in get_nonfiction_bestsellers: {e}")
                return https_fn.Response(
                    json.dumps({"error": str(e)}),
                    status=500,
                    headers={"Content-Type": "application/json"},
                )

        return get_nonfiction_bestsellers

    def register_functions(self):
        """Register all NYTimes functions with Firebase."""
        return {
            "get_bestseller_lists": self.create_bestseller_lists_function(),
            "get_list_names": self.create_list_names_function(),
            "get_book_reviews": self.create_book_reviews_function(),
            "get_historical_bestsellers": self.create_historical_bestsellers_function(),
            "get_bestseller_lists_with_covers": self.create_enhanced_bestseller_lists_function(),
            "get_fiction_bestsellers": self.create_fiction_bestsellers_function(),
            "get_nonfiction_bestsellers": self.create_nonfiction_bestsellers_function(),
        }
