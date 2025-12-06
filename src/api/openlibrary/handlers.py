"""
OpenLibrary Firebase Functions handlers.
Handles book search and cover lookup functionality.
"""

import json
import logging
from datetime import datetime

from firebase_functions import https_fn

from api.openlibrary.wrappers import openlibrary_wrapper

# Configure logging
logger = logging.getLogger(__name__)


class OpenLibraryHandler:
    """Class containing all OpenLibrary-focused Firebase Functions."""

    def __init__(self):
        """Initialize OpenLibrary handler. API keys are accessed from secrets at runtime."""
        logger.info("OpenLibraryHandler initialized -No API Keys needed")

    async def search_books(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for books using OpenLibrary API.

        Query Parameters:
        - query: General search query
        - title: Book title
        - author: Author name
        - isbn: ISBN (10 or 13 digit)
        - limit: Number of results (1-100, default: 10)
        - offset: Offset for pagination (default: 0)

        Returns:
            JSON response with book search results
        """
        try:
            # Parse query parameters
            query = req.args.get("query")
            title = req.args.get("title")
            author = req.args.get("author")
            isbn = req.args.get("isbn")
            limit = int(req.args.get("limit", 10))
            offset = int(req.args.get("offset", 0))

            # Validate parameters
            if not any([query, title, author, isbn]):
                return https_fn.Response(
                    json.dumps({"error": "At least one search parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 100:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Searching books: query={query}, title={title}, author={author}")

            # Call wrapper
            result = await openlibrary_wrapper.search_books(
                query=query,
                title=title,
                author=author,
                isbn=isbn,
                limit=limit,
                offset=offset,
            )

            if result.status_code != 200 or result.error:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully found {len(result.results)} books")

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
            logger.error(f"Unexpected error in search_books: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_book_covers(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get book cover images from OpenLibrary.

        Query Parameters:
        - isbn: ISBN (10 or 13 digit)
        - oclc: OCLC identifier
        - lccn: Library of Congress Control Number
        - olid: OpenLibrary identifier (e.g., OL123456M)
        - cover_id: Direct cover ID

        Returns:
            JSON response with cover URLs
        """
        try:
            # Parse request parameters
            isbn = req.args.get("isbn")
            oclc = req.args.get("oclc")
            lccn = req.args.get("lccn")
            olid = req.args.get("olid")
            cover_id = req.args.get("cover_id")

            # Validate at least one identifier
            if not any([isbn, oclc, lccn, olid, cover_id]):
                return https_fn.Response(
                    json.dumps({"error": "At least one identifier is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Getting book covers: isbn={isbn}, oclc={oclc}, olid={olid}")

            # Call wrapper
            result = await openlibrary_wrapper.get_cover_urls(
                isbn=isbn,
                oclc=oclc,
                lccn=lccn,
                olid=olid,
                cover_id=cover_id,
            )

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
                    "search_params": {
                        "isbn": isbn,
                        "oclc": oclc,
                        "lccn": lccn,
                        "olid": olid,
                        "cover_id": cover_id,
                    },
                    "generated_at": datetime.now().isoformat(),
                    "source": "OpenLibrary Covers API",
                },
            }

            covers_available = result.results[0].covers_available if result.results else False
            logger.info(f"Successfully fetched cover data: covers_available={covers_available}")

            return https_fn.Response(
                json.dumps(response_data),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
                },
            )

        except Exception as e:
            logger.error(f"Error in get_book_covers: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error", "message": str(e)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create global handler instance
openlibrary_handler = OpenLibraryHandler()
