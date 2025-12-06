"""
NYTimes Core Service - Base service for NYTimes Books API operations
Handles core API communication, book enrichment with cover images.
"""

import asyncio
from typing import Any

import aiohttp
from pydantic import ValidationError

from api.nytimes.auth import nytimes_auth
from api.nytimes.models import (
    NYTimesBestsellerListResponse,
    NYTimesBook,
    NYTimesListNamesResponse,
    NYTimesOverviewResponse,
    NYTimesReviewResponse,
)
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 24 hours for NYTimes data
CacheExpiration = 24 * 60 * 60  # 24 hours

# Request cache - separate from other caches, independent refresh
NYTimesRequestCache = RedisCache(
    defaultTTL=6 * 60 * 60,  # 6 hours - news data changes daily
    prefix="nytimes_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.1",  # Bumped for Redis migration
)

NYTimesCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="nytimes",
    verbose=False,
    isClassMethod=True,
    version="3.0.1",  # Bumped for Redis migration
)

logger = get_logger(__name__)


class NYTimesService(BaseAPIClient):
    """
    Core NYTimes service for API communication and book enrichment.
    Handles basic NYTimes operations and cover image enrichment.
    """

    # Rate limiter configuration: NYTimes API limits
    # Conservative limit: 5 requests per 60 seconds
    # This prevents hitting the API rate limits
    _rate_limit_max = 5
    _rate_limit_period = 60

    def __init__(self):
        """Initialize NYTimes service using auth service for API key.

        Raises:
            ValueError: If API key is not available in auth service
        """
        self.api_key = nytimes_auth.nytimes_api_key
        if not self.api_key:
            raise ValueError(
                "NYTimes API key is required. Set NYTIMES_API_KEY in Firebase secrets "
                "or environment variables."
            )

        self.base_url = "https://api.nytimes.com/svc/books/v3"
        self.covers_url = "https://covers.openlibrary.org/b"

    @RedisCache.use_cache(NYTimesRequestCache, prefix="nytimes_api")
    async def _make_request(
        self, endpoint: str, params: dict[str, Any] | None = None, max_retries: int = 3
    ) -> tuple[dict[str, Any], int | None]:
        """Make async HTTP request to NYTimes API.

        This method brokers the call to _core_async_request with NYTimes-specific config.
        Returns tuple format for compatibility with existing code.

        Args:
            endpoint: API endpoint (e.g., 'lists/current/hardcover-fiction.json')
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            tuple: (response_data, error_code) - error_code is None on success
        """
        # Prepare request parameters
        request_params = params or {}
        request_params["api-key"] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            result = await self._core_async_request(
                url=url,
                params=request_params,
                headers=None,
                timeout=30,
                max_retries=max_retries,
                rate_limit_max=self._rate_limit_max,
                rate_limit_period=self._rate_limit_period,
                return_status_code=True,
                return_exceptions=True,
            )

            # Handle tuple return (result, status_code) when return_status_code=True
            if isinstance(result, tuple):
                response_data, status_code = result
                if response_data is None:
                    # Map common status codes to appropriate error messages
                    error_messages = {
                        404: "Endpoint not found",
                        401: "Invalid API key",
                        429: "Rate limit exceeded",
                    }
                    error_msg = error_messages.get(status_code, "API request failed")
                    return {"error": error_msg}, status_code or 500
                return response_data, None

            # Fallback for non-tuple return (shouldn't happen with return_status_code=True)
            if result is None:
                return {"error": "API request failed"}, 500

            return result, None
        except Exception as e:
            logger.error(f"Exception in NYTimes API request: {e}")
            return {"error": "Internal server error"}, 500

    async def _enrich_single_book_with_cover(
        self, book: NYTimesBook, session: aiohttp.ClientSession
    ) -> NYTimesBook:
        """Enrich a single book with cover image from NYTimes (primary) and OpenLibrary (fallback).

        Args:
            book: Book object from NYTimes API
            session: aiohttp ClientSession for making requests

        Returns:
            Book enriched with cover image URLs
        """
        enriched_book = book.model_copy(deep=True)
        cover_urls = None
        cover_source = None

        # First, check if NYTimes provides a cover image
        if book.book_image:
            try:
                # Verify the NYTimes image is accessible
                async with session.head(
                    book.book_image, timeout=aiohttp.ClientTimeout(total=3)
                ) as response:
                    if response.status == 200:
                        cover_urls = {
                            "small": book.book_image,
                            "medium": book.book_image,
                            "large": book.book_image,
                            "nyt_original": book.book_image,
                        }
                        cover_source = "nytimes"
                        enriched_book.cover_urls = cover_urls
                        enriched_book.cover_available = True
                        enriched_book.cover_source = cover_source
            except Exception:
                pass  # Fall back to OpenLibrary

        # If no NYTimes cover, try OpenLibrary as fallback
        if not cover_urls:
            # Get ISBNs from the book data
            isbns: list[str] = []
            if book.isbns:
                for isbn_data in book.isbns:
                    if isinstance(isbn_data, dict):
                        if isbn_data.get("isbn13"):
                            isbns.append(isbn_data["isbn13"])
                        if isbn_data.get("isbn10"):
                            isbns.append(isbn_data["isbn10"])
                    # Note: ISBNs from API are always dicts, but check just in case
                    # elif isinstance(isbn_data, str) and isbn_data:
                    #     isbns.append(isbn_data)

            # Also check primary_isbn13 and primary_isbn10
            if book.primary_isbn13:
                isbns.append(book.primary_isbn13)
            if book.primary_isbn10:
                isbns.append(book.primary_isbn10)

            # Try each ISBN until we find covers
            for isbn in isbns:
                if isbn and len(isbn) >= 10:
                    cover_base_url = f"{self.covers_url}/isbn/{isbn}"
                    potential_cover_urls = {
                        "small": f"{cover_base_url}-S.jpg",
                        "medium": f"{cover_base_url}-M.jpg",
                        "large": f"{cover_base_url}-L.jpg",
                    }

                    # Check if cover exists by making a HEAD request
                    try:
                        async with session.head(
                            potential_cover_urls["medium"], timeout=aiohttp.ClientTimeout(total=3)
                        ) as response:
                            if response.status == 200:
                                cover_urls = potential_cover_urls
                                cover_source = "openlibrary"
                                enriched_book.cover_urls = cover_urls
                                enriched_book.cover_available = True
                                enriched_book.cover_source = cover_source
                                break
                    except Exception:
                        continue

            # Small delay between ISBN attempts
            if not cover_urls:
                await asyncio.sleep(0.05)

        # If no cover found from either source, but we have NYTimes book_image, use it anyway
        if not cover_urls and book.book_image:
            # Don't verify availability, just use it as fallback
            cover_urls = {
                "small": book.book_image,
                "medium": book.book_image,
                "large": book.book_image,
                "nyt_original": book.book_image,
            }
            cover_source = "nytimes_fallback"
            enriched_book.cover_urls = cover_urls
            enriched_book.cover_available = True
            enriched_book.cover_source = cover_source

        # If still no cover found from any source
        if not cover_urls:
            enriched_book.cover_available = False
            enriched_book.cover_urls = None
            enriched_book.cover_source = None

        return enriched_book

    async def enrich_books_with_covers(
        self, books: list[NYTimesBook]
    ) -> tuple[list[NYTimesBook], int]:
        """Enrich bestseller books with cover images from NYTimes (primary) and OpenLibrary (fallback).

        Processes books sequentially with small delays to avoid rate limiting.
        While this is slower than concurrent processing, it prevents hitting
        API rate limits when enriching multiple books.

        Args:
            books: List of book objects from NYTimes API

        Returns:
            Tuple of (enriched books list, count of books with covers)
        """
        if not books:
            return [], 0

        async with aiohttp.ClientSession() as session:
            # Process books sequentially to avoid rate limiting
            final_books: list[NYTimesBook] = []

            for i, book in enumerate(books):
                try:
                    enriched_book = await self._enrich_single_book_with_cover(book, session)
                    final_books.append(enriched_book)

                    # Add small delay between books to avoid rate limiting
                    # Skip delay after the last book
                    if i < len(books) - 1:
                        await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error enriching book {i} ({book.title}): {e}")
                    # Return the original book without enrichment
                    final_books.append(book)

            # Count books with covers
            books_with_covers = sum(1 for book in final_books if book.cover_available)

            return final_books, books_with_covers

    @RedisCache.use_cache(NYTimesCache, prefix="bestseller_lists")
    async def get_bestseller_lists(
        self,
        list_name: str | None = None,
        date: str | None = None,
        published_date: str = "current",
        **kwargs: Any,
    ) -> tuple[
        dict[str, Any] | NYTimesBestsellerListResponse | NYTimesOverviewResponse, int | None
    ]:
        """Get NYTimes bestseller lists.

        Args:
            list_name: Specific list name (e.g., 'hardcover-fiction')
            date: Specific date for overview (format: YYYY-MM-DD)
            published_date: 'current' or specific date for single list

        Returns:
            Tuple of (response data or model, error_code) - error_code is None on success
        """
        try:
            if list_name:
                endpoint = f"lists/{published_date}/{list_name}.json"
            else:
                endpoint = "lists/overview.json"
                if date:
                    endpoint = f"lists/{date}/overview.json"

            data, error_code = await self._make_request(endpoint)

            if error_code:
                return data, error_code

            # Validate and parse response
            try:
                if list_name:
                    response = NYTimesBestsellerListResponse.model_validate(data)
                    return response, None
                else:
                    response_overview = NYTimesOverviewResponse.model_validate(data)
                    return response_overview, None
            except ValidationError as e:
                logger.warning(f"Error validating NYTimes response: {e}")
                # Return raw data if validation fails
                return data, None

        except Exception as e:
            logger.error(f"Error in get_bestseller_lists: {e}")
            return {"error": str(e)}, 500

    @RedisCache.use_cache(NYTimesCache, prefix="list_names")
    async def get_list_names(
        self, **kwargs: Any
    ) -> tuple[dict[str, Any] | NYTimesListNamesResponse, int | None]:
        """Get all available NYTimes bestseller list names.

        Note: The /lists/names.json endpoint has been deprecated by NYTimes.
        This method now uses the /lists/overview.json endpoint and extracts
        list metadata from it.

        Returns:
            Tuple of (response data or model, error_code) - error_code is None on success
        """
        try:
            # Use overview endpoint since /lists/names.json is deprecated
            data, error_code = await self._make_request("lists/overview.json")

            if error_code:
                return data, error_code

            # Extract list names from overview response
            try:
                if "results" in data and "lists" in data["results"]:
                    lists = data["results"]["lists"]

                    # Transform overview lists to list names format
                    list_names = []
                    for lst in lists:
                        list_name_data = {
                            "list_name": lst.get("list_name", ""),
                            "display_name": lst.get("display_name", ""),
                            "list_name_encoded": lst.get("list_name_encoded", ""),
                            "oldest_published_date": "",  # Not available in overview
                            "newest_published_date": data["results"].get("published_date", ""),
                            "updated": lst.get("updated", ""),
                        }
                        list_names.append(list_name_data)

                    # Create response in expected format
                    response_data = {
                        "status": data.get("status", "OK"),
                        "copyright": data.get("copyright"),
                        "num_results": len(list_names),
                        "results": list_names,
                    }

                    # Validate and parse response
                    response = NYTimesListNamesResponse.model_validate(response_data)
                    return response, None
                else:
                    logger.warning("Unexpected response structure from overview endpoint")
                    return data, None

            except ValidationError as e:
                logger.warning(f"Error validating NYTimes response: {e}")
                # Return raw data if validation fails
                return data, None

        except Exception as e:
            logger.error(f"Error in get_list_names: {e}")
            return {"error": str(e)}, 500

    @RedisCache.use_cache(NYTimesCache, prefix="book_reviews")
    async def get_book_reviews(
        self,
        author: str | None = None,
        title: str | None = None,
        isbn: str | None = None,
        **kwargs: Any,
    ) -> tuple[dict[str, Any] | NYTimesReviewResponse, int | None]:
        """Get NYTimes book reviews.

        WARNING: This endpoint (reviews.json) has been deprecated by NYTimes
        and currently returns 404. This method is kept for backward compatibility
        but will return an error. Consider removing this functionality or finding
        an alternative source for book reviews.

        Args:
            author: Author name
            title: Book title
            isbn: ISBN number

        Returns:
            Tuple of (response data or model, error_code) - error_code is None on success
        """
        if not any([author, title, isbn]):
            return {"error": "At least one of author, title, or isbn parameters is required"}, 400

        try:
            params = {}
            if author:
                params["author"] = author
            if title:
                params["title"] = title
            if isbn:
                params["isbn"] = isbn

            data, error_code = await self._make_request("reviews.json", params)

            if error_code:
                return data, error_code

            # Validate and parse response
            try:
                response = NYTimesReviewResponse.model_validate(data)
                return response, None
            except ValidationError as e:
                logger.warning(f"Error validating NYTimes response: {e}")
                # Return raw data if validation fails
                return data, None

        except Exception as e:
            logger.error(f"Error in get_book_reviews: {e}")
            return {"error": str(e)}, 500
