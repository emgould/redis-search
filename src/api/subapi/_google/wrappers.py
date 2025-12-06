"""
Google Books Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using MCBaseItem pattern.
"""

from typing import Any

from api.subapi._google.models import GoogleBooksSearchResponse, GoogleBooksVolumeResponse
from api.subapi._google.search import google_books_search_service
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
GoogleBooksCache = RedisCache(
    defaultTTL=7 * 24 * 60 * 60,  # 7 days
    prefix="google_books_func",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="3.0.1",  # Version bump for Redis migration
)


class GoogleBooksWrapper:
    def __init__(self):
        self.service = google_books_search_service

    @RedisCache.use_cache(GoogleBooksCache, prefix="search_books_wrapper")
    async def search_books(
        self,
        query: str | None = None,
        title: str | None = None,
        author: str | None = None,
        isbn: str | None = None,
        max_results: int = 10,
        start_index: int = 0,
        order_by: str = "relevance",
        api_key: str | None = None,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Async wrapper function to search Google Books.

        Args:
            query: General search query
            title: Book title
            author: Author name
            isbn: ISBN (10 or 13 digit)
            max_results: Number of results to return (max 40)
            start_index: Starting index for pagination
            order_by: Sort order ('relevance' or 'newest')
            api_key: Optional Google Books API key
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            GoogleBooksSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            # Use service instance (API key is handled via google_books_auth)
            service = self.service

            # Handle different search types
            if isbn:
                # ISBN search returns GoogleBooksSearchResponse
                isbn_result = await service.search_by_isbn(isbn, **kwargs)
                if isbn_result.error:
                    isbn_result.status_code = 404
                    return isbn_result
                isbn_result.status_code = 200
                return isbn_result

            elif title or author:
                # Title/Author search returns GoogleBooksSearchResponse
                search_result = await service.search_by_title_and_author(
                    title=title,
                    author=author,
                    max_results=max_results,
                    **kwargs,
                )
                if search_result.error:
                    search_result.status_code = 404
                    return search_result
                search_result.status_code = 200
                return search_result
            else:
                # General query search
                if not query:
                    error_response = GoogleBooksSearchResponse(
                        kind="books#volumes",
                        totalItems=0,
                        items=[],
                        docs=[],
                        num_found=0,
                        query=None,
                        data_source="Google Books API",
                        error="At least one search parameter is required",
                        status_code=404,
                    )
                    return error_response

                search_result = await service.search_books(
                    query=query,
                    max_results=max_results,
                    start_index=start_index,
                    order_by=order_by,
                    **kwargs,
                )
                if search_result.error:
                    search_result.status_code = 404
                    return search_result
                search_result.status_code = 200
                return search_result

        except Exception as e:
            logger.error(f"Error in search_books: {e}")
            error_response = GoogleBooksSearchResponse(
                kind="books#volumes",
                totalItems=0,
                items=[],
                docs=[],
                num_found=0,
                query=query or title or author or isbn,
                data_source="Google Books API",
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(GoogleBooksCache, prefix="search_books_direct_wrapper")
    async def search_books_direct(
        self,
        query: str,
        max_results: int = 10,
        api_key: str | None = None,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Direct search that returns list of book dictionaries.
        This matches the interface used in unified_search.py.

        Args:
            query: Search query
            max_results: Maximum number of results (max 40)
            api_key: Optional Google Books API key
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            GoogleBooksSearchResponse: MCBaseItem derivative containing search results or error information
        """
        try:
            # Use service instance (API key is handled via google_books_auth)
            service = self.service
            search_result = await service.search_direct(query, max_results=max_results, **kwargs)
            if search_result.error:
                search_result.status_code = 400
                return search_result
            search_result.status_code = 200
            return search_result

        except Exception as e:
            logger.error(f"Error in search_books_direct: {e}")
            error_response = GoogleBooksSearchResponse(
                kind="books#volumes",
                totalItems=0,
                items=[],
                docs=[],
                num_found=0,
                query=query,
                data_source="Google Books API",
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(GoogleBooksCache, prefix="get_volume_by_id_wrapper")
    async def get_volume_by_id(
        self,
        volume_id: str,
        api_key: str | None = None,
        **kwargs: Any,
    ) -> GoogleBooksVolumeResponse:
        """
        Async wrapper function to get a specific volume by ID.

        Args:
            volume_id: Google Books volume ID
            api_key: Optional Google Books API key
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            GoogleBooksVolumeResponse: MCBaseItem derivative containing volume data or error information
        """
        try:
            # Use service instance (API key is handled via google_books_auth)
            service = self.service
            result = await service.get_volume_by_id(volume_id, **kwargs)

            if result.error:
                logger.warning(
                    f"Volume lookup returned error {result.error}: {result.model_dump()}"
                )
                error_msg = "Failed to fetch volume"
                error_response = GoogleBooksVolumeResponse(
                    volume=None,
                    data_source="Google Books API",
                    error=error_msg,
                    status_code=404,
                )
                return error_response

            # Convert raw volume dict to GoogleBooksItem
            # Bypass the search service wrapper and call core service directly to get raw dict
            # This avoids Pydantic validation issues where dict gets converted incorrectly
            volume_result, volume_error = await service.service.get_volume_by_id(
                volume_id, **kwargs
            )

            book = None
            if volume_result and not volume_error:
                # volume_result is the raw dict from Google Books API
                book = service._convert_volume_to_book_item(volume_result)
            if not book or book.error:
                error_msg = (
                    book.error if book and book.error else "Failed to convert volume to book item"
                )
                error_response = GoogleBooksVolumeResponse(
                    volume=None,
                    data_source="Google Books API",
                    error=error_msg,
                    status_code=500,
                )
                return error_response
            response = GoogleBooksVolumeResponse(
                volume=book,
                data_source="Google Books API",
                status_code=200,
            )
            return response

        except Exception as e:
            logger.error(f"Error in get_volume_by_id: {e}")
            error_response = GoogleBooksVolumeResponse(
                volume=None,
                data_source="Google Books API",
                error=str(e),
                status_code=500,
            )
            return error_response

    @RedisCache.use_cache(GoogleBooksCache, prefix="get_volume_by_isbn_wrapper")
    async def get_volume_by_isbn(
        self,
        isbn: str,
        api_key: str | None = None,
        **kwargs: Any,
    ) -> GoogleBooksVolumeResponse:
        """
        Async wrapper function to get a volume by ISBN.

        Args:
            isbn: ISBN-10 or ISBN-13
            api_key: Optional Google Books API key
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            GoogleBooksVolumeResponse: MCBaseItem derivative containing volume data or error information
        """
        try:
            # Use service instance (API key is handled via google_books_auth)
            service = self.service
            result = await service.search_by_isbn(isbn, **kwargs)

            if result.error:
                error_response = GoogleBooksVolumeResponse(
                    volume=None,
                    data_source="Google Books API",
                    error=result.error,
                    status_code=404,
                )
                return error_response

            # Check if we have items
            if not result.items or len(result.items) == 0:
                error_response = GoogleBooksVolumeResponse(
                    volume=None,
                    data_source="Google Books API",
                    error=f"No book found for ISBN: {isbn}",
                    status_code=404,
                )
                return error_response

            # Check if the first item has an error (failed conversion)
            first_item = result.items[0]
            if first_item.error:
                error_response = GoogleBooksVolumeResponse(
                    volume=None,
                    data_source="Google Books API",
                    error=first_item.error,
                    status_code=500,
                )
                return error_response

            # Convert to GoogleBooksItem if needed
            response = GoogleBooksVolumeResponse(
                volume=first_item,
                data_source="Google Books API",
                status_code=200,
            )
            return response

        except Exception as e:
            logger.error(f"Error in get_volume_by_isbn: {e}")
            error_response = GoogleBooksVolumeResponse(
                volume=None,
                data_source="Google Books API",
                error=str(e),
                status_code=500,
            )
            return error_response


google_books_wrapper = GoogleBooksWrapper()
