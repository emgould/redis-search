"""
OpenLibrary Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

from typing import Any, cast

import aiohttp
from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSources,
)

from api.openlibrary.models import (
    CoverUrlsResponse,
    MCBookItem,
    OpenLibraryAuthorSearchResponse,
    OpenLibraryCoverUrlsResponse,
    OpenLibrarySearchResponse,
)
from api.openlibrary.search import OpenLibrarySearchService
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for class methods
OpenLibraryCache = RedisCache(
    defaultTTL=60 * 60 * 24,  # 24 hours
    prefix="openlibrary_func",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="4.33.3",  # Added support for covers array from author works endpoint
)


class OpenLibraryWrapper:
    def __init__(self):
        self.service = OpenLibrarySearchService()

    @RedisCache.use_cache(OpenLibraryCache, prefix="search_books_wrapper")
    async def search_books(
        self,
        query: str | None = None,
        title: str | None = None,
        author: str | None = None,
        isbn: str | None = None,
        limit: int = 10,
        offset: int = 0,
        **kwargs: Any,
    ) -> OpenLibrarySearchResponse:
        """
        Async wrapper function to search for books.

        Args:
            query: General search query
            title: Book title
            author: Author name
            isbn: ISBN (10 or 13 digit)
            limit: Number of results to return (max 100)
            offset: Offset for pagination
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            OpenLibrarySearchResponse: MCSearchResponse derivative containing search results or error information
        """
        try:
            data = await self.service.search_books(
                query=query,
                title=title,
                author=author,
                isbn=isbn,
                limit=limit,
                offset=offset,
                **kwargs,
            )

            data.data_source = "search_books_async"

            if data.error:
                logger.warning(f"Book search returned error {data.error}: {data.results}")
                # Set appropriate status code based on error message
                if "At least one search parameter is required" in data.error:
                    data.status_code = 400
                else:
                    data.status_code = 500
                return cast(OpenLibrarySearchResponse, data)

            data.status_code = 200
            return cast(OpenLibrarySearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_books: {e}")
            return OpenLibrarySearchResponse(
                results=[],
                total_results=0,
                query=query or title or author or isbn,
                error=str(e),
                data_source="search_books_async",
                status_code=500,
            )

    @RedisCache.use_cache(OpenLibraryCache, prefix="get_cover_urls_wrapper")
    async def get_cover_urls(
        self,
        isbn: str | None = None,
        oclc: str | None = None,
        lccn: str | None = None,
        olid: str | None = None,
        cover_id: str | None = None,
        **kwargs: Any,
    ) -> OpenLibraryCoverUrlsResponse:
        """
        Get cover image URLs for a book using various identifiers.

        Args:
            isbn: ISBN (10 or 13 digit)
            oclc: OCLC identifier
            lccn: Library of Congress Control Number
            olid: OpenLibrary identifier (e.g., OL123456M)
            cover_id: Direct cover ID
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            OpenLibraryCoverUrlsResponse: MCSearchResponse derivative containing cover URLs or error information
        """
        try:
            # Determine the identifier type and value
            if cover_id:
                identifier_type = "id"
                identifier_value = cover_id
            elif isbn:
                identifier_type = "isbn"
                identifier_value = isbn
            elif oclc:
                identifier_type = "oclc"
                identifier_value = oclc
            elif lccn:
                identifier_type = "lccn"
                identifier_value = lccn
            elif olid:
                identifier_type = "olid"
                identifier_value = olid
            else:
                return OpenLibraryCoverUrlsResponse(
                    results=[],
                    total_results=0,
                    query="unknown",
                    error="At least one identifier is required",
                    data_source="get_cover_urls",
                    status_code=400,
                )

            # Build cover URLs for different sizes
            covers_url = "https://covers.openlibrary.org/b"
            base_url = f"{covers_url}/{identifier_type}/{identifier_value}"
            cover_urls = {
                "small": f"{base_url}-S.jpg",
                "medium": f"{base_url}-M.jpg",
                "large": f"{base_url}-L.jpg",
            }

            # Check if covers exist by making a HEAD request to the medium size
            covers_exist = False
            try:
                async with (
                    aiohttp.ClientSession() as session,
                    session.head(cover_urls["medium"]) as http_response,
                ):
                    covers_exist = http_response.status == 200
            except Exception as check_error:
                logger.warning(f"Error checking cover availability: {check_error}")
                covers_exist = False

            result = CoverUrlsResponse(
                identifier={"type": identifier_type, "value": identifier_value},
                covers_available=covers_exist,
                cover_urls=cover_urls if covers_exist else None,
            )

            return OpenLibraryCoverUrlsResponse(
                results=[result],
                total_results=1,
                query=f"identifier_type: {identifier_type} identifier_value: {identifier_value}",
                error=None,
                data_source="get_cover_urls",
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_cover_urls: {e}")
            try:
                query_str = (
                    f"identifier_type: {identifier_type} identifier_value: {identifier_value}"
                    if "identifier_type" in locals() and "identifier_value" in locals()
                    else "unknown"
                )
            except (NameError, UnboundLocalError):
                query_str = "unknown"
            return OpenLibraryCoverUrlsResponse(
                results=[],
                total_results=0,
                query=query_str,
                error=str(e),
                data_source="get_cover_urls",
                status_code=500,
            )

    @RedisCache.use_cache(OpenLibraryCache, prefix="search_authors_wrapper_v2")
    async def search_authors(
        self,
        query: str | None = None,
        limit: int = 10,
        offset: int = 0,
        **kwargs: Any,
    ) -> OpenLibraryAuthorSearchResponse:
        """
        Async wrapper function to search for authors.

        Args:
            query: General search query
            limit: Number of results to return (max 100)
            offset: Offset for pagination
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            OpenLibraryAuthorSearchResponse: MCSearchResponse derivative containing search results or error information
        """
        try:
            data = await self.service.search_authors(
                query=query,
                limit=limit,
                offset=offset,
                **kwargs,
            )

            data.data_source = "search_authors_async"

            if data.error:
                # Set appropriate status code based on error message
                if "At least one search parameter is required" in data.error:
                    data.status_code = 400
                else:
                    data.status_code = 500
                return cast(OpenLibraryAuthorSearchResponse, data)

            data.status_code = 200
            return cast(OpenLibraryAuthorSearchResponse, data)

        except Exception as e:
            logger.error(f"Error in search_authors: {e}")
            return OpenLibraryAuthorSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                data_source="search_authors_async",
                status_code=500,
            )

    @RedisCache.use_cache(OpenLibraryCache, prefix="search_person_works_v2")
    async def search_person_async(
        self,
        request: "MCPersonSearchRequest",
        limit: int | None = None,
    ) -> "MCPersonSearchResponse":
        """Search for author works (books) based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

        Args:
            request: MCPersonSearchRequest with author identification details
            limit: Maximum number of books to return (default: 50)

        Returns:
            MCPersonSearchResponse with author details and works
            - details: MCAuthorItem (author details)
            - works: list[MCBookItem] (books written by the author)
            - related: [] (empty, will be filled by search_broker)
        """
        from contracts.models import MCPersonSearchResponse

        logger.info(
            f"OpenLibrary: search_person_async called for {request.name} "
            f"(source={request.source}, source_id={request.source_id})"
        )

        try:
            # Validate that this is an OpenLibrary author
            logger.info(
                f"OPENLIBRARY: search_person_async called for {request.name} (source={request.source}, source_id={request.source_id})"
            )

            if request.source != MCSources.OPENLIBRARY:
                logger.info(
                    f"OPENLIBRARY: Invalid source for OpenLibrary author search: {request.source}"
                )
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source for OpenLibrary author search: {request.source}",
                    status_code=400,
                )

            # Validate source_id (must be provided and non-empty)
            # source_id is a string (OpenLibrary author key, e.g., "/authors/OL123456A")
            if not request.source_id or len(request.source_id.strip()) == 0:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source_id for OpenLibrary author: {request.source_id} (must be provided)",
                    status_code=400,
                )

            # Try to fetch the author directly by source_id first
            author_key = request.source_id.strip()
            author = None

            # Fetch author by key using the new get_author_by_key method
            logger.info(f"OPENLIBRARY: Fetching author by key: {author_key}")
            author_response = await self.get_author_by_key(author_key=author_key)

            if author_response.status_code == 200 and author_response.results:
                # Successfully found author by key
                author = author_response.results[0]
                logger.info(f"OPENLIBRARY: Fetching author by key: {author_key} DONE")

            else:
                logger.info(f"OPENLIBRARY: UNABLE TO FETACH AUTHOR BY KEY: {author_key}")
                # Key lookup failed - fall back to name search if name is provided
                author_name = request.name
                if not author_name or len(author_name.strip()) == 0:
                    # No name to fall back to, return error
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error="Author not found by key and name is required for fallback search",
                        status_code=404,
                    )

                # Fall back to name search
                author_limit = 1  # We only need the first match
                authors_response = await self.search_authors(query=author_name, limit=author_limit)

                if authors_response.status_code != 200 or not authors_response.results:
                    # Not found should always return 404
                    return MCPersonSearchResponse(
                        input=request,
                        details=None,
                        works=[],
                        related=[],
                        error=authors_response.error or "Author not found",
                        status_code=404,
                    )

                # Get the author details (first result)
                author = authors_response.results[0]

            # Fetch works (books) by this author using the new get_author_works method
            # Use the author's key (from the author object or the request source_id)
            author_key_for_works = author.key if author.key else author_key
            book_limit = limit if limit is not None else 50
            logger.info(f"OPENLIBRARY: Fetching works by author: {author_key_for_works}")
            works_response = await self.get_author_works(
                author_key=author_key_for_works, limit=book_limit
            )

            # Extract works (books)
            works: list[MCBookItem] = []
            if works_response.status_code == 200 and works_response.results:
                # Cast to MCBookItem since we know these are books from OpenLibrary
                works = [cast(MCBookItem, item) for item in works_response.results]
            logger.info(f"OPENLIBRARY: Fetching works by author: {author_key_for_works} DONE")

            # Return response with author details and works
            # related will be filled by search_broker
            logger.info(
                f"OpenLibrary: search_person_async completed for {request.name} - "
                f"found {len(works)} works, author={'present' if author else 'None'}"
            )
            logger.info(f"OPENLIBRARY: Works in total: {len(works)}")
            # Note: Works are already filtered in get_author_works wrapper, no need to filter again
            # Cast to list[MCBaseItem] for type compatibility with MCPersonSearchResponse
            works_base: list[MCBaseItem] = [cast(MCBaseItem, book) for book in works]
            return MCPersonSearchResponse(
                input=request,
                details=author,  # MCAuthorItem
                works=works_base,  # list[MCBaseItem]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching author works for {request.name}: {e}", exc_info=True)
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(OpenLibraryCache, prefix="get_author_by_key_wrapper")
    async def get_author_by_key(
        self,
        author_key: str,
        **kwargs: Any,
    ) -> OpenLibraryAuthorSearchResponse:
        """
        Async wrapper function to get author details by their unique OpenLibrary key.

        Args:
            author_key: Author key (e.g., "/authors/OL123456A" or "OL123456A")
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            OpenLibraryAuthorSearchResponse: MCSearchResponse derivative containing author details or error information
        """
        try:
            author = await self.service.get_author_by_key(author_key, **kwargs)

            if not author:
                return OpenLibraryAuthorSearchResponse(
                    results=[],
                    total_results=0,
                    query=author_key,
                    error=f"Author with key {author_key} not found",
                    data_source="get_author_by_key_async",
                    status_code=404,
                )

            return OpenLibraryAuthorSearchResponse(
                results=[author],
                total_results=1,
                query=author_key,
                error=None,
                data_source="get_author_by_key_async",
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_author_by_key: {e}")
            return OpenLibraryAuthorSearchResponse(
                results=[],
                total_results=0,
                query=author_key,
                error=str(e),
                data_source="get_author_by_key_async",
                status_code=500,
            )

    @RedisCache.use_cache(OpenLibraryCache, prefix="get_author_works_wrapper")
    async def get_author_works(
        self,
        author_key: str,
        limit: int = 50,
        offset: int = 0,
        **kwargs: Any,
    ) -> OpenLibrarySearchResponse:
        """
        Async wrapper function to get works (books) by an author using their unique OpenLibrary key.

        Args:
            author_key: Author key (e.g., "/authors/OL123456A" or "OL123456A")
            limit: Maximum number of works to return (default: 50)
            offset: Offset for pagination (default: 0)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            OpenLibrarySearchResponse: MCSearchResponse derivative containing author's works or error information
        """
        try:
            works = await self.service.get_author_works(
                author_key=author_key,
                limit=limit,
                offset=offset,
                **kwargs,
            )
            logger.info(f"OPENLIBRARY: Works: {len(works)}")
            # For author works, allow books without covers if they have work keys
            # (covers can be fetched later if needed)
            works = self.service.filter_books_by_images(works, require_cover=False)
            logger.info(f"OPENLIBRARY: Works after filter: {len(works)}")
            return OpenLibrarySearchResponse(
                results=works,
                total_results=len(works),
                query=author_key,
                error=None,
                data_source="get_author_works_async",
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_author_works: {e}")
            return OpenLibrarySearchResponse(
                results=[],
                total_results=0,
                query=author_key,
                error=str(e),
                data_source="get_author_works_async",
                status_code=500,
            )


openlibrary_wrapper = OpenLibraryWrapper()


async def search_person_async(
    request: "MCPersonSearchRequest",
    limit: int | None = None,
) -> "MCPersonSearchResponse":
    """Search for author works (books) based on person search request.

    This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

    Args:
        request: MCPersonSearchRequest with author identification details
        limit: Maximum number of books to return (default: 50)

    Returns:
        MCPersonSearchResponse with author details and works
        - details: MCAuthorItem (author details)
        - works: list[MCBookItem] (books written by the author)
        - related: [] (empty, will be filled by search_broker)
    """
    return cast(
        "MCPersonSearchResponse",
        await openlibrary_wrapper.search_person_async(request, limit=limit),
    )
