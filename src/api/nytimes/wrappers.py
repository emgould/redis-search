"""
NYTimes Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any

from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSearchResponse,
    MCSources,
)

from api.nytimes.core import NYTimesService
from api.nytimes.models import (
    NYTimesBestsellerListResponse,
    NYTimesBook,
    NYTimesHistoricalResponse,
    NYTimesListNamesResponse,
    NYTimesOverviewResponse,
    NYTimesReviewResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache
from utils.soft_comparison import soft_compare

logger = get_logger(__name__)

# Cache for wrapper class methods
NYTimesWrapperCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours
    prefix="nytimes_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="4.0.1",  # Bumped for Redis migration
)


class NYTimesWrapper:
    def __init__(self):
        self._service: NYTimesService | None = None

    @property
    def service(self) -> NYTimesService:
        """Lazy-load service instance on first use."""
        if self._service is None:
            self._service = NYTimesService()
        return self._service

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_fiction_bestsellers_wrapper")
    async def get_fiction_bestsellers(
        self, date: str | None = None, published_date: str = "current", **kwargs: Any
    ) -> NYTimesBestsellerListResponse:
        """
        Async wrapper function to get NYTimes fiction bestsellers with covers.

        Args:
            date: Specific date (YYYY-MM-DD format)
            published_date: 'current' or specific date string

        Returns:
            NYTimesBestsellerListResponse: MCSearchResponse derivative containing bestseller data or error information
        """
        try:
            list_name = "combined-print-and-e-book-fiction"

            if date:
                endpoint_date = date
            else:
                endpoint_date = published_date

            result, error_code = await self.service.get_bestseller_lists(
                list_name=list_name, published_date=endpoint_date
            )

            if error_code:
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                return NYTimesBestsellerListResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=f"Failed to fetch NYTimes fiction bestsellers: {error_msg}",
                    status_code=error_code,
                )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                response = NYTimesBestsellerListResponse.model_validate(result)
            else:
                response = result

            # Enrich with cover images if we have books
            if response.results:
                enriched_books, _ = await self.service.enrich_books_with_covers(response.results)
                response.results = enriched_books
                # Also update list_results if present
                if response.list_results:
                    response.list_results.books = enriched_books

            return response

        except Exception as e:
            logger.error(f"Error in get_fiction_bestsellers: {e}")
            return NYTimesBestsellerListResponse(
                status="ERROR",
                num_results=0,
                results=[],
                total_results=0,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_nonfiction_bestsellers_wrapper")
    async def get_nonfiction_bestsellers(
        self, date: str | None = None, published_date: str = "current", **kwargs: Any
    ) -> NYTimesBestsellerListResponse:
        """
        Async wrapper function to get NYTimes nonfiction bestsellers with covers.

        Args:
            date: Specific date (YYYY-MM-DD format)
            published_date: 'current' or specific date string

        Returns:
            NYTimesBestsellerListResponse: MCSearchResponse derivative containing bestseller data or error information
        """
        try:
            list_name = "combined-print-and-e-book-nonfiction"

            if date:
                endpoint_date = date
            else:
                endpoint_date = published_date

            result, error_code = await self.service.get_bestseller_lists(
                list_name=list_name, published_date=endpoint_date
            )

            if error_code:
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                return NYTimesBestsellerListResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=f"Failed to fetch NYTimes nonfiction bestsellers: {error_msg}",
                    status_code=error_code,
                )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                response = NYTimesBestsellerListResponse.model_validate(result)
            else:
                response = result

            # Enrich with cover images if we have books
            if response.results:
                enriched_books, _ = await self.service.enrich_books_with_covers(response.results)
                response.results = enriched_books
                # Also update list_results if present
                if response.list_results:
                    response.list_results.books = enriched_books

            return response

        except Exception as e:
            logger.error(f"Error in get_nonfiction_bestsellers: {e}")
            return NYTimesBestsellerListResponse(
                status="ERROR",
                num_results=0,
                results=[],
                total_results=0,
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_bestseller_lists_with_covers_wrapper")
    async def get_bestseller_lists_with_covers(
        self,
        list_name: str | None = None,
        date: str | None = None,
        published_date: str = "current",
        **kwargs: Any,
    ) -> MCSearchResponse:
        """
        Async wrapper function to get NYTimes bestseller lists with covers.

        Args:
            list_name: Specific list name (e.g., 'hardcover-fiction')
            date: Specific date for overview (YYYY-MM-DD format)
            published_date: 'current' or specific date for single list

        Returns:
            MCSearchResponse: Either NYTimesBestsellerListResponse or NYTimesOverviewResponse containing enriched bestseller data or error information
        """
        try:
            result, error_code = await self.service.get_bestseller_lists(
                list_name=list_name, date=date, published_date=published_date
            )

            if error_code:
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                if list_name:
                    return NYTimesBestsellerListResponse(
                        status="ERROR",
                        num_results=0,
                        results=[],
                        total_results=0,
                        error=f"Failed to fetch NYTimes bestseller lists: {error_msg}",
                        status_code=error_code,
                    )
                else:
                    return NYTimesOverviewResponse(
                        status="ERROR",
                        num_results=0,
                        results=[],
                        total_results=0,
                        error=f"Failed to fetch NYTimes bestseller lists: {error_msg}",
                        status_code=error_code,
                    )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                if list_name:
                    response: MCSearchResponse = NYTimesBestsellerListResponse.model_validate(
                        result
                    )
                else:
                    response = NYTimesOverviewResponse.model_validate(result)
            else:
                response = result

            # Enrich with cover images
            books_enriched = 0

            if list_name:
                # Single list response
                assert isinstance(response, NYTimesBestsellerListResponse)
                if response.results:
                    enriched_books, count = await self.service.enrich_books_with_covers(
                        response.results
                    )
                    response.results = enriched_books
                    # Also update list_results if present
                    if response.list_results:
                        response.list_results.books = enriched_books
                    books_enriched = count
            else:
                # Multiple lists overview response
                assert isinstance(response, NYTimesOverviewResponse)
                if response.results:
                    enriched_books, count = await self.service.enrich_books_with_covers(
                        response.results
                    )
                    response.results = enriched_books
                    # Also update overview_results if present
                    if response.overview_results:
                        # Update books in each list
                        book_index = 0
                        for list_data in response.overview_results.lists:
                            list_book_count = len(list_data.books)
                            if list_book_count > 0:
                                list_data.books = enriched_books[
                                    book_index : book_index + list_book_count
                                ]
                                book_index += list_book_count
                    books_enriched = count

            # Store books_enriched in metrics
            if not response.metrics:
                response.metrics = {}
            response.metrics["books_enriched_count"] = books_enriched

            return response

        except Exception as e:
            logger.error(f"Error in get_bestseller_lists_with_covers: {e}")
            if list_name:
                return NYTimesBestsellerListResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=str(e),
                    status_code=500,
                )
            else:
                return NYTimesOverviewResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=str(e),
                    status_code=500,
                )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_historical_bestsellers_wrapper")
    async def get_historical_bestsellers(
        self,
        list_name: str | None = None,
        weeks_back: int = 4,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> NYTimesHistoricalResponse:
        """
        Async wrapper function to get historical NYTimes bestsellers.

        Args:
            list_name: List name (e.g., 'hardcover-fiction')
            weeks_back: Number of weeks to look back (default: 4)
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)

        Returns:
            NYTimesHistoricalResponse: MCSearchResponse derivative containing historical bestseller data or error information
        """
        if not list_name:
            return NYTimesHistoricalResponse(
                historical_data=[],
                error="list parameter is required",
                status_code=400,
            )

        try:
            results = []

            if start_date and end_date:
                # Use specific date range
                result, error_code = await self.service.get_bestseller_lists(
                    list_name=list_name, published_date=start_date
                )
                if not error_code:
                    result_dict = result if isinstance(result, dict) else result.model_dump()
                    results.append(result_dict)
            else:
                # Fetch data for the last N weeks
                today = datetime.now()
                for week_offset in range(weeks_back):
                    target_date = today - timedelta(weeks=week_offset)
                    date_str = target_date.strftime("%Y-%m-%d")

                    result, error_code = await self.service.get_bestseller_lists(
                        list_name=list_name, published_date=date_str
                    )

                    if not error_code:
                        result_dict = result if isinstance(result, dict) else result.model_dump()
                        if result_dict.get("results"):
                            results.append({"date": date_str, "data": result_dict})

                    # Add small delay to be respectful to the API
                    await asyncio.sleep(0.1)

            return NYTimesHistoricalResponse(historical_data=results)

        except Exception as e:
            logger.error(f"Error in get_historical_bestsellers: {e}")
            return NYTimesHistoricalResponse(
                historical_data=[],
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_bestseller_lists_wrapper")
    async def get_bestseller_lists(
        self,
        list_name: str | None = None,
        date: str | None = None,
        published_date: str = "current",
        **kwargs: Any,
    ) -> MCSearchResponse:
        """
        Async wrapper function to get NYTimes bestseller lists.

        Args:
            list_name: Specific list name (e.g., 'hardcover-fiction')
            date: Specific date for overview (format: YYYY-MM-DD)
            published_date: 'current' or specific date for single list

        Returns:
            MCSearchResponse: Either NYTimesBestsellerListResponse or NYTimesOverviewResponse containing bestseller data or error information
        """
        try:
            result, error_code = await self.service.get_bestseller_lists(
                list_name=list_name, date=date, published_date=published_date
            )

            if error_code:
                # Create appropriate error response based on endpoint type
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                if list_name:
                    return NYTimesBestsellerListResponse(
                        status="ERROR",
                        num_results=0,
                        results=[],
                        total_results=0,
                        error=f"Failed to fetch NYTimes bestseller lists: {error_msg}",
                        status_code=error_code,
                    )
                else:
                    return NYTimesOverviewResponse(
                        status="ERROR",
                        num_results=0,
                        results=[],
                        total_results=0,
                        error=f"Failed to fetch NYTimes bestseller lists: {error_msg}",
                        status_code=error_code,
                    )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                if list_name:
                    response: MCSearchResponse = NYTimesBestsellerListResponse.model_validate(
                        result
                    )
                else:
                    response = NYTimesOverviewResponse.model_validate(result)
            else:
                response = result

            return response

        except Exception as e:
            logger.error(f"Error in get_bestseller_lists: {e}")
            if list_name:
                return NYTimesBestsellerListResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=str(e),
                    status_code=500,
                )
            else:
                return NYTimesOverviewResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    total_results=0,
                    error=str(e),
                    status_code=500,
                )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_list_names_wrapper")
    async def get_list_names(self, **kwargs: Any) -> NYTimesListNamesResponse:
        """
        Async wrapper function to get all available NYTimes bestseller list names.

        Returns:
            NYTimesListNamesResponse: MCSearchResponse derivative containing list names or error information
        """
        try:
            result, error_code = await self.service.get_list_names()

            if error_code:
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                return NYTimesListNamesResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    error=f"Failed to fetch NYTimes list names: {error_msg}",
                    status_code=error_code,
                )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                response = NYTimesListNamesResponse.model_validate(result)
            else:
                response = result

            return response

        except Exception as e:
            logger.error(f"Error in get_list_names: {e}")
            return NYTimesListNamesResponse(
                status="ERROR",
                num_results=0,
                results=[],
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="get_book_reviews_wrapper")
    async def get_book_reviews(
        self,
        author: str | None = None,
        title: str | None = None,
        isbn: str | None = None,
        **kwargs: Any,
    ) -> NYTimesReviewResponse:
        """
        Async wrapper function to get NYTimes book reviews.

        WARNING: This endpoint has been deprecated by NYTimes and returns 404.

        Args:
            author: Author name
            title: Book title
            isbn: ISBN number

        Returns:
            NYTimesReviewResponse: MCSearchResponse derivative containing book reviews or error information
        """
        if not any([author, title, isbn]):
            return NYTimesReviewResponse(
                status="ERROR",
                num_results=0,
                results=[],
                error="At least one of author, title, or isbn parameters is required",
                status_code=400,
            )

        try:
            result, error_code = await self.service.get_book_reviews(
                author=author, title=title, isbn=isbn
            )

            if error_code:
                error_msg = (
                    result.get("error", "Unknown error")
                    if isinstance(result, dict)
                    else "Unknown error"
                )
                return NYTimesReviewResponse(
                    status="ERROR",
                    num_results=0,
                    results=[],
                    error=f"Failed to fetch NYTimes book reviews: {error_msg}",
                    status_code=error_code,
                )

            # Convert to model if it's a dict
            if isinstance(result, dict):
                response = NYTimesReviewResponse.model_validate(result)
            else:
                response = result

            return response

        except Exception as e:
            logger.error(f"Error in get_book_reviews: {e}")
            return NYTimesReviewResponse(
                status="ERROR",
                num_results=0,
                results=[],
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(NYTimesWrapperCache, prefix="search_person_works")
    async def search_person_async(
        self,
        request: MCPersonSearchRequest,
        limit: int | None = None,
    ) -> MCPersonSearchResponse:
        """Search for author works in NYTimes bestseller lists based on person search request.

        This wrapper is called internally by the search_broker, not exposed as a direct endpoint.
        It checks if the author has any works in the bestsellers list and returns them if found.

        Args:
            request: MCPersonSearchRequest with author identification details
            limit: Maximum number of books to return (default: 50)

        Returns:
            MCPersonSearchResponse with author details and works
            - details: None (NYTimes doesn't have author details, only books)
            - works: list[NYTimesBook] (books by the author in bestseller lists)
            - related: [] (empty, will be filled by search_broker)
        """
        try:
            # Validate that this is a NYTimes author
            if request.source != MCSources.NYTIMES:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"Invalid source for NYTimes author search: {request.source}",
                    status_code=400,
                )

            # For NYTimes, we use the author name to search bestseller lists
            author_name = request.name
            if not author_name:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="Author name is required for NYTimes search",
                    status_code=400,
                )

            # Get overview of all bestseller lists (contains all current bestsellers)
            overview_response = await self.get_bestseller_lists()

            if overview_response.status_code != 200:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=overview_response.error or "Failed to fetch bestseller lists",
                    status_code=overview_response.status_code or 500,
                )

            # Extract all books from overview response
            all_books: list[NYTimesBook] = []
            if isinstance(overview_response, NYTimesOverviewResponse):
                if overview_response.results:
                    all_books = overview_response.results
                elif overview_response.overview_results:
                    # Extract books from all lists in overview
                    for list_data in overview_response.overview_results.lists:
                        all_books.extend(list_data.books)

            if not all_books:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error="No books found in bestseller lists",
                    status_code=404,
                )

            # Filter books by author name using soft comparison
            # This handles variations in author name formatting
            matching_books: list[MCBaseItem] = []
            author_name_normalized = author_name.strip()

            for book in all_books:
                if not book.author:
                    continue

                # Use soft comparison for fuzzy matching
                book_author = book.author.strip()
                match, exact_match = soft_compare(author_name_normalized, book_author)

                if match:
                    matching_books.append(book)

            # Apply limit if specified
            if limit is not None and limit > 0:
                matching_books = matching_books[:limit]

            # If no matching books found, return 404
            if not matching_books:
                return MCPersonSearchResponse(
                    input=request,
                    details=None,
                    works=[],
                    related=[],
                    error=f"No books found in bestseller lists for author: {author_name}",
                    status_code=404,
                )

            # Return response with matching books
            # NYTimes doesn't have author details, so details is None
            return MCPersonSearchResponse(
                input=request,
                details=None,  # NYTimes doesn't provide author details
                works=matching_books,  # list[NYTimesBook]
                related=[],  # Will be filled by search_broker
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error searching author works for {request.name}: {e}")
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=str(e),
                status_code=500,
            )


nytimes_wrapper = NYTimesWrapper()
