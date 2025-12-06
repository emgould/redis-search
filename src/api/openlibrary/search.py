"""
OpenLibrary Search Service - Search operations for OpenLibrary
Handles book search, Google Books enrichment, and result processing.
"""

import asyncio
from typing import Any

from api.openlibrary.core import BookCache, OpenLibraryService
from api.openlibrary.models import (
    MCAuthorItem,
    MCBookItem,
    OpenLibraryAuthorSearchResponse,
    OpenLibrarySearchResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache
from utils.soft_comparison import soft_compare

logger = get_logger(__name__)


class OpenLibrarySearchService(OpenLibraryService):
    """
    OpenLibrary Search Service - Handles book search and enrichment.
    Extends OpenLibraryService with search-specific functionality.
    """

    @RedisCache.use_cache(BookCache, prefix="book_search")
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
        Search for books using OpenLibrary Search API.

        Args:
            query: General search query
            title: Book title
            author: Author name
            isbn: ISBN (10 or 13 digit)
            limit: Number of results to return (max 100)
            offset: Offset for pagination
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            tuple: (list of book dicts, error_code) where error_code is None on success
        """
        try:
            params: dict[str, Any] = {
                "limit": min(limit, 100),  # OpenLibrary max is 100
                "offset": offset,
            }

            # Build search query
            search_parts = []
            if query:
                search_parts.append(query)
            if title:
                search_parts.append(f'title:"{title}"')
            if author:
                search_parts.append(f'author:"{author}"')
            if isbn:
                search_parts.append(f"isbn:{isbn}")

            if not search_parts:
                logger.error("error: At least one search parameter is required")
                return OpenLibrarySearchResponse(
                    results=[],
                    total_results=0,
                    query=query or title or author or isbn,
                    error="At least one search parameter is required",
                    data_source="search_books",
                )

            params["q"] = " ".join(search_parts)

            result, error = await self._make_request(self.search_url, params)

            if error:
                # Extract error message from result if available, otherwise use error code
                error_msg = (
                    result.get("error", str(error)) if isinstance(result, dict) else str(error)
                )
                return OpenLibrarySearchResponse(
                    results=[],
                    total_results=0,
                    query=query or title or author or isbn,
                    error=error_msg,
                    data_source="search_books",
                )

            docs = result.get("docs", [])
            total = len(docs)

            # Process each document into MCBookItem instances
            book_items: list[MCBookItem] = []
            if "docs" in result:
                for doc in result["docs"]:
                    book_item = self._process_book_doc(doc)
                    book_items.append(book_item)

            # Calculate max reads for normalization
            max_reads = max((book.readinglog_count or 0 for book in book_items), default=1)

            # Determine search query for title matching
            search_query = title or query or ""

            # Compute blended ranking score and sort
            books_with_index = [(i, book) for i, book in enumerate(book_items)]
            books_with_index.sort(
                key=lambda b_idx: self._calculate_blended_score(
                    b_idx[1], b_idx[0], total, max_reads, search_query
                ),
                reverse=True,
            )

            # Extract sorted MCBookItems
            sorted_books = [book for _, book in books_with_index]

            # Filter books by images
            filtered_books = self.filter_books_by_images(sorted_books)

            return OpenLibrarySearchResponse(
                results=filtered_books,
                total_results=total,
                query=query or title or author or isbn,
                data_source="search_books",
            )

        except Exception as e:
            logger.error(f"Error in search_books: {e}")
            return OpenLibrarySearchResponse(
                results=[],
                total_results=0,
                query=query or title or author or isbn,
                error=str(e),
                data_source="search_books",
            )

    @RedisCache.use_cache(BookCache, prefix="author_search_v2")
    async def search_authors(
        self,
        query: str | None = None,
        limit: int = 5,
        offset: int = 0,
        **kwargs: Any,
    ) -> OpenLibraryAuthorSearchResponse:
        """
        Search for authors using OpenLibrary Search API.

        Args:
            query: General search query
            limit: Number of results to return (max 100)
            offset: Offset for pagination
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            tuple: (search_results, error_code)
        """
        try:
            params: dict[str, Any] = {
                "limit": min(limit, 100),  # OpenLibrary max is 100
                "offset": offset,
            }

            # Build search query
            search_parts = []
            if query:
                search_parts.append(query)

            if not search_parts:
                logger.error("error: At least one search parameter is required")
                return OpenLibraryAuthorSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error="At least one search parameter is required",
                    data_source="search_authors",
                )

            params["q"] = " ".join(search_parts)

            # Make initial search request
            result, error = await self._make_request(self.authors_url, params)
            if error:
                # Extract error message from result if available, otherwise use error code
                error_msg = (
                    result.get("error", str(error)) if isinstance(result, dict) else str(error)
                )
                return OpenLibraryAuthorSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error=error_msg,
                    data_source="search_authors",
                )

            # Check if we have results
            docs = result.get("docs", [])
            if not docs:
                return OpenLibraryAuthorSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error=None,
                    data_source="search_authors",
                )

            # Process search results
            authors = []
            for doc in docs[:limit]:
                author = self.process_authors_search_doc(doc)
                if author:
                    authors.append(author)

            if not authors:
                return OpenLibraryAuthorSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error=None,
                    data_source="search_authors",
                )

            # Fetch detailed information for each author
            # author.key is already in format "/authors/OL123456A", so use base_url directly
            details_tasks = [
                self._make_request(f"{self.author_url}/{author.key}.json") for author in authors
            ]
            details_results = await asyncio.gather(*details_tasks, return_exceptions=True)

            # Process detail results
            processed_authors: list[MCAuthorItem] = []
            for author, detail_result in zip(authors, details_results, strict=False):
                if isinstance(detail_result, Exception):
                    logger.warning(
                        f"Error fetching author details for {author.key}: {detail_result}"
                    )
                    # Still include author with basic info
                    processed_authors.append(author)
                else:
                    # detail_result is a tuple from _make_request: (dict, int | None)
                    # Type narrowing: if not Exception, it's a tuple
                    assert isinstance(detail_result, tuple), "Expected tuple from _make_request"
                    detail_data, detail_error = detail_result
                    if detail_error:
                        logger.warning(
                            f"Error fetching author details for {author.key}: {detail_error}"
                        )
                        processed_authors.append(author)
                    else:
                        processed_author = self.process_authors_detail_doc(author, detail_data)
                        processed_authors.append(processed_author)

            # Filter authors using soft comparison to ensure they match the query
            # This prevents false positives from fuzzy search results
            validated_authors: list[MCAuthorItem] = []
            if query:
                query_normalized = query.strip()
                for author in processed_authors:
                    # Compare against author name (use name or full_name if available)
                    author_name = author.full_name or author.name or ""
                    if author_name:
                        names_match, _ = soft_compare(query_normalized, author_name)
                        if names_match:
                            validated_authors.append(author)
                        else:
                            logger.debug(
                                f"Filtered out author '{author_name}' - does not match query '{query_normalized}'"
                            )
                    else:
                        # If author has no name, skip it
                        logger.debug(
                            f"Filtered out author with key {author.key} - no name available"
                        )
            else:
                # If no query provided, include all processed authors
                validated_authors = processed_authors

            # Sort authors by work_count in descending order (most prolific first)
            sorted_authors = sorted(
                validated_authors, key=lambda author: author.work_count or 0, reverse=True
            )

            # Log the sorted authors for debugging
            logger.info(
                f"Author search for '{query}': found {len(validated_authors)} authors, "
                f"sorted by work_count: {[(a.name, a.work_count) for a in sorted_authors[:5]]}"
            )

            # Limit the results to the requested amount
            filtered_authors = sorted_authors[:limit]

            return OpenLibraryAuthorSearchResponse(
                results=filtered_authors,
                total_results=len(filtered_authors),
                query=query,
                error=None,
                data_source="search_authors",
            )

        except Exception as e:
            logger.error(f"Error in search_authors: {e}")
            return OpenLibraryAuthorSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
            )


openlibrary_search_service = OpenLibrarySearchService()
