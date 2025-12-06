"""
Google Books Search Service - Search operations for Google Books
Handles book search, volume lookup, and result processing.
"""

from typing import Any

from api.subapi._google.core import google_books_service
from api.subapi._google.models import (
    GoogleBooksItem,
    GoogleBooksSearchResponse,
    GoogleBooksVolumeResponse,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)


class GoogleBooksSearchService:
    """
    Google Books Search Service - Handles book search and enrichment.
    Extends GoogleBooksService with search-specific functionality.
    """

    def __init__(self):
        """Initialize Google Books Search Service."""
        self.service = google_books_service

    def _convert_volume_to_book_item(self, volume_raw: dict[str, Any]) -> GoogleBooksItem:
        """
        Convert Google Books API volume to GoogleBooksItem format.
        This is the core conversion logic extracted from unified_search.py.

        Args:
            volume_raw: Raw volume data from Google Books API

        Returns:
            GoogleBooksItem or None if conversion fails
        """
        try:
            volume_info = volume_raw.get("volumeInfo", {})
            sale_info = volume_raw.get("saleInfo", {})
            google_id = volume_raw.get("id", "")

            # Basic info
            title = volume_info.get("title", "")
            authors = volume_info.get("authors", [])

            if not title or not authors:
                logger.debug(f"Skipping book without title or author: {google_id}")
                return GoogleBooksItem(
                    error=f"Skipping book without title or author: {google_id}",
                )

            # Create book item
            book = GoogleBooksItem(
                google_id=google_id,
                title=title,
                subtitle=volume_info.get("subtitle"),
                author=", ".join(authors) if isinstance(authors, list) else str(authors),
                author_name=authors,
                description=volume_info.get("description", ""),
                google_description=volume_info.get("description", ""),
                publisher=volume_info.get("publisher", ""),
                published_date=volume_info.get("publishedDate"),
                first_publish_year=self.service._extract_year_from_date(
                    volume_info.get("publishedDate"),
                ),
                number_of_pages=volume_info.get("pageCount"),
                language=volume_info.get("language"),
                subjects=volume_info.get("categories", []),
                categories=volume_info.get("categories", []),
                # ISBNs
                isbn=[],
                primary_isbn13=None,
                primary_isbn10=None,
                # Cover images
                cover_available=False,
                cover_urls={},
                book_image=None,
                # Google Books specific
                google_info_link=volume_info.get("infoLink"),
                google_preview_link=volume_info.get("previewLink"),
                google_canonical_link=volume_info.get("canonicalVolumeLink"),
                google_ratings_average=volume_info.get("averageRating"),
                google_ratings_count=volume_info.get("ratingsCount"),
                # OpenLibrary compatibility
                rank=0,
                rank_last_week=0,
                weeks_on_list=0,
                price="0.00",
                readinglog_count=volume_info.get("ratingsCount", 0),
                # Generate key/ID
                key=f"/works/GOOGLE_{google_id}",
                openlibrary_key=f"/works/GOOGLE_{google_id}",
            )

            # Handle ISBNs
            industry_identifiers = volume_info.get("industryIdentifiers", [])
            for identifier in industry_identifiers:
                isbn_type = identifier.get("type")
                isbn_value = identifier.get("identifier")
                if isbn_type == "ISBN_13":
                    book.primary_isbn13 = isbn_value
                    book.isbn.append(isbn_value)
                elif isbn_type == "ISBN_10":
                    book.primary_isbn10 = isbn_value
                    book.isbn.append(isbn_value)

            # Handle cover images
            image_links = volume_info.get("imageLinks", {})
            if image_links:
                book.cover_available = True
                # Convert http to https for React Native compatibility
                thumbnail_url = self.service._ensure_https(image_links.get("thumbnail", ""))
                if thumbnail_url:
                    book.cover_urls = {
                        "small": thumbnail_url.replace("&zoom=1", "&zoom=0"),
                        "medium": thumbnail_url.replace("&zoom=1", "&zoom=1"),
                        "large": thumbnail_url.replace("&zoom=1", "&zoom=2"),
                    }
                    book.book_image = book.cover_urls["medium"]

            # Purchase links
            book.purchase_links = []
            if sale_info.get("buyLink"):
                book.purchase_links.append(
                    {
                        "retailer": "Google Books",
                        "url": sale_info.get("buyLink"),
                        "format": "ebook",
                    }
                )
                book.google_buy_link = sale_info.get("buyLink")

            if volume_info.get("infoLink"):
                book.purchase_links.append(
                    {
                        "retailer": "Google Books (Info)",
                        "url": volume_info.get("infoLink"),
                        "format": "info",
                    }
                )

            # Sale info
            book.google_country = sale_info.get("country")
            book.google_saleability = sale_info.get("saleability")
            if sale_info.get("retailPrice"):
                book.google_retail_price = sale_info.get("retailPrice", {})

            # Generate mc_id and mc_type (will be auto-generated by model validator)
            return book

        except Exception as e:
            logger.error(f"Error converting Google Books volume: {e}")
            error_response = GoogleBooksItem(
                error=str(e),
            )
            return error_response

    async def search_books(
        self,
        query: str,
        max_results: int = 10,
        start_index: int = 0,
        order_by: str = "relevance",
        print_type: str = "books",
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Search Google Books API for books.

        Args:
            query: Search query string
            max_results: Maximum number of results (1-40, Google Books limit)
            start_index: Starting index for pagination
            order_by: Sort order ('relevance' or 'newest')
            print_type: Type of content ('all', 'books', 'magazines')
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (search_response, error_code or None)
        """
        try:
            # Validate and limit max_results
            max_results = min(max_results, 40)  # Google Books max is 40
            max_results = max(max_results, 1)

            params: dict[str, str | int] = {
                "q": query,
                "maxResults": max_results,
                "startIndex": start_index,
                "orderBy": order_by,
                "printType": print_type,
                "projection": "full",
            }

            # Make request
            data, error = await self.service._make_request("volumes", params)

            if error:
                return GoogleBooksSearchResponse(
                    error=f"Google Books search returned error {error}: {data}",
                    query=query,
                    data_source="Google Books API",
                )

            # Process results
            total_items = data.get("totalItems", 0)
            raw_items = data.get("items", [])

            # Convert volumes to book items
            books: list[GoogleBooksItem] = []
            for raw_volume in raw_items:
                book = self._convert_volume_to_book_item(raw_volume)
                if not book.error:
                    books.append(book)

            # Create response
            return GoogleBooksSearchResponse(
                kind=data.get("kind", "books#volumes"),
                totalItems=total_items,
                items=books,
                docs=books,  # OpenLibrary compatibility
                num_found=total_items,
                query=query,
                data_source="Google Books API",
            )

        except Exception as e:
            logger.error(f"Error in Google Books search: {e}")
            error_response = GoogleBooksSearchResponse(
                num_found=0,
                query=query,
                data_source="Google Books API",
                error=str(e),
            )
            return error_response

    async def search_by_isbn(
        self,
        isbn: str,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Search for a book by ISBN.

        Args:
            isbn: ISBN-10 or ISBN-13
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (book_item or None, error_code or None)
        """
        try:
            data, error = await self.service.get_volume_by_isbn(isbn, **kwargs)

            if error:
                error_msg = "Failed to fetch data"
                if isinstance(data, dict):
                    error_msg = data.get("error", error_msg)
                elif isinstance(data, str):
                    error_msg = data
                error_response = GoogleBooksSearchResponse(
                    kind="books#volumes",
                    query=isbn,
                    data_source="Google Books API",
                    error=error_msg,
                )
                return error_response

            items = data.get("items", [])
            if not items:
                logger.info(f"No book found for ISBN: {isbn}")
                empty_response = GoogleBooksSearchResponse(
                    error=f"No book found for ISBN: {isbn} (404)",
                    kind="books#volumes",
                    query=isbn,
                    data_source="Google Books API",
                )
                return empty_response

            # Convert first result
            book = self._convert_volume_to_book_item(items[0])
            if book.error:
                empty_response = GoogleBooksSearchResponse(
                    error=f"No book found for ISBN: {isbn} (404)",
                    kind="books#volumes",
                    query=isbn,
                    data_source="Google Books API",
                )
                return empty_response

            logger.info(f"Found book for ISBN {isbn}: {book.title}")
            response = GoogleBooksSearchResponse(
                kind="books#volumes",
                totalItems=1,
                items=[book],
                docs=[book],
                num_found=1,
                query=isbn,
                data_source="Google Books API",
            )
            return response

        except Exception as e:
            logger.error(f"Error searching by ISBN: {e}")
            return GoogleBooksSearchResponse(
                kind="books#volumes",
                totalItems=0,
                items=[],
                docs=[],
                num_found=0,
                query=isbn,
                data_source="Google Books API",
                error=str(e),
            )

    async def get_volume_by_id(
        self,
        volume_id: str,
        **kwargs: Any,
    ) -> GoogleBooksVolumeResponse:
        """
        Get a specific volume by its Google Books ID.

        Args:
            volume_id: Google Books volume ID
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (volume_data, error_code or None)
        """
        result, error = await self.service.get_volume_by_id(volume_id, **kwargs)
        if result:
            return GoogleBooksVolumeResponse(
                volume=result,
                data_source="Google Books API",
            )
        else:
            return GoogleBooksVolumeResponse(
                volume=None,
                data_source="Google Books API",
                error=error,
            )

    async def search_by_title_and_author(
        self,
        title: str | None = None,
        author: str | None = None,
        max_results: int = 10,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Search for books by title and/or author.

        Args:
            title: Book title
            author: Author name
            max_results: Maximum number of results
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            Tuple of (search_response, error_code or None)
        """
        if not title and not author:
            return GoogleBooksSearchResponse(
                error="Either title or author must be provided",
                kind="books#volumes",
                totalItems=0,
                items=[],
                docs=[],
                num_found=0,
                query=None,
                data_source="Google Books API",
            )

        # Build query
        query_parts = []
        if title:
            query_parts.append(f'intitle:"{title}"')
        if author:
            query_parts.append(f'inauthor:"{author}"')

        query = " ".join(query_parts)

        return await self.search_books(query, max_results=max_results, **kwargs)

    async def search_direct(
        self,
        query: str,
        max_results: int = 10,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Direct search that returns raw book dictionaries.
        This matches the interface used in unified_search.py.

        Args:
            query: Search query
            max_results: Maximum number of results
            **kwargs: Additional arguments

        Returns:
            List of book dictionaries
        """
        return await self.search_books(query, max_results=max_results, **kwargs)

    async def _fetch_google_books_by_isbn(self, isbn: str) -> dict[str, Any]:
        """
        Fetch Google Books data by ISBN.

        Args:
            isbn: ISBN-10 or ISBN-13

        Returns:
            Dictionary with Google Books data in format expected by _apply_google_books_enrichment
        """
        try:
            result = await self.search_by_isbn(isbn)
            if result.error:
                return result.model_dump()

            return self._convert_book_item_to_enrichment_data(result.items[0])

        except Exception as e:
            logger.warning(f"Error fetching Google Books data by ISBN {isbn}: {e}")
            return {}

    async def _fetch_google_books_by_search(
        self, title: str, author: str | None = None
    ) -> dict[str, Any]:
        """
        Fetch Google Books data by title and author search.

        Args:
            title: Book title
            author: Optional author name

        Returns:
            Dictionary with Google Books data in format expected by _apply_google_books_enrichment
        """
        try:
            result = await self.search_by_title_and_author(
                title=title, author=author, max_results=1
            )
            if result.error:
                return result.model_dump()

            return self._convert_book_item_to_enrichment_data(result.items[0])
        except Exception as e:
            logger.warning(
                f"Error fetching Google Books data by search (title={title}, author={author}): {e}"
            )
            return {}

    def _convert_book_item_to_enrichment_data(self, book: GoogleBooksItem) -> dict[str, Any]:
        """
        Convert GoogleBooksItem to enrichment data format.

        Args:
            book: GoogleBooksItem instance

        Returns:
            Dictionary in format expected by _apply_google_books_enrichment
        """
        return {
            "google_description": book.google_description or book.description,
            "google_isbn13": book.primary_isbn13,
            "google_isbn10": book.primary_isbn10,
            "google_publisher": book.publisher,
            "google_page_count": book.number_of_pages,
            "google_categories": book.categories or book.subjects,
            "google_language": book.language,
            "google_ratings_average": book.google_ratings_average,
            "google_ratings_count": book.google_ratings_count,
            "google_image_links": book.cover_urls,
            "google_buy_link": book.google_buy_link,
            "google_info_link": book.google_info_link,
            "google_preview_link": book.google_preview_link,
            "google_subtitle": book.subtitle,
            "google_published_date": book.published_date,
            "google_retail_price": book.google_retail_price,
        }

    def _extract_enrichment_data_from_volume(
        self, volume_info: dict[str, Any], sale_info: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Extract enrichment data from raw Google Books volume data.

        Args:
            volume_info: Volume info dict from Google Books API
            sale_info: Sale info dict from Google Books API

        Returns:
            Dictionary in format expected by _apply_google_books_enrichment
        """
        # Extract ISBNs
        isbn13 = None
        isbn10 = None
        for identifier in volume_info.get("industryIdentifiers", []):
            if identifier.get("type") == "ISBN_13":
                isbn13 = identifier.get("identifier")
            elif identifier.get("type") == "ISBN_10":
                isbn10 = identifier.get("identifier")

        # Extract image links
        image_links = volume_info.get("imageLinks", {})
        google_image_links = {}
        if image_links.get("thumbnail"):
            google_image_links["thumbnail"] = self.service._ensure_https(
                image_links.get("thumbnail")
            )
        if image_links.get("small"):
            google_image_links["small"] = self.service._ensure_https(image_links.get("small"))
        if image_links.get("medium"):
            google_image_links["medium"] = self.service._ensure_https(image_links.get("medium"))
        if image_links.get("large"):
            google_image_links["large"] = self.service._ensure_https(image_links.get("large"))

        return {
            "google_description": volume_info.get("description"),
            "google_isbn13": isbn13,
            "google_isbn10": isbn10,
            "google_publisher": volume_info.get("publisher"),
            "google_page_count": volume_info.get("pageCount"),
            "google_categories": volume_info.get("categories", []),
            "google_language": volume_info.get("language"),
            "google_ratings_average": volume_info.get("averageRating"),
            "google_ratings_count": volume_info.get("ratingsCount"),
            "google_image_links": google_image_links,
            "google_buy_link": sale_info.get("buyLink"),
            "google_info_link": volume_info.get("infoLink"),
            "google_preview_link": volume_info.get("previewLink"),
            "google_subtitle": volume_info.get("subtitle"),
            "google_published_date": volume_info.get("publishedDate"),
            "google_retail_price": sale_info.get("retailPrice", {}),
        }

    async def _enrich_with_google_books(self, doc: dict[str, Any]) -> None:
        """
        Comprehensively enrich a single book document with Google Books data.

        Args:
            doc: Book document to enrich
        """
        google_data: dict[str, Any] = {}

        # Strategy 1: Try ISBN lookup first (most accurate)
        isbns = doc.get("isbn", [])
        if isbns:
            for isbn in isbns[:3]:  # Try first 3 ISBNs
                google_data = await self._fetch_google_books_by_isbn(str(isbn))
                if google_data:  # Found data, stop trying more ISBNs
                    break

        # Strategy 2: If no ISBN data found, try title + author search
        if not google_data:
            book_title = doc.get("title")
            book_authors = doc.get("author_name", [])
            author = book_authors[0] if book_authors else None

            if book_title:
                google_data = await self._fetch_google_books_by_search(book_title, author)

        # If we found Google Books data, enrich the document
        if google_data:
            self._apply_google_books_enrichment(doc, google_data)

    def _apply_google_books_enrichment(
        self, doc: dict[str, Any], google_data: dict[str, Any]
    ) -> None:
        """
        Apply Google Books data to enhance the OpenLibrary document.

        Args:
            doc: Book document to enrich
            google_data: Google Books data
        """
        # Description - Fill if missing or enhance existing
        if google_data.get("google_description"):
            if not doc.get("description"):
                doc["description"] = google_data["google_description"]
            # Also store Google's version for reference
            doc["google_description"] = google_data["google_description"]

        # ISBNs - Fill in missing primary ISBNs
        if google_data.get("google_isbn13") and not doc.get("primary_isbn13"):
            doc["primary_isbn13"] = google_data["google_isbn13"]
            if "isbns" not in doc:
                doc["isbns"] = []
            if not any(item.get("isbn13") == google_data["google_isbn13"] for item in doc["isbns"]):
                doc["isbns"].append({"isbn13": google_data["google_isbn13"]})

        if google_data.get("google_isbn10") and not doc.get("primary_isbn10"):
            doc["primary_isbn10"] = google_data["google_isbn10"]
            if "isbns" not in doc:
                doc["isbns"] = []
            if not any(item.get("isbn10") == google_data["google_isbn10"] for item in doc["isbns"]):
                doc["isbns"].append({"isbn10": google_data["google_isbn10"]})

        # Publisher - Use Google's if OpenLibrary doesn't have it
        if google_data.get("google_publisher") and not doc.get("publisher"):
            doc["publisher"] = google_data["google_publisher"]

        # Page count - Fill if missing
        if google_data.get("google_page_count") and not doc.get("number_of_pages"):
            doc["number_of_pages"] = google_data["google_page_count"]

        # Categories/Subjects - Merge with existing
        if google_data.get("google_categories"):
            existing_subjects = set(doc.get("subjects", []))
            google_subjects = set(google_data["google_categories"])
            all_subjects = list(existing_subjects.union(google_subjects))
            doc["subjects"] = all_subjects[:15]  # Limit to 15 total subjects

        # Language - Use Google's if more specific
        if google_data.get("google_language") and not doc.get("language"):
            lang_map = {
                "en": "English",
                "es": "Spanish",
                "fr": "French",
                "de": "German",
                "it": "Italian",
                "pt": "Portuguese",
                "ru": "Russian",
                "zh": "Chinese",
                "ja": "Japanese",
                "ko": "Korean",
                "ar": "Arabic",
                "hi": "Hindi",
            }
            doc["language"] = lang_map.get(
                google_data["google_language"], google_data["google_language"]
            )

        # Ratings - Use Google's if not present
        if (
            google_data.get("google_ratings_average") or google_data.get("google_ratings_count")
        ) and not doc.get("ratings"):
            doc["ratings"] = {
                "average": google_data.get("google_ratings_average"),
                "count": google_data.get("google_ratings_count"),
            }

        # Cover images - Use Google's if better quality or missing
        google_images = google_data.get("google_image_links", {})
        if google_images and not doc.get("cover_available"):
            # Create cover URLs from Google Books images
            google_cover_urls: dict[str, str] = {}
            if google_images.get("thumbnail"):
                google_cover_urls["small"] = google_images["thumbnail"]
            if google_images.get("small"):
                google_cover_urls["medium"] = google_images["small"]
            if google_images.get("medium") or google_images.get("large"):
                google_cover_urls["large"] = google_images.get("large") or google_images.get(
                    "medium"
                )

            if google_cover_urls:
                doc["cover_urls"] = google_cover_urls
                doc["cover_available"] = True
                doc["book_image"] = google_cover_urls.get("medium") or google_cover_urls.get(
                    "small"
                )

        # Purchase and info links
        purchase_links: list[dict[str, str]] = []
        if google_data.get("google_buy_link"):
            purchase_links.append(
                {
                    "retailer": "Google Books",
                    "url": google_data["google_buy_link"],
                    "format": "ebook",
                }
            )

        if google_data.get("google_info_link"):
            purchase_links.append(
                {
                    "retailer": "Google Books (Info)",
                    "url": google_data["google_info_link"],
                    "format": "info",
                }
            )

        if google_data.get("google_preview_link"):
            purchase_links.append(
                {
                    "retailer": "Google Books (Preview)",
                    "url": google_data["google_preview_link"],
                    "format": "preview",
                }
            )

        if purchase_links:
            doc["purchase_links"] = purchase_links

        # Store additional Google Books metadata
        if google_data.get("google_subtitle"):
            doc["google_subtitle"] = google_data["google_subtitle"]
        if google_data.get("google_published_date"):
            doc["google_published_date"] = google_data["google_published_date"]
        if google_data.get("google_retail_price"):
            doc["google_retail_price"] = google_data["google_retail_price"]

    async def search_books_with_google(
        self,
        query: str | None = None,
        title: str | None = None,
        author: str | None = None,
        isbn: str | None = None,
        limit: int = 10,
        offset: int = 0,
        **kwargs: Any,
    ) -> GoogleBooksSearchResponse:
        """
        Enhanced search that enriches OpenLibrary results with Google Books data.

        Args:
            query: General search query
            title: Book title
            author: Author name
            isbn: ISBN (10 or 13 digit)
            limit: Number of results to return
            offset: Offset for pagination
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            tuple: (enriched_search_results, error_code)
        """

        if isbn:
            isbn_result = await self.search_by_isbn(isbn, **kwargs)
            if isbn_result.error:
                return isbn_result
        elif title or author:
            search_result = await self.search_by_title_and_author(
                title=title, author=author, max_results=limit, **kwargs
            )
            if search_result.error:
                # Convert error result to dict if needed
                return search_result
        else:
            if not query:
                return GoogleBooksSearchResponse(
                    error="At least one search parameter is required",
                    kind="books#volumes",
                    totalItems=0,
                    items=[],
                    docs=[],
                    num_found=0,
                    query=None,
                    data_source="Google Books API",
                )
            search_result = await self.search_books(
                query=query, max_results=limit, start_index=offset, **kwargs
            )
            if search_result.error:
                return search_result

        # Enrich each book with Google Books data
        docs: list[GoogleBooksItem] = []
        for doc in search_result.docs:
            await self._enrich_with_google_books(doc.model_dump())
            docs.append(doc)

        return GoogleBooksSearchResponse(
            kind="books#volumes",
            totalItems=len(docs),
            items=docs,
            docs=docs,
            num_found=len(docs),
            query=query,
            data_source="Google Books API",
        )


google_books_search_service = GoogleBooksSearchService()
