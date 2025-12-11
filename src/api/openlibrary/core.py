"""
OpenLibrary Core Service - Base service for OpenLibrary API operations
Handles core API communication and basic operations.
"""

from datetime import UTC, datetime
from typing import Any, cast

from api.openlibrary.models import AuthorLinks, MCAuthorItem, MCBookItem
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 24 hours for book data
CacheExpiration = 60 * 60 * 24  # 24 hours

# Request cache - separate from other caches, independent refresh
OpenLibraryRequestCache = RedisCache(
    defaultTTL=12 * 60 * 60,  # 12 hours - book data stable
    prefix="openlibrary_request",
    verbose=False,
    isClassMethod=True,
    version="1.0.22",  # Added support for covers array from author works endpoint
)

BookCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="books",
    verbose=False,
    isClassMethod=True,
    version="1.4.5",  # Added sorting by work_count for author search results
)

logger = get_logger(__name__)


class OpenLibraryService(BaseAPIClient):
    """
    Core OpenLibrary service for API communication.
    Handles basic OpenLibrary operations and cover image URLs.
    """

    # Rate limiter configuration: OpenLibrary API limits
    # API zone: 180 requests per minute = 3 per second
    _rate_limit_max = 20
    _rate_limit_period = 1

    # Cover zone: 400 requests per minute ≈ 6.67 per second
    _cover_rate_limit_max = 6
    _cover_rate_limit_period = 1

    def __init__(self):
        """Initialize OpenLibrary service."""
        self.base_url = "https://openlibrary.org"
        self.search_url = f"{self.base_url}/search.json"
        self.covers_url = "https://covers.openlibrary.org/b"
        self.authors_url = f"{self.base_url}/search/authors.json"
        self.author_url = f"{self.base_url}/authors"
        self.author_images_url = "https://covers.openlibrary.org/a/olid/"
        self.author_images_sizes = ["S", "M", "L"]
        self.work_url = f"{self.base_url}/works"

    @RedisCache.use_cache(OpenLibraryRequestCache, prefix="openlibrary_api")
    async def _make_request(
        self, url: str, params: dict[str, Any] | None = None, max_retries: int = 3
    ) -> tuple[dict[str, Any], int | None]:
        """
        Make an async request to the OpenLibrary API with rate limiting and retry logic.

        This method brokers the call to _core_async_request with OpenLibrary-specific config.
        Uses different rate limits for covers vs API endpoints.

        Args:
            url: URL to request
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            tuple: (response_data, error_code) - error_code is None on success
        """
        headers = {"User-Agent": "mediacircle/1.0 (gould@emgtrading.net)"}

        # Determine rate limit based on URL (covers vs API)
        if "covers" in url:
            rate_limit_max = self._cover_rate_limit_max
            rate_limit_period = self._cover_rate_limit_period
        else:
            rate_limit_max = self._rate_limit_max
            rate_limit_period = self._rate_limit_period

        result = await self._core_async_request(
            url=url,
            params=params,
            headers=headers,
            timeout=60,
            max_retries=max_retries,
            rate_limit_max=rate_limit_max,
            rate_limit_period=rate_limit_period,
        )

        # Cast to expected type since return_status_code=False
        result_dict = cast(dict[str, Any] | None, result)

        if result_dict is None:
            return {"error": "API request failed"}, 500

        return result_dict, None

    def filter_books_by_images(
        self, books: list[MCBookItem], require_cover: bool = True
    ) -> list[MCBookItem]:
        """
        Filter out books without cover images.

        Args:
            books: List of MCBookItem instances to filter
            require_cover: If True, only return books with covers. If False, allow books with work keys even without covers.

        Returns:
            Filtered list of MCBookItem instances with cover images (or work keys if require_cover=False)
        """
        filtered = []

        for book in books:
            has_cover = bool(book.cover_available and (book.book_image or book.cover_urls))
            has_work_key = bool(book.openlibrary_key or book.key)

            if has_cover or (not require_cover and has_work_key):
                filtered.append(book)
            else:
                logger.debug(f"Filtered out book '{book.title}' - no cover image and no work key")

        logger.info(f"Book image filtering: {len(books)} → {len(filtered)} books")
        return filtered

    def process_authors_search_doc(self, author: dict[str, Any]) -> MCAuthorItem | None:
        """
        Process a single author document from OpenLibrary search results.

        Args:
            author: Raw author document from API

        Returns:
            Processed author document with enhanced fields, or None on error
        """
        try:
            key = author.get("key")
            if not key:
                logger.warning("Author document missing key field")
                return None

            author_item = MCAuthorItem(
                key=key,
                name=author.get("name", "Unknown Author"),
                birth_date=author.get("birth_date"),
                top_subjects=author.get("top_subjects", []),
                top_work=author.get("top_work"),
                work_count=author.get("work_count", 0),
                source_id=key,  # Set source_id explicitly
            )

            # Add OpenLibrary URL
            author_item.openlibrary_key = key
            author_item.openlibrary_url = f"{self.base_url}{key}"

            return author_item
        except Exception as e:
            logger.error(f"Error in _process_authors_search_doc: {e}")
            return None

    def process_authors_detail_doc(
        self, author: MCAuthorItem, detail_result: dict[str, Any] | None
    ) -> MCAuthorItem:
        """
        Process a single author document from OpenLibrary authors endpoint.

        Args:
            author: Author item
            detail_result: Raw author document from API (or None if request failed)

        Returns:
            Processed author document with enhanced fields
        """
        if not detail_result:
            author.photo_available = False
            return author

        try:
            # Handle photo URLs - OpenLibrary uses photo IDs from photos array
            photos = detail_result.get("photos", [])
            if photos and len(photos) > 0 and photos[0] != -1:
                photo_id = photos[0]
                # Extract OLID from author key (e.g., "/authors/OL123456A" -> "OL123456A")
                olid = author.key.replace("/authors/", "").replace("/a/", "")
                # Use OLID-based URL pattern for author photos (ensure HTTPS for React Native)
                base_photo_url = f"https://covers.openlibrary.org/a/olid/{olid}"
                author.photo_urls = {
                    "small": f"{base_photo_url}-S.jpg",
                    "medium": f"{base_photo_url}-M.jpg",
                    "large": f"{base_photo_url}-L.jpg",
                }
                author.photo_available = True
                author.author_image = author.photo_urls["medium"]
                author.photo_id = photo_id

                # Populate standardized images array for MCBaseItem consistency
                from contracts.models import MCImage, MCUrlType

                author.images = [
                    MCImage(
                        url=author.photo_urls["small"],
                        key="small",
                        type=MCUrlType.URL,
                        description="author photo",
                    ),
                    MCImage(
                        url=author.photo_urls["medium"],
                        key="medium",
                        type=MCUrlType.URL,
                        description="author photo",
                    ),
                    MCImage(
                        url=author.photo_urls["large"],
                        key="large",
                        type=MCUrlType.URL,
                        description="author photo",
                    ),
                ]
            else:
                author.photo_available = False
                author.images = []

            # Update bio if available
            bio_data = detail_result.get("bio")
            if bio_data:
                if isinstance(bio_data, dict):
                    author.bio = bio_data.get("value")
                else:
                    author.bio = str(bio_data)

            # Update remote IDs
            author.remote_ids = detail_result.get("remote_ids", {})

            # Process links
            links_data = detail_result.get("links", [])
            author.author_links = []
            for link in links_data:
                if isinstance(link, dict) and link.get("title") and link.get("url"):
                    try:
                        author.author_links.append(
                            AuthorLinks(title=link["title"], url=link["url"])
                        )
                    except Exception as link_error:
                        logger.warning(f"Error processing author link: {link_error}")

            # Update birth date if not already set
            if not author.birth_date:
                author.birth_date = detail_result.get("birth_date")

            # Update full name
            author.full_name = detail_result.get("fuller_name") or detail_result.get("name")

            return author
        except Exception as e:
            logger.error(f"Error in _process_authors_detail_doc: {e}")
            author.photo_available = False
            return author

    def _normalize_author_key(self, author_key: str) -> str:
        """
        Normalize author key to format needed for API calls.
        Handles keys in format "/authors/OL123456A" or "OL123456A".

        Args:
            author_key: Author key in various formats

        Returns:
            Normalized key (e.g., "OL123456A")
        """
        # Remove leading/trailing slashes and /authors/ prefix if present
        normalized = author_key.strip().strip("/")
        if normalized.startswith("authors/"):
            normalized = normalized.replace("authors/", "")
        elif normalized.startswith("/authors/"):
            normalized = normalized.replace("/authors/", "")
        return normalized

    @RedisCache.use_cache(OpenLibraryRequestCache, prefix="author_by_key")
    async def get_author_by_key(self, author_key: str, **kwargs: Any) -> MCAuthorItem | None:
        """
        Get author details by their unique OpenLibrary key.

        Args:
            author_key: Author key (e.g., "/authors/OL123456A" or "OL123456A")
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            MCAuthorItem with author details, or None if not found
        """
        try:
            # Normalize the key
            normalized_key = self._normalize_author_key(author_key)

            # Construct URL: /authors/{key}.json
            url = f"{self.author_url}/{normalized_key}.json"

            # Make request
            result, error = await self._make_request(url)

            if error:
                logger.warning(f"Error fetching author {author_key}: {error}")
                return None

            if not result:
                logger.warning(f"Author {author_key} not found")
                return None

            # Create basic author item from key
            full_key = f"/authors/{normalized_key}"
            author_item = MCAuthorItem(
                key=full_key,
                name=result.get("name", "Unknown Author"),
                birth_date=result.get("birth_date"),
                top_subjects=result.get("top_subjects", []),
                top_work=result.get("top_work"),
                work_count=result.get("work_count", 0),
                source_id=full_key,
            )

            # Add OpenLibrary URL
            author_item.openlibrary_key = full_key
            author_item.openlibrary_url = f"{self.base_url}{full_key}"

            # Process detail document to add photos, bio, etc.
            processed_author = self.process_authors_detail_doc(author_item, result)

            return processed_author

        except Exception as e:
            logger.error(f"Error in get_author_by_key for {author_key}: {e}")
            return None

    @RedisCache.use_cache(OpenLibraryRequestCache, prefix="work_details")
    async def get_work_detail(self, work_key: str, **kwargs: Any) -> MCBookItem | None:
        """
        Get work details by their unique OpenLibrary key.

        Args:
            work_key: Work key (e.g., "/works/OL123456A" or "OL123456A")
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            MCBookItem with work details, or None if not found
        """
        try:
            # Construct URL: /works/{key}.json
            url = f"{self.base_url}/{work_key}/editions.json?limit=1"

            # Make request
            result, error = await self._make_request(url)

            if error:
                logger.warning(f"Error fetching work {url}: {error}")
                return None

            if not result:
                logger.warning(f"Work {work_key} not found")
                return None

            entries = result.get("entries", [])
            if entries and len(entries) > 0:
                entry = entries[0]
                if isinstance(entry, dict):
                    return self._process_book_doc(entry)
                else:
                    logger.warning(f"Unexpected entry type for work {work_key}")
                    return None
            else:
                return None

        except Exception as e:
            logger.error(f"Error in get_work_detail for {work_key}: {e}")
            return None

    @RedisCache.use_cache(OpenLibraryRequestCache, prefix="author_works")
    async def get_author_works(
        self,
        author_key: str,
        limit: int = 50,
        offset: int = 0,
        **kwargs: Any,
    ) -> list[MCBookItem]:
        """
        Get works (books) by an author using their unique OpenLibrary key.

        Args:
            author_key: Author key (e.g., "/authors/OL123456A" or "OL123456A")
            limit: Maximum number of works to return (default: 50)
            offset: Offset for pagination (default: 0)
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            List of MCBookItem instances representing the author's works
        """
        try:
            # Normalize the key
            normalized_key = self._normalize_author_key(author_key)

            # Construct URL: /authors/{key}/works.json
            url = f"{self.author_url}/{normalized_key}/works.json"

            # Build params
            params: dict[str, Any] = {
                "limit": min(limit, 100),  # OpenLibrary max is 100
                "offset": offset,
            }

            # Make request
            result, error = await self._make_request(url, params)

            if error:
                logger.warning(f"Error fetching works for author {author_key}: {error}")
                return []

            if not result:
                logger.warning(f"No works found for author {author_key}")
                return []

            # Process works from entries
            entries = result.get("entries", [])
            book_items: list[MCBookItem] = []

            for entry in entries[:limit]:
                # Process each work entry into MCBookItem
                try:
                    book_item = self._process_book_doc(entry)
                    book_items.append(book_item)
                except Exception as e:
                    logger.error(f"Error in _process_book_doc for {entry}: {e}")
                    continue

            return book_items

        except Exception as e:
            logger.error(f"Error in get_author_works for {author_key}: {e}")
            return []

    def _process_book_doc(self, doc: dict[str, Any]) -> MCBookItem:
        """
        Process a single book document from OpenLibrary search results.

        Args:
            doc: Raw book document from API

        Returns:
            MCBookItem instance with processed and enhanced fields
        """
        # Process cover image URLs if cover_i is available
        cover_available = False
        cover_urls: dict[str, str] = {}
        book_image: str | None = None
        cover_i: int | None = None

        if "covers" in doc:
            cover_ids = doc["covers"]
            for cover_id in cover_ids:
                cover_urls = {
                    "small": f"{self.covers_url}/id/{cover_id}-S.jpg",
                    "medium": f"{self.covers_url}/id/{cover_id}-M.jpg",
                    "large": f"{self.covers_url}/id/{cover_id}-L.jpg",
                }
                cover_available = True
                book_image = cover_urls["medium"]
        if "cover_i" in doc:
            cover_id = doc["cover_i"]
            cover_i = cover_id
            # Ensure HTTPS for React Native compatibility
            covers_url = self.covers_url.replace("http://", "https://")
            cover_urls = {
                "small": f"{covers_url}/id/{cover_id}-S.jpg",
                "medium": f"{covers_url}/id/{cover_id}-M.jpg",
                "large": f"{covers_url}/id/{cover_id}-L.jpg",
            }
            cover_available = True
            book_image = cover_urls["medium"]

        # Process work URL if key is available
        openlibrary_key: str | None = None
        openlibrary_url: str | None = None
        if "key" in doc:
            openlibrary_key = doc["key"]
            openlibrary_url = f"{self.base_url}{doc['key']}"

        # Extract and format primary ISBNs
        primary_isbn13: str | None = None
        primary_isbn10: str | None = None
        isbns_formatted: list[dict[str, str]] = []

        if "isbn" in doc:
            isbns = doc["isbn"]
            # Extract primary ISBN13 and ISBN10
            isbn13s = [isbn for isbn in isbns if len(str(isbn)) == 13]
            isbn10s = [isbn for isbn in isbns if len(str(isbn)) == 10]

            if isbn13s:
                primary_isbn13 = isbn13s[0]
            if isbn10s:
                primary_isbn10 = isbn10s[0]

            # Format isbns array for frontend
            for isbn13 in isbn13s[:3]:  # Limit to first 3
                isbns_formatted.append({"isbn13": isbn13})
            for isbn10 in isbn10s[:3]:  # Limit to first 3
                isbns_formatted.append({"isbn10": isbn10})

        # Format author for frontend compatibility
        author: str | None = None
        if "author_name" in doc:
            authors = doc["author_name"]
            author = ", ".join(authors) if isinstance(authors, list) else str(authors)

        # Extract publisher information
        publisher: str | list[str] | None = doc.get("publisher")
        if publisher and isinstance(publisher, list) and publisher:
            publisher = publisher[0]

        # Format subjects for frontend
        subjects: list[str] = []
        if "subject" in doc:
            subject_list = doc["subject"]
            if isinstance(subject_list, list):
                subjects = subject_list[:10]  # Limit to 10 subjects

        # Format language for frontend
        language: str | list[str] | None = doc.get("language")
        if language and isinstance(language, list) and language:
            # Prefer English, otherwise use first language
            if "eng" in language or "en" in language:
                language = "English"
            else:
                lang_map = {
                    "spa": "Spanish",
                    "fre": "French",
                    "ger": "German",
                    "ita": "Italian",
                    "por": "Portuguese",
                    "rus": "Russian",
                    "chi": "Chinese",
                    "jpn": "Japanese",
                    "kor": "Korean",
                }
                language = lang_map.get(language[0], language[0])

        # Add description fallback
        description_raw: Any = doc.get("description")
        description: str | None = None

        # Handle description - can be str, dict, or None
        if description_raw is not None:
            if isinstance(description_raw, dict):
                description = description_raw.get("value")
            elif isinstance(description_raw, str):
                description = description_raw

        # Fallback to first_sentence if no description
        if (not description) and "first_sentence" in doc:
            first_sentences = doc["first_sentence"]
            if isinstance(first_sentences, list) and first_sentences:
                description = first_sentences[0]
        # Process ratings if available
        ratings: dict[str, Any] = {}
        if "ratings_average" in doc or "ratings_count" in doc:
            ratings = {
                "average": doc.get("ratings_average"),
                "count": doc.get("ratings_count"),
            }

        # Process number of pages if available
        number_of_pages: int | None = None
        if "number_of_pages_median" in doc:
            number_of_pages = doc["number_of_pages_median"]

        # Build MCBookItem with all processed fields
        book_item = MCBookItem(
            key=doc.get("key", ""),
            title=doc.get("title", ""),
            openlibrary_key=openlibrary_key,
            openlibrary_url=openlibrary_url,
            author_name=doc.get("author_name", []),
            author=author,
            isbn=doc.get("isbn", []),
            primary_isbn13=primary_isbn13,
            primary_isbn10=primary_isbn10,
            isbns=isbns_formatted,
            first_publish_year=doc.get("first_publish_year"),
            publisher=publisher,
            publish_date=doc.get("publish_date", []),
            publish_year=doc.get("publish_year", []),
            description=description,
            first_sentence=doc.get("first_sentence", []),
            cover_i=cover_i,
            cover_available=cover_available,
            cover_urls=cover_urls,
            book_image=book_image,
            subject=doc.get("subject", []),
            subjects=subjects,
            language=language,
            ratings=ratings,
            ratings_average=doc.get("ratings_average"),
            ratings_count=doc.get("ratings_count"),
            readinglog_count=doc.get("readinglog_count", 0),
            want_to_read_count=doc.get("want_to_read_count"),
            currently_reading_count=doc.get("currently_reading_count"),
            already_read_count=doc.get("already_read_count"),
            number_of_pages=number_of_pages,
            number_of_pages_median=doc.get("number_of_pages_median"),
            rank=doc.get("rank", 0),
            rank_last_week=doc.get("rank_last_week", 0),
            weeks_on_list=doc.get("weeks_on_list", 0),
            price=doc.get("price", "0.00"),
            purchase_links=doc.get("purchase_links", []),
            source_id=openlibrary_key or doc.get("key", ""),  # Set source_id explicitly
        )

        # mc_id and mc_type will be auto-generated by the model_validator
        return book_item

    def _calculate_blended_score(
        self, book: MCBookItem, idx: int, total: int, max_reads: int, search_query: str = ""
    ) -> float:
        """
        Calculate blended ranking score for a book.

        Args:
            book: MCBookItem instance
            idx: Position in original results
            total: Total number of results
            max_reads: Maximum readinglog_count in results
            search_query: The search query for title matching (optional)

        Returns:
            Blended score (higher is better)
        """
        now = datetime.now(UTC).year
        reads = book.readinglog_count or 0
        year = book.first_publish_year or now
        year_int = int(year) if year else now

        # Title match score - boost exact and near-exact matches
        title_match_score = 0.0
        if search_query and book.title:
            query_lower = search_query.lower().strip()
            title_lower = book.title.lower().strip()

            # Normalize by removing common articles for better matching
            def normalize_title(title: str) -> str:
                """Remove leading articles (the, a, an) for comparison"""
                for article in [" the ", " a ", " an "]:
                    if title.startswith(article[1:]):
                        title = title[len(article) - 1 :]
                        break
                return title.strip()

            query_normalized = normalize_title(query_lower)
            title_normalized = normalize_title(title_lower)

            if query_lower == title_lower:
                # Perfect exact match - highest boost
                title_match_score = 1.0
            elif query_normalized == title_normalized:
                # Exact match after article normalization - treat as perfect match
                # This ensures "The Secret of Secrets" matches "Secret of secrets"
                title_match_score = 1.0
            elif query_normalized in title_normalized or title_normalized in query_normalized:
                # Partial match - very high boost
                title_match_score = 0.9
            elif query_lower in title_lower or title_lower in query_lower:
                # Partial match without normalization - high boost
                title_match_score = 0.8
            else:
                # Check word overlap for fuzzy matching
                query_words = set(query_normalized.split())
                title_words = set(title_normalized.split())
                if query_words and title_words:
                    overlap = len(query_words & title_words) / len(query_words)
                    # Higher base score for word overlap
                    title_match_score = overlap * 0.7

        # Recency score with strong boost for very recent books
        # Heavily favor books from last 5 years (bestsellers, new releases)
        years_old = now - year_int
        if years_old <= 1:
            # Current year books: maximum boost
            recency = 1.0
        elif years_old <= 3:
            # Books 1-3 years old: very high boost (0.85-0.95)
            recency = 1.0 - (years_old * 0.05)
        elif years_old <= 5:
            # Books 3-5 years old: high boost (0.75-0.85)
            recency = 0.85 - ((years_old - 3) * 0.05)
        elif years_old <= 10:
            # Books 6-10 years old: medium boost (0.5-0.75)
            recency = 0.75 - ((years_old - 5) * 0.05)
        else:
            # Books 10+ years old: gradual fade
            recency = max(0.0, 0.5 - ((years_old - 10) / 30.0))

        # Relevance score (1.0 for first doc, ~0.0 for last)
        # Reduced weight since OpenLibrary often ranks older books higher
        relevance = max(0.0, 1.0 - idx / max(total, 1))

        # Popularity score (normalized by max reads)
        popularity = reads / max(1, max_reads) if max_reads > 0 else 0

        # Weighted blend optimized for title searches with recent books:
        # - 30% title match (exact/near-exact matches get priority)
        # - 50% recency (HEAVILY favor recent books - bestsellers should rank first)
        # - 15% relevance (OpenLibrary's ranking, minimal weight)
        # - 5% popularity (reader engagement, tie-breaker)
        #
        # Rationale: For title searches, users expect recent bestsellers first.
        # A 2025 Dan Brown book should beat a 1982 book with same title.
        return 0.30 * title_match_score + 0.50 * recency + 0.15 * relevance + 0.05 * popularity
