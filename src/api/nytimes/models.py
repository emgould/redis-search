"""
NYTimes Models - Pydantic models for NYTimes Books API data structures
Follows the same pattern as tmdb/models.py with Pydantic 2.0
"""

from typing import Any

from contracts.models import MCBaseItem, MCSearchResponse, MCSources, MCType, generate_mc_id
from pydantic import Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods

# ============================================================================
# Raw NYTimes API Response Models
# ============================================================================


class NYTimesISBN(BaseModelWithMethods):
    """Model for ISBN data from NYTimes API."""

    isbn10: str | None = None
    isbn13: str | None = None


class NYTimesBuyLink(BaseModelWithMethods):
    """Model for buy link data from NYTimes API."""

    name: str
    url: str


class NYTimesBook(MCBaseItem):
    """Model for a book from NYTimes bestseller list."""

    # MCBaseItem fields
    mc_type: MCType = MCType.BOOK
    source: MCSources = MCSources.NYTIMES

    # Book identifier for mc_id generation
    primary_isbn10: str | None = None
    primary_isbn13: str | None = None

    # NYTimes book fields
    rank: int = 0
    rank_last_week: int = 0
    weeks_on_list: int = 0
    asterisk: int = 0
    dagger: int = 0
    publisher: str = ""
    description: str = ""
    price: str | None = None
    title: str
    author: str
    contributor: str = ""
    contributor_note: str = ""
    book_image: str | None = None
    book_image_width: int | None = None
    book_image_height: int | None = None
    amazon_product_url: str | None = None
    age_group: str = ""
    book_review_link: str | None = None
    first_chapter_link: str | None = None
    sunday_review_link: str | None = None
    article_chapter_link: str | None = None
    isbns: list[dict[str, str]] = Field(default_factory=list)
    buy_links: list[dict[str, str]] = Field(default_factory=list)
    book_uri: str | None = None

    # Enhanced fields (added by enrichment)
    cover_urls: dict[str, str] | None = None
    cover_available: bool = False
    cover_source: str | None = None

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesBook":
        """Auto-generate mc_id, source_id if not provided."""
        if not self.mc_id:
            # Build a dict with the book data for mc_id generation
            # generate_mc_id will look for primary_isbn13, primary_isbn10, etc.
            book_data = {
                "primary_isbn13": self.primary_isbn13,
                "primary_isbn10": self.primary_isbn10,
                "title": self.title,
                "author": self.author,
            }
            self.mc_id = generate_mc_id(book_data, MCType.BOOK)

        # Set source_id to primary ISBN or mc_id
        if not self.source_id:
            self.source_id = self.primary_isbn13 or self.primary_isbn10 or self.mc_id

        return self


class NYTimesBestsellerList(BaseModelWithMethods):
    """Model for a bestseller list from NYTimes API."""

    list_id: int
    list_name: str
    list_name_encoded: str
    display_name: str
    updated: str
    list_image: str | None = None
    list_image_width: int | None = None
    list_image_height: int | None = None
    books: list[NYTimesBook] = Field(default_factory=list)

    # MediaCircle standardized fields
    mc_id: str = Field(default="")  # Always set (either provided or generated)
    mc_type: str | None = None

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesBestsellerList":
        """Auto-generate mc_id and mc_type if not provided. Always ensures mc_id is set."""
        if not self.mc_id:
            # Use list_id directly as the identifier for bestseller lists
            # Since lists don't map to BOOK type in generate_mc_id, create manually
            self.mc_id = f"nyt_list_{self.list_id}"
            self.mc_type = "book_list"  # Custom type for lists

        return self


class NYTimesBestsellerListResults(BaseModelWithMethods):
    """Model for bestseller list results (single list endpoint)."""

    bestsellers_date: str
    published_date: str
    published_date_description: str = ""
    previous_published_date: str | None = None
    next_published_date: str | None = None
    list_name: str
    list_name_encoded: str
    display_name: str = ""
    normal_list_ends_at: int = 0
    updated: str
    books: list[NYTimesBook] = Field(default_factory=list)
    corrections: list[Any] = Field(default_factory=list)


class NYTimesOverviewResults(BaseModelWithMethods):
    """Model for overview results (all lists endpoint)."""

    bestsellers_date: str
    published_date: str
    published_date_description: str = ""
    previous_published_date: str | None = None
    next_published_date: str | None = None
    lists: list[NYTimesBestsellerList] = Field(default_factory=list)


class NYTimesListName(MCBaseItem):
    """Model for list name from list names endpoint."""

    # MCBaseItem fields
    mc_type: MCType = MCType.BOOK  # List names are metadata about book lists
    source: MCSources = MCSources.NYTIMES

    # NYTimes list name fields
    list_name: str
    display_name: str
    list_name_encoded: str
    oldest_published_date: str
    newest_published_date: str
    updated: str

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesListName":
        """Auto-generate mc_id, source_id if not provided."""
        if not self.mc_id:
            # Use list_name_encoded as the unique identifier
            self.mc_id = f"nyt_list_{self.list_name_encoded}"

        # Set source_id to list_name_encoded
        if not self.source_id:
            self.source_id = self.list_name_encoded

        return self


class NYTimesReview(MCBaseItem):
    """Model for book review from NYTimes API."""

    # MCBaseItem fields
    mc_type: MCType = MCType.BOOK  # Reviews are about books
    source: MCSources = MCSources.NYTIMES

    # NYTimes review fields
    url: str | None = None
    publication_dt: str | None = None
    byline: str | None = None
    book_title: str
    book_author: str = ""
    summary: str | None = None
    isbn13: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesReview":
        """Auto-generate mc_id, source_id if not provided."""
        if not self.mc_id:
            # Use URL hash or ISBN as identifier
            if self.url:
                # Create hash from URL
                url_hash = hash(self.url) & 0x7FFFFFFF
                self.mc_id = f"nyt_review_{url_hash}"
            elif self.isbn13:
                # Use first ISBN
                self.mc_id = f"nyt_review_{self.isbn13[0]}"
            else:
                # Fallback to book title + author hash
                unique_str = f"{self.book_title}_{self.book_author}"
                title_hash = hash(unique_str) & 0x7FFFFFFF
                self.mc_id = f"nyt_review_{title_hash}"

        # Set source_id to URL or ISBN
        if not self.source_id:
            if self.url:
                self.source_id = self.url
            elif self.isbn13:
                self.source_id = self.isbn13[0]
            else:
                self.source_id = self.mc_id

        return self


# ============================================================================
# API Response Wrappers
# ============================================================================


class NYTimesBestsellerListResponse(MCSearchResponse):
    """Response wrapper for bestseller list API call - extends MCSearchResponse."""

    status: str
    copyright: str | None = None
    num_results: int = 0
    # Keep original structure for backward compatibility
    list_results: NYTimesBestsellerListResults | None = None
    # MCSearchResponse requires results: list[MCBaseItem]
    results: list[NYTimesBook] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "NYTimes Bestseller List"
    data_type: MCType = MCType.BOOK

    @model_validator(mode="before")
    @classmethod
    def transform_results(cls, data: Any) -> Any:
        """Transform results structure to MCSearchResponse format."""
        if isinstance(data, dict):
            # If we have results with books structure, extract them
            results_data = data.get("results")
            if results_data and isinstance(results_data, dict):
                books = results_data.get("books", [])
                if books:
                    # Store original structure
                    data["list_results"] = results_data
                    # Set results to books list for MCSearchResponse
                    data["results"] = books
                    # Set total_results from num_results or book count
                    data["total_results"] = data.get("num_results", len(books))
            elif isinstance(results_data, list):
                # Already in correct format
                data["total_results"] = data.get("num_results", len(results_data))
        return data

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesBestsellerListResponse":
        """Auto-generate fields and sync total_results."""
        # Sync total_results from num_results or book count
        if self.total_results == 0:
            if self.num_results > 0:
                self.total_results = self.num_results
            elif self.results:
                self.total_results = len(self.results)
            elif self.list_results and self.list_results.books:
                self.total_results = len(self.list_results.books)
                # Also sync results list if empty
                if not self.results:
                    self.results = self.list_results.books
        return self


class NYTimesOverviewResponse(MCSearchResponse):
    """Response wrapper for overview API call - extends MCSearchResponse."""

    status: str
    copyright: str | None = None
    num_results: int = 0
    # Keep original structure for backward compatibility
    overview_results: NYTimesOverviewResults | None = None
    # MCSearchResponse requires results: list[MCBaseItem]
    results: list[NYTimesBook] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "NYTimes Overview"
    data_type: MCType = MCType.BOOK

    @model_validator(mode="before")
    @classmethod
    def transform_results(cls, data: Any) -> Any:
        """Transform results structure to MCSearchResponse format."""
        if isinstance(data, dict):
            # If we have results with lists structure, extract all books
            results_data = data.get("results")
            if results_data and isinstance(results_data, dict):
                lists = results_data.get("lists", [])
                all_books = []
                for list_item in lists:
                    if isinstance(list_item, dict):
                        books = list_item.get("books", [])
                        all_books.extend(books)
                if all_books:
                    # Store original structure
                    data["overview_results"] = results_data
                    # Set results to flattened books list for MCSearchResponse
                    data["results"] = all_books
                    # Set total_results from num_results or book count
                    data["total_results"] = data.get("num_results", len(all_books))
            elif isinstance(results_data, list):
                # Already in correct format
                data["total_results"] = data.get("num_results", len(results_data))
        return data

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesOverviewResponse":
        """Auto-generate fields and sync total_results."""
        # Sync total_results from num_results or book count
        if self.total_results == 0:
            if self.num_results > 0:
                self.total_results = self.num_results
            elif self.results:
                self.total_results = len(self.results)
            elif self.overview_results and self.overview_results.lists:
                # Flatten books from all lists
                all_books = []
                for list_item in self.overview_results.lists:
                    all_books.extend(list_item.books)
                self.total_results = len(all_books)
                # Also sync results list if empty
                if not self.results:
                    self.results = all_books
        return self


class NYTimesListNamesResponse(MCSearchResponse):
    """Response wrapper for list names API call - extends MCSearchResponse."""

    status: str
    copyright: str | None = None
    num_results: int = 0
    # MCSearchResponse requires results: list[MCBaseItem]
    # Note: NYTimesListName doesn't extend MCBaseItem, so we'll use a wrapper or keep as-is
    # For now, we'll store them directly and they'll be treated as dict-like
    results: list[NYTimesListName] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "NYTimes List Names"
    data_type: MCType = MCType.BOOK

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesListNamesResponse":
        """Auto-generate fields and sync total_results."""
        # Sync total_results from num_results or list count
        if self.total_results == 0:
            if self.num_results > 0:
                self.total_results = self.num_results
            elif self.results:
                self.total_results = len(self.results)
        return self


class NYTimesReviewResponse(MCSearchResponse):
    """Response wrapper for book reviews API call - extends MCSearchResponse."""

    status: str
    copyright: str | None = None
    num_results: int = 0
    # MCSearchResponse requires results: list[MCBaseItem]
    # Note: NYTimesReview doesn't extend MCBaseItem, so we'll use a wrapper or keep as-is
    results: list[NYTimesReview] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "NYTimes Reviews"
    data_type: MCType = MCType.BOOK

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesReviewResponse":
        """Auto-generate fields and sync total_results."""
        # Sync total_results from num_results or review count
        if self.total_results == 0:
            if self.num_results > 0:
                self.total_results = self.num_results
            elif self.results:
                self.total_results = len(self.results)
        return self


class NYTimesHistoricalResponse(MCSearchResponse):
    """Response wrapper for historical bestseller data - extends MCSearchResponse."""

    historical_data: list[dict[str, Any]] = Field(default_factory=list)
    # MCSearchResponse requires results: list[MCBaseItem]
    # For historical data, we'll store historical_data items as results
    results: list[dict[str, Any]] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str = "NYTimes Historical"
    data_type: MCType = MCType.BOOK

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NYTimesHistoricalResponse":
        """Auto-generate fields and sync results."""
        # Sync results from historical_data
        if not self.results and self.historical_data:
            self.results = self.historical_data
        # Sync total_results
        if self.total_results == 0 and self.results:
            self.total_results = len(self.results)
        return self


# Type aliases for processed data (for JSON serialization)
NYTimesBookDict = dict[str, Any]  # Represents NYTimesBook.model_dump()
NYTimesBestsellerListDict = dict[str, Any]  # Represents NYTimesBestsellerList.model_dump()
