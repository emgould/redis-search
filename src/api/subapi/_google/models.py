"""
Google Books Models - Pydantic models for Google Books API data structures
Follows the same pattern as other API modules with Pydantic 2.0
"""

from typing import Any

from contracts.models import MCBaseItem, MCSources, MCType, generate_mc_id
from pydantic import BaseModel, Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods


class GoogleBooksIndustryIdentifier(BaseModel):
    """Model for industry identifiers (ISBN-10, ISBN-13, etc.)."""

    type: str  # "ISBN_10", "ISBN_13", etc.
    identifier: str


class GoogleBooksImageLinks(BaseModel):
    """Model for book cover image links."""

    smallThumbnail: str | None = None
    thumbnail: str | None = None
    small: str | None = None
    medium: str | None = None
    large: str | None = None
    extraLarge: str | None = None


class GoogleBooksSaleInfo(BaseModel):
    """Model for sale information."""

    country: str | None = None
    saleability: str | None = None
    isEbook: bool = False
    listPrice: dict[str, Any] = Field(default_factory=dict)
    retailPrice: dict[str, Any] = Field(default_factory=dict)
    buyLink: str | None = None


class GoogleBooksVolumeInfo(BaseModel):
    """Model for volume information from Google Books API."""

    title: str
    subtitle: str | None = None
    authors: list[str] = Field(default_factory=list)
    publisher: str | None = None
    publishedDate: str | None = None
    description: str | None = None
    industryIdentifiers: list[GoogleBooksIndustryIdentifier] = Field(default_factory=list)
    pageCount: int | None = None
    categories: list[str] = Field(default_factory=list)
    averageRating: float | None = None
    ratingsCount: int | None = None
    language: str | None = None
    imageLinks: GoogleBooksImageLinks | None = None
    previewLink: str | None = None
    infoLink: str | None = None
    canonicalVolumeLink: str | None = None


class GoogleBooksVolumeRaw(BaseModel):
    """Model for raw Google Books API volume response."""

    kind: str = "books#volume"
    id: str
    etag: str | None = None
    selfLink: str | None = None
    volumeInfo: GoogleBooksVolumeInfo
    saleInfo: GoogleBooksSaleInfo | None = None


class GoogleBooksItem(MCBaseItem):
    """
    Processed Google Books item - normalized for MediaCircle.
    Compatible with OpenLibrary MCBookItem structure for unified search.
    """

    # MCBaseItem fields - set defaults
    mc_type: MCType = MCType.BOOK
    source: MCSources = MCSources.GOOGLE_BOOKS

    # Core identification
    google_id: str | None = None
    title: str | None = None
    key: str | None = None  # Generated key for compatibility with OpenLibrary format

    # Authors
    author_name: list[str] = Field(default_factory=list)
    author: str | None = None

    # ISBNs
    isbn: list[str] = Field(default_factory=list)
    primary_isbn13: str | None = None
    primary_isbn10: str | None = None

    # Publication info
    publisher: str | None = None
    first_publish_year: int | None = None
    published_date: str | None = None

    # Content
    description: str | None = None
    google_description: str | None = None
    subtitle: str | None = None

    # Cover images
    cover_available: bool = False
    cover_urls: dict[str, str] = Field(default_factory=dict)
    book_image: str | None = None

    # Categories and subjects
    subjects: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    language: str | None = None

    # Ratings and popularity
    google_ratings_average: float | None = None
    google_ratings_count: int | None = None
    readinglog_count: int = 0  # Compatibility field

    # Physical details
    number_of_pages: int | None = None

    # Google Books specific
    google_info_link: str | None = None
    google_preview_link: str | None = None
    google_canonical_link: str | None = None

    # Purchase info
    purchase_links: list[dict[str, str]] = Field(default_factory=list)
    google_buy_link: str | None = None
    google_country: str | None = None
    google_saleability: str | None = None
    google_retail_price: dict[str, Any] = Field(default_factory=dict)

    # OpenLibrary compatibility fields
    openlibrary_key: str | None = None
    rank: int = 0
    rank_last_week: int = 0
    weeks_on_list: int = 0
    price: str = "0.00"

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "GoogleBooksItem":
        """Auto-generate mc_id if not provided. Always ensures mc_id is set."""
        # Ensure source is set
        if not self.source:
            self.source = MCSources.GOOGLE_BOOKS

        if not self.mc_id:
            # Build dict with identifiers for generate_mc_id
            # For BOOK type, generate_mc_id looks for openlibrary_key, isbn13, or isbn10
            id_dict = {}
            if self.openlibrary_key:
                id_dict["openlibrary_key"] = self.openlibrary_key
            if self.primary_isbn13:
                id_dict["primary_isbn13"] = self.primary_isbn13
            if self.primary_isbn10:
                id_dict["primary_isbn10"] = self.primary_isbn10

            # generate_mc_id always returns a value (uses hash fallback if needed)
            self.mc_id = generate_mc_id(id_dict, self.mc_type)

        return self


class GoogleBooksSearchResponse(BaseModelWithMethods):
    """Model for Google Books search response."""

    kind: str = "books#volumes"
    totalItems: int = 0
    items: list[GoogleBooksItem] = Field(default_factory=list)

    # Processed fields for compatibility
    docs: list[GoogleBooksItem] = Field(default_factory=list)  # OpenLibrary compatibility
    num_found: int = 0
    query: str | None = None
    data_source: str = "Google Books API"
    error: str | None = None  # If there was an error fetching the data
    status_code: int = 200

    @model_validator(mode="after")
    def sync_items_and_docs(self) -> "GoogleBooksSearchResponse":
        """Ensure items and docs are synchronized."""
        if self.items and not self.docs:
            self.docs = self.items
        if not self.num_found:
            self.num_found = self.totalItems
        return self


class GoogleBooksVolumeResponse(BaseModelWithMethods):
    """Model for single volume response."""

    volume: GoogleBooksItem | None = None
    data_source: str = "Google Books API"
    error: str | None = None  # If there was an error fetching the data
    status_code: int = 200


class GoogleBooksRawSearchResponse(BaseModel):
    """Model for raw Google Books API search response."""

    kind: str
    totalItems: int
    items: list[GoogleBooksVolumeRaw] = Field(default_factory=list)
