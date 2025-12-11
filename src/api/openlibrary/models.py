"""
OpenLibrary Models - Pydantic models for OpenLibrary data structures
Follows the TMDB pattern with Pydantic 2.0
"""

from typing import Any

from pydantic import Field, model_validator

from contracts.models import (
    MCBaseItem,
    MCSearchResponse,
    MCSources,
    MCSubType,
    MCType,
    generate_mc_id,
)
from utils.pydantic_tools import BaseModelWithMethods


class MCBookItem(MCBaseItem):
    """Model for a book item from OpenLibrary."""

    # Unique identifier for mc_id generation
    key: str
    title: str
    openlibrary_key: str | None = None
    openlibrary_url: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.BOOK
    source: MCSources = MCSources.OPENLIBRARY

    # Authors
    author_name: list[str] = Field(default_factory=list)
    author: str | None = None

    # ISBNs
    isbn: list[str] = Field(default_factory=list)
    primary_isbn13: str | None = None
    primary_isbn10: str | None = None
    isbns: list[dict[str, str]] = Field(default_factory=list)

    # Publication info
    first_publish_year: int | None = None
    publisher: str | list[str] | None = None
    publish_date: list[str] | str = Field(default_factory=list)
    publish_year: list[int] = Field(default_factory=list)

    # Content
    description: str | None = None
    first_sentence: list[str] = Field(default_factory=list)

    # Cover images
    cover_i: int | None = None
    cover_available: bool = False
    cover_urls: dict[str, str] = Field(default_factory=dict)
    book_image: str | None = None

    # Categories and subjects
    subject: list[str] = Field(default_factory=list)
    subjects: list[str] = Field(default_factory=list)
    language: str | list[str] | None = None

    # Ratings and popularity
    ratings: dict[str, Any] = Field(default_factory=dict)
    ratings_average: float | None = None
    ratings_count: int | None = None
    readinglog_count: int = 0
    want_to_read_count: int | None = None
    currently_reading_count: int | None = None
    already_read_count: int | None = None

    # Physical details
    number_of_pages: int | None = None
    number_of_pages_median: int | None = None

    # NYT data (if available)
    rank: int = 0
    rank_last_week: int = 0
    weeks_on_list: int = 0
    price: str = "0.00"

    purchase_links: list[dict[str, str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "MCBookItem":
        """Auto-generate mc_id, source_id, and populate images array if not provided."""
        if not self.mc_id:
            # Build dict with identifiers for generate_mc_id
            id_dict = {"openlibrary_key": self.openlibrary_key or self.key}
            if self.primary_isbn13:
                id_dict["primary_isbn13"] = self.primary_isbn13
            if self.primary_isbn10:
                id_dict["primary_isbn10"] = self.primary_isbn10

            self.mc_id = generate_mc_id(id_dict, MCType.BOOK)

        # Ensure source_id is set (use openlibrary_key or key)
        if not self.source_id:
            self.source_id = self.openlibrary_key or self.key

        # Populate images array from cover_urls if available and images is empty
        if not self.images and self.cover_urls:
            from contracts.models import MCImage, MCUrlType
            from utils.get_logger import get_logger

            logger = get_logger(__name__)
            logger.info(
                f"Populating images for book '{self.title}': "
                f"cover_urls has {len(self.cover_urls)} URLs"
            )

            for size, url in self.cover_urls.items():
                self.images.append(
                    MCImage(
                        url=url,
                        key=size,
                        type=MCUrlType.URL,
                        description="book_cover",
                    )
                )
        elif not self.cover_urls:
            from utils.get_logger import get_logger

            logger = get_logger(__name__)
            logger.info(
                f"Book '{self.title}' has NO cover_urls, "
                f"cover_i={self.cover_i}, cover_available={self.cover_available}"
            )

        return self


class BookSearchResponse(BaseModelWithMethods):
    """Model for OpenLibrary search response."""

    docs: list[MCBookItem]
    num_found: int
    offset: int = 0
    query: str | None = None


class CoverUrlsResponse(BaseModelWithMethods):
    """Model for OpenLibrary cover URLs response."""

    identifier: dict[str, str]
    covers_available: bool
    cover_urls: dict[str, str] | None = None


class AuthorLinks(BaseModelWithMethods):
    """Model for an author links from OpenLibrary."""

    title: str
    url: str


class MCAuthorItem(MCBaseItem):
    """Model for an author item from OpenLibrary."""

    # Unique identifier for mc_id generation
    key: str
    name: str

    # MCBaseItem fields
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.AUTHOR
    source: MCSources = MCSources.OPENLIBRARY
    top_subjects: list[str] = Field(default_factory=list)
    top_work: str | None = None
    work_count: int = 0

    # From authors endpoint
    bio: str | None = None
    remote_ids: dict[str, str] = Field(default_factory=dict)
    photo_id: int | None = None
    author_links: list[AuthorLinks] = Field(
        default_factory=list
    )  # OpenLibrary-specific links (not MCBaseItem.links)
    birth_date: str | None = None
    death_date: str | None = None
    full_name: str | None = None

    # Derived properties
    photo_available: bool = False
    photo_urls: dict[str, str] = Field(default_factory=dict)
    author_image: str | None = None
    openlibrary_key: str | None = None
    openlibrary_url: str | None = None

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "MCAuthorItem":
        """
        Auto-generate mc_id and source_id if not provided using key.
        Populate images array from photo_urls.
        """
        if not self.mc_id:
            self.mc_id = generate_mc_id({"key": self.key}, MCType.PERSON)

        # Ensure source_id is set (use key)
        if not self.source_id:
            self.source_id = self.key

        # Populate standardized images array from photo_urls for MCBaseItem consistency
        # Check if photo_urls has data and images is empty (default state)
        if self.photo_urls and len(self.images) == 0:
            from contracts.models import MCImage, MCUrlType

            for size_key, url in self.photo_urls.items():
                if url:
                    self.images.append(
                        MCImage(
                            url=url, key=size_key, type=MCUrlType.URL, description="author photo"
                        )
                    )
        # If no photo_urls but author_image is set, use that
        elif self.author_image and len(self.images) == 0:
            from contracts.models import MCImage, MCUrlType

            self.images.append(
                MCImage(
                    url=self.author_image,
                    key="medium",
                    type=MCUrlType.URL,
                    description="author photo",
                )
            )

        return self


class OpenLibrarySearchResponse(MCSearchResponse):
    """Model for OpenLibrary book search response."""

    results: list[MCBookItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str | None = None
    data_type: MCType = MCType.BOOK
    page: int = 1


class OpenLibraryAuthorSearchResponse(MCSearchResponse):
    """Model for OpenLibrary author search response."""

    results: list[MCAuthorItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str | None = None
    data_type: MCType = MCType.PERSON
    page: int = 1


class OpenLibraryCoverUrlsResponse(MCSearchResponse):
    """Model for OpenLibrary cover URLs response."""

    results: list[CoverUrlsResponse] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    data_source: str | None = None
    data_type: MCType = MCType.BOOK_COVER
    page: int = 1
