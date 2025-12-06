"""
NewsAI Models - Pydantic models for NewsAI (Event Registry) data structures
Follows Pydantic 2.0 patterns with full type safety.
Provides drop-in replacement for news API models.
"""

from datetime import datetime

from contracts.models import MCBaseItem, MCSearchResponse, MCSources, MCType
from pydantic import ConfigDict, Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods


class NewsSource(BaseModelWithMethods):
    """Model for news source information."""

    id: str | None = None
    name: str


class MCNewsItem(MCBaseItem):
    """Model for news article data from NewsAI/Event Registry."""

    model_config = ConfigDict(populate_by_name=True)

    # Core fields
    title: str
    description: str | None = None
    content: str | None = None  # body field from Event Registry
    url: str
    url_to_image: str | None = None  # image field from Event Registry
    published_at: str | None = None  # date/time fields from Event Registry
    author: str | None = None
    # API source (publisher) - maps to "source" in API response
    news_source: NewsSource | None = None

    # MCBaseItem fields - set defaults
    mc_type: MCType = MCType.NEWS_ARTICLE
    source: MCSources = MCSources.NEWSAI

    # For mc_id generation - use url or uri
    id: str | None = None  # uri field from Event Registry
    uri: str | None = None  # Event Registry unique identifier

    # Additional Event Registry fields
    lang: str | None = None
    is_duplicate: bool | None = None
    date: str | None = None  # Publishing date
    time: str | None = None  # Publishing time
    date_time: str | None = None  # Combined datetime
    sim: float | None = None  # Similarity score
    sentiment: float | None = None  # Sentiment score (-1 to 1)
    wgt: int | None = None  # Weight/importance score
    relevance: float | None = None  # Relevance score for search results

    @model_validator(mode="after")
    def set_mc_source(self) -> "MCNewsItem":
        """Set MCBaseItem source field to NEWSAI and generate mc_id and source_id."""
        # Generate mc_id if not already set - include url for news articles
        if not self.mc_id:
            from contracts.models import generate_mc_id

            item_dict = {}
            # Prefer uri (Event Registry unique ID), then id, then url
            if self.uri:
                item_dict["id"] = self.uri
            elif self.id:
                item_dict["id"] = self.id
            if self.url:
                item_dict["url"] = self.url

            if item_dict:
                self.mc_id = generate_mc_id(item_dict, self.mc_type)

        # Set source_id if not already set
        if not self.source_id:
            if self.uri:
                self.source_id = self.uri
            elif self.id:
                self.source_id = self.id
            elif self.url:
                # Generate a consistent ID from URL hash
                self.source_id = f"url_{hash(self.url) & 0x7FFFFFFF}"

        # Combine date/time into published_at if not set
        if not self.published_at and (self.date or self.date_time):
            if self.date_time:
                self.published_at = self.date_time
            elif self.date and self.time:
                self.published_at = f"{self.date}T{self.time}"
            elif self.date:
                self.published_at = self.date

        return self


class NewsSourceDetails(MCBaseItem):
    """Model for detailed news source information."""

    id: str | None = None
    uri: str | None = None  # Event Registry source URI
    name: str
    description: str | None = None
    url: str | None = None
    category: str | None = None
    language: str | None = None
    country: str | None = None

    # Additional Event Registry fields
    data_type: str | None = None  # news, pr, blog
    title: str | None = None  # Alternative to name
    location: dict | None = None  # Geographic location info
    ranking: dict | None = None  # Source importance ranking

    # MCBaseItem fields
    mc_type: MCType = MCType.NEWS_ARTICLE
    source: MCSources = MCSources.NEWSAI

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NewsSourceDetails":
        """Auto-generate mc_id and source_id if not provided."""
        if not self.mc_id:
            if self.uri:
                self.mc_id = f"news_source_{self.uri}"
            elif self.id:
                self.mc_id = f"news_source_{self.id}"
            elif self.name:
                # Generate ID from name
                name_hash = hash(self.name) & 0x7FFFFFFF
                self.mc_id = f"news_source_{name_hash}"

        # Set source_id if not already set
        if not self.source_id:
            if self.uri:
                self.source_id = self.uri
            elif self.id:
                self.source_id = self.id
            elif self.name:
                # Generate ID from name
                name_hash = hash(self.name) & 0x7FFFFFFF
                self.source_id = str(name_hash)

        return self


# ============================================================================
# Response Models
# These models represent responses from NewsAI operations
# ============================================================================


class TrendingNewsResponse(MCSearchResponse):
    """Model for trending news response."""

    results: list[MCNewsItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    country: str | None = None
    query: str | None = None
    category: str | None = None
    page_size: int = 20
    status: str | None = None
    fetched_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    data_source: str = "NewsAI Trending"
    data_type: MCType = MCType.NEWS_ARTICLE


class NewsSearchResponse(MCSearchResponse):
    """Model for news search response."""

    results: list[MCNewsItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str
    language: str = "en"
    sort_by: str = "publishedAt"
    from_date: str | None = None
    to_date: str | None = None
    page_size: int = 20
    status: str | None = None
    fetched_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    data_source: str = "NewsAI Search"
    data_type: MCType = MCType.NEWS_ARTICLE


class NewsSourcesResponse(MCSearchResponse):
    """Model for news sources response."""

    results: list[NewsSourceDetails] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    total_sources: int = 0  # Keep for backward compatibility
    category: str | None = None
    language: str | None = None
    country: str | None = None
    status: str | None = None
    data_source: str = "NewsAI Sources"
    data_type: MCType = MCType.NEWS_ARTICLE

    @model_validator(mode="after")
    def sync_total_sources(self) -> "NewsSourcesResponse":
        """Sync total_sources with total_results for backward compatibility."""
        if self.total_sources == 0 and self.total_results > 0:
            self.total_sources = self.total_results
        elif self.total_results == 0 and self.total_sources > 0:
            self.total_results = self.total_sources
        return self
