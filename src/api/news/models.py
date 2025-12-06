"""
News Models - Pydantic models for NewsAPI data structures
Follows Pydantic 2.0 patterns with full type safety.
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
    """Model for news article data."""

    model_config = ConfigDict(populate_by_name=True)

    # Core fields
    title: str
    description: str | None = None
    content: str | None = None
    url: str
    url_to_image: str | None = None
    published_at: str | None = None
    author: str | None = None
    # API source (publisher) - maps to "source" in API response
    news_source: NewsSource | None = None

    # MCBaseItem fields - set defaults
    mc_type: MCType = MCType.NEWS_ARTICLE
    source: MCSources = MCSources.NEWSAPI

    # For mc_id generation - use url if id not available
    id: str | None = None  # Will be generated from url if not available

    @model_validator(mode="after")
    def set_mc_source(self) -> "MCNewsItem":
        """Set MCBaseItem source field to NEWSAPI and generate mc_id and source_id."""
        # Generate mc_id if not already set - include url for news articles
        if not self.mc_id:
            from contracts.models import generate_mc_id

            item_dict = {}
            if self.id:
                item_dict["id"] = self.id
            if self.url:
                item_dict["url"] = self.url  # generate_mc_id uses url for news articles

            if item_dict:
                self.mc_id = generate_mc_id(item_dict, self.mc_type)

        # Set source_id if not already set - use id or generate from url
        if not self.source_id:
            if self.id:
                self.source_id = self.id
            elif self.url:
                # Generate a consistent ID from URL hash
                self.source_id = f"url_{hash(self.url) & 0x7FFFFFFF}"

        # Ensure source is set (MCBaseItem field)
        # Note: We don't override __getattribute__ anymore to avoid conflicts
        # The 'source' field refers to MCBaseItem.source (MCSources enum)
        # The 'news_source' field refers to the publisher (NewsSource object)
        return self


class NewsSourceDetails(MCBaseItem):
    """Model for detailed news source information."""

    id: str | None = None
    name: str
    description: str | None = None
    url: str | None = None
    category: str | None = None
    language: str | None = None
    country: str | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.NEWS_ARTICLE
    source: MCSources = MCSources.NEWSAPI

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "NewsSourceDetails":
        """Auto-generate mc_id and source_id if not provided."""
        if not self.mc_id:
            if self.id:
                self.mc_id = f"news_source_{self.id}"
            elif self.name:
                # Generate ID from name
                name_hash = hash(self.name) & 0x7FFFFFFF
                self.mc_id = f"news_source_{name_hash}"

        # Set source_id if not already set
        if not self.source_id:
            if self.id:
                self.source_id = self.id
            elif self.name:
                # Generate ID from name
                name_hash = hash(self.name) & 0x7FFFFFFF
                self.source_id = str(name_hash)

        return self


# ============================================================================
# Response Models
# These models represent responses from NewsAPI operations
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
    data_source: str = "NewsAPI Trending"
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
    data_source: str = "NewsAPI Search"
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
    data_source: str = "NewsAPI Sources"
    data_type: MCType = MCType.NEWS_ARTICLE

    @model_validator(mode="after")
    def sync_total_sources(self) -> "NewsSourcesResponse":
        """Sync total_sources with total_results for backward compatibility."""
        if self.total_sources == 0 and self.total_results > 0:
            self.total_sources = self.total_results
        elif self.total_results == 0 and self.total_sources > 0:
            self.total_results = self.total_sources
        return self
