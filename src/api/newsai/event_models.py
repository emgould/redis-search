"""
NewsAI Event Models - Pydantic models for Event Registry event data structures.
Events are clusters of related articles about the same news story.
"""

from datetime import datetime

from contracts.models import MCBaseItem, MCSearchResponse, MCSources, MCType
from pydantic import ConfigDict, Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods


class EventConcept(BaseModelWithMethods):
    """Model for concept/entity mentioned in an event."""

    uri: str
    type: str  # person, org, loc, wiki
    score: int
    label: dict[str, str] | None = None  # e.g., {"eng": "WandaVision"}
    title: str | None = None


class EventCategory(BaseModelWithMethods):
    """Model for event category."""

    uri: str
    label: str
    wgt: int  # Weight/importance


class EventArticleCounts(BaseModelWithMethods):
    """Model for article counts by language."""

    eng: int | None = None
    spa: int | None = None
    deu: int | None = None
    # Add other languages as needed


class EventInfoArticle(BaseModelWithMethods):
    """Model for the representative article of an event."""

    uri: str
    lang: str
    is_duplicate: bool = Field(alias="isDuplicate")
    date: str
    time: str
    date_time: str = Field(alias="dateTime")
    date_time_pub: str | None = Field(None, alias="dateTimePub")
    data_type: str = Field(alias="dataType")  # news, pr, blog
    sim: float  # Similarity score
    url: str
    source: dict | None = None
    location: dict | None = None
    categories: list[EventCategory] = Field(default_factory=list)
    images: list[str] = Field(default_factory=list)
    source_title: str | None = Field(None, alias="sourceTitle")
    category: str | None = None

    model_config = ConfigDict(populate_by_name=True)


class MCEventItem(MCBaseItem):
    """
    Model for Event Registry event data.
    An event is a cluster of related articles about the same news story.
    """

    model_config = ConfigDict(populate_by_name=True)

    # Event identification
    uri: str  # Event Registry unique identifier

    # Core fields
    title: str | dict[str, str]  # Can be string or dict with language keys
    summary: str | dict[str, str] | None = None  # Can be string or dict with language keys
    event_date: str = Field(alias="eventDate")
    total_article_count: int | str = Field(alias="totalArticleCount")  # Can be int or string

    # Concepts/entities mentioned in the event
    concepts: list[EventConcept] = Field(default_factory=list)

    # Categories
    categories: list[EventCategory] = Field(default_factory=list)

    # Representative article
    info_article: EventInfoArticle | None = Field(None, alias="infoArticle")

    # Article counts by language
    article_counts: EventArticleCounts | None = Field(None, alias="articleCounts")

    # Images - override MCBaseItem's images field to use simple strings for events
    images: list[str] = Field(default_factory=list)  # type: ignore[assignment]

    # Metrics
    social_score: str | float | None = Field(None, alias="socialScore")
    sentiment: float | None = None
    wgt: int | None = None  # Weight/importance
    relevance: int | None = None

    # Additional fields
    location_str: str | None = Field(None, alias="locationStr")
    source_title: str | None = Field(None, alias="sourceTitle")
    category: str | None = None
    rtl: bool | None = None  # Right-to-left text

    # MCBaseItem fields - set defaults
    mc_type: MCType = MCType.NEWS_ARTICLE
    source: MCSources = MCSources.NEWSAI

    @model_validator(mode="after")
    def set_mc_source(self) -> "MCEventItem":
        """Set MCBaseItem source field to NEWSAI and generate mc_id and source_id."""
        # Generate mc_id if not already set
        if not self.mc_id:
            from contracts.models import generate_mc_id

            item_dict = {"id": self.uri}
            self.mc_id = generate_mc_id(item_dict, self.mc_type)

        # Set source_id
        if not self.source_id:
            self.source_id = self.uri

        # Normalize title to string if it's a dict
        if isinstance(self.title, dict):
            # Prefer English, fallback to first available
            self.title = self.title.get("eng") or next(iter(self.title.values()), "")

        # Normalize summary to string if it's a dict
        if isinstance(self.summary, dict):
            self.summary = self.summary.get("eng") or next(iter(self.summary.values()), "")

        # Convert total_article_count to int if it's a string
        if isinstance(self.total_article_count, str):
            try:
                self.total_article_count = int(self.total_article_count)
            except (ValueError, TypeError):
                self.total_article_count = 0

        # Convert social_score to float if it's a string
        if isinstance(self.social_score, str):
            try:
                self.social_score = float(self.social_score)
            except (ValueError, TypeError):
                self.social_score = 0.0

        return self

    def get_primary_image(self) -> str | None:
        """Get the primary image URL for this event."""
        if self.images and len(self.images) > 0:
            return self.images[0]
        if self.info_article and self.info_article.images and len(self.info_article.images) > 0:
            return self.info_article.images[0]
        return None

    def get_url(self) -> str | None:
        """Get the URL of the representative article."""
        if self.info_article:
            return self.info_article.url
        return None


# ============================================================================
# Response Models for Events
# ============================================================================


class EventSearchResponse(MCSearchResponse):
    """Model for event search response."""

    results: list[MCEventItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str | None = None
    language: str | None = None
    sort_by: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    page_size: int = 20
    status: str | None = None
    fetched_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    data_source: str = "NewsAI Events"
    data_type: MCType = MCType.NEWS_ARTICLE


class TrendingEventsResponse(MCSearchResponse):
    """Model for trending events response."""

    results: list[MCEventItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    country: str | None = None
    query: str | None = None
    category: str | None = None
    page_size: int = 20
    status: str | None = None
    fetched_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    data_source: str = "NewsAI Trending Events"
    data_type: MCType = MCType.NEWS_ARTICLE
