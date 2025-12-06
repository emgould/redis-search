"""
YouTube Models - Pydantic models for YouTube Data API structures
Follows Pydantic 2.0 patterns with full type safety.
"""

from typing import Any

from contracts.models import MCBaseItem, MCImage, MCSearchResponse, MCSources, MCSubType, MCType
from pydantic import BaseModel, Field, model_validator

from utils.pydantic_tools import BaseModelWithMethods


# -------------------------------
# Dynamic YouTube Models
# -------------------------------
class DynamicYouTubeVideo(BaseModel):
    """Model for Dynamic YouTube video data (from web scraping)."""

    video_id: str
    title: str
    channel: str | None = None
    published_time: str | None = None
    view_count: str | None = None
    thumbnail_url: str | None = None
    description: str | None = None
    duration_seconds: int | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    publish_date: str | None = None
    is_live: bool = False
    url: str = Field(..., description="Full YouTube watch URL")
    images: list[MCImage] = Field(default_factory=list)
    error: str | None = None
    status_code: int = 200


class DynamicYouTubeSearchResponse(BaseModel):
    """Model for Dynamic YouTube search response."""

    query: str
    results: list[DynamicYouTubeVideo]
    total_results: int


class YouTubeVideo(MCBaseItem):
    """Model for YouTube video data."""

    # Core fields
    id: str
    video_id: str
    title: str
    description: str = ""
    channel_title: str = ""
    channel_id: str = ""
    published_at: str = ""

    # Media
    thumbnail_url: str | None = None
    url: str

    # Statistics
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0

    # Metadata
    duration: str | None = None
    tags: list[str] = Field(default_factory=list)
    category_id: str | None = None
    category: str | None = None
    default_language: str | None = None
    is_live: bool = False

    # MCBaseItem fields - set defaults
    mc_type: MCType = MCType.VIDEO
    source: MCSources = MCSources.YOUTUBE

    @model_validator(mode="before")
    @classmethod
    def set_source_id(cls, data: Any) -> Any:
        """Set source_id from video_id if not provided."""
        # If data is already a BaseModel instance, return it as-is
        if isinstance(data, BaseModel):
            return data
        # Ensure data is a dict for processing
        if not isinstance(data, dict):
            return data
        # Set source_id from video_id or id if not present
        if not data.get("source_id"):
            data["source_id"] = data.get("video_id") or data.get("id")
        return data

    @classmethod
    def from_dynamic(cls, dynamic: DynamicYouTubeVideo) -> "YouTubeVideo":
        """Convert a DynamicYouTubeVideo to a YouTubeVideo.

        Args:
            Dynamic: DynamicYouTubeVideo instance from web scraping

        Returns:
            YouTubeVideo instance with converted data
        """
        # Parse view count string to integer (e.g., "2.5M views" -> 2500000)
        view_count = 0
        if dynamic.view_count:
            view_str = dynamic.view_count.lower().replace("views", "").replace(",", "").strip()
            try:
                if "m" in view_str:
                    view_count = int(float(view_str.replace("m", "")) * 1_000_000)
                elif "k" in view_str:
                    view_count = int(float(view_str.replace("k", "")) * 1_000)
                else:
                    view_count = int(view_str)
            except (ValueError, AttributeError):
                view_count = 0

        # Convert duration_seconds to string format (e.g., "2:30" or "1:05:30")
        duration_str = None
        if dynamic.duration_seconds:
            hours = dynamic.duration_seconds // 3600
            minutes = (dynamic.duration_seconds % 3600) // 60
            seconds = dynamic.duration_seconds % 60
            if hours > 0:
                duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                duration_str = f"{minutes}:{seconds:02d}"

        return cls(
            id=dynamic.video_id,
            video_id=dynamic.video_id,
            source_id=dynamic.video_id,
            title=dynamic.title,
            channel_title=dynamic.channel or "",
            published_at=dynamic.published_time or "",
            thumbnail_url=dynamic.thumbnail_url,
            url=dynamic.url,
            view_count=view_count,
            mc_type=MCType.VIDEO,
            source=MCSources.YOUTUBE,
            description=dynamic.description or "",
            duration=duration_str,
            is_live=dynamic.is_live,
            category=dynamic.category or "",
            tags=dynamic.tags or [],
            images=dynamic.images or [],
        )


class YouTubeCategory(BaseModelWithMethods):
    """Model for YouTube video category."""

    id: str
    title: str
    assignable: bool = False


class YouTubeSearchResponse(MCSearchResponse):
    """Model for YouTube search response."""

    results: list[YouTubeVideo] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str = Field(..., description="query for search")
    data_source: str = "Youtube Videos"
    data_type: MCType = MCType.VIDEO
    date: str | None = Field(None, description="Response date")


class YouTubeTrendingResponse(BaseModelWithMethods):
    """Model for YouTube trending videos response."""

    date: str
    videos: list[YouTubeVideo]
    total_results: int
    region_code: str = "US"
    language: str = "en"
    category_id: str | None = None
    query: str | None = None
    fetched_at: str = ""
    next_page_token: str | None = None
    prev_page_token: str | None = None
    error: str | None = None
    status_code: int = 200


class YouTubeCategoriesResponse(BaseModelWithMethods):
    """Model for YouTube categories response."""

    date: str
    categories: list[YouTubeCategory]
    region_code: str = "US"
    language: str = "en"
    error: str | None = None
    status_code: int = 200


class YouTubePopularResponse(BaseModelWithMethods):
    """Model for YouTube popular videos response."""

    date: str
    videos: list[YouTubeVideo]
    total_results: int
    query: str
    type: str = "popular_videos"
    method: str = "search_with_viewcount_ordering"
    note: str = "Popular videos fetched via search API due to trending API restrictions"
    region_code: str = "US"
    language: str = "en"
    next_page_token: str | None = None
    prev_page_token: str | None = None
    error: str | None = None
    status_code: int = 200


# ----------------------------------------
# YouTube Creator Model
# ----------------------------------------
class YouTubeCreator(MCBaseItem):
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.YOUTUBE_CREATOR
    source: MCSources = MCSources.YOUTUBE
    id: str = Field(..., description="Unique channel ID")
    title: str = Field(..., description="Channel name")
    name: str = ""  # For compatibility with MCBaseItem person pattern
    url: str = Field(..., description="Channel URL")
    thumbnail: str | None = None
    subscriber_count: int = 0
    video_count: int = 0
    description: str = ""
    avatar: str | None = None
    banner: str | None = None
    country: str | None = None
    joined_date: str | None = None


class VideoSearchResponse(MCSearchResponse):
    """Model for podcaster search response - returns MCPodcaster items."""

    results: list[YouTubeVideo] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str = Field(..., description="query for search")
    data_source: str = "Youtube Videos"
    data_type: MCType = MCType.VIDEO
    date: str | None = Field(None, description="Response date")
