"""
Podcast Models - Pydantic models for PodcastIndex data structures
Follows Pydantic 2.0 patterns with full type safety.
"""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from contracts.models import MCBaseItem, MCSearchResponse, MCSources, MCSubType, MCType


class MCPodcastItem(MCBaseItem):
    """
    Model for a podcast search result from PodcastIndex.
    Represents a single podcast with all its metadata.
    """

    # Core identification
    id: int = Field(default=0, description="PodcastIndex feed ID")
    title: str = Field(default="", description="Podcast title")
    url: str = Field(default="", description="RSS feed URL")

    # Metadata
    site: str | None = Field(default=None, description="Podcast website URL")
    description: str | None = Field(default=None, description="Podcast description")
    author: str | None = Field(default=None, description="Podcast author")
    owner_name: str | None = Field(default=None, description="Podcast owner name")

    # Media
    image: str | None = Field(default=None, description="Podcast image URL")
    artwork: str | None = Field(default=None, description="Podcast artwork URL")

    # Timestamps and metrics
    last_update_time: str | None = Field(
        default=None, description="Last update timestamp (ISO format)"
    )
    trend_score: float | None = Field(default=None, description="Trending score from PodcastIndex")
    relevancy_score: float | None = Field(
        default=None, description="Relevancy score for search results"
    )

    # Classification
    language: str | None = Field(default=None, description="Podcast language code")
    categories: dict[str, Any] = Field(default_factory=dict, description="Podcast categories")

    # Counts and IDs
    episode_count: int = Field(default=0, description="Number of episodes")
    itunes_id: int | None = Field(default=None, description="iTunes ID")
    podcast_guid: str | None = Field(default=None, description="Podcast GUID")

    # External links
    spotify_url: str | None = Field(default=None, description="Spotify URL")

    # MediaCircle standardized fields (from MCBaseItem)
    mc_type: MCType = MCType.PODCAST
    source: MCSources = MCSources.PODCASTINDEX


class MCEpisodeItem(MCBaseItem):
    """
    Model for a podcast episode from PodcastIndex.
    Represents a single episode with all its metadata.
    """

    # Core identification
    id: int = Field(..., description="Episode ID from PodcastIndex")
    title: str = Field(..., description="Episode title")

    # Content
    description: str | None = Field(None, description="Episode description")
    link: str | None = Field(None, description="Episode web link")
    guid: str | None = Field(None, description="Episode GUID")

    # Publication
    date_published: str | None = Field(None, description="Publication date (ISO format)")

    # Media file
    enclosure_url: str | None = Field(None, description="Audio file URL for playback")
    enclosure_type: str | None = Field(None, description="Audio file MIME type")
    enclosure_length: int | None = Field(None, description="Audio file size in bytes")
    duration_seconds: int | None = Field(None, description="Episode duration in seconds")

    # Classification
    explicit: bool | None = Field(None, description="Whether episode contains explicit content")
    episode_type: str | None = Field(None, description="Episode type (full, trailer, bonus)")
    season: int | None = Field(None, description="Season number")
    episode: int | None = Field(None, description="Episode number")

    # Parent podcast
    feed_id: int | None = Field(None, description="Parent podcast feed ID")
    feed_title: str | None = Field(None, description="Parent podcast title")

    # Media
    image: str | None = Field(None, description="Episode image URL")

    # MediaCircle standardized fields (from MCBaseItem)
    mc_type: MCType = MCType.PODCAST_EPISODE
    source: MCSources = MCSources.PODCASTINDEX


class PodcastWithLatestEpisode(MCPodcastItem):
    """
    Extended podcast model that includes the latest episode.
    Inherits all fields from MCPodcastItem and adds latest_episode.
    """

    latest_episode: MCEpisodeItem | None = Field(default=None, description="Most recent episode")


class MCPodcaster(MCBaseItem):
    """
    Model for a podcaster (person who hosts/creates podcasts).
    Represents a person who hosts or creates one or more podcasts.
    """

    # Core identification
    name: str = Field(..., description="Podcaster name")
    id: str = Field(default="", description="Podcaster identifier (generated from name)")

    # Podcasts they host/create
    podcasts: list[MCPodcastItem] = Field(
        default_factory=list, description="List of podcasts this person hosts/creates"
    )

    # Aggregated metrics
    total_episodes: int = Field(default=0, description="Total episodes across all podcasts")
    podcast_count: int = Field(default=0, description="Number of podcasts they host/create")

    # Profile information (from primary podcast)
    image: str | None = Field(default=None, description="Profile image URL (from primary podcast)")
    bio: str | None = Field(default=None, description="Bio/description (from primary podcast)")
    website: str | None = Field(default=None, description="Website URL (from primary podcast)")

    # Additional metadata
    primary_podcast_title: str | None = Field(
        default=None, description="Title of their primary/most popular podcast"
    )
    primary_podcast_id: int | None = Field(default=None, description="ID of their primary podcast")

    # MediaCircle standardized fields (from MCBaseItem)
    mc_type: MCType = MCType.PERSON
    mc_subtype: MCSubType = MCSubType.PODCASTER
    source: MCSources = MCSources.PODCASTINDEX

    @model_validator(mode="after")
    def set_podcast_count(self) -> "MCPodcaster":
        """Set podcast_count based on podcasts list length."""
        self.podcast_count = len(self.podcasts)
        return self


# ============================================================================
# Response Models
# These models represent responses from PodcastIndex operations
# ============================================================================


class PodcastTrendingResponse(MCSearchResponse):
    """Model for trending podcasts response."""

    results: list[MCPodcastItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    data_source: str = "PodcastIndex Trending"
    data_type: MCType = MCType.PODCAST
    date: str | None = Field(None, description="Response date")


class PodcastSearchResponse(MCSearchResponse):
    """Model for podcast search response."""

    results: list[MCPodcastItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str
    data_source: str = "PodcastIndex Search"
    data_type: MCType = MCType.PODCAST
    date: str | None = Field(None, description="Response date")


class EpisodeListResponse(MCSearchResponse):
    """Model for episode list response."""

    results: list[MCEpisodeItem] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    feed_id: int = Field(..., description="Podcast feed ID")
    data_source: str = "PodcastIndex Episodes"
    data_type: MCType = MCType.PODCAST_EPISODE
    date: str | None = Field(None, description="Response date")


class PersonSearchResponse(MCSearchResponse):
    """Model for person search response - separates podcasts (hosts/creators) from episodes (guests)."""

    podcasts: list[MCPodcastItem] = Field(
        default_factory=list, description="Podcasts where person is host/creator"
    )
    episodes: list[MCEpisodeItem] = Field(
        default_factory=list, description="Episodes where person is guest"
    )
    total_podcasts: int = Field(default=0, description="Total podcasts found")
    total_episodes: int = Field(default=0, description="Total episodes found")
    person_name: str = Field(..., description="Person name searched")
    data_source: str = "PodcastIndex Person Search"
    data_type: MCType = MCType.PODCAST  # Primary type is podcast, but includes episodes
    date: str | None = Field(None, description="Response date")

    def model_post_init(self, __context: Any) -> None:
        """Set results and total_results after initialization."""
        # Combine podcasts and episodes into results for backward compatibility
        self.results = list(self.podcasts) + list(self.episodes)
        self.total_results = self.total_podcasts + self.total_episodes


class PodcasterSearchResponse(MCSearchResponse):
    """Model for podcaster search response - returns MCPodcaster items."""

    results: list[MCPodcaster] = Field(default_factory=list)  # type: ignore[assignment]
    total_results: int = 0
    query: str = Field(..., description="Person name searched")
    data_source: str = "PodcastIndex Podcaster Search"
    data_type: MCType = MCType.PERSON
    date: str | None = Field(None, description="Response date")


# ============================================================================
# RSS Feed Models
# These models represent parsed RSS feed data
# ============================================================================


class RSSEpisode(BaseModel):
    """Model for a podcast episode parsed from an RSS feed."""

    title: str = Field(..., description="Episode title")
    description: str | None = Field(None, description="Episode description (may contain HTML)")
    audio_url: str | None = Field(None, description="Audio file URL for playback")
    pub_date: str | None = Field(None, description="Publication date (ISO format)")
    duration_seconds: int | None = Field(None, description="Episode duration in seconds")
    guid: str | None = Field(None, description="Episode GUID from feed")
    link: str | None = Field(None, description="Episode web link")
    image: str | None = Field(None, description="Episode-specific image URL")


class RSSFeedResult(BaseModel):
    """Model for RSS feed parsing result."""

    episodes: list[RSSEpisode] = Field(default_factory=list, description="Parsed episodes")
    total_episodes: int = Field(default=0, description="Total episodes in feed")
    feed_title: str | None = Field(None, description="Feed title from RSS")
    feed_description: str | None = Field(None, description="Feed description from RSS")
    error: str | None = Field(None, description="Error message if parsing failed")
    status_code: int = Field(default=200, description="HTTP status code")
