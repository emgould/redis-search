"""
Watchmode Models - Pydantic models for Watchmode API structures
Follows the same pattern as TMDB models with Pydantic 2.0
"""

from __future__ import annotations

from typing import Any

from contracts.models import MCBaseItem, MCSources, MCType
from pydantic import Field, model_validator

from api.tmdb.models import MCMovieItem, MCTvItem
from utils.pydantic_tools import BaseModelWithMethods


class WatchmodeStreamingSource(BaseModelWithMethods):
    """Model for Watchmode streaming source."""

    source_id: int
    name: str
    type: str  # subscription, free, purchase, rent
    region: str = "US"
    ios_url: str | None = None
    android_url: str | None = None
    web_url: str | None = None
    format: str | None = None  # 4K, HD, SD
    price: float | None = None
    seasons: int | None = None
    episodes: int | None = None


class WatchmodeRelease(BaseModelWithMethods):
    """Model for Watchmode new release item."""

    id: int  # Watchmode ID
    tmdb_id: int | None = None
    imdb_id: str | None = None
    title: str
    type: str  # movie, tv, tv_special, tv_miniseries, short_film
    year: int | None = None
    release_date: str | None = None  # YYYYMMDD format
    runtime_minutes: int | None = None
    user_rating: float | None = None
    critic_score: int | None = None
    us_rating: str | None = None  # G, PG, PG-13, R, NC-17
    poster: str | None = None
    backdrop: str | None = None
    original_title: str | None = None
    original_language: str | None = None
    genre_names: list[str] = Field(default_factory=list)
    similar_titles: list[int] = Field(default_factory=list)
    networks: list[int] = Field(default_factory=list)
    sources: list[WatchmodeStreamingSource] = Field(default_factory=list)


class WatchmodeTitleDetails(BaseModelWithMethods):
    """Model for detailed Watchmode title information."""

    id: int  # Watchmode ID
    title: str
    original_title: str | None = None
    plot_overview: str | None = None
    type: str  # movie, tv, etc.
    runtime_minutes: int | None = None
    year: int | None = None
    end_year: int | None = None  # For TV shows
    release_date: str | None = None
    imdb_id: str | None = None
    tmdb_id: int | None = None
    tmdb_type: str | None = None  # movie or tv

    # Ratings
    user_rating: float | None = None
    critic_score: int | None = None
    us_rating: str | None = None

    # Media
    poster: str | None = None
    backdrop: str | None = None
    original_language: str | None = None

    # Classifications
    genre_names: list[str] = Field(default_factory=list)
    similar_titles: list[int] = Field(default_factory=list)
    networks: list[int] = Field(default_factory=list)

    # TV specific
    network_names: list[str] = Field(default_factory=list)

    # Streaming
    sources: list[WatchmodeStreamingSource] = Field(default_factory=list)


class WatchmodeSearchResult(BaseModelWithMethods):
    """Model for Watchmode search result item."""

    id: int  # Watchmode ID
    name: str
    title: str | None = None
    type: str
    year: int | None = None
    result_type: str  # title or person
    tmdb_id: int | None = None
    tmdb_type: str | None = None
    image_url: str | None = None


class WatchmodeWhatsNewResponse(MCBaseItem):
    """Model for Watchmode 'What's New' response."""

    results: list[MCMovieItem | MCTvItem] = Field(default_factory=list)
    total_results: int
    region: str = "US"
    generated_at: str
    data_source: str = "watchmode_list + tmdb_complete"

    # MCBaseItem fields
    mc_type: MCType = MCType.MIXED  # Contains both movies and TV shows
    source: MCSources = MCSources.TMDB  # Data comes from TMDB (via Watchmode list)

    @model_validator(mode="after")
    def generate_mc_fields(self) -> WatchmodeWhatsNewResponse:
        """Auto-generate mc_id and ensure all results have required fields."""
        if not self.mc_id:
            # Generate ID from region and generated_at date
            date_part = (
                self.generated_at.split("T")[0]
                if "T" in self.generated_at
                else self.generated_at.split()[0]
            )
            self.mc_id = f"watchmode_whats_new_{self.region}_{date_part}"
            self.source_id = self.mc_id

        # Ensure all results have required fields (mc_id, mc_type, source, source_id)
        # These are already set by MCMovieItem/MCTvItem models, but verify they exist
        for item in self.results:
            if not item.mc_id:
                # This should not happen, but handle it defensively
                from contracts.models import generate_mc_id

                item.mc_id = generate_mc_id(item.model_dump(), item.mc_type)
            if not item.source_id:
                # Use tmdb_id as source_id for TMDB items
                if hasattr(item, "tmdb_id") and item.tmdb_id:
                    item.source_id = str(item.tmdb_id)
                else:
                    item.source_id = item.mc_id

        return self


# Import after class definition to resolve forward references for runtime

# Rebuild model to resolve forward references
WatchmodeWhatsNewResponse.model_rebuild()


class WatchmodeTitleDetailsResponse(MCBaseItem):
    """Model for Watchmode title details response (with streaming sources)."""

    # Include all fields from WatchmodeTitleDetails
    id: int  # Watchmode ID - used for mc_id generation
    title: str
    original_title: str | None = None
    plot_overview: str | None = None
    type: str
    runtime_minutes: int | None = None
    year: int | None = None
    end_year: int | None = None
    release_date: str | None = None
    imdb_id: str | None = None
    tmdb_id: int | None = None
    tmdb_type: str | None = None
    user_rating: float | None = None
    critic_score: int | None = None
    us_rating: str | None = None
    poster: str | None = None
    backdrop: str | None = None
    original_language: str | None = None
    genre_names: list[str] = Field(default_factory=list)
    similar_titles: list[int] = Field(default_factory=list)
    networks: list[int] = Field(default_factory=list)
    network_names: list[str] = Field(default_factory=list)

    # Additional streaming sources field
    streaming_sources: list[WatchmodeStreamingSource] = Field(default_factory=list)

    # MCBaseItem fields
    mc_type: MCType = MCType.MIXED  # Could be movie or TV, depends on type field
    source: MCSources = MCSources.TMDB  # Data sourced from Watchmode but represents TMDB content

    @model_validator(mode="after")
    def generate_mc_fields(self) -> WatchmodeTitleDetailsResponse:
        """Auto-generate mc_id and source_id if not provided."""
        if not self.mc_id:
            # Use watchmode id for mc_id generation
            self.mc_id = f"watchmode_{self.id}"
        if not self.source_id:
            # Use watchmode id as source_id
            self.source_id = str(self.id)
        return self


class WatchmodeSearchResponse(MCBaseItem):
    """Model for Watchmode search response."""

    results: list[WatchmodeSearchResult] = Field(default_factory=list)
    total_results: int = 0
    query: str

    # MCBaseItem fields
    mc_type: MCType = MCType.MIXED  # Search can return movies, TV shows, or people
    source: MCSources = MCSources.WATCHMODE

    @model_validator(mode="after")
    def generate_mc_fields(self) -> WatchmodeSearchResponse:
        """Auto-generate mc_id and source_id if not provided."""
        if not self.mc_id:
            # Use query for mc_id generation
            safe_query = self.query.replace(" ", "_").lower()[:50]
            self.mc_id = f"watchmode_search_{safe_query}"
        if not self.source_id:
            self.source_id = self.mc_id
        return self


# ============================================================================
# Type Aliases for Dict Representations
# These type aliases represent the dict form of models after .model_dump()
# They provide better documentation than generic dict[str, Any]
# ============================================================================

WatchmodeStreamingSourceDict = dict[str, Any]  # Represents WatchmodeStreamingSource.model_dump()
WatchmodeReleaseDict = dict[str, Any]  # Represents WatchmodeRelease.model_dump()
WatchmodeTitleDetailsDict = dict[str, Any]  # Represents WatchmodeTitleDetails.model_dump()
WatchmodeSearchResultDict = dict[str, Any]  # Represents WatchmodeSearchResult.model_dump()
