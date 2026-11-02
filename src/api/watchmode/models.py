"""
Watchmode Models - Pydantic models for Watchmode API structures
Follows the same pattern as TMDB models with Pydantic 2.0
"""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field, model_validator

from api.tmdb.models import MCMovieItem, MCTvItem
from contracts.models import MCBaseItem, MCSources, MCType
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


class WatchmodeSportsParticipant(BaseModelWithMethods):
    """Text-only participant identity. No assumption that every event has teams."""
    model_config = ConfigDict(extra="ignore")
    id: int
    name: str | None = None


class WatchmodeSportsEvent(BaseModelWithMethods):
    """Sports-specific model; deliberately independent of TMDB/media-title IDs."""
    model_config = ConfigDict(extra="ignore")
    id: int
    name: str
    event_type: str = "sport"
    league: dict[str, Any] | None = None
    home_team: WatchmodeSportsParticipant | None = None
    away_team: WatchmodeSportsParticipant | None = None
    start_time: str
    status: str = "unknown"
    venue: str | None = None
    season: str | None = None
    round: str | None = None
    broadcasts: list[dict[str, Any]] = Field(default_factory=list)
    sources: list[dict[str, Any]] = Field(default_factory=list)
    event_destinations: list[dict[str, Any]] = Field(default_factory=list)
    sources_state: str = "unknown"
    sources_fetched_at: str | None = None
    status_fetched_at: str | None = None
    display_kind: str = "event"
    primary_action: str = "where_to_watch"
    section: str = "upcoming"
    viewing_links_need_refresh: bool = False

    @model_validator(mode="after")
    def prepare_frontend(self) -> WatchmodeSportsEvent:
        """Validate links on EVERY response, after Redis retrieval, not before caching."""
        from copy import deepcopy
        from datetime import UTC, datetime
        self.display_kind = "matchup" if self.home_team and self.away_team else "event"
        if self.league:
            self.league = {k: v for k, v in self.league.items() if "logo" not in k.lower()}
        now = datetime.now(UTC)
        for name in ("sources", "event_destinations"):
            items = deepcopy(getattr(self, name))
            for source in items:
                for key in list(source):
                    if "logo" in key.lower():
                        del source[key]
                valid_links = []
                for link in source.get("links", []):
                    valid = source.get("freshness") != "stale"
                    deadline = link.get("refresh_after")
                    if deadline:
                        try:
                            parsed = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
                            valid = valid and parsed.tzinfo is not None and parsed > now
                        except (ValueError, TypeError, AttributeError):
                            valid = False
                    if valid:
                        valid_links.append(link)
                    else:
                        self.viewing_links_need_refresh = True
                source["links"] = valid_links
            setattr(self, name, items)
        return self


class WatchmodeSportsResponse(BaseModelWithMethods):
    """Uniform response for the Sports handler; all public fields JSON serializable."""
    model_config = ConfigDict(extra="ignore")
    status_code: int = 200
    error: str | None = None
    code: str | None = None
    region: str = "US"
    date: str | None = None
    timezone: str | None = None
    fetched_at: str | None = None
    status_fetched_at: str | None = None
    version: str | None = None
    date_range_limited: bool = False
    effective_date_from: str | None = None
    effective_date_to: str | None = None
    events: list[WatchmodeSportsEvent] = Field(default_factory=list)
    event: WatchmodeSportsEvent | None = None
    leagues: list[dict[str, Any]] = Field(default_factory=list)
    teams: list[dict[str, Any]] = Field(default_factory=list)
    coverage: dict[str, Any] | None = None
    sources: list[dict[str, Any]] = Field(default_factory=list)
    event_destinations: list[dict[str, Any]] = Field(default_factory=list)
    sources_state: str | None = None
    viewing_links_need_refresh: bool = False
    market: dict[str, Any] | None = None
    total_results: int = 0
    page: int = 1
    has_more: bool = False
    preferences_required: bool = False
    status_refresh_failed: bool = False
