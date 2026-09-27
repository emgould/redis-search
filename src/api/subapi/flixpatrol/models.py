"""
FlixPatrol Models - Pydantic models for FlixPatrol data structures.
Follows Pydantic 2.0 patterns with full type safety.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from contracts.models import MCBaseItem, MCSources, MCType
from utils.pydantic_tools import BaseModelWithMethods


class FlixPatrolMediaItem(MCBaseItem):
    """Model for FlixPatrol media item (show or movie)."""

    # Unique identifier for mc_id generation
    id: str | None = None

    rank: int
    title: str
    score: int
    platform: str | None = None
    content_type: str | None = None  # 'tv' or 'movie'

    mc_type: MCType = MCType.FLIXPATROL
    source: MCSources = MCSources.FLIXPATROL


class FlixPatrolMetadata(BaseModelWithMethods):
    """Model for FlixPatrol response metadata."""

    source: str = "FlixPatrol"
    total_shows: int = 0
    total_movies: int = 0
    platforms: list[str] = Field(default_factory=list)


class FlixPatrolResponse(MCBaseItem):
    """Model for complete FlixPatrol response."""

    date: str
    shows: dict[str, list[FlixPatrolMediaItem]] = Field(default_factory=dict)
    movies: dict[str, list[FlixPatrolMediaItem]] = Field(default_factory=dict)
    top_trending_tv_shows: list[FlixPatrolMediaItem] = Field(default_factory=list)
    top_trending_movies: list[FlixPatrolMediaItem] = Field(default_factory=list)
    metadata: FlixPatrolMetadata | None = None

    mc_type: MCType = MCType.FLIXPATROL
    source: MCSources = MCSources.FLIXPATROL

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "FlixPatrolResponse":
        """Auto-generate mc_id and mc_type if not provided."""
        if not self.mc_id and self.date:
            self.mc_id = f"flixpatrol_{self.date}"

        return self


class FlixPatrolPlatformData(BaseModelWithMethods):
    """Model for platform-specific FlixPatrol data."""

    platform: str
    shows: list[FlixPatrolMediaItem] = Field(default_factory=list)
    movies: list[FlixPatrolMediaItem] = Field(default_factory=list)


class FlixPatrolParsedData(BaseModel):
    """Model for parsed FlixPatrol HTML data."""

    date: str
    shows: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    movies: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class FlixPatrolHistoryRecord(BaseModel):
    """One stored chart row for a title, company, and date range."""

    model_config = ConfigDict(populate_by_name=True)

    date_type: int
    date_from: str
    date_to: str
    ranking: int
    ranking_last: int | None = Field(default=None, alias="rankingLast")
    value: int | None = None
    value_last: int | None = Field(default=None, alias="valueLast")
    value_total: int | None = Field(default=None, alias="valueTotal")
    value_change: str | None = Field(default=None, alias="valueChange")
    days: int | None = None
    days_total: int | None = Field(default=None, alias="daysTotal")


class FlixPatrolHistory(BaseModel):
    """Historical FlixPatrol peaks and chart records stored on a media document."""

    model_config = ConfigDict(populate_by_name=True)

    peak_total: int | None = None
    peak_rank: int | None = None
    max_days: int | None = None
    last_date: str | None = None
    start_date: str | None = None
    records: list[FlixPatrolHistoryRecord] = Field(default_factory=list)


class FlixPatrolTitleFile(BaseModel):
    """One title file written by the historical rankings transformer."""

    model_config = ConfigDict(populate_by_name=True)

    mc_id: str
    title: str
    data: FlixPatrolHistory
