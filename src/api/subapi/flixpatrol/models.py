"""
FlixPatrol Models - Pydantic models for FlixPatrol data structures.
Follows Pydantic 2.0 patterns with full type safety.
"""

from typing import Any

from contracts.models import MCBaseItem, MCSources, MCType
from pydantic import BaseModel, Field, model_validator

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
