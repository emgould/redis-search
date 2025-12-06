"""
Comscore Models - Pydantic models for Comscore box office data structures
Follows Pydantic 2.0 patterns with full type safety.
"""

from contracts.models import MCBaseItem, MCSources, MCType
from pydantic import BaseModel, Field, model_validator


class BoxOfficeRanking(MCBaseItem):
    """Model for a single box office ranking entry."""

    rank: int
    title_name: str
    weekend_estimate: str
    dom_distributor: str | None = None
    intl_distributor: str | None = None

    mc_type: MCType = MCType.COMSCORE
    source: MCSources = MCSources.COMSCORE

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "BoxOfficeRanking":
        """Auto-generate mc_id and mc_type if not provided."""
        if not self.mc_id and self.rank and self.title_name:
            self.mc_id = f"comscore_{self.rank}_{self.title_name}"

        return self


class BoxOfficeData(MCBaseItem):
    """Model for complete box office rankings response."""

    rankings: list[BoxOfficeRanking] = Field(default_factory=list)
    exhibition_week: str
    fetched_at: str

    mc_type: MCType = MCType.COMSCORE
    source: MCSources = MCSources.COMSCORE

    @model_validator(mode="after")
    def generate_mc_fields(self) -> "BoxOfficeData":
        """Auto-generate mc_id and mc_type if not provided."""
        if not self.mc_id and self.exhibition_week:
            self.mc_id = f"comscore_{self.exhibition_week}"

        return self


class BoxOfficeResponse(BaseModel):
    """Model for box office rankings API response."""

    data: BoxOfficeData | None = None
    error: dict[str, str] | None = None
