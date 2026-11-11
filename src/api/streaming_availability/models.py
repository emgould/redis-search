"""
Streaming Availability Models - Pydantic models for Streaming Availability API structures.
Follows the same pattern as Watchmode models with Pydantic 2.0.

API Documentation: https://rapidapi.com/movie-of-the-night-movie-of-the-night-default/api/streaming-availability
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, model_validator

from contracts.models import MCBaseItem, MCSources, MCType, generate_mc_id
from utils.pydantic_tools import BaseModelWithMethods

# ============================================================================
# Nested Models for Streaming Availability API Response
# ============================================================================


class SAGenre(BaseModelWithMethods):
    """Model for a genre in Streaming Availability API response."""

    id: str
    name: str


class SALocale(BaseModelWithMethods):
    """Model for locale information (language/region)."""

    language: str
    region: str | None = None


class SAAudio(BaseModelWithMethods):
    """Model for audio track information."""

    language: str
    region: str | None = None


class SASubtitle(BaseModelWithMethods):
    """Model for subtitle information."""

    closed_captions: bool = Field(alias="closedCaptions", default=False)
    locale: SALocale


class SAPrice(BaseModelWithMethods):
    """Model for price information."""

    amount: str
    currency: str
    formatted: str


class SAServiceImageSet(BaseModelWithMethods):
    """Model for streaming service logo images."""

    light_theme_image: str = Field(alias="lightThemeImage", default="")
    dark_theme_image: str = Field(alias="darkThemeImage", default="")
    white_image: str = Field(alias="whiteImage", default="")


class SAService(BaseModelWithMethods):
    """Model for streaming service information."""

    id: str
    name: str
    home_page: str = Field(alias="homePage", default="")
    theme_color_code: str = Field(alias="themeColorCode", default="")
    image_set: SAServiceImageSet = Field(alias="imageSet", default_factory=SAServiceImageSet)


class SAAddon(BaseModelWithMethods):
    """Model for streaming service addon (e.g., Paramount+ through Prime Video)."""

    id: str
    name: str
    home_page: str = Field(alias="homePage", default="")
    theme_color_code: str = Field(alias="themeColorCode", default="")
    image_set: SAServiceImageSet = Field(alias="imageSet", default_factory=SAServiceImageSet)


class SAStreamingOption(BaseModelWithMethods):
    """Model for a single streaming option."""

    service: SAService
    type: str  # "subscription", "rent", "buy", "free", "addon"
    link: str
    video_link: str | None = Field(alias="videoLink", default=None)
    quality: str | None = None  # "sd", "hd", "uhd"
    audios: list[SAAudio] = Field(default_factory=list)
    subtitles: list[SASubtitle] = Field(default_factory=list)
    price: SAPrice | None = None
    addon: SAAddon | None = None
    expires_soon: bool = Field(alias="expiresSoon", default=False)
    expires_on: int | None = Field(alias="expiresOn", default=None)  # Unix timestamp
    available_since: int = Field(alias="availableSince", default=0)  # Unix timestamp


class SAVerticalPosterSet(BaseModelWithMethods):
    """Model for vertical poster images at various resolutions."""

    w240: str | None = None
    w360: str | None = None
    w480: str | None = None
    w600: str | None = None
    w720: str | None = None


class SAHorizontalPosterSet(BaseModelWithMethods):
    """Model for horizontal poster images at various resolutions."""

    w360: str | None = None
    w480: str | None = None
    w720: str | None = None
    w1080: str | None = None
    w1440: str | None = None


class SAVerticalBackdropSet(BaseModelWithMethods):
    """Model for vertical backdrop images at various resolutions."""

    w240: str | None = None
    w360: str | None = None
    w480: str | None = None
    w600: str | None = None
    w720: str | None = None


class SAHorizontalBackdropSet(BaseModelWithMethods):
    """Model for horizontal backdrop images at various resolutions."""

    w360: str | None = None
    w480: str | None = None
    w720: str | None = None
    w1080: str | None = None
    w1440: str | None = None


class SAImageSet(BaseModelWithMethods):
    """Model for all image sets in various orientations and sizes."""

    vertical_poster: SAVerticalPosterSet = Field(
        alias="verticalPoster", default_factory=SAVerticalPosterSet
    )
    horizontal_poster: SAHorizontalPosterSet = Field(
        alias="horizontalPoster", default_factory=SAHorizontalPosterSet
    )
    vertical_backdrop: SAVerticalBackdropSet = Field(
        alias="verticalBackdrop", default_factory=SAVerticalBackdropSet
    )
    horizontal_backdrop: SAHorizontalBackdropSet = Field(
        alias="horizontalBackdrop", default_factory=SAHorizontalBackdropSet
    )


# ============================================================================
# Main Response Model
# ============================================================================


class SAShowResult(BaseModelWithMethods):
    """
    Model for the main Streaming Availability API show/movie result.

    This represents the full response from the /shows/{id} endpoint.
    """

    item_type: str = Field(alias="itemType", default="show")  # "show"
    show_type: str = Field(alias="showType", default="")  # "movie" or "series"
    id: str  # Streaming Availability internal ID
    imdb_id: str | None = Field(alias="imdbId", default=None)
    tmdb_id: str | None = Field(alias="tmdbId", default=None)  # Format: "movie/238" or "tv/1234"
    title: str
    overview: str | None = None
    release_year: int | None = Field(alias="releaseYear", default=None)
    original_title: str | None = Field(alias="originalTitle", default=None)
    genres: list[SAGenre] = Field(default_factory=list)
    directors: list[str] = Field(default_factory=list)
    cast: list[str] = Field(default_factory=list)
    rating: int | None = None  # 0-100 scale
    image_set: SAImageSet = Field(alias="imageSet", default_factory=SAImageSet)
    streaming_options: dict[str, list[SAStreamingOption]] = Field(
        alias="streamingOptions", default_factory=dict
    )

    # Optional fields for TV series
    first_air_year: int | None = Field(alias="firstAirYear", default=None)
    last_air_year: int | None = Field(alias="lastAirYear", default=None)
    season_count: int | None = Field(alias="seasonCount", default=None)
    episode_count: int | None = Field(alias="episodeCount", default=None)

    def get_streaming_options_for_country(self, country_code: str) -> list[SAStreamingOption]:
        """Get streaming options for a specific country code (e.g., 'us', 'gb')."""
        return self.streaming_options.get(country_code.lower(), [])

    def get_all_countries(self) -> list[str]:
        """Get all country codes that have streaming options."""
        return list(self.streaming_options.keys())

    def get_tmdb_numeric_id(self) -> int | None:
        """Extract numeric TMDB ID from the tmdbId field (e.g., 'movie/238' -> 238)."""
        if self.tmdb_id:
            parts = self.tmdb_id.split("/")
            if len(parts) == 2:
                try:
                    return int(parts[1])
                except ValueError:
                    pass
        return None

    def get_tmdb_type(self) -> str | None:
        """Extract TMDB type from the tmdbId field (e.g., 'movie/238' -> 'movie')."""
        if self.tmdb_id:
            parts = self.tmdb_id.split("/")
            if len(parts) == 2:
                return parts[0]
        return None


# ============================================================================
# MCBaseItem Response Wrapper
# ============================================================================


class StreamingAvailabilityResponse(MCBaseItem):
    """
    Response model for Streaming Availability API lookup.
    Wraps the SAShowResult in MCBaseItem format for consistent API responses.
    """

    # Include core result fields
    sa_id: str = ""  # Streaming Availability internal ID
    tmdb_id: int | None = None
    tmdb_type: str | None = None  # "movie" or "tv"
    imdb_id: str | None = None
    title: str = ""
    overview: str | None = None
    release_year: int | None = None
    original_title: str | None = None
    show_type: str = ""  # "movie" or "series"
    rating: int | None = None  # 0-100 scale

    # Media metadata
    genres: list[SAGenre] = Field(default_factory=list)
    directors: list[str] = Field(default_factory=list)
    cast: list[str] = Field(default_factory=list)

    # Images
    image_set: SAImageSet = Field(default_factory=SAImageSet)

    # Streaming options keyed by country code
    streaming_options: dict[str, list[SAStreamingOption]] = Field(default_factory=dict)

    # TV series specific
    first_air_year: int | None = None
    last_air_year: int | None = None
    season_count: int | None = None
    episode_count: int | None = None

    # MCBaseItem fields
    mc_type: MCType = MCType.MIXED  # Could be movie or TV
    source: MCSources = MCSources.WATCHMODE  # Using WATCHMODE as closest equivalent

    @model_validator(mode="after")
    def generate_mc_fields(self) -> StreamingAvailabilityResponse:
        """Auto-generate mc_id and source_id if not provided."""
        # Set mc_type based on show_type
        if self.show_type == "movie":
            self.mc_type = MCType.MOVIE
        elif self.show_type == "series":
            self.mc_type = MCType.TV_SERIES

        if not self.mc_id:
            if self.tmdb_id:
                # Use generate_mc_id to create proper format: tmdb_movie_xxx or tmdb_tv_xxx
                item_dict = {"tmdb_id": self.tmdb_id, "id": self.tmdb_id}
                self.mc_id = generate_mc_id(item_dict, self.mc_type)
            elif self.sa_id:
                self.mc_id = f"sa_{self.sa_id}"

        if not self.source_id:
            if self.tmdb_id:
                self.source_id = str(self.tmdb_id)
            elif self.sa_id:
                self.source_id = self.sa_id

        return self

    @classmethod
    def from_sa_result(cls, result: SAShowResult) -> StreamingAvailabilityResponse:
        """Create a StreamingAvailabilityResponse from a SAShowResult."""
        tmdb_numeric_id = result.get_tmdb_numeric_id()
        tmdb_type = result.get_tmdb_type()

        return cls(
            sa_id=result.id,
            tmdb_id=tmdb_numeric_id,
            tmdb_type=tmdb_type,
            imdb_id=result.imdb_id,
            title=result.title,
            overview=result.overview,
            release_year=result.release_year,
            original_title=result.original_title,
            show_type=result.show_type,
            rating=result.rating,
            genres=result.genres,
            directors=result.directors,
            cast=result.cast,
            image_set=result.image_set,
            streaming_options=result.streaming_options,
            first_air_year=result.first_air_year,
            last_air_year=result.last_air_year,
            season_count=result.season_count,
            episode_count=result.episode_count,
        )


# ============================================================================
# Type Aliases for Dict Representations
# ============================================================================

SAGenreDict = dict[str, Any]
SAStreamingOptionDict = dict[str, Any]
SAShowResultDict = dict[str, Any]
StreamingAvailabilityResponseDict = dict[str, Any]
