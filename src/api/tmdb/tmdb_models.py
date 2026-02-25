"""
TMDB Models - Pydantic models for TMDB data structures
Follows the same pattern as podcast_models.py with Pydantic 2.0
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from contracts.models import MCBaseItem, MCSearchResponse, MCType
from utils.pydantic_tools import BaseModelWithMethods

# ============================================================================
# Raw TMDB API  Models
# These models represent the actual structure returned by TMDB search APIs
# ============================================================================


class TMDBSearchMovie(BaseModel):
    """Model for a movie result from TMDB search API."""

    adult: bool = False
    backdrop_path: str | None = None
    id: int | None = None
    title: str | None = None
    original_language: str | None = None
    original_title: str | None = None
    overview: str = ""
    poster_path: str | None = None
    media_type: Literal["movie"] = "movie"
    genre_ids: list[int] = Field(default_factory=list)
    popularity: float = 0.0
    release_date: str | None = None
    video: bool = False
    vote_average: float = 0.0
    vote_count: int = 0


class TMDBSearchMovieEnhanced(TMDBSearchMovie):
    pass


class TMDBMovieDiscover(TMDBSearchMovie):
    pass


class TMDBMovieMultiSearch(TMDBSearchMovie):
    pass


class TMDBSearchTv(BaseModel):
    """Model for tv search result from TMDB search API."""

    adult: bool = False
    backdrop_path: str | None = None
    genre_ids: list[int] = Field(default_factory=list)
    id: int
    origin_country: list[str] = Field(default_factory=list)
    original_language: str | None = None
    original_name: str | None = None
    overview: str = ""
    popularity: float = 0.0
    poster_path: str | None = None
    first_air_date: str | None = None
    name: str | None = None
    vote_average: float = 0.0
    vote_count: int = 0
    media_type: Literal["tv"] = "tv"


class TMDBTvDiscover(TMDBSearchTv):
    pass


class TMDBTVMultiSearch(TMDBSearchTv):
    pass


class TMDBPersonTvCredits(BaseModel):
    """Model for a TV show result from TMDB search API."""

    adult: bool = False
    backdrop_path: str | None = None
    genre_ids: list[int] = Field(default_factory=list)
    id: int
    origin_country: list[str] = Field(default_factory=list)
    original_language: str | None = None
    original_name: str | None = None
    overview: str | None = None
    popularity: float | None = None
    poster_path: str | None = None
    first_air_date: str | None = None
    name: str | None = None
    vote_average: float = 0.0
    vote_count: int = 0
    character: str | None = None
    credit_id: str | None = None
    episode_count: int | None = None
    media_type: Literal["tv"] = "tv"


class TMDBSearchPersonItem(BaseModelWithMethods):
    """Model for a person result from TMDB search API."""

    adult: bool = False
    gender: int | None = None
    id: int
    known_for_department: str | None = None
    name: str
    original_name: str
    popularity: float = 0.0
    profile_path: str | None = None
    known_for: list[dict[str, Any]] = Field(default_factory=list)


class TMDBPersonDetailsResult(BaseModelWithMethods):
    """Model for TMDB person details API response."""

    adult: bool = False
    also_known_as: list[str] = Field(default_factory=list)
    biography: str = ""
    birthday: str | None = None
    deathday: str | None = None
    gender: int | None = None
    homepage: str | None = None
    id: int
    imdb_id: str | None = None
    known_for_department: str | None = None
    name: str
    place_of_birth: str | None = None
    popularity: float = 0.0
    profile_path: str | None = None


class TMDBSearchPersonResult(BaseModelWithMethods):
    results: list[TMDBSearchPersonItem]
    total_results: int
    total_pages: int
    page: int
    error: str | None = None
    status_code: int = 200


# Type alias for multi-search results
TMDBMultiSearch = TMDBSearchMovie | TMDBTvDiscover | TMDBSearchPersonResult

# Type alias for processed media item dicts (for JSON serialization)
MCBaseMediaItemDict = dict[str, Any]  # Represents MCBaseMediaItem.model_dump()
MCPersonItemDict = dict[str, Any]  # Represents MCPersonItem.model_dump()


# ============================================================================
# Enhanced/Processed Models
# These models represent enriched data after processing
# ============================================================================


class TMDBGenre(BaseModelWithMethods):
    """Model for TMDB genre data."""

    id: int
    name: str


class TMDBProductionCompany(BaseModelWithMethods):
    """Model for TMDB production company data."""

    id: int
    name: str
    logo_path: str | None = None
    origin_country: str = ""


class TMDBProductionCountry(BaseModelWithMethods):
    """Model for TMDB production country data."""

    iso_3166_1: str
    name: str


class TMDBSpokenLanguage(BaseModelWithMethods):
    """Model for TMDB spoken language data."""

    english_name: str
    iso_639_1: str
    name: str


class TMDBMovieDetailsResult(BaseModelWithMethods):
    """Model for detailed TMDB movie data from /movie/{id} endpoint."""

    # Core identification
    id: int
    imdb_id: str | None = None
    title: str
    original_title: str
    original_language: str

    # Content metadata
    adult: bool = False
    video: bool = False
    overview: str = ""
    tagline: str | None = None
    homepage: str | None = None
    status: str | None = None

    # Media paths
    backdrop_path: str | None = None
    poster_path: str | None = None

    # Dates and runtime
    release_date: str | None = None
    runtime: int | None = None

    # Ratings and popularity
    vote_average: float = 0.0
    vote_count: int = 0
    popularity: float = 0.0

    # Financial data
    budget: int = 0
    revenue: int = 0

    # Complex nested objects
    genres: list[TMDBGenre] = Field(default_factory=list)
    production_companies: list[TMDBProductionCompany] = Field(default_factory=list)
    production_countries: list[TMDBProductionCountry] = Field(default_factory=list)
    spoken_languages: list[TMDBSpokenLanguage] = Field(default_factory=list)
    belongs_to_collection: dict[str, Any] | None = None


class TMDBTvDetailsResult(BaseModelWithMethods):
    """Model for detailed TMDB TV show data from /tv/{id} endpoint."""

    # Core identification
    id: int
    name: str
    original_name: str
    original_language: str

    # Content metadata
    adult: bool = False
    overview: str = ""
    tagline: str | None = None
    homepage: str | None = None
    status: str | None = None
    type: str | None = None

    # Media paths
    backdrop_path: str | None = None
    poster_path: str | None = None

    # Dates and airing info
    first_air_date: str | None = None
    last_air_date: str | None = None
    number_of_seasons: int | None = None
    number_of_episodes: int | None = None
    episode_run_time: list[int] = Field(default_factory=list)

    # Ratings and popularity
    vote_average: float = 0.0
    vote_count: int = 0
    popularity: float = 0.0

    # TV-specific fields
    origin_country: list[str] = Field(default_factory=list)
    in_production: bool = False
    languages: list[str] = Field(default_factory=list)

    # Complex nested objects
    genres: list[TMDBGenre] = Field(default_factory=list)
    production_companies: list[TMDBProductionCompany] = Field(default_factory=list)
    production_countries: list[TMDBProductionCountry] = Field(default_factory=list)
    spoken_languages: list[TMDBSpokenLanguage] = Field(default_factory=list)
    networks: list[TMDBNetwork] = Field(default_factory=list)
    created_by: list[TMDBCreatedBy] = Field(default_factory=list)
    seasons: list[TMDBSeasonSummary] = Field(default_factory=list)
    last_episode_to_air: TMDBEpisodeSummary | None = None
    next_episode_to_air: TMDBEpisodeSummary | None = None


class TMDBVideo(BaseModelWithMethods):
    """Model for TMDB video/trailer data."""

    id: str
    key: str
    name: str
    site: str
    type: str
    official: bool = False
    published_at: str | None = None
    size: int = 1080
    iso_639_1: str = "en"
    iso_3166_1: str = "US"
    url: str | None = None
    embed_url: str | None = None
    thumbnail_url: str | None = None


class TMDBCastMember(BaseModelWithMethods):
    """Model for TMDB cast member data."""

    id: int
    name: str
    character: str | None = None
    order: int = 999
    gender: int | None = None
    profile_path: str | None = None
    profile_images: dict[str, str] | None = None
    profile_image_url: str | None = None
    image_url: str | None = None
    has_image: bool = False


class TMDBWatchProvider(MCBaseItem):
    """Model for TMDB watch provider data."""

    provider_id: int
    provider_name: str
    logo_path: str | None = None
    display_priority: int = 999
    display_priorities: dict[str, int] = Field(default_factory=dict)
    mc_type: MCType = MCType.PROVIDER

    @model_validator(mode="after")
    def generate_mc_fields(self) -> TMDBWatchProvider:
        """Auto-generate mc_id and mc_type if not provided."""
        if not self.mc_id:
            self.mc_id = f"provider_id_{self.provider_id}"
        return self


class TMDBProvidersResponse(MCBaseItem):
    """Model for TMDB providers list AzPI response (watch/providers/tv or watch/providers/movie)."""

    list_type: Literal["tv", "movie"]
    results: list[TMDBWatchProvider] = Field(default_factory=list)
    mc_type: MCType = MCType.PROVIDERS_LIST
    region: str = "US"

    @model_validator(mode="after")
    def generate_mc_fields(self) -> TMDBProvidersResponse:
        """Auto-generate mc_id and mc_type if not provided."""
        if not self.mc_id:
            self.mc_id = f"providers_list_{self.region}"
        return self


class TMDBCreatedBy(BaseModelWithMethods):
    """Model for TMDB TV created_by entry."""

    id: int
    credit_id: str | None = None
    name: str
    gender: int | None = None
    profile_path: str | None = None


class TMDBNetwork(BaseModelWithMethods):
    """Model for TMDB TV network entry."""

    id: int
    name: str
    logo_path: str | None = None
    origin_country: str = ""


class TMDBEpisodeSummary(BaseModelWithMethods):
    """Summary model for last/next episode objects on TV details."""

    id: int
    name: str
    overview: str = ""
    vote_average: float = 0.0
    vote_count: int = 0
    air_date: str | None = None
    episode_number: int | None = None
    episode_type: str | None = None
    production_code: str | None = None
    runtime: int | None = None
    season_number: int | None = None
    show_id: int | None = None
    still_path: str | None = None


class TMDBSeasonSummary(BaseModelWithMethods):
    """Summary model for season objects on TV details."""

    id: int
    name: str
    overview: str = ""
    air_date: str | None = None
    episode_count: int = 0
    poster_path: str | None = None
    season_number: int = 0
    vote_average: float = 0.0


class TMDBSeasonEpisode(BaseModelWithMethods):
    """Episode model for TV season detail responses."""

    id: int
    episode_number: int
    name: str
    overview: str = ""
    still_path: str | None = None
    air_date: str | None = None
    runtime: int | None = None


class TMDBSeasonDetailsResult(BaseModelWithMethods):
    """Model for TMDB TV season details from /tv/{id}/season/{season_number}."""

    id: int
    name: str
    season_number: int
    episodes: list[TMDBSeasonEpisode] = Field(default_factory=list)


class TMDBKeyword(BaseModelWithMethods):
    """Model for a TMDB keyword."""

    id: int
    name: str


class TMDBKeywordSearchResult(BaseModelWithMethods):
    """Model for TMDB keyword search API response."""

    results: list[TMDBKeyword]
    total_results: int
    total_pages: int
    page: int


class TMDBKeywordGenreResponse(BaseModelWithMethods):
    """Model for TMDB genre search response."""

    genres: list[TMDBGenre]


class TMDBSearchPersonResponse(MCSearchResponse):
    """Model for TMDB person search response."""

    results: list[TMDBSearchPersonItem]  # type: ignore[assignment]
    data_type: MCType = MCType.PERSON


class TMDBRawMultiSearchRawResponse(BaseModelWithMethods):
    """Model for raw TMDB multi-search API response (unparsed results)."""

    results: list[dict[str, Any]]
    total_results: int
    total_pages: int
    page: int


class TMDBSearchMultiResult(BaseModelWithMethods):
    """Model for raw TMDB multi-search API response (unparsed results)."""

    results: list[TMDBMultiSearch]
    total_results: int
    total_pages: int
    page: int


class TMDBSearchMovieResult(BaseModelWithMethods):
    """Model for raw TMDB discover/movie API response (unparsed movie results)."""

    results: list[TMDBSearchMovie]
    total_results: int
    total_pages: int
    page: int


class TMDBSearchTvResult(BaseModelWithMethods):
    """Model for raw TMDB discover/movie API response (unparsed movie results)."""

    results: list[TMDBSearchTv]
    total_results: int
    total_pages: int
    page: int


class TMDBRawDiscoverTVResponse(BaseModelWithMethods):
    """Model for raw TMDB discover/tv API response (unparsed TV results)."""

    results: list[TMDBTvDiscover]
    total_results: int
    total_pages: int
    page: int


class TMDBPersonMovieCastCredit(TMDBSearchMovie):
    """Model for a cast credit in person movie credits response."""

    character: str | None = None
    credit_id: str | None = None
    order: int = 0


class TMDBPersonMovieCrewCredit(TMDBSearchMovie):
    """Model for a crew credit in person movie credits response."""

    credit_id: str | None = None
    department: str | None = None
    job: str | None = None


class TMDBPersonMovieCreditsResponse(BaseModelWithMethods):
    """Model for TMDB person movie credits API response (/person/{id}/movie_credits)."""

    cast: list[TMDBPersonMovieCastCredit] = Field(default_factory=list)
    crew: list[TMDBPersonMovieCrewCredit] = Field(default_factory=list)


class TMDBPersonTvCastCredit(TMDBSearchTv):
    """Model for a cast credit in person TV credits response."""

    character: str | None = None
    credit_id: str | None = None
    episode_count: int | None = None


class TMDBPersonTvCrewCredit(TMDBSearchTv):
    """Model for a crew credit in person TV credits response."""

    credit_id: str | None = None
    department: str | None = None
    job: str | None = None
    episode_count: int | None = None


class TMDBPersonTvCreditsResponse(BaseModelWithMethods):
    """Model for TMDB person TV credits API response (/person/{id}/tv_credits)."""

    cast: list[TMDBPersonTvCastCredit] = Field(default_factory=list)
    crew: list[TMDBPersonTvCrewCredit] = Field(default_factory=list)
    id: int
