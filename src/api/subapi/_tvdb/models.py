"""
TVDB Models - Pydantic models for TVDB API structures.
Follows Pydantic 2.0 patterns with full type safety.
"""


from pydantic import BaseModel, Field


class TVDBShow(BaseModel):
    """Model for TVDB show data."""

    # Core fields
    id: int
    tvdb_id: int
    name: str
    slug: str | None = None
    overview: str = ""
    year: int | None = None
    score: float = 0.0

    # Air dates and status
    first_aired: str | None = None
    last_aired: str | None = None
    next_aired: str | None = None
    status: str = "Unknown"
    airs_time: str | None = None
    airs_days: dict | None = None

    # Geographic and language info
    original_country: str | None = None
    original_language: str | None = None
    primary_language: str | None = None

    # Content details
    average_runtime: int | None = None
    is_order_randomized: bool = False
    default_season_type: int | None = None

    # Networks
    original_network: str = "Unknown"
    latest_network: str = "Unknown"
    network: str = "Unknown"

    # Rich content (extended mode)
    genres: list[str] = Field(default_factory=list)
    content_ratings: list[dict] = Field(default_factory=list)
    trailers: list[dict] = Field(default_factory=list)
    characters: list[dict] = Field(default_factory=list)
    companies: list[dict] = Field(default_factory=list)
    seasons: list[dict] = Field(default_factory=list)
    seasons_count: int = 0
    episodes_count: int = 0
    external_ids: dict = Field(default_factory=dict)
    tags: list[dict] = Field(default_factory=list)
    image: str | None = None
    last_updated: str | None = None

    # Images (for complete data)
    images: dict | None = None

    # TMDB data (when enriched)
    tmdb_id: int | None = None
    tmdb_popularity: float | None = None
    tmdb_vote_average: float | None = None
    tmdb_vote_count: int | None = None
    tmdb_cast: dict | None = None
    tmdb_videos: dict | None = None
    watch_providers: dict | None = None
    streaming_platform: str | None = None
    main_cast: list[dict] = Field(default_factory=list)
    director: dict | None = None
    primary_trailer: dict | None = None

    # Trending data
    trending_rank: int | None = None
    trending_period: str | None = None
    content_type: str = "tv"
    tvdb_data_available: bool = True

    # TMDB-only fields (for fallback data)
    poster_path: str | None = None
    backdrop_path: str | None = None
    vote_average: float | None = None
    vote_count: int | None = None
    popularity: float | None = None
    genre_ids: list[int] = Field(default_factory=list)
    media_type: str | None = None
    first_air_date: str | None = None
    release_date: str | None = None


class TVDBSearchResult(BaseModel):
    """Model for TVDB search result item."""

    id: int
    name: str
    overview: str = ""
    first_air_date: str | None = None
    status: str = "Unknown"
    network: str = "Unknown"
    original_language: str = "Unknown"
    score: float = 0.0


class TVDBSearchResponse(BaseModel):
    """Model for TVDB search response."""

    shows: list[TVDBSearchResult]
    total_count: int
    query: str


class TVDBImageData(BaseModel):
    """Model for TVDB image data."""

    tvdbid: int | None = None
    platform: str = "Unknown"
    show_name: str | None = None
    poster: str | None = None
    poster_thumbnail: str | None = None
    logo: str | None = None
    logo_thumbnail: str | None = None
    banner: str | None = None
    banner_thumbnail: str | None = None
    background: str | None = None
    background_thumbnail: str | None = None
    clearart: str | None = None
    clearart_thumbnail: str | None = None
    icon: str | None = None
    icon_thumbnail: str | None = None


class TVDBImagesResponse(BaseModel):
    """Model for TVDB images response."""

    show: TVDBImageData
    query: str
    tvdb_id: int | None = None


class TVDBShowDetailsResponse(BaseModel):
    """Model for TVDB show details response."""

    show: TVDBShow
    tvdb_id: int


class TVDBCompleteDataResponse(BaseModel):
    """Model for TVDB complete data response."""

    show: TVDBShow
    tvdb_id: int


class TVDBTrendingResponse(BaseModel):
    """Model for TVDB trending response."""

    trending_shows: list[TVDBShow]
    total_count: int
    time_window: str
    content_type: str
    enhanced_with_tvdb: bool
    error: str | None = None


class TMDBMultiSearchResult(BaseModel):
    """Model for TMDB multi search result."""

    id: int
    tmdb_id: int
    name: str
    title: str
    overview: str = ""
    release_date: str | None = None
    first_air_date: str | None = None
    vote_average: float = 0.0
    vote_count: int = 0
    popularity: float = 0.0
    poster_path: str | None = None
    backdrop_path: str | None = None
    genre_ids: list[int] = Field(default_factory=list)
    original_language: str | None = None
    media_type: str
    content_type: str
    status: str = "unknown"
    adult: bool = False
    origin_country: list[str] = Field(default_factory=list)


class TMDBMultiSearchResponse(BaseModel):
    """Model for TMDB multi search response."""

    results: list[TMDBMultiSearchResult]
    page: int
    total_pages: int
    total_results: int
    total_api_results: int
    query: str
    data_source: str = "TMDB Multi Search"
    error: str | None = None


class TMDBCastMember(BaseModel):
    """Model for TMDB cast member."""

    id: int
    name: str
    character: str | None = None
    order: int = 999
    gender: int | None = None
    known_for_department: str | None = None
    popularity: float = 0.0
    profile_path: str | None = None
    profile_images: dict | None = None
    has_image: bool = False
    profile_image_url: str | None = None
    image_url: str | None = None


class TMDBCastResponse(BaseModel):
    """Model for TMDB cast response."""

    cast: list[TMDBCastMember]
    total_cast: int
    cast_count: int
    optimized_for_mobile: bool = True
    cache_enabled: bool = True


class TMDBWatchProvider(BaseModel):
    """Model for TMDB watch provider."""

    provider_name: str
    provider_id: int
    logo_path: str | None = None
    display_priority: int = 999
    type: str = "flatrate"


class TMDBWatchProvidersResponse(BaseModel):
    """Model for TMDB watch providers response."""

    region: str
    link: str | None = None
    tmdb_id: int
    content_type: str
    flatrate: list[dict] = Field(default_factory=list)
    buy: list[dict] = Field(default_factory=list)
    rent: list[dict] = Field(default_factory=list)
    primary_provider: TMDBWatchProvider | None = None

