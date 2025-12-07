

from typing import Any

from media_manager.mediacircle.api.tmdb.tmdb_models import (
    TMDBMovieDetailsResult,
    TMDBSearchMovie,
    TMDBSearchTv,
    TMDBTvDetailsResult,
)
from media_manager.mediacircle.contracts.models import (
    MCBaseItem,
    MCImage,
    MCSources,
    MCType,
    MCUrlType,
)
from pydantic import Field


class MCBaseMediaItem(MCBaseItem):
    """Base model for TMDB media items (movies and TV shows)."""

    # Required fields
    tmdb_id: int
    name: str | None = None
    title: str | None = None
    media_type: str | None = None
    content_type: str | None = None

    # Common fields
    overview: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    vote_average: float = 0.0
    vote_count: int = 0
    popularity: float = 0.0
    genre_ids: list[int] = Field(default_factory=list)
    genres: list[str] = Field(default_factory=list)
    original_language: str | None = None
    adult: bool = False

    # Streaming availability
    streaming_platform: str | None = None
    availability_type: str | None = None

    # Enhanced data (optional)
    tmdb_cast: dict[str, Any] = Field(default_factory=dict)
    main_cast: list[dict[str, Any]] = Field(default_factory=list)
    director: dict[str, Any] = Field(default_factory=dict)
    tmdb_videos: dict[str, Any] = Field(default_factory=dict)
    primary_trailer: dict[str, Any] = Field(default_factory=dict)
    trailers: list[dict[str, Any]] = Field(default_factory=list)
    clips: list[dict[str, Any]] = Field(default_factory=list)
    watch_providers: dict[str, Any] = Field(default_factory=dict)
    keywords: list[dict[str, Any]] = Field(default_factory=list)
    keywords_count: int = 0

    # Status
    status: str | None = None

    # Search/sorting metadata
    relevancy_debug: dict[str, Any] | None = None
    final_score: float | None = None


class MCMovieItem(MCBaseMediaItem):
    """Base model for TMDB media items (movies and TV shows)."""

    mc_type: MCType = MCType.MOVIE
    release_date: str | None = None
    revenue: int | None = None
    runtime: int | None = None
    spoken_languages: list[str] = Field(default_factory=list)

    @classmethod
    def from_movie_search(
        cls, item: "TMDBSearchMovie", image_base_url: str | None = None
    ) -> "MCMovieItem":
        """Process and normalize a media item from TMDB."""

        # Ensure we have a valid ID
        if item.id is None:
            raise ValueError("Movie item must have an id")

        # Get name/title
        if item.title is None and item.original_title is None:
            name = "Untitled"
        elif item.title is not None:
            name = item.title
        else:
            name = item.original_title or "Untitled"

        # Build processed item data - direct attribute access
        metrics = {
            "vote_average": item.vote_average,
            "vote_count": item.vote_count,
            "popularity": item.popularity,
        }

        # Initialize image dictionaries
        poster_images = {}
        backdrop_images = {}

        if item.poster_path and image_base_url:
            poster_images = {
                "small": f"{image_base_url}w45{item.poster_path}",
                "medium": f"{image_base_url}w185{item.poster_path}",
                "large": f"{image_base_url}h632{item.poster_path}",
                "original": f"{image_base_url}original{item.poster_path}",
            }
        if item.backdrop_path and image_base_url:
            backdrop_images = {
                "small": f"{image_base_url}w45{item.backdrop_path}",
                "medium": f"{image_base_url}w185{item.backdrop_path}",
                "large": f"{image_base_url}h632{item.backdrop_path}",
                "original": f"{image_base_url}original{item.backdrop_path}",
            }

        images = []
        for key, value in poster_images.items():
            images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="poster"))
        for key, value in backdrop_images.items():
            images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="backdrop"))

        movie_item = MCMovieItem(
            ## MC BastItem Common Fields
            source_id=str(item.id),
            source=MCSources.TMDB,
            mc_type=MCType.MOVIE,
            images=images,
            metrics=metrics,
            ## TMDB MediaItem Specific Fields
            tmdb_id=item.id,
            name=name,
            title=name,
            overview=item.overview,
            genre_ids=item.genre_ids,
            original_language=item.original_language,
            media_type=MCType.MOVIE.value,
            content_type=MCType.MOVIE.value,  # Use MCType enum value for type safety
            adult=item.adult,
            poster_path=item.poster_path,
            backdrop_path=item.backdrop_path,
            release_date=item.release_date,
        )

        return movie_item

    @classmethod
    def from_movie_details(
        cls, item: "TMDBMovieDetailsResult", image_base_url: str | None = None
    ) -> "MCMovieItem":
        """Process and normalize a media item from TMDB
        Args:
            item: Typed model (TMDBSearchMovie or TMDBTvDiscover)
            media_type: 'movie' or 'tv' (optional, can be inferred from item)

        Returns:
            MCBaseMediaItem with standardized fields
        """
        genre_ids = [genre.id for genre in item.genres]

        # Build processed item data - direct attribute access
        images = []
        if item.poster_path and image_base_url:
            poster_images = {
                "small": f"{image_base_url}w45{item.poster_path}",
                "medium": f"{image_base_url}w185{item.poster_path}",
                "large": f"{image_base_url}h632{item.poster_path}",
                "original": f"{image_base_url}original{item.poster_path}",
            }
            for key, value in poster_images.items():
                images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="poster"))
        if item.backdrop_path and image_base_url:
            backdrop_images = {
                "small": f"{image_base_url}w45{item.backdrop_path}",
                "medium": f"{image_base_url}w185{item.backdrop_path}",
                "large": f"{image_base_url}h632{item.backdrop_path}",
                "original": f"{image_base_url}original{item.backdrop_path}",
            }
            for key, value in backdrop_images.items():
                images.append(
                    MCImage(url=value, type=MCUrlType.URL, key=key, description="backdrop")
                )

        metrics = {
            "vote_average": item.vote_average,
            "vote_count": item.vote_count,
            "popularity": item.popularity,
        }

        media_item = MCMovieItem(
            ## MC BastItem Common Fields
            source_id=str(item.id),
            source=MCSources.TMDB,
            metrics=metrics,
            ## TMDB MediaItem Specific Fields
            tmdb_id=item.id,
            ## TMDB MediaItem Specific Fields
            name=item.title,
            title=item.title,
            overview=item.overview,
            genre_ids=genre_ids,
            original_language=item.original_language,
            images=images,
            status=item.status,
            media_type=MCType.MOVIE.value,
            content_type=MCType.MOVIE.value,
            adult=item.adult,
            poster_path=item.poster_path,
            backdrop_path=item.backdrop_path,
            release_date=item.release_date,
            runtime=item.runtime,
            revenue=item.revenue,
            spoken_languages=[lang.english_name for lang in item.spoken_languages],
        )
        return media_item


class MCTvItem(MCBaseMediaItem):
    """Base model for TMDB media items (movies and TV shows)."""

    mc_type: MCType = MCType.TV_SERIES

    first_air_date: str | None = None
    last_air_date: str | None = None
    number_of_seasons: int | None = None
    number_of_episodes: int | None = None
    origin_country: list[str] = Field(default_factory=list)
    status: str | None = None
    spoken_languages: list[str] = Field(default_factory=list)
    network: str | None = None  # Normalized from TMDB networks array (first network name)

    @classmethod
    def from_tv_search(cls, item: "TMDBSearchTv", image_base_url: str | None = None) -> "MCTvItem":
        """Process and normalize a media item from TMDB."""

        # Build processed item data - direct attribute access
        metrics = {
            "vote_average": item.vote_average,
            "vote_count": item.vote_count,
            "popularity": item.popularity,
        }

        # Initialize image dictionaries
        poster_images = {}
        backdrop_images = {}

        if item.poster_path and image_base_url:
            poster_images = {
                "small": f"{image_base_url}w45{item.poster_path}",
                "medium": f"{image_base_url}w185{item.poster_path}",
                "large": f"{image_base_url}h632{item.poster_path}",
                "original": f"{image_base_url}original{item.poster_path}",
            }
        if item.backdrop_path and image_base_url:
            backdrop_images = {
                "small": f"{image_base_url}w45{item.backdrop_path}",
                "medium": f"{image_base_url}w185{item.backdrop_path}",
                "large": f"{image_base_url}h632{item.backdrop_path}",
                "original": f"{image_base_url}original{item.backdrop_path}",
            }

        images = []
        for key, value in poster_images.items():
            images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="poster"))
        for key, value in backdrop_images.items():
            images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="backdrop"))

        tv_item = MCTvItem(
            ## MC BastItem Common Fields
            source_id=str(item.id),
            source=MCSources.TMDB,
            mc_type=MCType.TV_SERIES,
            images=images,
            metrics=metrics,
            ## TMDB MediaItem Specific Fields
            tmdb_id=item.id,
            name=item.name,
            title=item.name,
            overview=item.overview,
            genre_ids=item.genre_ids,
            original_language=item.original_language,
            media_type=MCType.TV_SERIES.value,
            content_type=MCType.TV_SERIES.value,  # Use MCType enum value for type safety
            adult=item.adult,
            poster_path=item.poster_path,
            backdrop_path=item.backdrop_path,
            spoken_languages=[item.original_language] if item.original_language else [],
            first_air_date=item.first_air_date,
            origin_country=item.origin_country,
        )

        return tv_item

    @classmethod
    def from_tv_details(
        cls, item: "TMDBTvDetailsResult", image_base_url: str | None = None
    ) -> "MCTvItem":
        """Process and normalize a TV show item from TMDB details endpoint.

        Args:
            item: TMDBTvDetailsResult from TMDB API
            image_base_url: Optional base URL for generating image URLs

        Returns:
            MCTvItem with standardized fields
        """
        genre_ids = [genre.id for genre in item.genres]

        # Build processed item data - direct attribute access
        images = []
        if item.poster_path and image_base_url:
            poster_images = {
                "small": f"{image_base_url}w45{item.poster_path}",
                "medium": f"{image_base_url}w185{item.poster_path}",
                "large": f"{image_base_url}h632{item.poster_path}",
                "original": f"{image_base_url}original{item.poster_path}",
            }
            for key, value in poster_images.items():
                images.append(MCImage(url=value, type=MCUrlType.URL, key=key, description="poster"))
        if item.backdrop_path and image_base_url:
            backdrop_images = {
                "small": f"{image_base_url}w45{item.backdrop_path}",
                "medium": f"{image_base_url}w185{item.backdrop_path}",
                "large": f"{image_base_url}h632{item.backdrop_path}",
                "original": f"{image_base_url}original{item.backdrop_path}",
            }
            for key, value in backdrop_images.items():
                images.append(
                    MCImage(url=value, type=MCUrlType.URL, key=key, description="backdrop")
                )

        metrics = {
            "vote_average": item.vote_average,
            "vote_count": item.vote_count,
            "popularity": item.popularity,
        }

        media_item = MCTvItem(
            ## MC BastItem Common Fields
            source_id=str(item.id),
            source=MCSources.TMDB,
            metrics=metrics,
            ## TMDB MediaItem Specific Fields
            tmdb_id=item.id,
            name=item.name,
            title=item.name,
            overview=item.overview,
            genre_ids=genre_ids,
            original_language=item.original_language,
            images=images,
            status=item.status,
            media_type=MCType.TV_SERIES.value,
            content_type=MCType.TV_SERIES.value,
            adult=item.adult,
            poster_path=item.poster_path,
            backdrop_path=item.backdrop_path,
            spoken_languages=[lang.english_name for lang in item.spoken_languages],
            first_air_date=item.first_air_date,
            origin_country=item.origin_country,
            last_air_date=item.last_air_date,
            number_of_seasons=item.number_of_seasons,
            number_of_episodes=item.number_of_episodes,
            network=item.networks[0].name if item.networks else None,
        )
        return media_item


class MCMovieCreditMediaItem(MCMovieItem):
    """Model for TMDB credit media item."""

    credit_id: str | None = None
    character: str | None = None
    order: int = 999


class MCTvCreditMediaItem(MCTvItem):
    """Model for TMDB credit media item."""

    credit_id: str | None = None
    character: str | None = None
    episode_count: int | None = None
    order: int = 999
