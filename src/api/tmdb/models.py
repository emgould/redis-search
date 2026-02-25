"""
Models - Consumer Models. These are models of data that will
be returned to the front end as part of fullfilling a request
Follows the same pattern as podcast_models.py with Pydantic 2.0
"""

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import Field

from api.tmdb.tmdb_models import (
    TMDBEpisodeSummary,
    TMDBMovieDetailsResult,
    TMDBPersonDetailsResult,
    TMDBSearchMovie,
    TMDBSearchTv,
    TMDBTvDetailsResult,
)
from contracts.models import (
    MCBaseItem,
    MCImage,
    MCSearchResponse,
    MCSources,
    MCSubType,
    MCType,
    MCUrlType,
    generate_mc_id,
)
from utils.pydantic_tools import BaseModelWithMethods

if TYPE_CHECKING:
    from api.tmdb.tmdb_models import TMDBSearchPersonItem


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
    us_rating: str | None = None
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
    release_dates: dict[str, Any] = Field(default_factory=dict)  # TMDB release dates by country

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
        genres = [genre.name for genre in item.genres]

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
            genres=genres,
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


class MCEpisodeSummary(BaseModelWithMethods):
    """Lightweight episode summary for last/next episode on TV detail responses."""

    _MAX_PROVIDER_IDS: ClassVar[set[int]] = {384, 1899}
    _APPLE_PROVIDER_IDS: ClassVar[set[int]] = {2, 350}

    episode_id: int
    name: str
    air_date: str | None = None
    episode_number: int | None = None
    episode_type: str | None = None
    season_number: int | None = None
    still_image: dict[str, str] | None = None
    air_datetime_utc: str | None = None

    @staticmethod
    def _parse_air_date(air_date: str | None) -> datetime | None:
        if not air_date:
            return None
        try:
            base_date = datetime.strptime(air_date, "%Y-%m-%d")
            return base_date.replace(tzinfo=UTC)
        except ValueError:
            return None

    @classmethod
    def _is_max_source(cls, source_names: list[str], provider_ids: list[int] | None = None) -> bool:
        if provider_ids and any(
            provider_id in cls._MAX_PROVIDER_IDS for provider_id in provider_ids
        ):
            return True
        normalized_names = [name.strip().lower() for name in source_names]
        return any(
            name == "max"
            or name.startswith("max ")
            or name.endswith(" max")
            or ("max" in name and "cinemax" not in name)
            or "hbo max" in name
            or "hbo" in name
            or "home box office" in name
            or "max amazon channel" in name
            for name in normalized_names
        )

    @classmethod
    def _is_apple_tv_source(
        cls, source_names: list[str], provider_ids: list[int] | None = None
    ) -> bool:
        if provider_ids and any(
            provider_id in cls._APPLE_PROVIDER_IDS for provider_id in provider_ids
        ):
            return True
        normalized_names = [name.strip().lower() for name in source_names]
        return any(
            "apple tv" in name
            or "apple tv+" in name
            or "appletv+" in name
            or "apple tv plus" in name
            for name in normalized_names
        )

    @classmethod
    def _compute_air_datetime_utc(
        cls, air_date: str | None, source_names: list[str], provider_ids: list[int] | None = None
    ) -> str | None:
        base_air_datetime = cls._parse_air_date(air_date)
        if not base_air_datetime:
            return None

        if cls._is_apple_tv_source(source_names, provider_ids):
            # Apple TV episodes are effectively available the day after the TMDB air_date.
            availability_datetime = base_air_datetime + timedelta(days=1)
        elif cls._is_max_source(source_names, provider_ids):
            availability_datetime = base_air_datetime.replace(hour=21, minute=0, second=0)
        else:
            availability_datetime = base_air_datetime

        return availability_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def from_tmdb(
        cls,
        episode: TMDBEpisodeSummary | None,
        image_base_url: str | None = None,
        source_names: list[str] | None = None,
        provider_ids: list[int] | None = None,
    ) -> "MCEpisodeSummary | None":
        if episode is None:
            return None
        still_image = None
        if episode.still_path and image_base_url:
            still_image = {
                "small": f"{image_base_url}w185{episode.still_path}",
                "medium": f"{image_base_url}w300{episode.still_path}",
                "original": f"{image_base_url}original{episode.still_path}",
            }

        resolved_source_names = source_names or []
        air_datetime_utc = cls._compute_air_datetime_utc(
            episode.air_date, resolved_source_names, provider_ids
        )

        return cls(
            episode_id=episode.id,
            name=episode.name,
            air_date=episode.air_date,
            episode_number=episode.episode_number,
            episode_type=episode.episode_type,
            season_number=episode.season_number,
            still_image=still_image,
            air_datetime_utc=air_datetime_utc,
        )


SeriesStatus = Literal["new_season", "active", "binge", "catch_up", "over"]


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
    airdate_time: datetime | None = None
    duration: int | None = None
    last_episode_to_air: MCEpisodeSummary | None = None
    next_episode_to_air: MCEpisodeSummary | None = None
    series_status: SeriesStatus = "binge"

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

    def _episode_source_names(self) -> tuple[list[str], list[int]]:
        source_names: list[str] = []
        provider_ids: list[int] = []

        watch_providers = getattr(self, "watch_providers", {})
        if isinstance(watch_providers, dict):
            primary = watch_providers.get("primary_provider")
            if isinstance(primary, dict) and isinstance(primary.get("provider_name"), str):
                source_names.append(primary["provider_name"])
            if isinstance(primary, dict) and isinstance(primary.get("provider_id"), int):
                provider_ids.append(primary["provider_id"])

            # Episode availability timing should use primary_provider first
            # and fall back to flatrate providers only.
            flatrate = watch_providers.get("flatrate")
            if isinstance(flatrate, list):
                for provider in flatrate:
                    if isinstance(provider, dict) and isinstance(
                        provider.get("provider_name"), str
                    ):
                        source_names.append(provider["provider_name"])
                    if isinstance(provider, dict) and isinstance(provider.get("provider_id"), int):
                        provider_ids.append(provider["provider_id"])

        return source_names, provider_ids

    def apply_episode_availability_rules(self) -> None:
        source_names, provider_ids = self._episode_source_names()
        last_episode = getattr(self, "last_episode_to_air", None)
        if last_episode:
            last_episode.air_datetime_utc = MCEpisodeSummary._compute_air_datetime_utc(
                last_episode.air_date, source_names, provider_ids
            )
        next_episode = getattr(self, "next_episode_to_air", None)
        if next_episode:
            next_episode.air_datetime_utc = MCEpisodeSummary._compute_air_datetime_utc(
                next_episode.air_date, source_names, provider_ids
            )

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
        genres = [genre.name for genre in item.genres]

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
            genres=genres,
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
            last_episode_to_air=MCEpisodeSummary.from_tmdb(episode=item.last_episode_to_air),
            next_episode_to_air=MCEpisodeSummary.from_tmdb(episode=item.next_episode_to_air),
        )
        return media_item


def compute_series_status(
    tmdb_status: str | None,
    next_episode_to_air: TMDBEpisodeSummary | MCEpisodeSummary | None,
    last_episode_to_air: TMDBEpisodeSummary | MCEpisodeSummary | None,
) -> SeriesStatus:
    next_episode_exists = next_episode_to_air is not None
    if next_episode_exists:
        next_episode_number = next_episode_to_air.episode_number
    else:
        next_episode_number = None

    if last_episode_to_air is not None:
        last_episode_type = last_episode_to_air.episode_type
    else:
        last_episode_type = None
    """Classify series state using MediaCircle status precedence rules."""
    normalized_status = (tmdb_status or "").strip().lower()
    if normalized_status in {"canceled", "cancelled"}:
        return "over"
    if (last_episode_type or "").strip().lower() == "finale" and next_episode_exists:
        return "catch_up"
    if next_episode_exists and next_episode_number == 1:
        return "new_season"
    if next_episode_exists:
        return "active"
    return "binge"


class MCTvLifecycleEnrichment(BaseModelWithMethods):
    """Lifecycle metadata for TV series progression and episode availability."""

    series_status: SeriesStatus = "binge"
    series_completed: bool = False
    next_episode_air_date: str | None = None
    next_episode_number: int | None = None
    next_episode_season: int | None = None
    last_episode_air_date: str | None = None
    last_episode_number: int | None = None
    last_episode_season: int | None = None
    num_seasons_released: int = 0
    num_episodes_released: int = 0
    runtime: int | None = None

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        """Parse YYYY-MM-DD values returned by TMDB."""
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None

    @classmethod
    def _compute_series_status(
        cls,
        tmdb_status: str | None,
        next_episode_exists: bool,
        next_episode_number: int | None,
        last_episode_type: str | None,
    ) -> SeriesStatus:
        """Classify series state using MediaCircle status precedence rules."""
        normalized_status = (tmdb_status or "").strip().lower()
        if normalized_status in {"canceled", "cancelled"}:
            return "over"
        if (last_episode_type or "").strip().lower() == "finale" and next_episode_exists:
            return "catch_up"
        if next_episode_exists and next_episode_number == 1:
            return "new_season"
        if next_episode_exists:
            return "active"
        return "binge"

    @classmethod
    def _compute_released_counts(cls, details: "TMDBTvDetailsResult") -> tuple[int, int]:
        """Estimate released seasons/episodes using aired-season metadata."""
        last_episode = details.last_episode_to_air
        valid_seasons = [season for season in details.seasons if season.season_number > 0]

        if last_episode and last_episode.season_number and last_episode.episode_number:
            released_season_count = max(0, last_episode.season_number)
            previous_episodes = sum(
                max(0, season.episode_count)
                for season in valid_seasons
                if season.season_number < released_season_count
            )
            released_episodes = previous_episodes + max(0, last_episode.episode_number)
            return released_season_count, released_episodes

        today = date.today()
        released_seasons = []
        for season in valid_seasons:
            season_air_date = cls._parse_date(season.air_date)
            if season_air_date is None or season_air_date <= today:
                released_seasons.append(season)
        released_episodes = sum(max(0, season.episode_count) for season in released_seasons)
        return len(released_seasons), released_episodes

    @staticmethod
    def _is_valid_runtime(value: int | None) -> bool:
        """Validate runtime candidates from TMDB payloads."""
        return isinstance(value, int) and value > 0

    @classmethod
    def _estimate_runtime(cls, details: "TMDBTvDetailsResult") -> int | None:
        """
        Estimate per-episode runtime using all available TMDB TV detail signals.

        Source priority:
        1) show-level `episode_run_time` values
        2) `last_episode_to_air.runtime`
        3) `next_episode_to_air.runtime`
        """
        candidates: list[int] = []

        for runtime in details.episode_run_time:
            if cls._is_valid_runtime(runtime):
                candidates.append(runtime)

        last_episode_runtime = (
            details.last_episode_to_air.runtime if details.last_episode_to_air else None
        )
        next_episode_runtime = (
            details.next_episode_to_air.runtime if details.next_episode_to_air else None
        )

        if isinstance(last_episode_runtime, int) and last_episode_runtime > 0:
            candidates.append(last_episode_runtime)
        if isinstance(next_episode_runtime, int) and next_episode_runtime > 0:
            candidates.append(next_episode_runtime)

        if not candidates:
            return None

        # Median guards against outliers like specials or double-length premieres.
        sorted_candidates = sorted(candidates)
        middle_index = len(sorted_candidates) // 2
        if len(sorted_candidates) % 2 == 1:
            return sorted_candidates[middle_index]
        return round((sorted_candidates[middle_index - 1] + sorted_candidates[middle_index]) / 2)

    @classmethod
    def from_tv_details(
        cls,
        details: "TMDBTvDetailsResult",
    ) -> "MCTvLifecycleEnrichment":
        """Build lifecycle metadata from TMDB TV details."""
        next_episode = details.next_episode_to_air
        last_episode = details.last_episode_to_air
        runtime = cls._estimate_runtime(details)

        released_seasons, released_episodes = cls._compute_released_counts(details)

        series_status = cls._compute_series_status(
            tmdb_status=details.status,
            next_episode_exists=next_episode is not None,
            next_episode_number=next_episode.episode_number if next_episode else None,
            last_episode_type=last_episode.episode_type if last_episode else None,
        )
        series_completed = (details.status or "").strip().lower() == "ended"

        return cls(
            series_status=series_status,
            series_completed=series_completed,
            next_episode_air_date=next_episode.air_date if next_episode else None,
            next_episode_number=next_episode.episode_number if next_episode else None,
            next_episode_season=next_episode.season_number if next_episode else None,
            last_episode_air_date=last_episode.air_date if last_episode else None,
            last_episode_number=last_episode.episode_number if last_episode else None,
            last_episode_season=last_episode.season_number if last_episode else None,
            num_seasons_released=released_seasons,
            num_episodes_released=released_episodes,
            runtime=runtime,
        )

    @classmethod
    def from_batch_item(cls, item: "MCTvBatchEnrichmentItem") -> "MCTvLifecycleEnrichment | None":
        """Map batch-enrichment output into the nested TV lifecycle payload."""
        if item.error:
            return None
        return cls(
            series_status=item.series_status,
            series_completed=item.series_completed,
            next_episode_air_date=item.next_episode_air_date,
            next_episode_number=item.next_episode_number,
            next_episode_season=item.next_episode_season,
            last_episode_air_date=item.last_episode_air_date,
            last_episode_number=item.last_episode_number,
            last_episode_season=item.last_episode_season,
            num_seasons_released=item.num_seasons_released,
            num_episodes_released=item.num_episodes_released,
            runtime=item.runtime,
        )


class MCTvSeasonRuntimeEpisode(BaseModelWithMethods):
    """Per-episode runtime payload for a TV season."""

    episode_id: int
    episode_number: int
    name: str
    overview: str | None = None
    image: str | None = None
    air_date: str | None = None
    runtime: int | None = None


class MCTvSeasonRuntimeResponse(BaseModelWithMethods):
    """TV season runtime summary payload."""

    tmdb_id: int
    season_number: int
    num_episodes: int = 0
    avg_runtime: int | None = None
    cume_runtime: int = 0
    episodes: list[MCTvSeasonRuntimeEpisode] = Field(default_factory=list)
    data_source: str = "TMDB TV Season Runtime"
    error: str | None = None
    status_code: int = 200


class MCTvBatchEnrichmentRequestItem(BaseModelWithMethods):
    """Request item for the TV batch enrichment endpoint."""

    mc_id: str
    mc_source_id: str

    def tmdb_id(self) -> int | None:
        """Return TMDB ID parsed from mc_source_id."""
        try:
            return int(self.mc_source_id)
        except (TypeError, ValueError):
            return None


class MCTvBatchEnrichmentRequest(BaseModelWithMethods):
    """Payload model for TV batch enrichment requests."""

    items: list[MCTvBatchEnrichmentRequestItem] = Field(default_factory=list)


class MCTvBatchEnrichmentItem(BaseModelWithMethods):
    """Lightweight TV metadata used to classify release lifecycle."""

    mc_id: str
    mc_source_id: str
    series_status: SeriesStatus = "binge"
    series_completed: bool = False
    next_episode_air_date: str | None = None
    next_episode_number: int | None = None
    next_episode_season: int | None = None
    last_episode_air_date: str | None = None
    last_episode_number: int | None = None
    last_episode_season: int | None = None
    num_seasons_released: int = 0
    num_episodes_released: int = 0
    runtime: int | None = None
    error: str | None = None

    @classmethod
    def from_tv_details(
        cls,
        request_item: MCTvBatchEnrichmentRequestItem,
        details: "TMDBTvDetailsResult",
    ) -> "MCTvBatchEnrichmentItem":
        """Create a lightweight enrichment item from TMDB TV details."""
        lifecycle = MCTvLifecycleEnrichment.from_tv_details(details)

        return cls(
            mc_id=request_item.mc_id,
            mc_source_id=request_item.mc_source_id,
            series_status=lifecycle.series_status,
            series_completed=lifecycle.series_completed,
            next_episode_air_date=lifecycle.next_episode_air_date,
            next_episode_number=lifecycle.next_episode_number,
            next_episode_season=lifecycle.next_episode_season,
            last_episode_air_date=lifecycle.last_episode_air_date,
            last_episode_number=lifecycle.last_episode_number,
            last_episode_season=lifecycle.last_episode_season,
            num_seasons_released=lifecycle.num_seasons_released,
            num_episodes_released=lifecycle.num_episodes_released,
            runtime=lifecycle.runtime,
        )

    @classmethod
    def from_error(
        cls, request_item: MCTvBatchEnrichmentRequestItem, error: str
    ) -> "MCTvBatchEnrichmentItem":
        """Create an enrichment item for failed per-item fetches."""
        return cls(
            mc_id=request_item.mc_id,
            mc_source_id=request_item.mc_source_id,
            error=error,
        )


class MCTvBatchEnrichmentResponse(BaseModelWithMethods):
    """Response model for TV batch enrichment endpoint."""

    items: list[MCTvBatchEnrichmentItem] = Field(default_factory=list)
    total_results: int = 0
    data_source: str = "TMDB TV batch enrichment"
    error: str | None = None
    status_code: int = 200


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


class MCPersonItem(MCBaseItem):
    """Model for TMDB person/actor data."""

    mc_type: MCType = MCType.PERSON
    source: MCSources = MCSources.TMDB

    adult: bool = False
    id: int
    name: str
    gender: int | None = None
    biography: str = ""
    birthday: str | None = None
    deathday: str | None = None
    place_of_birth: str | None = None
    known_for_department: str | None = None
    popularity: float = 0.0
    also_known_as: list[str] = Field(default_factory=list)
    homepage: str | None = None
    profile_path: str | None = None
    profile_images: dict[str, str] | None = None
    has_image: bool = False
    known_for: list[dict[str, Any]] = Field(default_factory=list)

    # MediaCircle standardized fie

    @classmethod
    def from_search_person(
        cls, search_person: "TMDBSearchPersonItem", image_base_url: str | None = None
    ) -> "MCPersonItem":
        """Create an MCPersonItem from a TMDBSearchPersonItem.

        Args:
            search_person: The TMDBSearchPersonItem to convert
            image_base_url: Optional base URL for generating profile image URLs

        Returns:
            A new MCPersonItem instance with properties filled from the search person
        """
        # Build also_known_as list - include original_name if different from name
        also_known_as = []
        if search_person.original_name and search_person.original_name != search_person.name:
            also_known_as.append(search_person.original_name)

        # Generate profile images if profile_path exists and image_base_url is provided
        profile_images = None
        if search_person.profile_path and image_base_url:
            profile_images = {
                "small": f"{image_base_url}w45{search_person.profile_path}",
                "medium": f"{image_base_url}w185{search_person.profile_path}",
                "large": f"{image_base_url}h632{search_person.profile_path}",
                "original": f"{image_base_url}original{search_person.profile_path}",
            }

        images = []
        if profile_images and search_person.profile_path:
            images = [MCImage(url=search_person.profile_path, type=MCUrlType.URL, key="small")]
            images.append(MCImage(url=profile_images["medium"], type=MCUrlType.URL, key="medium"))
            images.append(MCImage(url=profile_images["large"], type=MCUrlType.URL, key="large"))
            images.append(
                MCImage(url=profile_images["original"], type=MCUrlType.URL, key="original")
            )

        # Create the MCPersonItem instance
        person_item = cls(
            adult=search_person.adult,
            id=search_person.id,
            source_id=str(search_person.id),  # Convert to string to match MCBaseItem type
            name=search_person.name,
            gender=search_person.gender,
            known_for_department=search_person.known_for_department,
            popularity=search_person.popularity,
            also_known_as=also_known_as,
            images=images,
            profile_path=search_person.profile_path,
            profile_images=profile_images,
            has_image=search_person.profile_path is not None,
            known_for=search_person.known_for or [],
        )

        # Generate MediaCircle standardized fields
        person_item.mc_id = generate_mc_id({"id": search_person.id}, MCType.PERSON)
        person_item.mc_type = MCType.PERSON
        if person_item.known_for_department:
            if person_item.known_for_department == "Acting":
                person_item.mc_subtype = MCSubType.ACTOR
            elif person_item.known_for_department == "Writing":
                person_item.mc_subtype = MCSubType.WRITER
            elif person_item.known_for_department == "Directing":
                person_item.mc_subtype = MCSubType.DIRECTOR
            elif person_item.known_for_department == "Producing":
                person_item.mc_subtype = MCSubType.PRODUCER
            else:
                person_item.mc_subtype = MCSubType.PERSON
        person_item.source = MCSources.TMDB
        person_item.source_id = str(search_person.id)  # Convert to string to match MCBaseItem type
        return person_item

    @classmethod
    def from_person_details(
        cls, person_details: "TMDBPersonDetailsResult", image_base_url: str | None = None
    ) -> "MCPersonItem":
        """Create an MCPersonItem from a TMDBSearchPersonItem.

        Args:
            person_details: The TMDBPersonDetailsResult to convert
            image_base_url: Optional base URL for generating profile image URLs

        Returns:
            A new MCPersonItem instance with properties filled from the search person
        """
        # Build also_known_as list - include original_name if different from name

        # Generate profile images if profile_path exists and image_base_url is provided
        profile_images = None
        if person_details.profile_path and image_base_url:
            profile_images = {
                "small": f"{image_base_url}w45{person_details.profile_path}",
                "medium": f"{image_base_url}w185{person_details.profile_path}",
                "large": f"{image_base_url}h632{person_details.profile_path}",
                "original": f"{image_base_url}original{person_details.profile_path}",
            }

        images = []
        if profile_images and person_details.profile_path:
            images = [MCImage(url=person_details.profile_path, type=MCUrlType.URL, key="small")]
            images.append(MCImage(url=profile_images["medium"], type=MCUrlType.URL, key="medium"))
            images.append(MCImage(url=profile_images["large"], type=MCUrlType.URL, key="large"))
            images.append(
                MCImage(url=profile_images["original"], type=MCUrlType.URL, key="original")
            )
        # Create the MCPersonItem instance
        person_item = cls(
            adult=person_details.adult,
            also_known_as=person_details.also_known_as,
            id=person_details.id,
            source_id=str(person_details.id),
            name=person_details.name,
            gender=person_details.gender,
            biography=person_details.biography or "",
            birthday=person_details.birthday,
            deathday=person_details.deathday,
            place_of_birth=person_details.place_of_birth,
            known_for_department=person_details.known_for_department,
            popularity=person_details.popularity,
            homepage=person_details.homepage,
            external_ids={"imdb_id": person_details.imdb_id},
            profile_path=person_details.profile_path,
            profile_images=profile_images,
            images=images,
            has_image=person_details.profile_path is not None,
        )

        # Generate MediaCircle standardized fields
        person_item.mc_id = generate_mc_id({"id": person_details.id}, MCType.PERSON)
        person_item.mc_type = MCType.PERSON
        if person_details.known_for_department:
            if person_details.known_for_department == "Acting":
                person_item.mc_subtype = MCSubType.ACTOR
            elif person_details.known_for_department == "Writing":
                person_item.mc_subtype = MCSubType.WRITER
            elif person_details.known_for_department == "Directing":
                person_item.mc_subtype = MCSubType.DIRECTOR
            elif person_details.known_for_department == "Producing":
                person_item.mc_subtype = MCSubType.PRODUCER
            else:
                person_item.mc_subtype = MCSubType.PERSON
        person_item.source = MCSources.TMDB
        person_item.source_id = str(person_details.id)
        return person_item


class MCKeywordItem(MCBaseItem):
    """Model for TMDB keyword item."""

    id: int
    name: str
    mc_type: MCType = MCType.KEYWORD


"""
Seach Response Models
"""


class MCKeywordSearchResponse(MCSearchResponse):
    """Model for TMDB keyword search response."""

    results: list[MCKeywordItem]  # type: ignore[assignment]
    total_pages: int


class MCGenreItem(MCBaseItem):
    """Model for TMDB keyword item."""

    id: int
    name: str
    mc_type: MCType = MCType.GENRE


"""
Genre Response Models
"""


class MCGenreSearchResponse(MCSearchResponse):
    """Model for TMDB keyword search response."""

    results: list[MCGenreItem]  # type: ignore[assignment]
    total_pages: int


class MCDiscoverResponse(MCSearchResponse):
    results: list[MCMovieItem | MCTvItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MOVIE


class MCNowPlayingResponse(MCSearchResponse):
    results: list[MCMovieItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MOVIE


class MCPopularTVResponse(MCSearchResponse):
    """Model for TMDB popular TV response."""

    results: list[MCTvItem] = Field(default_factory=list)  # type: ignore[assignment]
    data_type: MCType = MCType.TV_SERIES


class MCSearchPersonResponse(MCSearchResponse):
    """Model for TMDB popular TV response."""

    results: list[MCPersonItem]  # type: ignore[assignment]
    data_type: MCType = MCType.PERSON


class MCPersonCreditsResult(BaseModelWithMethods):
    """Model for TMDB cast details response (person + credits).

    Results are stored as dicts for JSON serialization compatibility.
    Note: person is Optional to allow partial results from get_person_movie_credits
    and get_person_tv_credits methods.
    """

    person: MCPersonItem | None = None
    movies: list[MCMovieItem | MCMovieCreditMediaItem] = Field(default_factory=list)
    tv_shows: list[MCTvItem | MCTvCreditMediaItem] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MCGetTrendingShowResult(MCSearchResponse):
    """Model for Last.fm artist search response."""

    results: list[MCTvItem]  # type: ignore[assignment]
    data_type: MCType = MCType.TV_SERIES


class MCGetTrendingMovieResult(MCSearchResponse):
    """Model for TMDB trending movie response."""

    results: list[MCMovieItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MOVIE


class MCPersonDetailsResponse(MCSearchResponse):
    """Model for TMDB person details response."""

    results: list[MCPersonItem] = Field(default_factory=list)  # type: ignore[assignment]
    data_type: MCType = MCType.PERSON


class MCPersonCreditsResponse(MCSearchResponse):
    """Model for TMDB person credits response."""

    results: list[MCPersonCreditsResult]  # type: ignore[assignment]
    data_type: MCType = MCType.CREDITS


class TMDBSearchMultiResponse(MCSearchResponse):
    """Model for TMDB multi search response (movies and TV shows)."""

    results: list[MCMovieItem | MCTvItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MIXED
    data_source: str = "TMDB Multi Search"


class TMDBSearchTVResponse(MCSearchResponse):
    """Model for TMDB TV show search response."""

    results: list[MCTvItem]  # type: ignore[assignment]
    data_type: MCType = MCType.TV_SERIES
    data_source: str = "TMDB TV Search"


class TMDBSearchMovieResponse(MCSearchResponse):
    """Model for TMDB movie search response."""

    results: list[MCMovieItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MOVIE
    data_source: str = "TMDB Movie Search"


class TMDBSearchGenreResponse(MCSearchResponse):
    """Model for TMDB genre search response."""

    results: list[MCMovieItem | MCTvItem]  # type: ignore[assignment]
    data_type: MCType = MCType.MIXED
    data_source: str = "TMDB Search by Genres"
