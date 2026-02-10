"""
Models - Consumer Models. These are models of data that will
be returned to the front end as part of fullfilling a request
Follows the same pattern as podcast_models.py with Pydantic 2.0
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import Field

from api.tmdb.tmdb_models import (
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
    airdate_time: datetime | None = None
    duration: int | None = None

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
