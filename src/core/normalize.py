"""
Normalization layer for TMDB data sources.

This module provides a consistent interface for transforming data from
TMDB into a unified document format suitable for Redis Search indexing.

Every normalizer MUST set mc_type and mc_subtype because the index
contains heterogeneous content types (movies, TV, people, etc.).

TAG values are normalized (lowercase, special chars replaced with underscore)
to ensure consistent filtering in Redis Search.
"""

import math
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, cast

from contracts.models import MCSources, MCSubType, MCType
from core.iptc import expand_keywords, normalize_tag

# Regex to strip apostrophes (straight + curly) from titles for search indexing.
# RediSearch tokenizes apostrophes as word separators, so "It's" becomes ["it", "s"].
# Stripping them produces "Its" which tokenizes as ["its"] — matching user queries.
_APOSTROPHE_RE = re.compile(r"[\u0027\u2018\u2019\u02BC]")  # ' ' ' ʼ


def normalize_search_title(title: str) -> str:
    """
    Normalize a title for RediSearch TEXT field indexing.

    Strips apostrophes so that possessives and contractions become single tokens.
    Examples:
        "It's Complicated" -> "Its Complicated"
        "Schindler's List" -> "Schindlers List"
        "Don't Look Up"   -> "Dont Look Up"

    Args:
        title: The original title string.

    Returns:
        Title with apostrophes removed, suitable for search indexing.
    """
    return _APOSTROPHE_RE.sub("", title)


@dataclass
class SearchDocument:
    """
    Unified document structure for search indexing.
    All data sources must normalize to this format.

    MCType and MCSubType are required for proper categorization
    in the heterogeneous search index.

    TAG fields are normalized (lowercase, special chars -> underscore).
    """

    # Indexed fields
    id: str  # Unique identifier (e.g., "tmdb_12345")
    search_title: str  # Primary searchable title
    mc_type: MCType  # Content type (movie, tv, book, etc.)
    mc_subtype: MCSubType | None  # Subtype (actor, director, author, etc.)
    source: MCSources  # Data source identifier
    source_id: str  # Original ID from source (e.g., "12345" for TMDB)
    # Sortable/filterable fields
    year: int | None  # Release/publish year
    popularity: float  # Normalized MC popularity score (0-100)
    rating: float  # Rating score (0-10)
    # Display fields (stored, not indexed)
    image: str | None  # Medium poster/profile image URL
    overview: str | None  # Description/bio
    # Genre fields (indexed as TagFields - normalized)
    genre_ids: list[str]  # TMDB genre IDs as strings (e.g., ["35", "18", "10751"])
    genres: list[str]  # Genre names normalized (e.g., ["comedy", "drama", "family"])
    # Cast fields (indexed as TagFields - normalized)
    cast_ids: list[str]  # TMDB person IDs as strings for cast members
    cast_names: list[str]  # Cast member names normalized for search/filter
    cast: list[str]  # Cast member names for display (NOT normalized)
    # Director object (indexed via JSONPath into $.director.id, $.director.name_normalized)
    director: dict[str, str] | None = None
    # Keywords (indexed as TagFields - IPTC expanded and normalized)
    keywords: list[str] = field(default_factory=list)
    # Origin country (indexed as TagFields - normalized ISO codes)
    origin_country: list[str] = field(default_factory=list)
    original_language: str | None = None  # Original language for the media item
    original_title: str | None = None  # Original title for the media item
    # Date fields (stored, not indexed)
    release_date: str | None = None  # YYYY-MM-DD, movies only
    first_air_date: str | None = None  # YYYY-MM-DD, TV only
    last_air_date: str | None = None  # YYYY-MM-DD, TV only
    # Content rating and streaming providers
    us_rating: str | None = None  # e.g. "R", "PG-13", "TV-MA"
    watch_providers: dict[str, Any] | None = None
    # Document lifecycle timestamps
    created_at: int | None = None  # Unix seconds, set on first index
    modified_at: int | None = None  # Unix seconds, updated on every write
    _source: str | None = None  # Write provenance (e.g. "backfill")
    # Person-specific fields (for people index)
    also_known_as: str | None = None  # Pipe-separated alternate names for search
    status: str | None = None  # TV show status
    series_status: str | None = None  # TV show series status
    tagline: str | None = None  # Tagline for the media item
    vote_count: int | None = None  # Vote count for the media item
    vote_average: float | None = None  # Vote average for the media item
    popularity_tmdb: float | None = None  # TMDB Popularity score for the media item
    runtime: int | None = None  # Runtime for the media item
    number_of_seasons: int | None = None  # Number of seasons for the media item
    number_of_episodes: int | None = None  # Number of episodes for the media item
    created_by: list[str] | None = None  # List of creators for the media item
    created_by_ids: list[int] | None = None  # List of creator IDs for the media item
    networks: list[str] | None = None  # List of networks for the media item
    network: str | None = None  # Normalized from TMDB networks array (first network name)
    production_companies: list[str] | None = None  # List of production companies for the media item
    production_countries: list[str] | None = None  # List of production countries for the media item
    budget: int | None = None  # Budget for the media item
    revenue: int | None = None  # Revenue for the media item
    spoken_languages: list[str] | None = None  # List of spoken languages for the media item


class BaseNormalizer(ABC):
    """Abstract base class for data source normalizers."""

    @property
    @abstractmethod
    def source(self) -> MCSources:
        """Return the source identifier for this data source."""

    @property
    @abstractmethod
    def mc_type(self) -> MCType:
        """Return the MCType this normalizer produces."""

    @property
    def mc_subtype(self) -> MCSubType | None:
        """Return the MCSubType this normalizer produces. Override for subtypes."""
        return None

    @abstractmethod
    def normalize(
        self, raw: dict, genre_mapping: dict[int, str] | None = None
    ) -> SearchDocument | None:
        """
        Transform raw data into a SearchDocument.

        Args:
            raw: Raw data to normalize
            genre_mapping: Optional genre ID to name mapping

        Returns None if the data cannot be normalized.
        """

    @abstractmethod
    def extract_id(self, raw: dict) -> str | None:
        """Extract the unique ID from raw data."""


class BaseTMDBNormalizer(BaseNormalizer):
    """Base normalizer for TMDB (The Movie Database) data."""

    @property
    def source(self) -> MCSources:
        return MCSources.TMDB

    def extract_source_id(self, raw: dict) -> str | None:
        """Extract the numeric TMDB ID from raw data."""
        tmdb_id = raw.get("tmdb_id") or raw.get("id")

        if isinstance(tmdb_id, str):
            for prefix in ("tmdb_movie_", "tmdb_tv_", "tmdb_person_", "tmdb_"):
                if tmdb_id.startswith(prefix):
                    tmdb_id = tmdb_id[len(prefix):]
                    break

        return str(tmdb_id) if tmdb_id else None

    def extract_id(self, raw: dict) -> str | None:
        """Extract the canonical TMDB identifier from raw data."""
        source_id = self.extract_source_id(raw)
        if source_id:
            if self.mc_type in (MCType.MOVIE, MCType.TV_SERIES):
                return f"tmdb_{source_id}"
            return f"tmdb_person_{source_id}"
        return None

    def _extract_title(self, raw: dict) -> str:
        """Extract the best title from TMDB data."""
        return (
            raw.get("title")
            or raw.get("name")
            or raw.get("original_title")
            or raw.get("original_name")
            or ""
        )

    def _extract_year(self, raw: dict) -> int | None:
        """Extract release/air year from TMDB data."""
        date_str = raw.get("release_date") or raw.get("first_air_date")
        if date_str and len(date_str) >= 4:
            try:
                return int(date_str[:4])
            except ValueError:
                pass
        return None

    def _compute_popularity(self, raw: dict) -> float:
        """
        Compute a normalized popularity score (0-100).

        Combines multiple signals:
        - TMDB popularity score
        - Vote count (more votes = more popular)
        - Vote average (quality signal)
        """
        metrics = raw.get("metrics", {})

        # Get raw values
        tmdb_popularity = metrics.get("popularity") or raw.get("popularity") or 0
        vote_count = metrics.get("vote_count") or raw.get("vote_count") or 0
        vote_average = metrics.get("vote_average") or raw.get("vote_average") or 0

        # Normalize each component to 0-100 scale
        # TMDB popularity is typically 0-1000+ for popular content
        popularity_score = min(tmdb_popularity / 10, 100)

        # Vote count: log scale, cap at 10000 votes = 100
        vote_count_score = min(math.log10(vote_count + 1) * 25, 100) if vote_count > 0 else 0

        # Vote average is 0-10, multiply by 10
        rating_score = vote_average * 10

        # Weighted combination
        combined = (
            popularity_score * 0.50  # TMDB popularity (50%)
            + vote_count_score * 0.30  # Vote count (30%)
            + rating_score * 0.20  # Rating (20%)
        )

        return round(combined, 2)

    def _extract_rating(self, raw: dict) -> float:
        """Extract rating score (0-10)."""
        metrics = raw.get("metrics", {})
        return metrics.get("vote_average") or raw.get("vote_average") or 0.0

    def _extract_image(self, raw: dict) -> str | None:
        """Extract medium poster/profile image URL."""
        images = raw.get("images", [])
        for img in images:
            if img.get("key") == "medium" and img.get("description") == "poster":
                url: str | None = img.get("url")
                return url
        # Fallback: check for profile images (for persons)
        for img in images:
            if img.get("key") == "medium" and img.get("description") == "profile":
                url = img.get("url")
                return url
        return None

    def _extract_genre_ids(self, raw: dict) -> list[str]:
        """Extract genre IDs from TMDB data as strings for Redis TagField."""
        genre_ids = raw.get("genre_ids", [])
        return [str(gid) for gid in genre_ids if isinstance(gid, int)]

    @staticmethod
    def _normalize_id_array(values: Any) -> list[str]:
        """
        Normalize ID arrays to string lists for TagField compatibility.

        RediSearch TagField only accepts strings or nulls; integers must be converted.
        """
        if values is None or isinstance(values, bool):
            return []
        if isinstance(values, int):
            return [str(values)]
        if isinstance(values, str):
            normalized = values.strip()
            return [normalized] if normalized else []
        if not isinstance(values, list):
            return []

        ids: list[str] = []
        for raw_value in values:
            ids.extend(BaseTMDBNormalizer._normalize_id_array(raw_value))
        return ids

    def _normalize_watch_providers(self, watch_providers: Any) -> dict[str, Any] | None:
        """
        Normalize watch provider ID arrays for Redis TagField compatibility.

        Keeps watch provider payload shape stable while converting only provider ID arrays.
        """
        if not isinstance(watch_providers, dict):
            return None

        normalized_watch_providers = dict(watch_providers)
        streaming_ids = watch_providers.get("streaming_platform_ids")
        if streaming_ids is not None:
            normalized_watch_providers["streaming_platform_ids"] = self._normalize_id_array(streaming_ids)

        on_demand_ids = watch_providers.get("on_demand_platform_ids")
        if on_demand_ids is not None:
            normalized_watch_providers["on_demand_platform_ids"] = self._normalize_id_array(on_demand_ids)

        return normalized_watch_providers

    def _extract_genres(self, raw: dict, genre_mapping: dict[int, str] | None = None) -> list[str]:
        """
        Extract and normalize genre names from TMDB data.

        If genre_mapping is provided, resolves genre_ids to names.
        Otherwise, uses the 'genres' field if available.

        All genre names are normalized (lowercase, special chars -> underscore).
        """
        names: list[str] = []

        # Try direct genres field first (may already have names)
        genres = raw.get("genres", [])
        if genres:
            # Handle both formats: list of dicts with 'name' or list of strings
            for g in genres:
                if isinstance(g, dict) and g.get("name"):
                    names.append(g["name"])
                elif isinstance(g, str):
                    names.append(g)

        # Fall back to resolving genre_ids if mapping provided and no names found
        if not names and genre_mapping:
            genre_ids = self._extract_genre_ids(raw)
            # genre_ids are strings, convert to int for mapping lookup
            names = [genre_mapping[int(gid)] for gid in genre_ids if int(gid) in genre_mapping]

        # Normalize all genre names for TAG indexing
        return [normalize_tag(name) for name in names if normalize_tag(name)]

    def _extract_cast_data(
        self, raw: dict, limit: int = 5
    ) -> tuple[list[str], list[str], list[str]]:
        """
        Extract cast data from TMDB data.

        Returns:
            Tuple of (cast_ids, cast_names, cast)
            - cast_ids: List of TMDB person IDs as strings (for Redis TagField)
            - cast_names: List of actor names normalized (for Redis TagField filtering)
            - cast: List of actor names for display (NOT normalized)
        """
        cast_ids: list[str] = []
        cast_names: list[str] = []  # Normalized for TAG indexing
        cast_display: list[str] = []  # Original for display

        # Try main_cast first, then tmdb_cast.cast
        main_cast = raw.get("main_cast", [])
        if not main_cast:
            tmdb_cast = raw.get("tmdb_cast", {})
            if isinstance(tmdb_cast, dict):
                main_cast = tmdb_cast.get("cast", [])

        for actor in main_cast[:limit]:
            if not actor.get("name"):
                continue

            actor_id = actor.get("id")
            actor_name = actor.get("name")

            if actor_id:
                cast_ids.append(str(actor_id))  # Convert to string for Redis TagField
            if actor_name:
                normalized_name = normalize_tag(actor_name)
                if normalized_name:
                    cast_names.append(normalized_name)
                cast_display.append(actor_name)  # Keep original for display

        return cast_ids, cast_names, cast_display

    def _extract_director(self, raw: dict) -> dict[str, str] | None:
        """
        Extract director data from TMDB data as a structured object.

        Returns:
            Dict with id, name, name_normalized — or None when absent.
        """
        director_raw: dict[str, Any] | None = None

        # In MCBaseMediaItem / get_media_details, director is exposed as top-level 'director'
        # Check this first.
        candidate = raw.get("director", {})
        if isinstance(candidate, dict) and candidate.get("id"):
            director_raw = candidate

        # Fallback to legacy tmdb_cast nested object
        if not director_raw:
            tmdb_cast = raw.get("tmdb_cast", {})
            if isinstance(tmdb_cast, dict):
                candidate = tmdb_cast.get("director", {})
                if isinstance(candidate, dict) and candidate.get("id"):
                    director_raw = candidate

        if director_raw is None:
            return None

        name = director_raw.get("name", "")
        return {
            "id": str(director_raw["id"]),
            "name": name,
            "name_normalized": normalize_tag(name) or "",
        }

    def _extract_dates(self, raw: dict, mc_type: MCType) -> dict[str, str | None]:
        """Extract date fields based on content type."""
        if mc_type == MCType.MOVIE:
            return {
                "release_date": raw.get("release_date") or None,
                "first_air_date": None,
                "last_air_date": None,
            }
        if mc_type == MCType.TV_SERIES:
            return {
                "release_date": None,
                "first_air_date": raw.get("first_air_date") or None,
                "last_air_date": raw.get("last_air_date") or None,
            }
        return {"release_date": None, "first_air_date": None, "last_air_date": None}

    def _extract_origin_country(self, raw: dict) -> list[str]:
        """
        Extract and normalize origin country codes from TMDB data.

        Returns:
            List of normalized ISO country codes (e.g., ["us"], ["kr", "jp"])
        """
        # Try origin_country (TV shows)
        countries = raw.get("origin_country", [])

        # Try production_countries (movies)
        if not countries:
            prod_countries = raw.get("production_countries", [])
            if prod_countries:
                countries = [c.get("iso_3166_1", "") for c in prod_countries if isinstance(c, dict)]

        # Normalize country codes (lowercase)
        return [normalize_tag(c) for c in countries if c and normalize_tag(c)]

    def _extract_keywords(self, raw: dict) -> list[str]:
        """
        Extract keywords from TMDB data and expand using IPTC aliases.

        Returns:
            List of normalized, IPTC-expanded keywords.
        """
        keywords = raw.get("keywords", [])
        if not keywords:
            return []

        return cast(list[str], expand_keywords(keywords))

    def _build_profile_url(self, profile_path: str | None) -> str | None:
        """Build full profile image URL from TMDB path."""
        if not profile_path:
            return None
        return f"https://image.tmdb.org/t/p/w185{profile_path}"

    def _extract_overview(self, raw: dict) -> str | None:
        """Extract overview/description."""
        overview = raw.get("overview") or raw.get("biography") or ""
        return overview if overview else None


class TMDBMovieNormalizer(BaseTMDBNormalizer):
    """Normalizer for TMDB movie data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.MOVIE

    def normalize(
        self, raw: dict, genre_mapping: dict[int, str] | None = None
    ) -> SearchDocument | None:
        """Transform TMDB movie data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        source_id = self.extract_source_id(raw)
        if not doc_id or not source_id:
            return None

        title = self._extract_title(raw)
        if not title:
            return None

        cast_ids, cast_names, cast_display = self._extract_cast_data(raw)
        director = self._extract_director(raw)
        dates = self._extract_dates(raw, self.mc_type)
        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            source=self.source,
            source_id=source_id,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            vote_count=raw.get("vote_count"),
            vote_average=raw.get("vote_average"),
            popularity_tmdb=raw.get("popularity"),
            rating=self._extract_rating(raw),
            image=self._extract_image(raw),
            overview=self._extract_overview(raw),
            genre_ids=self._extract_genre_ids(raw),
            genres=self._extract_genres(raw, genre_mapping),
            cast_ids=cast_ids,
            cast_names=cast_names,
            cast=cast_display,
            director=director,
            keywords=self._extract_keywords(raw),
            origin_country=self._extract_origin_country(raw),
            original_title=raw.get("original_title"),
            original_language=raw.get("original_language"),
            release_date=dates["release_date"],
            first_air_date=dates["first_air_date"],
            last_air_date=dates["last_air_date"],
            us_rating=raw.get("us_rating"),
            watch_providers=self._normalize_watch_providers(raw.get("watch_providers")),
            status=raw.get("status"),
            tagline=raw.get("tagline"),
            production_companies=raw.get("production_companies"),
            production_countries=raw.get("production_countries"),
            budget=raw.get("budget"),
            revenue=raw.get("revenue"),
            spoken_languages=raw.get("spoken_languages"),
            runtime=raw.get("runtime"),
        )


class TMDBTvNormalizer(BaseTMDBNormalizer):
    """Normalizer for TMDB TV series data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.TV_SERIES

    def normalize(
        self, raw: dict, genre_mapping: dict[int, str] | None = None
    ) -> SearchDocument | None:
        """Transform TMDB TV data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        source_id = self.extract_source_id(raw)
        if not doc_id or not source_id:
            return None

        title = self._extract_title(raw)
        if not title:
            return None

        cast_ids, cast_names, cast_display = self._extract_cast_data(raw)
        dates = self._extract_dates(raw, self.mc_type)
        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            source=self.source,
            source_id=source_id,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            vote_count=raw.get("vote_count"),
            vote_average=raw.get("vote_average"),
            popularity_tmdb=raw.get("popularity"),
            rating=self._extract_rating(raw),
            image=self._extract_image(raw),
            overview=self._extract_overview(raw),
            genre_ids=self._extract_genre_ids(raw),
            genres=self._extract_genres(raw, genre_mapping),
            cast_ids=cast_ids,
            cast_names=cast_names,
            cast=cast_display,
            director=None,
            keywords=self._extract_keywords(raw),
            origin_country=self._extract_origin_country(raw),
            original_language=raw.get("original_language"),
            original_title=raw.get("original_title"),
            release_date=dates["release_date"],
            first_air_date=dates["first_air_date"],
            last_air_date=dates["last_air_date"],
            us_rating=raw.get("us_rating"),
            watch_providers=self._normalize_watch_providers(raw.get("watch_providers")),
            status=raw.get("status"),
            series_status=raw.get("series_status"),
            number_of_seasons=raw.get("number_of_seasons"),
            number_of_episodes=raw.get("number_of_episodes"),
            created_by=raw.get("created_by"),
            created_by_ids=raw.get("created_by_ids"),
            tagline=raw.get("tagline"),
            networks=raw.get("networks"),
            network=raw.get("network"),
            production_companies=raw.get("production_companies"),
            production_countries=raw.get("production_countries"),
        )


class TMDBPersonNormalizer(BaseTMDBNormalizer):
    """Normalizer for TMDB person data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.PERSON

    def _detect_subtype(self, raw: dict) -> MCSubType | None:
        """Detect the person subtype based on their known_for_department."""
        department = raw.get("known_for_department", "").lower()
        if department == "acting":
            return MCSubType.ACTOR
        if department == "directing":
            return MCSubType.DIRECTOR
        if department == "writing":
            return MCSubType.WRITER
        if department == "production":
            return MCSubType.PRODUCER
        return MCSubType.PERSON

    def normalize(
        self, raw: dict, genre_mapping: dict[int, str] | None = None
    ) -> SearchDocument | None:
        """Transform TMDB person data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        source_id = self.extract_source_id(raw)
        if not doc_id or not source_id:
            return None

        name = raw.get("name") or ""
        if not name:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=name,
            mc_type=self.mc_type,
            mc_subtype=self._detect_subtype(raw),
            source=self.source,
            source_id=source_id,
            year=None,  # Persons don't have a year
            popularity=self._compute_popularity(raw),
            rating=0.0,  # Persons don't have ratings
            image=self._extract_image(raw),
            overview=self._extract_overview(raw),  # Uses biography
            # Persons don't have genres, cast, director, keywords, or origin_country
            genre_ids=[],
            genres=[],
            cast_ids=[],
            cast_names=[],
            cast=[],
            # New fields default to None/empty for persons
        )


# Registry of available normalizers by (source, mc_type)
NORMALIZERS: dict[tuple[MCSources, MCType], BaseNormalizer] = {
    (MCSources.TMDB, MCType.MOVIE): TMDBMovieNormalizer(),
    (MCSources.TMDB, MCType.TV_SERIES): TMDBTvNormalizer(),
    (MCSources.TMDB, MCType.PERSON): TMDBPersonNormalizer(),
}

# Legacy string-based lookup for backward compatibility
NORMALIZERS_BY_NAME: dict[str, BaseNormalizer] = {
    "tmdb_movie": TMDBMovieNormalizer(),
    "tmdb_tv": TMDBTvNormalizer(),
    "tmdb_person": TMDBPersonNormalizer(),
}


def get_normalizer(source: MCSources, mc_type: MCType) -> BaseNormalizer | None:
    """Get a normalizer by source and MCType."""
    return NORMALIZERS.get((source, mc_type))


def get_normalizer_by_name(name: str) -> BaseNormalizer | None:
    """Get a normalizer by name (e.g., 'tmdb_movie', 'tmdb_tv')."""
    return NORMALIZERS_BY_NAME.get(name.lower())


def detect_source_and_type(raw: dict) -> tuple[MCSources | None, MCType | None]:
    """
    Auto-detect the data source and MCType from raw data structure.
    Returns (source, mc_type) tuple or (None, None) if undetectable.
    """
    source: MCSources | None = None
    mc_type: MCType | None = None

    # Check for explicit mc_type in data
    if raw.get("mc_type"):
        mc_type_val = raw["mc_type"]
        if isinstance(mc_type_val, MCType):
            mc_type = mc_type_val
        elif isinstance(mc_type_val, str):
            try:
                mc_type = MCType(mc_type_val)
            except ValueError:
                pass

    # TMDB indicators
    if (
        raw.get("mc_id", "").startswith("tmdb_")
        or raw.get("tmdb_id")
        or raw.get("source") == "tmdb"
        or "poster_path" in raw
    ):
        source = MCSources.TMDB

    # Detect mc_type if not already set
    if mc_type is None:
        # Movie indicators
        if raw.get("release_date") or raw.get("media_type") == "movie":
            mc_type = MCType.MOVIE
        # TV indicators
        elif raw.get("first_air_date") or raw.get("media_type") == "tv":
            mc_type = MCType.TV_SERIES
        # Person indicators
        elif raw.get("known_for_department") or raw.get("media_type") == "person":
            mc_type = MCType.PERSON

    return source, mc_type


def normalize_document(
    raw: dict,
    source: MCSources | None = None,
    mc_type: MCType | None = None,
    genre_mapping: dict[int, str] | None = None,
) -> SearchDocument | None:
    """
    Normalize a raw document from any supported source.

    Args:
        raw: The raw document data
        source: Optional source hint. If not provided, will auto-detect.
        mc_type: Optional MCType hint. If not provided, will auto-detect.
        genre_mapping: Optional genre ID to name mapping for resolving genre names.

    Returns:
        SearchDocument if successful, None otherwise
    """
    if source is None or mc_type is None:
        detected_source, detected_type = detect_source_and_type(raw)
        if source is None:
            source = detected_source
        if mc_type is None:
            mc_type = detected_type

    if source is None or mc_type is None:
        return None

    normalizer = get_normalizer(source, mc_type)
    if normalizer is None:
        return None

    return normalizer.normalize(raw, genre_mapping=genre_mapping)


def document_to_redis(doc: SearchDocument) -> dict[str, Any]:
    """
    Convert a SearchDocument to the dict format stored in Redis.

    TAG fields are already normalized in SearchDocument.
    search_title is normalized (apostrophes stripped) for consistent tokenization.
    The original title is preserved in the 'title' field for display.
    """
    # Canonical identifier used by media docs and exact-match lookups.
    # For movie/tv this must be {source}_{source_id} (e.g. tmdb_550).
    media_mc_id = (
        f"{doc.source.value}_{doc.source_id}"
        if doc.mc_type in (MCType.MOVIE, MCType.TV_SERIES)
        else doc.id
    )
    result: dict[str, Any] = {
        "id": doc.id,
        "mc_id": media_mc_id,
        "title": doc.search_title,
        "search_title": normalize_search_title(doc.search_title),
        "mc_type": doc.mc_type.value,
        "mc_subtype": doc.mc_subtype.value if doc.mc_subtype else None,
        "source": doc.source.value,
        "source_id": doc.source_id,
        "year": doc.year,
        "popularity": doc.popularity,
        "rating": doc.rating,
        "image": doc.image,
        "overview": doc.overview,
        "genre_ids": doc.genre_ids,
        "genres": doc.genres,
        "cast_ids": doc.cast_ids,
        "cast_names": doc.cast_names,
        "cast": doc.cast,
        "director": doc.director,
        "keywords": doc.keywords,
        "origin_country": doc.origin_country,
        "release_date": doc.release_date,
        "first_air_date": doc.first_air_date,
        "last_air_date": doc.last_air_date,
        "us_rating": doc.us_rating,
        "watch_providers": doc.watch_providers,
        "status": doc.status,
        "series_status": doc.series_status,
        "tagline": doc.tagline,
        "vote_count": doc.vote_count,
        "vote_average": doc.vote_average,
        "popularity_tmdb": doc.popularity_tmdb,
        "runtime": doc.runtime,
        "original_language": doc.original_language,
        "original_title": doc.original_title,
        "number_of_seasons": doc.number_of_seasons,
        "number_of_episodes": doc.number_of_episodes,
        "created_by": doc.created_by,
        "created_by_ids": doc.created_by_ids,
        "networks": doc.networks,
        "network": doc.network,
        "production_companies": doc.production_companies,
        "production_countries": doc.production_countries,
        "budget": doc.budget,
        "revenue": doc.revenue,
        "spoken_languages": doc.spoken_languages,
        "created_at": doc.created_at,
        "modified_at": doc.modified_at,
        "_source": doc._source,
    }
    if doc.also_known_as is not None:
        result["also_known_as"] = doc.also_known_as
    return result


BACKFILL_DEFAULT_TS = 1771891200  # 2026-02-23 00:00:00 UTC


def resolve_timestamps(
    existing_doc: dict[str, Any] | None,
    now_ts: int,
    source_tag: str | None = None,
) -> tuple[int, int, str | None]:
    """
    Determine created_at, modified_at, and _source for a document write.

    Args:
        existing_doc: The existing Redis document (None if new).
        now_ts: Current Unix timestamp.
        source_tag: Optional provenance tag (e.g. "backfill").

    Returns:
        (created_at, modified_at, _source)
    """
    if existing_doc and isinstance(existing_doc, dict):
        created_at = existing_doc.get("created_at")
        if created_at is None:
            created_at = BACKFILL_DEFAULT_TS
    else:
        created_at = now_ts
    return int(created_at), now_ts, source_tag


# Legacy function for backward compatibility
def derive_search_title(doc: dict) -> str | None:
    """
    Legacy function - derive search title from a document.
    Prefer using normalize_document() for new code.
    """
    title = (
        doc.get("title")
        or doc.get("name")
        or doc.get("headline")
        or doc.get("volumeInfo", {}).get("title")
    )
    return str(title) if title else None
