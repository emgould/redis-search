"""
Normalization layer for TMDB data sources.

This module provides a consistent interface for transforming data from
TMDB into a unified document format suitable for Redis Search indexing.

Every normalizer MUST set mc_type and mc_subtype because the index
contains heterogeneous content types (movies, TV, people, etc.).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.contracts.models import MCSources, MCSubType, MCType


@dataclass
class SearchDocument:
    """
    Unified document structure for search indexing.
    All data sources must normalize to this format.

    MCType and MCSubType are required for proper categorization
    in the heterogeneous search index.
    """

    # Indexed fields
    id: str  # mc_id - Unique identifier (e.g., "tmdb_movie_12345")
    search_title: str  # Primary searchable title
    mc_type: MCType  # Content type (movie, tv, book, etc.)
    mc_subtype: MCSubType | None  # Subtype (actor, director, author, etc.)
    source: MCSources  # Data source identifier
    source_id: str  # Original ID from source (e.g., "12345" for TMDB)
    # Sortable/filterable fields
    year: int | None  # Release/publish year
    popularity: float  # Normalized popularity score (0-100)
    rating: float  # Rating score (0-10)
    # Display fields (stored, not indexed)
    image: str | None  # Medium poster/profile image URL
    overview: str | None  # Truncated description/bio
    # Genre fields (indexed as TagFields - must be strings for Redis)
    genre_ids: list[str]  # TMDB genre IDs as strings (e.g., ["35", "18", "10751"])
    genres: list[str]  # Genre names (e.g., ["Comedy", "Drama", "Family"])
    # Cast fields (indexed as TagFields - must be strings for Redis)
    cast_ids: list[str]  # TMDB person IDs as strings for cast members
    cast_names: list[str]  # Cast member names for search/filter
    cast: list[str]  # Cast member names for display (same as cast_names)
    # Person-specific fields (for people index)
    also_known_as: str | None = None  # Pipe-separated alternate names for search


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

        # If tmdb_id is a string like "tmdb_1396", extract the numeric part
        if isinstance(tmdb_id, str) and tmdb_id.startswith("tmdb_"):
            tmdb_id = tmdb_id[5:]  # Remove "tmdb_" prefix

        return str(tmdb_id) if tmdb_id else None

    def extract_id(self, raw: dict) -> str | None:
        """Extract the mc_id (e.g., tmdb_movie_12345) from raw data."""
        # Always regenerate mc_id with type prefix to avoid collisions
        # between movies and TV shows that share the same TMDB numeric ID
        type_prefix = (
            "movie"
            if self.mc_type == MCType.MOVIE
            else "tv"
            if self.mc_type == MCType.TV_SERIES
            else "person"
        )

        source_id = self.extract_source_id(raw)
        if source_id:
            return f"tmdb_{type_prefix}_{source_id}"
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
        import math

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

    def _extract_genres(self, raw: dict, genre_mapping: dict[int, str] | None = None) -> list[str]:
        """
        Extract genre names from TMDB data.

        If genre_mapping is provided, resolves genre_ids to names.
        Otherwise, uses the 'genres' field if available.
        """
        # Try direct genres field first (may already have names)
        genres = raw.get("genres", [])
        if genres:
            # Handle both formats: list of dicts with 'name' or list of strings
            names = []
            for g in genres:
                if isinstance(g, dict) and g.get("name"):
                    names.append(g["name"])
                elif isinstance(g, str):
                    names.append(g)
            if names:
                return names

        # Fall back to resolving genre_ids if mapping provided
        if genre_mapping:
            genre_ids = self._extract_genre_ids(raw)
            # genre_ids are strings, convert to int for mapping lookup
            return [genre_mapping[int(gid)] for gid in genre_ids if int(gid) in genre_mapping]

        return []

    def _extract_cast_data(
        self, raw: dict, limit: int = 5
    ) -> tuple[list[str], list[str], list[str]]:
        """
        Extract cast data from TMDB data.

        Returns:
            Tuple of (cast_ids, cast_names, cast)
            - cast_ids: List of TMDB person IDs as strings (for Redis TagField)
            - cast_names: List of actor names (for Redis TagField filtering)
            - cast: List of actor names (for display, same as cast_names)
        """
        cast_ids: list[str] = []
        cast_names: list[str] = []

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
                cast_names.append(actor_name)

        # cast is same as cast_names (for display)
        return cast_ids, cast_names, cast_names

    def _build_profile_url(self, profile_path: str | None) -> str | None:
        """Build full profile image URL from TMDB path."""
        if not profile_path:
            return None
        return f"https://image.tmdb.org/t/p/w185{profile_path}"

    def _extract_overview(self, raw: dict, max_length: int = 200) -> str | None:
        """Extract and truncate overview/description."""
        overview = raw.get("overview") or raw.get("biography") or ""
        if not overview:
            return None
        if len(overview) <= max_length:
            return overview
        return overview[:max_length].rsplit(" ", 1)[0] + "..."


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

        # Extract cast data (ids, names, and rich objects)
        cast_ids, cast_names, cast_objects = self._extract_cast_data(raw)

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            source=self.source,
            source_id=source_id,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=self._extract_rating(raw),
            image=self._extract_image(raw),
            overview=self._extract_overview(raw),
            genre_ids=self._extract_genre_ids(raw),
            genres=self._extract_genres(raw, genre_mapping),
            cast_ids=cast_ids,
            cast_names=cast_names,
            cast=cast_objects,
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

        # Extract cast data (ids, names, and rich objects)
        cast_ids, cast_names, cast_objects = self._extract_cast_data(raw)

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            source=self.source,
            source_id=source_id,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=self._extract_rating(raw),
            image=self._extract_image(raw),
            overview=self._extract_overview(raw),
            genre_ids=self._extract_genre_ids(raw),
            genres=self._extract_genres(raw, genre_mapping),
            cast_ids=cast_ids,
            cast_names=cast_names,
            cast=cast_objects,
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
            # Persons don't have genres or cast
            genre_ids=[],
            genres=[],
            cast_ids=[],
            cast_names=[],
            cast=[],
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


def document_to_redis(doc: SearchDocument) -> dict:
    """
    Convert a SearchDocument to the dict format stored in Redis.
    """
    result = {
        "id": doc.id,
        "search_title": doc.search_title,
        "mc_type": doc.mc_type.value,
        "mc_subtype": doc.mc_subtype.value if doc.mc_subtype else None,
        "source": doc.source.value,
        "source_id": doc.source_id,
        "year": doc.year,
        "popularity": doc.popularity,
        "rating": doc.rating,
        "image": doc.image,
        "overview": doc.overview,
        # Genre fields (indexed as TagFields)
        "genre_ids": doc.genre_ids,
        "genres": doc.genres,
        # Cast fields (indexed as TagFields)
        "cast_ids": doc.cast_ids,
        "cast_names": doc.cast_names,
        "cast": doc.cast,
    }
    # Add also_known_as for person documents
    if doc.also_known_as is not None:
        result["also_known_as"] = doc.also_known_as
    return result


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
