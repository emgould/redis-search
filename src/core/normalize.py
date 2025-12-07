"""
Normalization layer for heterogeneous data sources.

This module provides a consistent interface for transforming data from
various sources (TMDB, IMDB, custom APIs, etc.) into a unified document
format suitable for Redis Search indexing.

Every normalizer MUST set mc_type and mc_subtype because the index
contains heterogeneous content types (movies, TV, people, books, podcasts, etc.).
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
    id: str  # Unique identifier (e.g., "tmdb_12345")
    search_title: str  # Primary searchable title
    mc_type: MCType  # Content type (movie, tv, book, etc.)
    mc_subtype: MCSubType | None  # Subtype (actor, director, author, etc.)
    year: int | None  # Release/publish year
    popularity: float  # Normalized popularity score (0-100)
    rating: float  # Rating score (0-10)
    source: MCSources  # Data source identifier
    # Display fields (stored, not indexed)
    image: str | None  # Medium poster/profile image URL
    cast: list[str]  # First two actor names (for movies/TV)
    overview: str | None  # Truncated description/bio


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
    def normalize(self, raw: dict) -> SearchDocument | None:
        """
        Transform raw data into a SearchDocument.
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

    def extract_id(self, raw: dict) -> str | None:
        # TMDB data may have mc_id or we construct from tmdb_id
        if raw.get("mc_id"):
            return str(raw["mc_id"])
        if raw.get("tmdb_id"):
            return f"tmdb_{raw['tmdb_id']}"
        if raw.get("id"):
            return f"tmdb_{raw['id']}"
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

    def _extract_cast(self, raw: dict) -> list[str]:
        """Extract first two actor names from main_cast."""
        main_cast = raw.get("main_cast", [])
        return [actor.get("name") for actor in main_cast[:2] if actor.get("name")]

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

    def normalize(self, raw: dict) -> SearchDocument | None:
        """Transform TMDB movie data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = self._extract_title(raw)
        if not title:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=self._extract_rating(raw),
            source=self.source,
            image=self._extract_image(raw),
            cast=self._extract_cast(raw),
            overview=self._extract_overview(raw),
        )


class TMDBTvNormalizer(BaseTMDBNormalizer):
    """Normalizer for TMDB TV series data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.TV_SERIES

    def normalize(self, raw: dict) -> SearchDocument | None:
        """Transform TMDB TV data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = self._extract_title(raw)
        if not title:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=self._extract_rating(raw),
            source=self.source,
            image=self._extract_image(raw),
            cast=self._extract_cast(raw),
            overview=self._extract_overview(raw),
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

    def normalize(self, raw: dict) -> SearchDocument | None:
        """Transform TMDB person data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        name = raw.get("name") or ""
        if not name:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=name,
            mc_type=self.mc_type,
            mc_subtype=self._detect_subtype(raw),
            year=None,  # Persons don't have a year
            popularity=self._compute_popularity(raw),
            rating=0.0,  # Persons don't have ratings
            source=self.source,
            image=self._extract_image(raw),
            cast=[],  # Persons don't have cast
            overview=self._extract_overview(raw),  # Uses biography
        )


class BaseIMDBNormalizer(BaseNormalizer):
    """
    Base normalizer for IMDB data.
    """

    @property
    def source(self) -> MCSources:
        # IMDB is often accessed via TMDB or other sources
        # For now, we treat it as TMDB since that's the primary source
        return MCSources.TMDB

    def extract_id(self, raw: dict) -> str | None:
        imdb_id = raw.get("imdb_id") or raw.get("tconst")
        return f"imdb_{imdb_id}" if imdb_id else None

    def _extract_year(self, raw: dict) -> int | None:
        if raw.get("startYear"):
            try:
                return int(raw["startYear"])
            except ValueError:
                pass
        return None

    def _compute_popularity(self, raw: dict) -> float:
        """Compute popularity from IMDB votes and rating."""
        import math

        num_votes: int = raw.get("numVotes", 0)
        avg_rating: float = raw.get("averageRating", 0)

        popularity = (
            (min(math.log10(num_votes + 1) * 20, 80) + avg_rating * 2)
            if num_votes > 0
            else avg_rating * 2
        )

        return float(round(popularity, 2))


class IMDBMovieNormalizer(BaseIMDBNormalizer):
    """Normalizer for IMDB movie data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.MOVIE

    def normalize(self, raw: dict) -> SearchDocument | None:
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = raw.get("primaryTitle") or raw.get("title") or ""
        if not title:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=raw.get("averageRating", 0.0),
            source=self.source,
            image=None,  # IMDB data doesn't include images in same format
            cast=[],  # IMDB data doesn't include cast in same format
            overview=raw.get("plot"),  # IMDB uses 'plot' field
        )


class IMDBTvNormalizer(BaseIMDBNormalizer):
    """Normalizer for IMDB TV series data."""

    @property
    def mc_type(self) -> MCType:
        return MCType.TV_SERIES

    def normalize(self, raw: dict) -> SearchDocument | None:
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = raw.get("primaryTitle") or raw.get("title") or ""
        if not title:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=title,
            mc_type=self.mc_type,
            mc_subtype=self.mc_subtype,
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=raw.get("averageRating", 0.0),
            source=self.source,
            image=None,  # IMDB data doesn't include images in same format
            cast=[],  # IMDB data doesn't include cast in same format
            overview=raw.get("plot"),  # IMDB uses 'plot' field
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
    "imdb_movie": IMDBMovieNormalizer(),
    "imdb_tv": IMDBTvNormalizer(),
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

    # IMDB indicators (override source if IMDB-specific)
    # IMDB data often comes via TMDB, so we use TMDB as the source
    if raw.get("tconst") or raw.get("imdb_id") or raw.get("source") == "imdb":
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
        # IMDB title type
        elif raw.get("titleType"):
            title_type = raw["titleType"].lower()
            if title_type in ("movie", "short", "tvmovie"):
                mc_type = MCType.MOVIE
            elif title_type in ("tvseries", "tvminiseries", "tvepisode"):
                mc_type = MCType.TV_SERIES

    return source, mc_type


def normalize_document(
    raw: dict,
    source: MCSources | None = None,
    mc_type: MCType | None = None,
) -> SearchDocument | None:
    """
    Normalize a raw document from any supported source.

    Args:
        raw: The raw document data
        source: Optional source hint. If not provided, will auto-detect.
        mc_type: Optional MCType hint. If not provided, will auto-detect.

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

    return normalizer.normalize(raw)


def document_to_redis(doc: SearchDocument) -> dict:
    """
    Convert a SearchDocument to the dict format stored in Redis.
    """
    return {
        "id": doc.id,
        "search_title": doc.search_title,
        "mc_type": doc.mc_type.value,
        "mc_subtype": doc.mc_subtype.value if doc.mc_subtype else None,
        "year": doc.year,
        "popularity": doc.popularity,
        "rating": doc.rating,
        "source": doc.source.value,
        "image": doc.image,
        "cast": doc.cast,
        "overview": doc.overview,
    }


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
