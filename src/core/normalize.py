
"""
Normalization layer for heterogeneous data sources.

This module provides a consistent interface for transforming data from
various sources (TMDB, IMDB, custom APIs, etc.) into a unified document
format suitable for Redis Search indexing.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class SearchDocument:
    """
    Unified document structure for search indexing.
    All data sources must normalize to this format.
    """
    id: str                          # Unique identifier (e.g., "tmdb_12345")
    search_title: str                # Primary searchable title
    type: str                        # Content type (movie, tv, book, etc.)
    year: int | None              # Release/publish year
    popularity: float                # Normalized popularity score (0-100)
    rating: float                    # Rating score (0-10)
    source: str                      # Data source identifier
    original: dict                   # Original raw data for display


class BaseNormalizer(ABC):
    """Abstract base class for data source normalizers."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the identifier for this data source."""

    @abstractmethod
    def normalize(self, raw: dict) -> SearchDocument | None:
        """
        Transform raw data into a SearchDocument.
        Returns None if the data cannot be normalized.
        """

    @abstractmethod
    def extract_id(self, raw: dict) -> str | None:
        """Extract the unique ID from raw data."""


class TMDBNormalizer(BaseNormalizer):
    """Normalizer for TMDB (The Movie Database) data."""

    @property
    def source_name(self) -> str:
        return "tmdb"

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
        date_str = (
            raw.get("release_date")
            or raw.get("first_air_date")
        )
        if date_str and len(date_str) >= 4:
            try:
                return int(date_str[:4])
            except ValueError:
                pass
        return None

    def _extract_type(self, raw: dict) -> str:
        """Extract content type from TMDB data."""
        return (
            raw.get("content_type")
            or raw.get("mc_type")
            or raw.get("media_type")
            or "unknown"
        )

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
        import math
        vote_count_score = min(math.log10(vote_count + 1) * 25, 100) if vote_count > 0 else 0

        # Vote average is 0-10, multiply by 10
        rating_score = vote_average * 10

        # Weighted combination
        combined = (
            popularity_score * 0.50 +    # TMDB popularity (50%)
            vote_count_score * 0.30 +    # Vote count (30%)
            rating_score * 0.20          # Rating (20%)
        )

        return round(combined, 2)

    def _extract_rating(self, raw: dict) -> float:
        """Extract rating score (0-10)."""
        metrics = raw.get("metrics", {})
        return (
            metrics.get("vote_average")
            or raw.get("vote_average")
            or 0.0
        )

    def normalize(self, raw: dict) -> SearchDocument | None:
        """Transform TMDB data into a SearchDocument."""
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = self._extract_title(raw)
        if not title:
            return None

        return SearchDocument(
            id=doc_id,
            search_title=title,
            type=self._extract_type(raw),
            year=self._extract_year(raw),
            popularity=self._compute_popularity(raw),
            rating=self._extract_rating(raw),
            source=self.source_name,
            original=raw,
        )


class IMDBNormalizer(BaseNormalizer):
    """
    Normalizer for IMDB data.
    Placeholder for future implementation.
    """

    @property
    def source_name(self) -> str:
        return "imdb"

    def extract_id(self, raw: dict) -> str | None:
        imdb_id = raw.get("imdb_id") or raw.get("tconst")
        return f"imdb_{imdb_id}" if imdb_id else None

    def normalize(self, raw: dict) -> SearchDocument | None:
        doc_id = self.extract_id(raw)
        if not doc_id:
            return None

        title = raw.get("primaryTitle") or raw.get("title") or ""
        if not title:
            return None

        year = None
        if raw.get("startYear"):
            try:
                year = int(raw["startYear"])
            except ValueError:
                pass

        # IMDB has numVotes and averageRating
        num_votes = raw.get("numVotes", 0)
        avg_rating = raw.get("averageRating", 0)

        import math
        popularity = (
            min(math.log10(num_votes + 1) * 20, 80) +
            avg_rating * 2
        ) if num_votes > 0 else avg_rating * 2

        return SearchDocument(
            id=doc_id,
            search_title=title,
            type=raw.get("titleType", "unknown"),
            year=year,
            popularity=round(popularity, 2),
            rating=avg_rating,
            source=self.source_name,
            original=raw,
        )


# Registry of available normalizers
NORMALIZERS = {
    "tmdb": TMDBNormalizer(),
    "imdb": IMDBNormalizer(),
}


def get_normalizer(source: str) -> BaseNormalizer | None:
    """Get a normalizer by source name."""
    return NORMALIZERS.get(source.lower())


def detect_source(raw: dict) -> str | None:
    """
    Auto-detect the data source from raw data structure.
    Returns the source name or None if undetectable.
    """
    # TMDB indicators
    if raw.get("mc_id", "").startswith("tmdb_"):
        return "tmdb"
    if raw.get("tmdb_id") or raw.get("source") == "tmdb":
        return "tmdb"
    if "first_air_date" in raw or "poster_path" in raw:
        return "tmdb"

    # IMDB indicators
    if raw.get("tconst") or raw.get("imdb_id"):
        return "imdb"
    if raw.get("source") == "imdb":
        return "imdb"

    return None


def normalize_document(raw: dict, source: str | None = None) -> SearchDocument | None:
    """
    Normalize a raw document from any supported source.

    Args:
        raw: The raw document data
        source: Optional source hint. If not provided, will auto-detect.

    Returns:
        SearchDocument if successful, None otherwise
    """
    if source is None:
        source = detect_source(raw)

    if source is None:
        return None

    normalizer = get_normalizer(source)
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
        "type": doc.type,
        "year": doc.year,
        "popularity": doc.popularity,
        "rating": doc.rating,
        "source": doc.source,
        "original": doc.original,
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
