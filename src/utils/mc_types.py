"""
MediaCircle Item Type System

This module provides standardized item identification across all content types.
Every item returned by the backend must include:
- mc_id: Deterministic unique identifier
- mc_type: Content type enum value
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class RatingEmoji(str, Enum):
    """
    Rating emoji system for MediaCircle.
    Each rating has an associated emoji (Unicode), label, and numeric score.
    """

    LOVE = "U+2764"  # ❤️ Red heart
    GOOD = "U+1F44D"  # 👍 Thumbs up
    MEH = "U+1F610"  # 😐 Neutral face
    NOT_GOOD = "U+1F641"  # 🙁 Slightly frowning face
    TERRIBLE = "U+1F44E"  # 👎 Thumbs down


# Rating emoji metadata
RATING_METADATA = {
    RatingEmoji.LOVE: {"label": "LOVE", "score": 5, "unicode": "U+2764"},
    RatingEmoji.GOOD: {"label": "Good", "score": 4, "unicode": "U+1F44D"},
    RatingEmoji.MEH: {"label": "Meh", "score": 3, "unicode": "U+1F610"},
    RatingEmoji.NOT_GOOD: {"label": "Not Good", "score": 2, "unicode": "U+1F641"},
    RatingEmoji.TERRIBLE: {"label": "Terrible", "score": 1, "unicode": "U+1F44E"},
}


def get_rating_score(emoji: str) -> int:
    """
    Get the numeric score for a rating emoji.

    Args:
        emoji: The rating emoji Unicode string (e.g., "U+2764")

    Returns:
        int: Score from 1-5, or 0 if invalid

    Examples:
        >>> get_rating_score("U+2764")
        5
        >>> get_rating_score("U+1F44E")
        1
    """
    try:
        rating = RatingEmoji(emoji)
        score: Any = RATING_METADATA[rating]["score"]
        return int(score)
    except (ValueError, KeyError):
        return 0


def get_rating_from_score(score: int) -> str | None:
    """
    Get the rating emoji Unicode from a numeric score.

    Args:
        score: Numeric score from 1-5

    Returns:
        str: Rating emoji Unicode, or None if invalid score

    Examples:
        >>> get_rating_from_score(5)
        'U+2764'
        >>> get_rating_from_score(1)
        'U+1F44E'
    """
    for emoji, metadata in RATING_METADATA.items():
        if metadata["score"] == score:
            return emoji.value
    return None


# =============================================================================
# RATING DATACLASSES
# =============================================================================


@dataclass
class Rating:
    """Individual user rating for an item."""

    rating_id: str
    user_id: str
    mc_id: str
    rating: int  # 1-5 score
    rating_unicode: str  # Unicode emoji (e.g., "U+2764")
    timestamp: str  # ISO format timestamp

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Rating":
        """Create Rating from Firestore document."""
        return Rating(
            rating_id=d.get("rating_id", ""),
            user_id=d.get("userId", ""),
            mc_id=d.get("mc_id", ""),
            rating=int(d.get("rating", 0)),
            rating_unicode=d.get("rating_unicode", ""),
            timestamp=d.get("timestamp", ""),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to Firestore document format."""
        return {
            "userId": self.user_id,
            "mc_id": self.mc_id,
            "rating": self.rating,
            "rating_unicode": self.rating_unicode,
            "timestamp": self.timestamp,
        }


@dataclass
class UserRatingItem:
    """Minimal item data for user's ratings list."""

    rating_id: str
    mc_id: str
    mc_type: str
    mc_title: str
    mc_image: str
    rating: int
    rating_unicode: str
    timestamp: str

    @staticmethod
    def from_rating_and_item(rating: Rating, item_data: dict[str, Any]) -> "UserRatingItem":
        """Create UserRatingItem from Rating and item metadata."""
        return UserRatingItem(
            rating_id=rating.rating_id,
            mc_id=rating.mc_id,
            mc_type=item_data.get("mc_type", ""),
            mc_title=item_data.get("mc_title", "Unknown"),
            mc_image=item_data.get("mc_image", ""),
            rating=rating.rating,
            rating_unicode=rating.rating_unicode,
            timestamp=rating.timestamp,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "rating_id": self.rating_id,
            "mc_id": self.mc_id,
            "mc_type": self.mc_type,
            "mc_title": self.mc_title,
            "mc_image": self.mc_image,
            "rating": self.rating,
            "rating_unicode": self.rating_unicode,
            "timestamp": self.timestamp,
        }


@dataclass
class ItemRating:
    """Aggregated rating data for an item."""

    mc_id: str
    mc_type: str
    mc_title: str
    mc_image: str
    count: int
    average: float
    last_rated: str | None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "ItemRating":
        """Create ItemRating from Firestore document."""
        return ItemRating(
            mc_id=d.get("mc_id", ""),
            mc_type=d.get("mc_type", ""),
            mc_title=d.get("mc_title", ""),
            mc_image=d.get("mc_image", ""),
            count=int(d.get("count", 0)),
            average=float(d.get("average", 0.0)),
            last_rated=d.get("last_rated"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to Firestore document format."""
        return {
            "mc_type": self.mc_type,
            "mc_title": self.mc_title,
            "mc_image": self.mc_image,
            "count": self.count,
            "average": self.average,
            "last_rated": self.last_rated,
        }


@dataclass
class RatingSubmission:
    """Data required to submit a rating."""

    user_id: str
    mc_id: str
    mc_type: str
    mc_title: str
    mc_image: str
    rating_unicode: str

    def validate(self) -> None:
        """Validate the submission data."""
        if get_rating_score(self.rating_unicode) == 0:
            raise ValueError(f"Invalid rating emoji: {self.rating_unicode}")
