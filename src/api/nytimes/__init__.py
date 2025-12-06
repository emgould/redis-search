"""
NYTimes Service Package - Modular NYTimes service.

This package provides:
- NYTimesService: Core service for NYTimes data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.nytimes.core import NYTimesService
from api.nytimes.handlers import NYTimesHandler
from api.nytimes.models import (
    NYTimesBestsellerList,
    NYTimesBestsellerListDict,
    NYTimesBestsellerListResponse,
    NYTimesBestsellerListResults,
    NYTimesBook,
    NYTimesBookDict,
    NYTimesBuyLink,
    NYTimesISBN,
    NYTimesListName,
    NYTimesListNamesResponse,
    NYTimesOverviewResponse,
    NYTimesOverviewResults,
    NYTimesReview,
    NYTimesReviewResponse,
)
from api.nytimes.wrappers import nytimes_wrapper

__all__ = [
    # Core
    "NYTimesService",
    # Handlers
    "NYTimesHandler",
    # Models
    "NYTimesBook",
    "NYTimesBestsellerList",
    "NYTimesISBN",
    "NYTimesBuyLink",
    "NYTimesListName",
    "NYTimesReview",
    "NYTimesBestsellerListResults",
    "NYTimesOverviewResults",
    # Type Aliases
    "NYTimesBookDict",
    "NYTimesBestsellerListDict",
    # Response Models
    "NYTimesBestsellerListResponse",
    "NYTimesOverviewResponse",
    "NYTimesListNamesResponse",
    "NYTimesReviewResponse",
    # Wrappers
    "nytimes_wrapper",
]
