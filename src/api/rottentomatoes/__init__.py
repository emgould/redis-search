"""
RottenTomatoes Service Package - Modular RottenTomatoes API service using Algolia.

This package provides:
- RottenTomatoesService: Core service for RottenTomatoes search operations via Algolia
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
- Handlers: Firebase Functions HTTP endpoint handlers
"""

from api.rottentomatoes.handlers import RottenTomatoesHandler, rottentomatoes_handler
from api.rottentomatoes.models import (
    AlgoliaMultiQueryResponse,
    ContentRtHit,
    MCRottenTomatoesItem,
    PeopleRtHit,
    RottenTomatoes,
    RottenTomatoesSearchResponse,
)
from api.rottentomatoes.wrappers import RTMetrics, get_rt_metrics, rottentomatoes_wrapper

__all__ = [
    # Handlers
    "RottenTomatoesHandler",
    "rottentomatoes_handler",
    # Models
    "MCRottenTomatoesItem",
    "ContentRtHit",
    "PeopleRtHit",
    "RottenTomatoes",
    "AlgoliaMultiQueryResponse",
    "RottenTomatoesSearchResponse",
    "RTMetrics",
    # Wrappers
    "rottentomatoes_wrapper",
    "get_rt_metrics",
]

