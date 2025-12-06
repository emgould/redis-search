"""
Comscore Service Package - Modular box office rankings service.

This package provides:
- ComscoreService: Core service for box office data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking, BoxOfficeResponse
from api.subapi.comscore.wrappers import comscore_wrapper

__all__ = [
    # Core
    "comscore_service",
    # Models
    "BoxOfficeRanking",
    "BoxOfficeData",
    "BoxOfficeResponse",
    # Wrappers
    "comscore_wrapper",
]
