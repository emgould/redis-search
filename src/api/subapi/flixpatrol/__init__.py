"""
FlixPatrol Service Package - Modular FlixPatrol streaming rankings service.

This package provides:
- FlixPatrolService: Core service for FlixPatrol data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.subapi.flixpatrol.models import (
    FlixPatrolMediaItem,
    FlixPatrolMetadata,
    FlixPatrolParsedData,
    FlixPatrolPlatformData,
    FlixPatrolResponse,
)
from api.subapi.flixpatrol.wrappers import flixpatrol_wrapper

__all__ = [
    # Models
    "FlixPatrolMediaItem",
    "FlixPatrolPlatformData",
    "FlixPatrolMetadata",
    "FlixPatrolResponse",
    "FlixPatrolParsedData",
    # Wrappers
    "flixpatrol_wrapper",
]
