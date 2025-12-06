"""
YouTube Service Package - Modular YouTube service.

This package provides:
- YouTubeService: Core service for YouTube data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.youtube.handlers import YouTubeHandler, youtube_handler
from api.youtube.models import (
    YouTubeCategoriesResponse,
    YouTubeCategory,
    YouTubePopularResponse,
    YouTubeSearchResponse,
    YouTubeTrendingResponse,
    YouTubeVideo,
)
from api.youtube.wrappers import youtube_wrapper

__all__ = [
    # Handlers
    "YouTubeHandler",
    "youtube_handler",
    # Models
    "YouTubeVideo",
    "YouTubeCategory",
    "YouTubeSearchResponse",
    "YouTubeTrendingResponse",
    "YouTubeCategoriesResponse",
    "YouTubePopularResponse",
    # Wrappers
    "youtube_wrapper",
]
