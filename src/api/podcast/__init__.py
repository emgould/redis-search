"""
Podcast Service Package - Modular podcast service for PodcastIndex integration.

This package provides:
- PodcastService: Core service for podcast data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.podcast.handlers import PodcastHandler, podcast_handler
from api.podcast.models import (
    EpisodeListResponse,
    MCEpisodeItem,
    MCPodcastItem,
    PodcastSearchResponse,
    PodcastTrendingResponse,
    PodcastWithLatestEpisode,
)
from api.podcast.wrappers import podcast_wrapper

__all__ = [
    # Handlers
    "PodcastHandler",
    "podcast_handler",
    # Models
    "MCPodcastItem",
    "MCEpisodeItem",
    "PodcastWithLatestEpisode",
    "PodcastTrendingResponse",
    "PodcastSearchResponse",
    "EpisodeListResponse",
    # Wrappers
    "podcast_wrapper",
]
