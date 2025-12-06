"""
NewsAI Service Package - Modular news service for Event Registry API integration.

This package provides a drop-in replacement for the news API using Event Registry (NewsAI):
- NewsAIService: Core service class for Event Registry operations
- NewsAIHandler: Firebase Functions handlers for news endpoints
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers using ApiWrapperResponse pattern
"""

from api.newsai.auth import Auth
from api.newsai.core import NewsAIService
from api.newsai.event_models import (
    EventSearchResponse,
    MCEventItem,
    TrendingEventsResponse,
)
from api.newsai.handlers import NewsAIHandler, newsai_handler
from api.newsai.models import (
    MCNewsItem,
    NewsSearchResponse,
    NewsSource,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from api.newsai.search import NewsAISearchService
from api.newsai.wrappers import newsai_wrapper

__all__ = [
    # Auth
    "Auth",
    # Services
    "NewsAIService",
    "NewsAISearchService",
    # Handlers
    "NewsAIHandler",
    "newsai_handler",
    # Article Models
    "MCNewsItem",
    "NewsSource",
    "NewsSourceDetails",
    "TrendingNewsResponse",
    "NewsSearchResponse",
    "NewsSourcesResponse",
    # Event Models
    "MCEventItem",
    "TrendingEventsResponse",
    "EventSearchResponse",
    # Wrappers
    "newsai_wrapper",
]
