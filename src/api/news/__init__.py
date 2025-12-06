"""
News Service Package - Modular news service for NewsAPI integration.

This package provides:
- NewsService: Core service class for NewsAPI operations
- NewsHandler: Firebase Functions handlers for news endpoints
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers using ApiWrapperResponse pattern
"""

from api.news.auth import Auth
from api.news.core import NewsService
from api.news.handlers import NewsHandler, news_handler
from api.news.models import (
    MCNewsItem,
    NewsSearchResponse,
    NewsSource,
    NewsSourceDetails,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from api.news.search import NewsSearchService
from api.news.wrappers import news_wrapper

__all__ = [
    # Auth
    "Auth",
    # Services
    "NewsService",
    "NewsSearchService",
    # Handlers
    "NewsHandler",
    "news_handler",
    # Models
    "MCNewsItem",
    "NewsSource",
    "NewsSourceDetails",
    "TrendingNewsResponse",
    "NewsSearchResponse",
    "NewsSourcesResponse",
    # Wrappers
    "news_wrapper",
]
