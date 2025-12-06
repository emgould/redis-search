"""
TVDB Service - Modular TVDB API service.

This package provides a clean, testable interface to the TVDB API
with proper separation of concerns:
- models.py: Pydantic models for type safety
- core.py: Core service class with business logic
- wrappers.py: Async wrapper functions for backward compatibility
"""

from api.subapi._tvdb.core import TVDBCache, TVDBMonthCache, TVDBService
from api.subapi._tvdb.models import (
    TMDBCastMember,
    TMDBCastResponse,
    TMDBMultiSearchResponse,
    TMDBMultiSearchResult,
    TMDBWatchProvider,
    TMDBWatchProvidersResponse,
    TVDBCompleteDataResponse,
    TVDBImageData,
    TVDBImagesResponse,
    TVDBSearchResponse,
    TVDBSearchResult,
    TVDBShow,
    TVDBShowDetailsResponse,
    TVDBTrendingResponse,
)
from api.subapi._tvdb.wrappers import (
    get_all_images_async,
    get_show_complete_data_async,
    get_show_details_extended_async,
    search_async,
    search_by_external_id_async,
    search_tmdb_multi_async,
    search_tvdb_images_async,
)

__all__ = [
    # Core service
    "TVDBService",
    # Cache
    "TVDBCache",
    "TVDBMonthCache",
    # Models
    "TVDBShow",
    "TVDBSearchResult",
    "TVDBSearchResponse",
    "TVDBImageData",
    "TVDBImagesResponse",
    "TVDBShowDetailsResponse",
    "TVDBCompleteDataResponse",
    "TVDBTrendingResponse",
    "TMDBMultiSearchResult",
    "TMDBMultiSearchResponse",
    "TMDBCastMember",
    "TMDBCastResponse",
    "TMDBWatchProvider",
    "TMDBWatchProvidersResponse",
    # Wrappers
    "search_async",
    "search_tvdb_images_async",
    "get_show_details_extended_async",
    "get_show_complete_data_async",
    "search_by_external_id_async",
    "get_all_images_async",
    "search_tmdb_multi_async",
]
