"""
Watchmode Service Package - Modular Watchmode service.

This package provides:
- WatchmodeService: Core service for Watchmode data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.watchmode.auth import WatchmodeAuth, watchmode_auth
from api.watchmode.core import WatchmodeService
from api.watchmode.handlers import WatchmodeHandler, watchmode_handler
from api.watchmode.models import (
    WatchmodeRelease,
    WatchmodeReleaseDict,
    WatchmodeSearchResponse,
    WatchmodeSearchResult,
    WatchmodeSearchResultDict,
    WatchmodeStreamingSource,
    WatchmodeStreamingSourceDict,
    WatchmodeTitleDetails,
    WatchmodeTitleDetailsDict,
    WatchmodeTitleDetailsResponse,
    WatchmodeWhatsNewResponse,
)
from api.watchmode.wrappers import watchmode_wrapper

__all__ = [
    # Auth
    "WatchmodeAuth",
    "watchmode_auth",
    # Core
    "WatchmodeService",
    # Handlers
    "WatchmodeHandler",
    "watchmode_handler",
    # Models
    "WatchmodeStreamingSource",
    "WatchmodeRelease",
    "WatchmodeTitleDetails",
    "WatchmodeSearchResult",
    "WatchmodeWhatsNewResponse",
    "WatchmodeTitleDetailsResponse",
    "WatchmodeSearchResponse",
    # Type Aliases
    "WatchmodeStreamingSourceDict",
    "WatchmodeReleaseDict",
    "WatchmodeTitleDetailsDict",
    "WatchmodeSearchResultDict",
    # Wrappers
    "watchmode_wrapper",
]
