"""
OpenLibrary Service Package - Modular OpenLibrary service.

This package provides:
- OpenLibraryService: Core service for OpenLibrary data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.openlibrary.handlers import OpenLibraryHandler, openlibrary_handler
from api.openlibrary.models import (
    BookSearchResponse,
    CoverUrlsResponse,
    MCAuthorItem,
    MCBookItem,
    OpenLibraryAuthorSearchResponse,
    OpenLibraryCoverUrlsResponse,
    OpenLibrarySearchResponse,
)
from api.openlibrary.wrappers import openlibrary_wrapper

__all__ = [
    # Handlers
    "OpenLibraryHandler",
    "openlibrary_handler",
    # Models
    "MCBookItem",
    "MCAuthorItem",
    "BookSearchResponse",
    "CoverUrlsResponse",
    "OpenLibrarySearchResponse",
    "OpenLibraryAuthorSearchResponse",
    "OpenLibraryCoverUrlsResponse",
    # Wrappers
    "openlibrary_wrapper",
]
