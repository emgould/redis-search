"""
Google Books Service Package - Modular Google Books service.

This package provides:
- GoogleBooksService: Core service for Google Books data operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
"""

from api.subapi._google.handlers import google_books_handler
from api.subapi._google.models import (
    GoogleBooksImageLinks,
    GoogleBooksIndustryIdentifier,
    GoogleBooksItem,
    GoogleBooksRawSearchResponse,
    GoogleBooksSaleInfo,
    GoogleBooksSearchResponse,
    GoogleBooksVolumeInfo,
    GoogleBooksVolumeRaw,
    GoogleBooksVolumeResponse,
)
from api.subapi._google.wrappers import google_books_wrapper

__all__ = [
    # Handlers
    "google_books_handler",
    # Models
    "GoogleBooksItem",
    "GoogleBooksVolumeInfo",
    "GoogleBooksVolumeRaw",
    "GoogleBooksIndustryIdentifier",
    "GoogleBooksImageLinks",
    "GoogleBooksSaleInfo",
    # Response Models
    "GoogleBooksSearchResponse",
    "GoogleBooksVolumeResponse",
    "GoogleBooksRawSearchResponse",
    # Wrappers
    "google_books_wrapper",
]
