"""
Streaming Availability Service Package - Modular Streaming Availability API service.

This package provides:
- StreamingAvailabilityService: Core service for Streaming Availability API operations
- Models: Pydantic models for type-safe data structures
- Wrappers: Firebase Functions compatible async wrappers
- Handlers: Firebase Functions handlers

API Documentation: https://rapidapi.com/movie-of-the-night-movie-of-the-night-default/api/streaming-availability
"""

from api.streaming_availability.auth import StreamingAvailabilityAuth, streaming_availability_auth
from api.streaming_availability.core import StreamingAvailabilityService
from api.streaming_availability.handlers import (
    StreamingAvailabilityHandler,
    streaming_availability_handler,
)
from api.streaming_availability.models import (
    SAAddon,
    SAAudio,
    SAGenre,
    SAGenreDict,
    SAHorizontalBackdropSet,
    SAHorizontalPosterSet,
    SAImageSet,
    SALocale,
    SAPrice,
    SAService,
    SAServiceImageSet,
    SAShowResult,
    SAShowResultDict,
    SAStreamingOption,
    SAStreamingOptionDict,
    SASubtitle,
    SAVerticalBackdropSet,
    SAVerticalPosterSet,
    StreamingAvailabilityResponse,
    StreamingAvailabilityResponseDict,
)
from api.streaming_availability.wrappers import (
    StreamingAvailabilityWrapper,
    streaming_availability_wrapper,
)

__all__ = [
    # Auth
    "StreamingAvailabilityAuth",
    "streaming_availability_auth",
    # Core
    "StreamingAvailabilityService",
    # Handlers
    "StreamingAvailabilityHandler",
    "streaming_availability_handler",
    # Models - Main
    "SAShowResult",
    "StreamingAvailabilityResponse",
    # Models - Nested
    "SAGenre",
    "SALocale",
    "SAAudio",
    "SASubtitle",
    "SAPrice",
    "SAServiceImageSet",
    "SAService",
    "SAAddon",
    "SAStreamingOption",
    "SAVerticalPosterSet",
    "SAHorizontalPosterSet",
    "SAVerticalBackdropSet",
    "SAHorizontalBackdropSet",
    "SAImageSet",
    # Type Aliases
    "SAGenreDict",
    "SAStreamingOptionDict",
    "SAShowResultDict",
    "StreamingAvailabilityResponseDict",
    # Wrappers
    "StreamingAvailabilityWrapper",
    "streaming_availability_wrapper",
]
