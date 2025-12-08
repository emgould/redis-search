"""
TMDB Services - Modular TMDB API services
Provides organized access to TMDB functionality through specialized api.
"""

import api.tmdb.wrappers as tmdb_wrapper
from api.tmdb.models import (
    MCBaseMediaItem,
    MCDiscoverResponse,
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCMovieCreditMediaItem,
    MCMovieItem,
    MCNowPlayingResponse,
    MCPersonCreditsResponse,
    MCPersonCreditsResult,
    MCPersonDetailsResponse,
    MCPersonItem,
    MCPopularTVResponse,
    MCTvCreditMediaItem,
    MCTvItem,
)
from api.tmdb.person import TMDBPersonService
from contracts.models import MCSearchResponse

__all__ = [
    # Wrappers
    "tmdb_wrapper",
    # Handlers
    "tmdb_handler",
    # Models
    "MCBaseMediaItem",
    "MCMovieItem",
    "MCTvItem",
    "MCMovieCreditMediaItem",
    "MCTvCreditMediaItem",
    "MCPersonItem",
    "MCSearchResponse",
    "MCDiscoverResponse",
    "MCNowPlayingResponse",
    "MCPopularTVResponse",
    "MCPersonCreditsResult",
    "MCGetTrendingShowResult",
    "MCGetTrendingMovieResult",
    "MCPersonDetailsResponse",
    "MCPersonCreditsResponse",
]
