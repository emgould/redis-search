"""
TMDB Firebase Functions Handlers
HTTP endpoint handlers for TMDB-related Firebase Functions.
"""

import json
import logging
import random

from firebase_functions import https_fn
from firebase_functions.https_fn import FunctionsErrorCode

import api.tmdb.wrappers as TMDB  # noqa: N812
from contracts.models import MCType
from utils.redis_cache import RedisCache

# Configure logging
logger = logging.getLogger(__name__)

# Cache for trending handler - v1.0.0: Updated enrichment logic (uses search_multi internally via _search_with_keywords)
TrendingHandlerCache = RedisCache(
    defaultTTL=6 * 60 * 60,  # 6 hours (FlixPatrol data updates daily, but enrichment is expensive)
    prefix="trending_handler",
    verbose=False,
    isClassMethod=True,  # Instance methods on TMDBHandler
    version="1.0.1",  # Version bump for Redis migration
)

GENRES = {
    "tv": {
        "Action & Adventure": 10759,
        "Animation": 16,
        "Comedy": 35,
        "Crime": 80,
        "Documentary": 99,
        "Drama": 18,
        "Family": 10751,
        "Kids": 10762,
        "Mystery": 9648,
        "News": 10763,
        "Reality": 10764,
        "Sci-Fi & Fantasy": 10765,
        "Soap": 10766,
        "Talk": 10767,
        "War & Politics": 10768,
        "Western": 37,
    },
    "movies": {
        "Action": 28,
        "Adventure": 12,
        "Animation": 16,
        "Comedy": 35,
        "Crime": 80,
        "Documentary": 99,
        "Drama": 18,
        "Family": 10751,
        "Fantasy": 14,
        "History": 36,
        "Horror": 27,
        "Music": 10402,
        "Mystery": 9648,
        "Romance": 10749,
        "Science Fiction": 878,
        "TV Movie": 10770,
        "Thriller": 53,
        "War": 10752,
        "Western": 37,
    },
}


class TMDBHandler:
    """Class containing all TMDB-focused Firebase Functions."""

    def __init__(self):
        """Initialize TMDB handler. API keys are accessed from secrets at runtime."""
        logger.info("TMDBHandler initialized - API keys will be resolved at runtime")

    async def get_trending(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get trending movies and TV shows using FlixPatrol data enriched with TMDB metadata.

        Usage:
        GET /get_trending                              # Default: top 10 shows + 10 movies for week
        GET /get_trending?time_window=day             # Daily trending
        GET /get_trending?shows_limit=5&movies_limit=15  # Custom limits
        GET /get_trending?shows_only=true            # Only shows
        GET /get_trending?movies_only=true           # Only movies
        """
        try:
            # Parse request parameters - set generous defaults for frontend consumption
            limit = int(req.args.get("limit", 50))  # Default to 50 shows
            media_type_param = req.args.get("media_type", "tv").lower()

            # Convert string parameter to MCType enum
            if media_type_param == "movie":
                media_type = MCType.MOVIE
            elif media_type_param == "tv":
                media_type = MCType.TV_SERIES
            elif media_type_param == "all":
                media_type = MCType.MIXED
            else:
                return https_fn.Response(
                    json.dumps({"error": "media_type must be 'movie' or 'tv'"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate limits (allow up to 200 for internal filtering purposes)
            if limit < 0 or limit > 200:
                return https_fn.Response(
                    json.dumps({"error": "limits must be between 0 and 200"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Is this a warmup request?
            no_cache = req.args.get("no_cache", "").lower() == "true"

            # Get trending data from FlixPatrol
            response = await TMDB.get_trending_async(
                limit=limit, media_type=media_type, no_cache=no_cache
            )

            if response.status_code != 200:
                logger.error(f"Error get_trending:{response.status_code}")
                return https_fn.Response(
                    json.dumps(response.model_dump()),
                    status=response.status_code,
                    headers={"Content-Type": "application/json"},
                )

            if response.results:
                random.shuffle(response.results)
            return https_fn.Response(
                json.dumps(response.model_dump()),
                status=200,
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.error(f"Error in get_trending: {e}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_multi(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for movies and TV shows using TMDB's multi search endpoint.
        Returns unified results with enhanced data including watch providers.
        Supports keyword syntax: keyword: "name" for keyword-based discovery.

        Args (query parameters):
            query: Search query string (required). May include keyword syntax: keyword: "name"
            limit: Maximum number of results (default: 20, max: 50)
            page: Page number for pagination (default: 1, min: 1)

        Usage:
        GET /search_multi?query=batman&limit=20&page=1
        GET /search_multi?query=keyword:"space opera"&limit=10

        Returns:
            HTTP Response with JSON containing search results or error message
        """
        try:
            # Parse request parameters
            query = req.args.get("query")
            try:
                limit = int(req.args.get("limit", 20))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            try:
                page = int(req.args.get("page", 1))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "page must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be greater than 0"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async search
            result = await TMDB.search_multi_async(query=query, page=page, limit=limit)

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched TMDB multi for: '{query}' - {len(result.results)} results"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in search_multi function: {str(e)}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_tv_shows(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for TV shows using TMDB with weighted sorting by relevancy and recency.
        Returns TV show results with enhanced sorting giving stronger weight to more recent shows.

        Args (query parameters):
            query: Search query string (required)
            limit: Maximum number of results (default: 50, max: 50)
            page: Page number for pagination (default: 1, min: 1)
            no_cache: If 'true', bypass cache (default: false)

        Usage:
        GET /search_tv_shows?query=stranger+things&limit=50&page=1&no_cache=true

        Returns:
            HTTP Response with JSON containing TV show search results or error message
        """
        try:
            # Parse request parameters
            query = req.args.get("query")
            try:
                limit = int(req.args.get("limit", 50))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            try:
                page = int(req.args.get("page", 1))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "page must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            no_cache = req.args.get("no_cache", "").lower() == "true"

            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be greater than 0"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Log cache busting for debugging
            if no_cache:
                logger.info(
                    f"🔄 TV show search endpoint: cache busting enabled for query '{query}'"
                )

            # Run the async search
            result = await TMDB.search_tv_shows_async(
                query=query, page=page, limit=limit, no_cache=no_cache
            )

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched TMDB TV shows for: '{query}' - {len(result.results)} results"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in search_tv_shows function: {str(e)}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_by_genre(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for movies and TV shows by genre IDs using TMDB discover endpoints.
        Returns interleaved results with enhanced sorting by popularity, recency, and quality.

        Args (query parameters):
            genre_ids: Comma-separated genre IDs (required, e.g., "18,80" for Drama and Crime)
            limit: Maximum number of results (default: 50, max: 50)
            page: Page number for pagination (default: 1, min: 1)
            include_details: If 'true', include watch providers, cast, videos, and keywords (default: true)

        Usage:
        GET /search_by_genre?genre_ids=18,80&limit=50&page=1
        GET /search_by_genre?genre_ids=28&limit=20&include_details=false

        Returns:
            HTTP Response with JSON containing genre search results or error message
        """
        try:
            # Parse request parameters
            genre_ids = req.args.get("genre_ids")
            try:
                limit = int(req.args.get("limit", 50))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            try:
                page = int(req.args.get("page", 1))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "page must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            include_details = req.args.get("include_details", "true").lower() == "true"

            if not genre_ids:
                return https_fn.Response(
                    json.dumps({"error": "genre_ids parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate genre_ids format (should be comma-separated numbers)
            try:
                genre_id_list = [int(gid.strip()) for gid in genre_ids.split(",")]
                if not genre_id_list or any(gid < 1 for gid in genre_id_list):
                    raise ValueError("Invalid genre ID")
            except ValueError:
                return https_fn.Response(
                    json.dumps(
                        {
                            "error": "genre_ids must be comma-separated positive integers (e.g., '18,80')"
                        }
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be greater than 0"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async search
            result = await TMDB.search_by_genre_async(
                genre_ids=genre_ids,
                page=page,
                limit=limit,
                include_details=include_details,
            )

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched by genre IDs '{genre_ids}': {len(result.results)} results"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in search_by_genre function: {str(e)}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_by_keywords(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for movies and TV shows by keyword IDs using TMDB discover endpoints.
        Returns interleaved results with enhanced sorting by popularity, recency, and quality.

        Args (query parameters):
            keyword_ids: Comma-separated keyword IDs (required, e.g., "825,1721" for space opera keywords)
            limit: Maximum number of results (default: 50, max: 50)
            page: Page number for pagination (default: 1, min: 1)
            include_details: If 'true', include watch providers, cast, videos, and keywords (default: true)

        Usage:
        GET /search_by_keywords?keyword_ids=825&limit=50&page=1
        GET /search_by_keywords?keyword_ids=825,1721&limit=20&include_details=false

        Returns:
            HTTP Response with JSON containing keyword search results or error message
        """
        try:
            # Parse request parameters
            keyword_ids = req.args.get("keyword_ids")
            try:
                limit = int(req.args.get("limit", 50))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            try:
                page = int(req.args.get("page", 1))
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "page must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            include_details = req.args.get("include_details", "true").lower() == "true"

            if not keyword_ids:
                return https_fn.Response(
                    json.dumps({"error": "keyword_ids parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate keyword_ids format (should be comma-separated numbers)
            try:
                keyword_id_list = [int(kid.strip()) for kid in keyword_ids.split(",")]
                if not keyword_id_list or any(kid < 1 for kid in keyword_id_list):
                    raise ValueError("Invalid keyword ID")
            except ValueError:
                return https_fn.Response(
                    json.dumps(
                        {
                            "error": "keyword_ids must be comma-separated positive integers (e.g., '825,1721')"
                        }
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be greater than 0"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async search
            result = await TMDB.search_by_keywords_async(
                keyword_ids=keyword_ids,
                page=page,
                limit=limit,
                include_details=include_details,
            )

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched by keyword IDs '{keyword_ids}': {len(result.results)} results"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in search_by_keywords function: {str(e)}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_providers_list(self, req: https_fn.CallableRequest) -> dict:
        """
        Get list of available streaming providers from TMDB.

        Usage (client):
        await httpsCallable(functions, 'get_providers_list')({
            content_type: 'movie',
            region: 'US',
        })
        """
        try:
            # Log authentication context for debugging
            logger.info(f"🔍 get_providers_list called - Auth context: {req.auth}")
            if req.auth:
                logger.info(f"✅ Authenticated user: {req.auth.uid}")
            else:
                logger.warning("⚠️ No auth context found in request")

            # Extract params from callable payload, with defaults
            data = req.data or {}
            content_type = data.get("content_type", "movie")
            region = data.get("region", "US")

            logger.info(f"📋 Fetching {content_type} providers for region: {region}")

            if content_type not in ["tv", "movie"]:
                raise https_fn.HttpsError(
                    code=FunctionsErrorCode.INVALID_ARGUMENT,
                    message="content_type must be 'tv' or 'movie'",
                )

            # Run the async function
            if content_type == "tv":
                response = await TMDB.get_providers_async(MCType.TV_SERIES, region=region)
            else:
                response = await TMDB.get_providers_async(MCType.MOVIE, region=region)

            # Transform response to match frontend expectations
            # Frontend expects: {providers: [], content_type: str, region: str, count: int}
            # Backend returns: {list_type: str, results: [], mc_type: ...}
            response_dict = response.model_dump()
            return {
                "providers": response_dict.get("results", []),
                "content_type": content_type,
                "region": region,
                "count": len(response_dict.get("results", [])),
            }

        except https_fn.HttpsError:
            raise  # Firebase will serialize this correctly
        except Exception as e:
            logger.error(f"Error in get_providers_list: {e}")
            raise https_fn.HttpsError(
                code=FunctionsErrorCode.INTERNAL, message="Internal server error"
            )

    async def get_media_details(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get detailed information for a movie or TV show including cast, trailers, and watch providers.

        Usage:
        GET /get_media_details?tmdb_id=12345&content_type=movie
        GET /get_media_details?tmdb_id=67890&content_type=tv
        """
        try:
            # Parse request parameters
            tmdb_id = req.args.get("tmdb_id")
            content_type = req.args.get("content_type")

            if not tmdb_id:
                return https_fn.Response(
                    json.dumps({"error": "tmdb_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if content_type not in ["tv", "movie"]:
                return https_fn.Response(
                    json.dumps({"error": "content_type must be 'tv' or 'movie'"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                tmdb_id_int = int(tmdb_id)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "tmdb_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async function
            response = await TMDB.get_media_details_async(
                tmdb_id=tmdb_id_int, content_type=content_type
            )

            if response.status_code != 200:
                return https_fn.Response(
                    json.dumps(response.model_dump()),
                    status=response.status_code,
                    headers={"Content-Type": "application/json"},
                )
            else:
                return https_fn.Response(
                    json.dumps(response.model_dump()),
                    status=200,
                    headers={"Content-Type": "application/json"},
                )

        except Exception as e:
            logger.error(f"Error in get_media_details function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {"error": "Internal server error", "message": "Failed to fetch media details"}
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_now_playing(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get movies currently playing in theaters from TMDB with optional box office sorting.

        Args (query parameters):
            region: Region code for theaters (e.g., 'US', 'CA', 'GB'). Default: 'US'
            limit: Maximum number of movies to return (default: 50, max: 50)
            sort_by_box_office: If 'true', sort movies by Comscore box office rankings (US only)

        Usage:
        GET /get_now_playing                                      # Default: 50 movies in US region
        GET /get_now_playing?region=CA&limit=10                  # Custom region and limit
        GET /get_now_playing?sort_by_box_office=true             # Sort by Comscore box office rankings
        GET /get_now_playing?region=US&limit=20&sort_by_box_office=true  # Full customization

        Returns:
            HTTP Response with JSON containing movies list or error message
        """
        try:
            # Parse request parameters
            region = req.args.get(
                "region", "US"
            ).upper()  # Default to US region, normalize to uppercase
            try:
                limit = int(req.args.get("limit", 50))  # Default to 50 movies
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )
            sort_by_box_office = req.args.get("sort_by_box_office", "").lower() == "true"

            # Validate parameters
            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate region code (should be 2 characters)
            if not region or len(region) != 2:
                return https_fn.Response(
                    json.dumps(
                        {"error": "region must be a valid 2-character code (e.g., US, CA, GB)"}
                    ),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async function with box office sorting option
            result = await TMDB.get_now_playing_async(
                region=region,
                limit=limit,
                sort_by_box_office=sort_by_box_office,
            )

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            sort_info = " (sorted by box office)" if sort_by_box_office else ""
            logger.info(
                f"Successfully fetched now playing movies for region {region}: {len(result.results)} movies{sort_info}"
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_now_playing function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch now playing movies",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_popular_tv(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get popular TV shows from TMDB, filtered to shows from the past year.

        Args (query parameters):
            limit: Maximum number of TV shows to return (default: 50, max: 50)

        Usage:
        GET /get_popular_tv                  # Default: 50 popular TV shows from past year
        GET /get_popular_tv?limit=10        # Custom limit

        Returns:
            HTTP Response with JSON containing TV shows list or error message
        """
        try:
            # Parse request parameters
            try:
                limit = int(req.args.get("limit", 50))  # Default to 50 shows
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "limit must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Validate parameters
            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async function
            result = await TMDB.get_popular_tv_async(limit=limit)

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                "get_popular_tv returning %d/%d shows (requested limit: %d)",
                len(result.results),
                result.total_results,
                limit,
            )

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_popular_tv function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch popular TV shows",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def search_people(self, req: https_fn.Request) -> https_fn.Response:
        """
        Search for people/actors using TMDB's person search endpoint.

        Usage:
        GET /search_people?query=tom+hanks&limit=20&page=1
        """
        try:
            # Parse request parameters
            query = req.args.get("query")
            limit = int(req.args.get("limit", 20))
            page = int(req.args.get("page", 1))

            if not query:
                return https_fn.Response(
                    json.dumps({"error": "query parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 50:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 50"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if page < 1:
                return https_fn.Response(
                    json.dumps({"error": "page must be greater than 0"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async search
            result = await TMDB.search_people_async(query=query, page=page, limit=limit)

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(
                f"Successfully searched TMDB people for: '{query}' - {len(result.results)} results"
            )

            # Serialize with proper type information for MCPersonItem fields
            response_dict = result.model_dump()
            # Ensure items are fully serialized with all fields
            response_dict["results"] = [item.model_dump() for item in result.results]

            return https_fn.Response(
                json.dumps(response_dict),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=1800",  # Cache for 30 minutes
                },
            )

        except Exception as e:
            logger.error(f"Error in search_people function: {str(e)}")
            return https_fn.Response(
                json.dumps({"error": "Internal server error"}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_person_details(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get detailed information for a person/actor from TMDB.

        Usage:
        GET /get_person_details?person_id=31
        """
        try:
            # Parse request parameters
            person_id = req.args.get("person_id")

            if not person_id:
                return https_fn.Response(
                    json.dumps({"error": "person_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                person_id_int = int(person_id)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "person_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async function
            result = await TMDB.get_person_details_async(person_id=person_id_int)

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched person details for ID {person_id_int}")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_person_details function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch person details",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def get_person_credits(self, req: https_fn.Request) -> https_fn.Response:
        """
        Get complete person credits including movies and TV shows from TMDB.

        Usage:
        GET /get_person_credits?person_id=31&limit=50
        """
        try:
            # Parse request parameters
            person_id = req.args.get("person_id")
            limit = int(req.args.get("limit", 50))

            if not person_id:
                return https_fn.Response(
                    json.dumps({"error": "person_id parameter is required"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            try:
                person_id_int = int(person_id)
            except ValueError:
                return https_fn.Response(
                    json.dumps({"error": "person_id must be a valid integer"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            if limit < 1 or limit > 100:
                return https_fn.Response(
                    json.dumps({"error": "limit must be between 1 and 100"}),
                    status=400,
                    headers={"Content-Type": "application/json"},
                )

            # Run the async function
            result = await TMDB.get_person_credits_async(person_id=person_id_int, limit=limit)

            if result.status_code != 200:
                return https_fn.Response(
                    json.dumps(result.model_dump()),
                    status=result.status_code,
                    headers={"Content-Type": "application/json"},
                )

            logger.info(f"Successfully fetched person credits for ID {person_id_int}")

            return https_fn.Response(
                json.dumps(result.model_dump()),
                status=200,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                },
            )

        except Exception as e:
            logger.error(f"Error in get_person_credits function: {str(e)}")
            return https_fn.Response(
                json.dumps(
                    {
                        "error": "Internal server error",
                        "message": "Failed to fetch person credits",
                    }
                ),
                status=500,
                headers={"Content-Type": "application/json"},
            )


# Create the handler instance
tmdb_handler = TMDBHandler()
