"""
TMDB Async Wrappers - Firebase Functions compatible async wrappers
Provides standalone async functions for Firebase Functions integration.
These maintain backward compatibility with existing Firebase Functions.
"""

from typing import Any

from api.tmdb.core import (
    TMDBContentRatingCache,
    TMDBFunctionCache,
    TMDBService,
)
from api.tmdb.models import (
    MCBaseMediaItem,
    MCGenreSearchResponse,
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCKeywordSearchResponse,
    MCNowPlayingResponse,
    MCPersonCreditsResponse,
    MCPersonDetailsResponse,
    MCPopularTVResponse,
    MCTvBatchEnrichmentRequestItem,
    MCTvBatchEnrichmentResponse,
    MCTvItem,
    MCTvSeasonRuntimeResponse,
    TMDBSearchGenreResponse,
    TMDBSearchMovieResponse,
    TMDBSearchMultiResponse,
    TMDBSearchTVResponse,
)
from api.tmdb.person import TMDBPersonService
from api.tmdb.search import TMDBSearchService
from api.tmdb.search_with_credits import search_person_with_credits
from api.tmdb.tmdb_models import TMDBProvidersResponse
from api.tmdb.trending import get_trending_movies, get_trending_tv_shows
from contracts.models import (
    MCBaseItem,
    MCPersonSearchRequest,
    MCPersonSearchResponse,
    MCSearchResponse,
    MCSources,
    MCType,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)


async def _get_tv_network_name(tmdb_id: int, watch_region: str) -> str | None:
    try:
        service = TMDBService()
        tv_details = await service.get_media_details(
            tmdb_id=tmdb_id,
            media_type=MCType.TV_SERIES,
            include_cast=False,
            include_videos=False,
            include_watch_providers=False,
            include_keywords=False,
        )
        network_name = getattr(tv_details, "network", None)
        return network_name if isinstance(network_name, str) and network_name.strip() else None
    except Exception:
        logger.debug(
            "Unable to fetch TV network for streaming provider resolution",
            extra={"tmdb_id": tmdb_id, "watch_region": watch_region},
        )
        return None


"""
Trending Wrappers
"""


@RedisCache.use_cache(TMDBFunctionCache, prefix="trending_wrapper")
async def get_trending_async(
    limit: int = 50,
    media_type: MCType = MCType.TV_SERIES,
    **kwargs: Any,
) -> MCGetTrendingMovieResult | MCGetTrendingShowResult:
    """Async wrapper for getting trending content.

    Args:
        limit: Maximum number of results
        media_type: MCType enum (MCType.MOVIE or MCType.TV_SERIES). Default is MCType.TV_SERIES.
                    Other values will return an error.
        **kwargs: Additional arguments

    Returns:
        MCGetTrendingMovieResult or MCGetTrendingShowResult - MCSearchResponse derivative
    """
    try:
        if media_type == MCType.MOVIE:
            response: (
                MCGetTrendingMovieResult | MCGetTrendingShowResult
            ) = await get_trending_movies(limit=limit, **kwargs)
        elif media_type == MCType.TV_SERIES:
            response = await get_trending_tv_shows(limit=limit, **kwargs)
        elif media_type == MCType.MIXED:
            """
            This is only for cache warmup purposes.
            """
            response = await get_trending_tv_shows(limit=limit, **kwargs)
            movie_response = await get_trending_tv_shows(limit=limit, **kwargs)
            response.results = response.results + movie_response.results
            response.total_results = response.total_results + movie_response.total_results
            response.data_type = MCType.MIXED
            return response
        else:
            return MCGetTrendingMovieResult(
                results=[],
                total_results=0,
                query=None,
                data_source="get_trending_async",
                error="Invalid content type",
                status_code=400,
            )
        return response
    except Exception as e:
        logger.error(f"Error getting trending content: {e}")
        # Create error response - determine which response type based on media_type
        if media_type == MCType.MOVIE:
            return MCGetTrendingMovieResult(
                results=[],
                total_results=0,
                query=None,
                data_source="get_trending_async",
                error=str(e),
                status_code=500,
            )
        else:
            return MCGetTrendingShowResult(
                results=[],
                total_results=0,
                query=None,
                data_source="get_trending_async",
                error=str(e),
                status_code=500,
            )


@RedisCache.use_cache(TMDBFunctionCache, prefix="now_playing_wrapper")
async def get_now_playing_async(
    region: str = "US",
    limit: int = 50,
    sort_by_box_office: bool = False,
    **kwargs: Any,
) -> MCNowPlayingResponse:
    """Async wrapper for getting now playing movies.

    Args:
        region: Region code for theaters (e.g., 'US', 'CA', 'GB'). Default: 'US'
        limit: Maximum number of movies to return (default: 50, max: 50)
        sort_by_box_office: If True, sort movies by Comscore box office rankings (US only)
        **kwargs: Additional arguments passed to search service

    Returns:
        MCNowPlayingResponse - MCSearchResponse derivative
    """
    data_source = "TMDB Now Playing (theatrical releases)"
    try:
        service = TMDBSearchService()
        results = await service.get_now_playing(
            region=region,
            limit=limit,
            include_details=True,
            sort_by_box_office=sort_by_box_office,
        )

        return MCNowPlayingResponse(
            results=results,
            total_results=len(results),
            data_source=data_source,
        )

    except Exception as e:
        logger.error(f"Error getting now playing movies: {e}")
        return MCNowPlayingResponse(
            results=[],
            total_results=0,
            data_source=data_source,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="popular_tv_wrapper")
async def get_popular_tv_async(limit: int = 50, **kwargs: Any) -> MCPopularTVResponse:
    """Async wrapper for getting popular TV shows from the past year.

    Args:
        limit: Maximum number of results (default: 50, max: 50)
        **kwargs: Additional arguments passed to search service

    Returns:
        MCPopularTVResponse - MCSearchResponse derivative
    """

    try:
        service = TMDBSearchService()
        results = await service.get_popular_tv(limit=limit, include_details=True)

        return MCPopularTVResponse(
            results=results,
            total_results=len(results),
            data_source="TMDB Popular TV Shows (past year)",
        )

    except Exception as e:
        logger.error(f"Error getting popular TV shows: {e}")
        return MCPopularTVResponse(
            results=[],
            total_results=0,
            data_source="TMDB Popular TV Shows",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="tv_batch_enrichment_wrapper_v0.01")
async def get_tv_batch_enrichment_async(
    items: list[MCTvBatchEnrichmentRequestItem], **kwargs: Any
) -> MCTvBatchEnrichmentResponse:
    """Async wrapper for lightweight TV lifecycle enrichment by TMDB ID."""
    data_source = "TMDB TV Batch Enrichment"
    try:
        service = TMDBSearchService()
        results = await service.get_tv_batch_enrichment(items=items, **kwargs)

        return MCTvBatchEnrichmentResponse(
            items=results,
            total_results=len(results),
            data_source=data_source,
        )
    except Exception as e:
        logger.error(f"Error in get_tv_batch_enrichment_async: {e}")
        return MCTvBatchEnrichmentResponse(
            items=[],
            total_results=0,
            data_source=data_source,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="tv_season_runtime_wrapper")
async def get_tv_season_runtime_async(
    tmdb_id: int, season_number: int, **kwargs: Any
) -> MCTvSeasonRuntimeResponse:
    """Async wrapper for TMDB TV season runtime rollup."""
    data_source = "TMDB TV Season Runtime"
    try:
        service = TMDBSearchService()
        response = await service.get_tv_season_runtime(
            tmdb_id=tmdb_id,
            season_number=season_number,
            **kwargs,
        )
        response.data_source = data_source
        return response
    except Exception as e:
        logger.error(f"Error in get_tv_season_runtime_async: {e}")
        return MCTvSeasonRuntimeResponse(
            tmdb_id=tmdb_id,
            season_number=season_number,
            data_source=data_source,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="movie_now_playing_ids")
async def get_movie_now_playing_ids_async(region: str = "US") -> list[int]:
    """Return tmdb IDs currently in theaters from TMDB now-playing endpoint."""
    try:
        service = TMDBSearchService()
        results = await service.get_now_playing(
            region=region,
            limit=200,
            include_details=False,
            sort_by_box_office=False,
        )
        return [
            item.tmdb_id
            for item in results
            if hasattr(item, "tmdb_id") and isinstance(item.tmdb_id, int)
        ]
    except Exception as e:
        logger.error("Error getting now-playing movie IDs for region %s: %s", region, e)
        return []


"""
Search Wrappers
"""


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_wrapper")
async def search_multi_async(
    query: str, page: int = 1, limit: int = 20, **kwargs: Any
) -> TMDBSearchMultiResponse:
    """Async wrapper for multi search with keyword syntax support.

    Args:
        query: Search query (may contain keyword: syntax)
        page: Page number
        limit: Maximum results
        **kwargs: Additional arguments

    Returns:
        TMDBSearchMultiResponse - MCSearchResponse derivative
    """

    try:
        service = TMDBSearchService()
        results = await service._search_with_keywords(query, page, limit)

        # Convert MCSearchResponse to TMDBSearchMultiResponse
        return TMDBSearchMultiResponse(
            results=results.results,  # type: ignore[arg-type]
            total_results=results.total_results,
            page=results.page,
            query=results.query,
            data_type=results.data_type or MCType.MIXED,
            data_source=results.data_source or "TMDB Multi Search",
            error=results.error,
            status_code=results.status_code,
            metrics=results.metrics,
        )

    except Exception as e:
        logger.error(f"Error searching content: {e}")
        return TMDBSearchMultiResponse(
            results=[],
            total_results=0,
            page=page,
            query=query,
            data_type=MCType.MIXED,
            data_source="TMDB Multi Search",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_tv_shows_wrapper")
async def search_tv_shows_async(
    query: str,
    page: int = 1,
    limit: int = 50,
    enrich: bool = False,
    **kwargs: Any,
) -> TMDBSearchTVResponse:
    """Async wrapper for TV show search with weighted sorting.

    Args:
        query: Search query string (required)
        page: Page number for pagination (default: 1)
        limit: Maximum number of results (default: 50, max: 50)
        no_cache: If True, bypass cache (default: False)
        **kwargs: Additional arguments passed to search service

    Returns:
        TMDBSearchTVResponse - MCSearchResponse derivative
    """

    try:
        service = TMDBSearchService()

        results = await service.search_tv_shows(
            query, page, limit, enrich=enrich, num_to_enrich=limit, **kwargs
        )

        # Convert MCSearchResponse to TMDBSearchTVResponse
        return TMDBSearchTVResponse(
            results=results.results,  # type: ignore[arg-type]
            total_results=results.total_results,
            page=results.page,
            query=results.query,
            data_type=results.data_type or MCType.TV_SERIES,
            data_source=results.data_source or "TMDB TV Search",
            error=results.error,
            status_code=results.status_code,
            metrics=results.metrics,
        )

    except Exception as e:
        logger.error(f"Error searching TV shows: {e}")
        return TMDBSearchTVResponse(
            results=[],
            total_results=0,
            page=page,
            query=query,
            data_type=MCType.TV_SERIES,
            data_source="TMDB TV Search",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_movies_wrapper")
async def search_movies_async(
    query: str,
    page: int = 1,
    limit: int = 50,
    num_with_extended: int = 0,
    num_with_full: int = 0,
    **kwargs: Any,
) -> TMDBSearchMovieResponse:
    """Async wrapper for movie search with weighted sorting.

    Args:
        query: Search query string (required)
        page: Page number for pagination (default: 1)
        limit: Maximum number of results (default: 50, max: 50)
        no_cache: If True, bypass cache (default: False)
        **kwargs: Additional arguments passed to search service

    Returns:
        TMDBSearchMovieResponse - MCSearchResponse derivative
    """

    try:
        service = TMDBSearchService()

        results = await service.search_movies(
            query=query,
            page=page,
            limit=limit,
            num_with_extended=num_with_extended,
            num_with_full=num_with_full,
            **kwargs,
        )

        # Convert MCSearchResponse to TMDBSearchMovieResponse
        return TMDBSearchMovieResponse(
            results=results.results,  # type: ignore[arg-type]
            total_results=results.total_results,
            page=results.page,
            query=results.query,
            data_type=results.data_type or MCType.MOVIE,
            data_source=results.data_source or "TMDB Movie Search",
            error=results.error,
            status_code=results.status_code,
            metrics=results.metrics,
        )

    except Exception as e:
        logger.error(f"Error searching movies: {e}")
        return TMDBSearchMovieResponse(
            results=[],
            total_results=0,
            page=page,
            query=query,
            data_type=MCType.MOVIE,
            data_source="TMDB Movie Search",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_by_genre_wrapper")
async def search_by_genre_async(
    genre_ids: str,
    page: int = 1,
    limit: int = 50,
    include_details: bool = True,
    media_type: MCType = MCType.MIXED,
    **kwargs: Any,
) -> TMDBSearchGenreResponse:
    """Async wrapper for searching movies and TV shows by genre IDs.

    Args:
        genre_ids: Comma-separated genre IDs (e.g., "18,80" for Drama and Crime)
        page: Page number for pagination (default: 1)
        limit: Maximum number of results (default: 50, max: 50)
        include_details: If True, include watch providers, cast, videos, and keywords (default: True)
        **kwargs: Additional arguments passed to search service

    Returns:
        TMDBSearchGenreResponse - MCSearchResponse derivative
    """

    try:
        service = TMDBSearchService()

        # Initialize empty responses
        movie_results: MCSearchResponse = MCSearchResponse(
            results=[], total_results=0, page=page, query=""
        )
        tv_results: MCSearchResponse = MCSearchResponse(
            results=[], total_results=0, page=page, query=""
        )

        if media_type == MCType.MOVIE or media_type == MCType.MIXED:
            movie_results = await service.search_movie_by_genre(
                genre_ids=genre_ids,
                page=page,
                limit=limit,
                include_details=include_details,
                **kwargs,
            )
        if media_type == MCType.TV_SERIES or media_type == MCType.MIXED:
            tv_results = await service.search_tv_by_genre(
                genre_ids=genre_ids,
                page=page,
                limit=limit,
                include_details=include_details,
                **kwargs,
            )

        # Convert MCSearchResponse to TMDBSearchGenreResponse
        return TMDBSearchGenreResponse(
            results=movie_results.results + tv_results.results,  # type: ignore[arg-type]
            total_results=movie_results.total_results + tv_results.total_results,
            page=movie_results.page,
            query=f"genre search using: genre ids: {genre_ids}",
            data_type=media_type,
            data_source="TMDB Search by Genres",
            error=movie_results.error or tv_results.error,
            status_code=movie_results.status_code or tv_results.status_code,
            metrics=movie_results.metrics or tv_results.metrics,
        )

    except Exception as e:
        logger.error(f"Error searching by genre: {e}")
        return TMDBSearchGenreResponse(
            results=[],
            total_results=0,
            page=page,
            query=f"genre ids: {genre_ids}",
            data_type=MCType.MIXED,
            data_source="TMDB Search by Genres",
            error=str(e),
            status_code=500,
        )


"""
Utilities Wrappers
"""


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_keywords_wrapper")
async def find_keywords_async(query: str, page: int = 1) -> MCKeywordSearchResponse:
    """Async wrapper for searching keywords.

    Args:
        query: Search query
        page: Page number

    Returns:
        MCKeywordSearchResponse - MCSearchResponse derivative
    """
    try:
        service = TMDBSearchService()
        result = await service.search_keywords(query, page)

        return result
    except Exception as e:
        logger.error(f"Error searching keywords: {e}")
        return MCKeywordSearchResponse(
            results=[],
            total_results=0,
            total_pages=0,
            page=page,
            query=query,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_genres_wrapper")
async def find_genres_async(query: str = "", page: int = 1) -> MCGenreSearchResponse:
    """Async wrapper for getting all genres.

    Args:
        query: Search query (not used, kept for compatibility)
        page: Page number (not used, kept for compatibility)

    Returns:
        MCGenreSearchResponse - MCSearchResponse derivative
    """
    try:
        service = TMDBService()
        genres = await service.find_genres_async()

        from api.tmdb.models import MCGenreItem

        genre_items = [MCGenreItem(id=g.id, name=g.name) for g in genres]

        return MCGenreSearchResponse(
            results=genre_items,
            total_results=len(genre_items),
            total_pages=1,
            page=page,
            query=query,
            error=None,
            status_code=200,
        )
    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        return MCGenreSearchResponse(
            results=[],
            total_results=0,
            total_pages=0,
            page=page,
            query=query,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_by_keywords_wrapper")
async def search_by_keywords_async(
    keyword_ids: str,
    page: int = 1,
    limit: int = 50,
    include_details: bool = True,
    **kwargs: Any,
) -> TMDBSearchGenreResponse:
    """Async wrapper for searching movies and TV shows by keyword IDs.

    Args:
        keyword_ids: Comma-separated keyword IDs (e.g., "825,1721" for space opera keywords)
        page: Page number for pagination (default: 1)
        limit: Maximum number of results (default: 50, max: 50)
        include_details: If True, include watch providers, cast, videos, and keywords (default: True)
        **kwargs: Additional arguments passed to search service

    Returns:
        TMDBSearchGenreResponse - MCSearchResponse derivative (reusing genre response model for mixed results)
    """
    try:
        service = TMDBSearchService()
        result = await service.search_by_keywords(
            keyword_ids=keyword_ids,
            page=page,
            limit=limit,
            include_details=include_details,
            **kwargs,
        )

        # Convert MCSearchResponse to TMDBSearchGenreResponse (for mixed results)
        return TMDBSearchGenreResponse(
            results=result.results,  # type: ignore[arg-type]
            total_results=result.total_results,
            page=result.page,
            query=result.query or f"keyword ids: {keyword_ids}",
            data_type=result.data_type or MCType.MIXED,
            data_source=result.data_source or "TMDB Search by Keywords",
            error=result.error,
            status_code=result.status_code,
            metrics=result.metrics,
        )
    except Exception as e:
        logger.error(f"Error searching by keywords: {e}")
        return TMDBSearchGenreResponse(
            results=[],
            total_results=0,
            page=page,
            query=f"keyword ids: {keyword_ids}",
            data_type=MCType.MIXED,
            data_source="TMDB Search by Keywords",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="details_wrapper")
async def get_media_details_async(
    tmdb_id: int, content_type: str, **kwargs: Any
) -> MCBaseMediaItem:
    """Async wrapper for getting media details.

    Args:
        tmdb_id: TMDB ID
        content_type: 'movie' or 'tv'
        **kwargs: Additional arguments

    Returns:
        TMDBMovieItem or TMDBTvItem
    """

    try:
        # Preserve force-refresh intent for downstream calls while still
        # allowing Redis decorator-level no_cache handling.
        force_refresh = bool(kwargs.pop("force_refresh", False))
        if force_refresh:
            kwargs["no_cache"] = True

        # Convert content_type string to MCType enum
        if content_type == "tv":
            mc_type = MCType.TV_SERIES
        elif content_type == "movie":
            mc_type = MCType.MOVIE
        else:
            # Default to movie for unknown types
            mc_type = MCType.MOVIE

        service = TMDBSearchService()
        details = await service.get_media_details(
            tmdb_id=tmdb_id,
            media_type=mc_type,
            include_cast=True,
            include_videos=True,
            include_watch_providers=True,
            include_keywords=True,
            **kwargs,
        )

        if isinstance(details, MCTvItem):
            details.apply_episode_availability_rules()

        if details.error:
            # Return error dict with proper structure
            return details  # type: ignore[no-any-return]

        # Convert MCBaseMediaItem to dict for Firebase function response
        # details is already a MCBaseMediaItem derivative (MCMovieItem or MCTvItem)
        return details  # type: ignore[no-any-return]

    except Exception as e:
        logger.error(f"Error getting media details: {e}")
        # Return error dict with proper structure
        mc_type = MCType.TV_SERIES if content_type == "tv" else MCType.MOVIE
        return MCBaseMediaItem(
            mc_type=mc_type,
            tmdb_id=tmdb_id,
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="get_watch_providers_for_title_wrapper")
async def get_watch_providers_for_title_async(
    tmdb_id: int, content_type: str, region: str = "US"
) -> dict[str, Any]:
    """
    Get watch providers (streaming, rent, buy) for a specific movie or TV show.

    Args:
        tmdb_id: TMDB ID of the movie or TV show
        content_type: "movie" or "tv"
        region: Region code (default "US")

    Returns:
        Dict with watch_providers (flatrate, rent, buy arrays) and streaming_platform
    """
    try:
        service = TMDBService()

        # Map content_type to MCType
        if content_type == "movie":
            media_type = MCType.MOVIE
        elif content_type == "tv":
            media_type = MCType.TV_SERIES
        else:
            return {"error": f"Invalid content_type: {content_type}"}

        # Call the internal method to get watch providers
        result = await service._get_watch_providers(tmdb_id, media_type, region)

        if not result:
            return {
                "tmdb_id": tmdb_id,
                "content_type": content_type,
                "watch_providers": {},
                "streaming_platform": None,
            }

        return {
            "tmdb_id": tmdb_id,
            "content_type": content_type,
            "watch_providers": result.get("watch_providers", {}),
            "streaming_platform": result.get("streaming_platform"),
        }

    except Exception as e:
        logger.error(f"Error getting watch providers for {content_type} {tmdb_id}: {e}")
        return {
            "tmdb_id": tmdb_id,
            "content_type": content_type,
            "error": str(e),
        }


@RedisCache.use_cache(TMDBContentRatingCache, prefix="content_rating")
async def get_content_rating_async(
    tmdb_id: int, region: str = "US", content_type: str = "tv"
) -> dict[str, str | None] | None:
    """
    Get content rating (and release date when available) for a movie or TV title.

    Args:
        tmdb_id: TMDB movie/TV ID.
        region: Region code (default "US").
        content_type: "tv" or "movie" (default "tv").

    Returns:
        Dict with rating and release_date, or None if not found.
    """
    try:
        normalized_content_type = content_type.strip().lower() if content_type else "tv"
        if normalized_content_type not in {"movie", "tv"}:
            raise ValueError(f"Invalid content_type: {content_type}")

        service = TMDBService()
        return await service.get_content_rating(
            tmdb_id=tmdb_id,
            region=region,
            media_type=normalized_content_type,
        )
    except Exception as error:
        logger.error(
            "Error getting content rating for %s in region %s: %s",
            tmdb_id,
            region,
            error,
        )
        return None


@RedisCache.use_cache(TMDBFunctionCache, prefix="get_tv_providers_wrapper")
async def get_providers_async(
    media_type: MCType, region: str = "US", **kwargs: Any
) -> TMDBProvidersResponse:
    """
    Get list of available TV streaming providers from TMDB.

    Args:
        region: Region code (default "US")
        **kwargs: Additional arguments

    Returns:
        List of TV providers sorted by display_priority or None if not found
    """
    try:
        service = TMDBService()
        providers = await service.get_providers(media_type, region, **kwargs)

        if providers.error:
            logger.error(f"Error getting providers for region {region}: {providers.error}")
            return providers  # type: ignore[no-any-return]

        return providers  # type: ignore[no-any-return]

    except Exception as e:
        logger.error(f"Error getting providers for region {region}: {e}")
        list_type = "tv" if media_type == MCType.TV_SERIES else "movie"
        return TMDBProvidersResponse(
            list_type=list_type,  # type: ignore[arg-type]
            results=[],
            mc_type=MCType.PROVIDERS_LIST,
            error=str(e),
            status_code=500,
        )


"""
Person Wrappers
"""


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_people")
async def search_people_async(query: str, page: int = 1, limit: int = 20) -> MCSearchResponse:
    """Search for people/actors using TMDB's person search endpoint.

    Args:
        query: Search query
        page: Page number for pagination
        limit: Maximum number of results per page to return

    Returns:
        MCSearchResponse with results of type MCPersonItem
        Status code: 200 for success, 400/404/500 for errors
    """
    if not query.strip():
        return MCSearchResponse(
            results=[],
            total_results=0,
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            error="Search query is required",
            status_code=400,
        )

    try:
        service = TMDBPersonService()
        result = await service.search_people(query, page, limit)

        # Filter out writers - they should come from OpenLibrary instead
        filtered_results = [
            person for person in result.results if person.known_for_department != "Writing"
        ]

        logger.info(
            f"TMDB person search for '{query}': {len(result.results)} total, "
            f"{len(result.results) - len(filtered_results)} writers filtered out, "
            f"{len(filtered_results)} remaining"
        )

        # Convert TMDBSearchPersonResult to MCSearchResponse
        return MCSearchResponse(
            results=filtered_results,  # type: ignore[arg-type]
            total_results=len(filtered_results),
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
        )

    except Exception as e:
        logger.error(f"Error in person search for '{query}': {e}")
        return MCSearchResponse(
            results=[],
            total_results=0,
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="person_details")
async def get_person_details_async(
    person_id: int, limit: int | None = None
) -> MCPersonDetailsResponse:
    """Get person details for a person.

    Args:
        person_id: TMDB person ID
        limit: Unused parameter (kept for API compatibility)

    Returns:
        MCPersonDetailsResponse
        Status code: 200 for success, 400/404/500 for errors
    """
    try:
        service = TMDBPersonService()
        details = await service.get_person_details(person_id)
        if not details:
            return MCPersonDetailsResponse(
                results=[],
                total_results=0,
                data_source="TMDB Person Details",
                error=f"Person with ID {person_id} not found",
                status_code=404,
            )

        return MCPersonDetailsResponse(
            results=[details],
            total_results=1,
            data_source="TMDB Person Details",
        )
    except Exception as e:
        logger.error(f"Error getting person details for {person_id}: {e}")
        return MCPersonDetailsResponse(
            results=[],
            total_results=0,
            data_source="TMDB Person Details",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="person_credits")
async def get_person_credits_async(
    person_id: int, limit: int | None = None
) -> MCPersonCreditsResponse:
    """Get person credits (movies and TV shows) for a person.

    Args:
        person_id: TMDB person ID
        limit: Maximum number of credits to return per type (movies/TV)

    Returns:
        MCPersonCreditsResponse
        Status code: 200 for success, 400/404/500 for errors
    """
    try:
        service = TMDBPersonService()
        # Handle None limit by using default or None
        credit_limit = limit if limit is not None else 50
        credit_response = await service.get_person_credits(person_id, limit=credit_limit)

        if not credit_response:
            return MCPersonCreditsResponse(
                results=[],
                total_results=0,
                data_source="TMDB Person Credits",
                error="Person credits not found",
                status_code=404,
            )

        return MCPersonCreditsResponse(
            results=[credit_response],
            total_results=credit_response.metadata.get("total_results", 0),
            data_source="TMDB Person Credits",
        )
    except Exception as e:
        logger.error(f"Error getting person credits for {person_id}: {e}")
        from api.tmdb.models import MCPersonCreditsResult

        error_result = MCPersonCreditsResult(
            person=None,
            movies=[],
            tv_shows=[],
            metadata={"error": str(e)},
        )
        return MCPersonCreditsResponse(
            results=[error_result],
            total_results=0,
            data_source="TMDB Person Credits",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="person_movie_credits")
async def get_person_movie_credits_async(person_id: int, **kwargs: Any) -> MCPersonCreditsResponse:
    """Get movie credits for a person.

    Args:
        person_id: TMDB person ID
        **kwargs: Additional arguments

    Returns:
        MCPersonCreditsResponse with movie credits
        Status code: 200 for success, 400/404/500 for errors
    """
    try:
        service = TMDBPersonService()
        credits_result = await service.fetch_movie_credits(person_id)

        return MCPersonCreditsResponse(
            results=[credits_result],
            total_results=credits_result.metadata.get("total_results", len(credits_result.movies)),
            data_source="TMDB Person Movie Credits",
        )
    except Exception as e:
        logger.error(f"Error getting person movie credits for {person_id}: {e}")
        from api.tmdb.models import MCPersonCreditsResult

        error_result = MCPersonCreditsResult(
            person=None,
            movies=[],
            tv_shows=[],
            metadata={"error": str(e)},
        )
        return MCPersonCreditsResponse(
            results=[error_result],
            total_results=0,
            data_source="TMDB Person Movie Credits",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="person_tv_credits")
async def get_person_tv_credits_async(
    person_id: int, limit: int | None = None, **kwargs: Any
) -> MCPersonCreditsResponse:
    """Get TV credits for a person.

    Args:
        person_id: TMDB person ID
        limit: Maximum number of TV credits to return (default: 50)
        **kwargs: Additional arguments

    Returns:
        MCPersonCreditsResponse with TV credits
        Status code: 200 for success, 400/404/500 for errors
    """
    try:
        service = TMDBPersonService()
        credit_limit = limit if limit is not None else 50
        credits_result = await service.fetch_tv_credits(person_id, limit=credit_limit)

        return MCPersonCreditsResponse(
            results=[credits_result],
            total_results=credits_result.metadata.get(
                "total_results", len(credits_result.tv_shows)
            ),
            data_source="TMDB Person TV Credits",
        )
    except Exception as e:
        logger.error(f"Error getting person TV credits for {person_id}: {e}")
        from api.tmdb.models import MCPersonCreditsResult

        error_result = MCPersonCreditsResult(
            person=None,
            movies=[],
            tv_shows=[],
            metadata={"error": str(e)},
        )
        return MCPersonCreditsResponse(
            results=[error_result],
            total_results=0,
            data_source="TMDB Person TV Credits",
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="search_person_works")
async def search_person_async(
    request: "MCPersonSearchRequest",
    media_type: MCType = MCType.MIXED,
    limit: int | None = None,
) -> "MCPersonSearchResponse":
    """Search for person works (movies and TV shows) based on person search request.

    This wrapper is called internally by the search_broker, not exposed as a direct endpoint.

    Args:
        request: MCPersonSearchRequest with person identification details
        limit: Maximum number of works to return per type (default: 50)

    Returns:
        MCPersonSearchResponse with person details and works
        - details: MCPersonItem (person details)
        - works: list[MCMovieItem | MCTvItem] (combined movies and TV shows)
        - related: [] (empty, will be filled by search_broker)
    """

    try:
        # Validate that this is a TMDB person
        if request.source != MCSources.TMDB:
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=f"Invalid source for TMDB person search: {request.source}",
                status_code=400,
            )

        # Validate source_id (TMDB uses positive integer IDs)
        person_id_str = request.source_id
        try:
            person_id = int(person_id_str)
        except ValueError:
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=f"Invalid source_id for TMDB person: {request.source_id} (must be a number)",
                status_code=404,
            )

        if person_id <= 0:
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=f"Invalid source_id for TMDB person: {request.source_id} (must be positive)",
                status_code=400,
            )

        # Get person credits (includes person details, movies, and TV shows)
        credit_limit = limit if limit is not None else 50
        credits_response = await get_person_credits_async(person_id, limit=credit_limit)

        if credits_response.status_code != 200 or not credits_response.results:
            return MCPersonSearchResponse(
                input=request,
                details=None,
                works=[],
                related=[],
                error=credits_response.error or "Person not found",
                status_code=credits_response.status_code or 404,
            )

        # Extract person details and works from credits response
        credits_result = credits_response.results[0]

        # Combine movies and TV shows into works array
        works: list[MCBaseItem] = []
        works.extend(credits_result.movies)  # MCMovieItem | MCMovieCreditMediaItem
        works.extend(credits_result.tv_shows)  # MCTvItem | MCTvCreditMediaItem

        # Return response with person details and works
        # related will be filled by search_broker
        return MCPersonSearchResponse(
            input=request,
            details=credits_result.person,  # MCPersonItem
            works=works,  # list[MCMovieItem | MCTvItem]
            related=[],  # Will be filled by search_broker
            status_code=200,
        )

    except Exception as e:
        logger.error(f"Error searching person works for {request.name}: {e}")
        return MCPersonSearchResponse(
            input=request,
            details=None,
            works=[],
            related=[],
            error=str(e),
            status_code=500,
        )


@RedisCache.use_cache(TMDBFunctionCache, prefix="person_search_full_v0.01")
async def person_search_full(
    query: str, mc_type: MCType = MCType.MIXED, **kwargs: Any
) -> MCSearchResponse:
    """Search for people by name with exact matching and conditional credit fetching.

        This wrapper function delegates to search_person_with_credits in person_composite.py.

        This function:
        1. Searches TMDB for people matching the query
        2. Normalizes both the query and result names (lowercase, remove special chars)
        3. Returns only exact matches
        4. If exactly one match, fetches detailed person information and credit counts
           based on mc_type:
           - MCType.TV_SERIES: only fetch TV credits
           - MCType.MOVIE: only fetch movie credits
           - MCType.MIXED: fetch both movie and TV credits

        Args:
            query: The search query (person name) - REQUIRED
            mc_type: MCType enum (MCType.TV_SERIES, MCType.MOVIE, or MCType.MIXED)
                     Determines which credits to fetch. Default is MCType.MIXED.
            **kwargs: Additional arguments (unused, kept for API compatibility)

    Returns:
            MCSearchResponse with person results (empty array if no exact matches)
            Status code: 200 for success, 400/500 for errors
    """
    return await search_person_with_credits(query, mc_type)
