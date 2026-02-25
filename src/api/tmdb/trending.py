"""
TMDB Firebase Functions Handlers
HTTP endpoint handlers for TMDB-related Firebase Functions.
"""

import asyncio
from typing import Any, Union, cast

from api.subapi.flixpatrol.wrappers import flixpatrol_wrapper
from api.tmdb.core import TMDBFunctionCache
from api.tmdb.models import (
    MCBaseMediaItem,
    MCGetTrendingMovieResult,
    MCGetTrendingShowResult,
    MCMovieItem,
    MCTvItem,
)
from api.tmdb.search import tmdb_search_service
from api.tmdb.tmdb_models import (
    TMDBSearchMovie,
    TMDBSearchTv,
)
from contracts.models import MCSearchResponse, MCType
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)


async def get_trending_tv_shows(
    limit: int = 60,
    **kwargs: Any,
) -> MCGetTrendingShowResult:
    """
    Get trending TV shows using FlixPatrol data enriched with TMDB metadata.

    Args:
        limit: Maximum number of TV shows to return (default: 60)

    Returns:
        MCGetTrendingShowResult with enriched TV show results

    Usage:
        result = await get_trending_tv_shows(limit=30)
    """
    try:
        # Get trending data from FlixPatrol
        flixpatrol_data = await flixpatrol_wrapper.get_flixpatrol_data(**kwargs)

        # Check for errors
        if flixpatrol_data.status_code != 200 or flixpatrol_data.error:
            error_msg = flixpatrol_data.error or "Unknown error"
            raise Exception(f"FlixPatrol error: {error_msg}")

        # Extract FlixPatrol items (just titles and rankings)
        flixpatrol_tv_shows = [item.model_dump() for item in flixpatrol_data.top_trending_tv_shows]

        # Enrich with TMDB data (with error handling and caching)

        trending_tv_shows = await _enrich_flixpatrol_with_tmdb(
            flixpatrol_tv_shows, MCType.TV_SERIES, limit, **kwargs
        )

    except Exception as tv_error:
        logger.warning(f"Error enriching TV shows: {tv_error}")
        try:
            trending_tv_shows = await _get_tmdb_trending(media_type=MCType.TV_SERIES, limit=limit)
        except Exception as fallback_error:
            logger.error(f"Error in fallback TMDB trending: {fallback_error}")
            trending_tv_shows = []

    return MCGetTrendingShowResult(
        results=trending_tv_shows,
        total_results=len(trending_tv_shows),
        query=f"limit:{limit}",
        data_source=f"top_trending_tv_shows({limit})",
    )


async def get_trending_movies(limit: int = 60, **kwargs) -> MCGetTrendingMovieResult:
    """
    Get trending movies using FlixPatrol data enriched with TMDB metadata.

    Args:
        limit: Maximum number of movies to return (default: 60)

    Returns:
        MCGetTrendingMovieResult with enriched movie results

    Usage:
        result = await get_trending_movies(limit=30)
    """
    try:
        # Get trending data from FlixPatrol
        flixpatrol_data = await flixpatrol_wrapper.get_flixpatrol_data(**kwargs)

        # Check for errors
        if flixpatrol_data.status_code != 200 or flixpatrol_data.error:
            error_msg = flixpatrol_data.error or "Unknown error"
            raise Exception(f"FlixPatrol error: {error_msg}")

        # Extract FlixPatrol items (just titles and rankings)
        flixpatrol_movies = [item.model_dump() for item in flixpatrol_data.top_trending_movies]

        # Enrich with TMDB data (with error handling and caching)
        trending_movies = await _enrich_flixpatrol_with_tmdb(
            flixpatrol_movies, MCType.MOVIE, limit, **kwargs
        )

    except Exception as movie_error:
        logger.warning(f"Error using flixpatrol for movies: {movie_error}")
        try:
            trending_movies = await _get_tmdb_trending(media_type=MCType.MOVIE, limit=limit)
        except Exception as fallback_error:
            logger.error(f"Error in fallback TMDB trending: {fallback_error}")
            trending_movies = []
    return MCGetTrendingMovieResult(
        results=trending_movies,
        total_results=len(trending_movies),
        query=f"limit:{limit}",
        data_source=f"top_trending_movies({limit})",
    )


@RedisCache.use_cache(TMDBFunctionCache, prefix="_enrich_item")
async def _enrich_item(
    fp_item: dict, media_type: MCType, **kwargs
) -> Union[MCMovieItem, MCTvItem, None]:
    """
    Enrich a single FlixPatrol item with TMDB metadata by searching TMDB.
    Uses title similarity validation to avoid false matches.

    Args:
        fp_item: FlixPatrol item dict with at least a 'title' field
        media_type: MCType enum (MCType.TV_SERIES or MCType.MOVIE)
        **kwargs: Additional keyword arguments (for cache compatibility)

    Returns:
        MCBaseMediaItem if a match is found, None otherwise
    """
    title = fp_item.get("title")
    if not title:
        return None

    try:
        # Request top 3 results to find best match (instead of just 1)
        if media_type == MCType.TV_SERIES:
            tv_search_results: MCSearchResponse = await tmdb_search_service.search_tv_shows(
                query=title, page=1, limit=3, enrich=True, num_to_enrich=3, **kwargs
            )
            search_results = tv_search_results
        else:
            movie_search_results: MCSearchResponse = await tmdb_search_service.search_movies(
                query=title, page=1, limit=3, num_with_full=3, **kwargs
            )
            search_results = movie_search_results

        if not search_results or not search_results.results:
            # Not all FlixPatrol titles have TMDB matches - this is expected
            logger.debug(f"No TMDB match found for '{title}' (skipping)")
            return None

        # Normalize title for comparison (remove articles, lowercase)
        def normalize_title_for_match(text: str) -> str:
            if not text:
                return ""
            text_lower = text.lower().strip()
            for article in ["the ", "a ", "an "]:
                if text_lower.startswith(article):
                    text_lower = text_lower[len(article) :].strip()
            return text_lower

        query_normalized = normalize_title_for_match(title)

        # STEP 1: Check for EXACT matches first (highest priority)
        for result in search_results.results:
            if not isinstance(result, (MCMovieItem, MCTvItem)):
                continue

            result_title = result.title or result.name
            if not result_title:
                continue

            result_normalized = normalize_title_for_match(result_title)

            # Exact match found - return immediately
            if query_normalized == result_normalized:
                logger.debug(f"Exact match found: '{title}' -> '{result_title}'")
                return result

        # STEP 2: No exact match found, look for high-similarity matches
        # Minimum threshold of 0.90 to avoid false matches like "The Family Plan" matching "The Family Plan 2"
        MIN_SIMILARITY_THRESHOLD = 0.90
        best_match = None
        best_similarity = 0.0

        from utils.soft_comparison import _levenshtein_distance

        for result in search_results.results:
            if not isinstance(result, (MCMovieItem, MCTvItem)):
                continue

            result_title = result.title or result.name
            if not result_title:
                continue

            result_normalized = normalize_title_for_match(result_title)

            # Calculate similarity using Levenshtein distance
            distance = _levenshtein_distance(query_normalized, result_normalized)
            max_len = max(len(query_normalized), len(result_normalized))
            similarity = 1.0 - (distance / max_len) if max_len > 0 else 0.0

            # Check if this is the best match so far
            if similarity > best_similarity and similarity >= MIN_SIMILARITY_THRESHOLD:
                best_similarity = similarity
                best_match = result

        if best_match:
            logger.debug(
                f"High-similarity match: '{title}' -> '{best_match.title or best_match.name}' (similarity: {best_similarity:.2f})"
            )
            return best_match
        else:
            logger.debug(
                f"No sufficiently similar TMDB match found for '{title}' (best similarity: {best_similarity:.2f}, threshold: {MIN_SIMILARITY_THRESHOLD})"
            )
            return None

    except Exception as e:
        logger.warning(f"Error enriching '{title}': {e}")
        import traceback

        logger.debug(f"Traceback for '{title}': {traceback.format_exc()}")
        return None


@RedisCache.use_cache(TMDBFunctionCache, prefix="enrich_flixpatrol")
async def _enrich_flixpatrol_with_tmdb(
    flixpatrol_items: list[dict], media_type: MCType, limit: int, **kwargs: Any
) -> list[MCMovieItem | MCTvItem]:
    """
    Enrich FlixPatrol items (titles only) with full TMDB data.
    Uses parallel processing for better performance.

    Args:
        flixpatrol_items: List of FlixPatrol items with title, rank, score
        media_type: MCType enum (MCType.TV_SERIES or MCType.MOVIE)
        limit: Maximum number of items to return

    Returns:
        List of items enriched with full TMDB data
    """
    if not flixpatrol_items:
        return []

    # Request more than limit to account for failures
    items_to_process = flixpatrol_items[: min(int(limit * 1.2), len(flixpatrol_items))]
    # Check if no_cache is in kwargs and pass it through to _enrich_item
    no_cache = kwargs.get("no_cache", False)
    tasks = [_enrich_item(item, media_type, no_cache=no_cache) for item in items_to_process]
    # Batch process to avoid overwhelming rate limits
    # Each _enrich_item makes search requests, batch to stay under 35 req/sec limit
    results = await tmdb_search_service._batch_process(
        tasks, batch_size=20, delay_between_batches=0.2
    )

    # Filter out None results and exceptions, keep only successful enrichments
    if media_type == MCType.TV_SERIES:
        enriched_items = [
            result
            for result in results
            if result and not isinstance(result, Exception) and isinstance(result, MCTvItem)
        ]
    else:
        enriched_items = [
            result
            for result in results
            if result and not isinstance(result, Exception) and isinstance(result, MCMovieItem)
        ]

    # Deduplicate by mc_id (same title on multiple platforms will have same TMDB ID)
    # Keep first occurrence (highest score due to FlixPatrol sorting)
    seen_mc_ids = set()
    deduplicated_items = []
    for item in enriched_items:
        if item.mc_id not in seen_mc_ids:
            deduplicated_items.append(item)
            seen_mc_ids.add(item.mc_id)

    logger.info(
        f"Enriched {len(deduplicated_items)} unique {media_type} items from {len(flixpatrol_items)} FlixPatrol items (removed {len(enriched_items) - len(deduplicated_items)} duplicates)"
    )
    return deduplicated_items[:limit]


async def _get_tmdb_trending(
    media_type: MCType = MCType.TV_SERIES,
    time_window: str = "week",
    limit: int = 60,
    cast_limit: int = 5,
    include_details: bool = True,
) -> list[MCBaseMediaItem]:
    """
    Get trending content from TMDB with optional detailed information.
    Supports concurrent pagination to fetch more than 20 items.

    Args:
        media_type: MCType enum (MCType.TV_SERIES or MCType.MOVIE)
        time_window: 'day' or 'week'
        limit: Maximum number of results
        cast_limit: Maximum number of cast members to include (default: 5)
        include_details: Whether to include watch providers and cast data

    Returns:
        List of trending media items
    """
    try:
        if media_type == MCType.TV_SERIES:
            path = "tv"
        else:
            path = "movie"

        endpoint = f"trending/{path}/{time_window}"
        base_params = {"language": "en-US"}

        # Calculate how many pages we need (TMDB returns ~20 items per page)
        pages_needed = max(1, (limit + 19) // 20)  # Round up division
        pages_needed = min(pages_needed, 10)  # Limit to 10 pages (200 items max)

        # Create concurrent requests for all needed pages
        async def fetch_page(page_num: int) -> dict[str, Any] | None:
            params = {**base_params, "page": page_num}
            result = await tmdb_search_service._make_request(endpoint, params)
            return cast(dict[str, Any] | None, result)

        # Fetch pages in smaller batches to avoid connection pool exhaustion
        # This prevents timeout errors when many requests try to connect simultaneously
        batch_size = 3  # Fetch 3 pages at a time
        page_results = []
        for i in range(0, pages_needed, batch_size):
            batch_pages = range(i + 1, min(i + batch_size + 1, pages_needed + 1))
            batch_tasks = [fetch_page(page) for page in batch_pages]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            page_results.extend(batch_results)

            # Small delay between batches to let rate limiter recover
            if i + batch_size < pages_needed:
                await asyncio.sleep(0.1)

        # Combine all results from all pages and deduplicate by TMDB ID
        all_items = []
        seen_ids = set()

        for page_data in page_results:
            if isinstance(page_data, Exception):
                logger.warning(f"Error fetching trending page: {page_data}")
                continue

            if page_data and isinstance(page_data, dict) and "results" in page_data:
                for item in page_data["results"]:
                    tmdb_id = item.get("id")
                    if tmdb_id and tmdb_id not in seen_ids:
                        seen_ids.add(tmdb_id)
                        all_items.append(item)

                        # Stop when we have enough unique items
                        if len(all_items) >= limit:
                            break

                # Break outer loop if we have enough items
                if len(all_items) >= limit:
                    break

        # Process items into MCBaseMediaItem dicts
        processed_items: list[MCBaseMediaItem] = []

        # Normalize media_type to MCType enum for type-safe comparison
        for item in all_items:
            if media_type == MCType.TV_SERIES:
                mc_item: MCBaseMediaItem = MCTvItem.from_tv_search(
                    TMDBSearchTv.model_validate(item),
                    image_base_url=tmdb_search_service.image_base_url,
                )
            else:
                mc_item = MCMovieItem.from_movie_search(
                    TMDBSearchMovie.model_validate(item),
                    image_base_url=tmdb_search_service.image_base_url,
                )
            processed_items.append(mc_item)

        # Add detailed information if requested - fetch all in parallel
        if include_details:
            # Create tasks for all items with poster_path
            detail_tasks = []
            items_with_details = []

            for processed_item in [item for item in processed_items if item.poster_path]:
                # Convert media_type string to MCType enum
                if processed_item.media_type == "tv":
                    mc_type = MCType.TV_SERIES
                else:
                    mc_type = MCType.MOVIE
                detail_tasks.append(
                    tmdb_search_service.get_media_details(
                        processed_item.tmdb_id, mc_type, cast_limit=cast_limit
                    )
                )
                items_with_details.append(processed_item)

            # Fetch all details in batches to avoid overwhelming rate limits
            # Each get_media_details makes ~5 requests (1 basic + 4 enrichment)
            # Processing 10 items = ~50 requests per batch, but batching prevents bursts
            if detail_tasks:
                detailed_results = await tmdb_search_service._batch_process(
                    detail_tasks, batch_size=20, delay_between_batches=0.2
                )
                results = []
                for idx, detailed_item in enumerate(detailed_results):
                    if isinstance(detailed_item, Exception):
                        logger.warning(
                            f"Error fetching details for item {items_with_details[idx].tmdb_id}: {detailed_item}"
                        )
                        # Fall back to basic item if details fetch failed
                        results.append(items_with_details[idx])
                    elif detailed_item:
                        # Validate based on media type
                        if (
                            media_type == MCType.MOVIE
                            and not tmdb_search_service.is_vaild_movie(detailed_item)
                        ) or (
                            media_type == MCType.TV_SERIES
                            and not tmdb_search_service.is_vaild_tv(detailed_item)
                        ):
                            continue
                        results.append(detailed_item)
            else:
                results = processed_items
        else:
            results = processed_items

        logger.info(
            f"Retrieved {len(results)} trending {media_type} items from {pages_needed} pages"
        )
        return results

    except Exception as e:
        logger.error(f"Error getting TMDB trending: {e}")
        return []
