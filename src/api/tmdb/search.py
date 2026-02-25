"""
TMDB Search Service - Search operations for TMDB
Handles all search, trending, discovery, and keyword operations.
"""

import asyncio
import math
import re
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from api.subapi.comscore import comscore_wrapper
from api.tmdb.core import TMDBService
from api.tmdb.models import (
    MCBaseMediaItem,
    MCKeywordItem,
    MCKeywordSearchResponse,
    MCMovieItem,
    MCTvBatchEnrichmentItem,
    MCTvBatchEnrichmentRequestItem,
    MCTvItem,
    MCTvSeasonRuntimeEpisode,
    MCTvSeasonRuntimeResponse,
)
from api.tmdb.tmdb_models import (
    TMDBRawMultiSearchRawResponse,
    TMDBSearchMovie,
    TMDBSearchTv,
    TMDBSeasonDetailsResult,
    TMDBTvDetailsResult,
)
from contracts.models import MCBaseItem, MCSearchResponse, MCType
from utils.get_logger import get_logger
from utils.soft_comparison import _levenshtein_distance

logger = get_logger(__name__)


def _normalize_media_type_to_mctype(media_type: str) -> MCType:
    """Normalize TMDB media_type string to MCType enum.

    TMDB API returns "tv" or "movie" as strings. This function normalizes
    them to MCType enum values for type-safe comparisons.
    """
    # Map TMDB string values to MCType enum
    if media_type == "tv":
        return MCType.TV_SERIES
    elif media_type == "movie":
        return MCType.MOVIE
    elif media_type == MCType.TV_SERIES.value:
        return MCType.TV_SERIES
    elif media_type == MCType.MOVIE.value:
        return MCType.MOVIE
    else:
        # Default to movie for unknown types
        return MCType.MOVIE


class TMDBSearchService(TMDBService):
    """
    TMDB Search Service - Handles all search and discovery operations.
    Extends TMDBService with search-specific functionality.
    """

    async def get_now_playing(
        self,
        region: str = "US",
        limit: int = 50,
        include_details: bool = True,
        sort_by_box_office: bool = False,
        **kwargs,
    ) -> list[MCMovieItem]:
        """
        Get movies currently playing in theaters from TMDB.
        Supports concurrent pagination to fetch multiple pages efficiently.

        Args:
            region: Region code for theaters (e.g., 'US', 'CA', 'GB')
            limit: Maximum number of movies to return (default: 50)
            include_details: Whether to include watch providers, cast, videos, and keywords
            sort_by_box_office: If True, sort movies by Comscore box office rankings (US only)
            **kwargs: Additional arguments passed to media details enhancement

        Returns:
            List of MCMovieItem objects representing now playing movies
        """
        endpoint = "movie/now_playing"
        base_params = {"language": "en-US", "region": region}

        # Calculate how many pages we need
        pages_needed = max(1, (limit + 19) // 20)
        pages_needed = min(pages_needed, 10)

        # Create concurrent requests for all needed pages
        async def fetch_page(page_num: int) -> dict[str, Any] | None:
            params = {**base_params, "page": page_num}
            result = await self._make_request(endpoint, params)
            return cast(dict[str, Any] | None, result)

        # Fetch all pages concurrently
        page_tasks = [fetch_page(page) for page in range(1, pages_needed + 1)]
        page_results = await asyncio.gather(*page_tasks, return_exceptions=True)

        # Combine all results and deduplicate
        all_items = []
        seen_ids = set()

        for page_data in page_results:
            if isinstance(page_data, Exception):
                logger.warning(f"Error fetching now playing page: {page_data}")
                continue

            if page_data and isinstance(page_data, dict) and "results" in page_data:
                for item in page_data["results"]:
                    tmdb_id = item.get("id")
                    if tmdb_id and tmdb_id not in seen_ids and item.get("poster_path") is not None:
                        seen_ids.add(tmdb_id)
                        all_items.append(item)

                    if len(all_items) >= limit:
                        break

            if len(all_items) >= limit:
                break

        # Process items into MCBaseMediaItem dicts
        processed_items: list[MCMovieItem] = []
        for item in all_items:
            mc_item = MCMovieItem.from_movie_search(
                TMDBSearchMovie.model_validate(item), image_base_url=self.image_base_url
            )

            processed_items.append(mc_item)

        # Add detailed information if requested - fetch all in parallel
        if include_details:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in processed_items
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            results: list[MCMovieItem] = []
            for idx, detailed_item in enumerate(enhanced_results):
                if isinstance(detailed_item, Exception):
                    logger.warning(
                        f"Error enhancing item {processed_items[idx].tmdb_id}: {detailed_item}"
                    )
                    # Only append if it's a valid MCMovieItem
                    fallback_item = processed_items[idx]
                    if isinstance(fallback_item, MCMovieItem):
                        results.append(fallback_item)
                elif detailed_item is not None and isinstance(detailed_item, MCMovieItem):
                    if not detailed_item.overview or len(detailed_item.overview) == 0:
                        continue
                    detailed_item.streaming_platform = "In Theaters"
                    detailed_item.availability_type = "theatrical"

                    watch_providers = detailed_item.watch_providers
                    if watch_providers and watch_providers.get("buy"):
                        detailed_item.streaming_platform = "On Demand"
                        detailed_item.availability_type = "On Demand"
                    results.append(detailed_item)
        else:
            # Filter to only MCMovieItem since this method returns list[MCMovieItem]
            results = [x for x in processed_items if isinstance(x, MCMovieItem)]

        # Apply box office sorting if requested
        if sort_by_box_office:
            try:
                # Fetch box office rankings
                box_office_data = await comscore_wrapper.get_domestic_rankings()

                if box_office_data.status_code == 200 and box_office_data.error is None:
                    # Sort movies by box office rankings - filter to only MCMovieItem
                    movie_results_list = [x for x in results if isinstance(x, MCMovieItem)]
                    sorted_results = self.sort_movies_by_box_office(
                        movie_results_list, box_office_data
                    )
                    # sort_movies_by_box_office returns list[dict[str, Any]] - convert back to MCMovieItem
                    if isinstance(sorted_results, list) and len(sorted_results) > 0:
                        # Convert dicts back to MCMovieItem
                        results = cast(
                            list[MCMovieItem],
                            [
                                MCMovieItem.model_validate(m) if isinstance(m, dict) else m
                                for m in sorted_results
                                if isinstance(m, (MCMovieItem, dict))
                            ],
                        )
                    else:
                        results = []
                    logger.info(f"Applied Comscore box office sorting to {len(results)} movies")
                else:
                    logger.warning(
                        f"Failed to fetch box office rankings: status_code={box_office_data.status_code}, error={box_office_data.error or 'Unknown error'}"
                    )

            except Exception as sort_error:
                logger.error(f"Error applying box office sorting: {sort_error}")
                # Filter to only MCMovieItem for sorting by release_date
                movie_results = [x for x in results if isinstance(x, MCMovieItem)]
                results = sorted(
                    movie_results,
                    key=lambda x: datetime.fromisoformat(
                        x.release_date or "2025-01-01"
                    ).timestamp(),
                    reverse=True,
                )

            return results

        logger.info(f"Retrieved {len(results)} now playing movies from {pages_needed} pages")
        return results

    async def get_popular_tv(
        self, limit: int = 50, include_details: bool = True, **kwargs
    ) -> list[MCTvItem]:
        """
        Get popular TV shows from TMDB using the discover endpoint.

        Uses discover/tv with filters for:
        - US availability (watch_region=US)
        - Streaming availability (with_watch_monetization_types=flatrate|rent|buy)
        - Recent shows (first_air_date.gte = 1 year ago)
        - Quality filter (vote_count.gte=10)
        - English language content

        Args:
            limit: Maximum number of results (default: 50)
            include_details: Whether to include watch providers, cast, videos, and keywords
            **kwargs: Additional arguments passed to media details enhancement

        Returns:
            List of MCTvItem objects representing popular TV shows available in the US
        """
        # Calculate the date one year ago for filtering
        one_year_ago = datetime.now() - timedelta(days=365)
        cutoff_date = one_year_ago.strftime("%Y-%m-%d")

        # Use discover endpoint with US availability and streaming filters
        endpoint = "discover/tv"
        base_params = {
            "language": "en-US",
            "sort_by": "popularity.desc",
            "watch_region": "US",
            # Include flatrate (subscription), rent, and buy options
            "with_watch_monetization_types": "flatrate|rent|buy",
            "first_air_date.gte": cutoff_date,
            "vote_count.gte": 10,  # Quality filter - shows with at least 10 votes
            "with_original_language": "en",  # English language shows
        }

        # Calculate how many pages we need (20 results per page from discover)
        pages_needed = max(1, (limit + 19) // 20)
        pages_needed = min(pages_needed, 5)  # Cap at 5 pages (100 results max)

        logger.info(
            "get_popular_tv: Using discover endpoint with US streaming filter (cutoff: %s)",
            cutoff_date,
        )

        # Create concurrent requests for all needed pages
        async def fetch_page(page_num: int) -> dict[str, Any] | None:
            params = {**base_params, "page": page_num}
            result = await self._make_request(endpoint, params)
            return cast(dict[str, Any] | None, result)

        # Fetch all pages concurrently
        page_tasks = [fetch_page(page) for page in range(1, pages_needed + 1)]
        page_results = await asyncio.gather(*page_tasks, return_exceptions=True)

        # Combine results with deduplication
        all_items = []
        seen_ids = set()

        for page_data in page_results:
            if isinstance(page_data, Exception):
                logger.warning(f"Error fetching discover TV page: {page_data}")
                continue

            if page_data and isinstance(page_data, dict) and "results" in page_data:
                for item in page_data["results"]:
                    tmdb_id = item.get("id")
                    if tmdb_id and tmdb_id not in seen_ids and item.get("poster_path") is not None:
                        seen_ids.add(tmdb_id)
                        all_items.append(item)

        logger.info(
            "get_popular_tv: Discover returned %d unique items from %d pages",
            len(all_items),
            pages_needed,
        )

        # Limit to requested amount
        all_items = all_items[:limit]

        # Process results into MCTvItem objects
        processed_items: list[MCTvItem] = []
        for item in all_items:
            mc_item = MCTvItem.from_tv_search(
                TMDBSearchTv.model_validate(item), image_base_url=self.image_base_url
            )
            processed_items.append(mc_item)

        # Add detailed information if requested - fetch all in parallel
        if include_details:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in processed_items
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            results: list[MCTvItem] = []
            for idx, detailed_item in enumerate(enhanced_results):
                if isinstance(detailed_item, Exception):
                    logger.warning(
                        f"Error enhancing item {processed_items[idx].tmdb_id}: {detailed_item}"
                    )
                    # Fall back to basic item if enhancement fails
                    if isinstance(processed_items[idx], MCTvItem):
                        results.append(processed_items[idx])
                elif detailed_item is not None and isinstance(detailed_item, MCTvItem):
                    results.append(detailed_item)

            # Light validation - just check for name and overview (streaming already filtered by API)
            pre_filter_count = len(results)
            results = [
                item
                for item in results
                if (item.name or item.title)
                and item.overview
                and len(item.overview) > 0
                and item.poster_path is not None
            ]
            if pre_filter_count != len(results):
                logger.info(
                    "get_popular_tv: After validation filter: %d/%d items remain",
                    len(results),
                    pre_filter_count,
                )
        else:
            # Without details, only filter by overview and poster
            results = [
                item
                for item in processed_items
                if item.overview and len(item.overview) > 0 and item.poster_path is not None
            ]

        # Sort by popularity (highest first) - should already be sorted but ensure it
        results.sort(key=lambda x: x.popularity or 0, reverse=True)

        logger.info(
            "get_popular_tv: Returning %d popular TV shows (US streaming, past year)",
            len(results),
        )
        return results

    async def get_tv_batch_enrichment(
        self,
        items: list[MCTvBatchEnrichmentRequestItem],
        batch_size: int = 10,
        **kwargs: Any,
    ) -> list[MCTvBatchEnrichmentItem]:
        """Fetch lightweight TV lifecycle metadata for a batch of TMDB IDs."""
        if not items:
            return []

        async def enrich_item(
            item: MCTvBatchEnrichmentRequestItem,
        ) -> MCTvBatchEnrichmentItem:
            tmdb_id = item.tmdb_id()
            if tmdb_id is None or tmdb_id <= 0:
                return MCTvBatchEnrichmentItem.from_error(item, "Invalid mc_source_id")

            try:
                payload = await self._make_request(
                    f"tv/{tmdb_id}",
                    {"language": "en-US"},
                    no_cache=True,
                )
                if not payload:
                    return MCTvBatchEnrichmentItem.from_error(item, "TV details not found")

                details = TMDBTvDetailsResult.model_validate(payload)
                enriched = MCTvBatchEnrichmentItem.from_tv_details(item, details)

                # If show-level runtime is missing, aggregate from season runtimes concurrently.
                if enriched.runtime is None:
                    season_count = details.number_of_seasons
                    if season_count:
                        if season_count <= 0:
                            season_count = len([s for s in details.seasons if s.season_number > 0])
                        avg_runtime, _cume_runtime = await self._get_series_runtime_aggregate(
                            tmdb_id=tmdb_id,
                            num_seasons=season_count,
                            no_cache=kwargs.get("no_cache", False),
                        )
                        if avg_runtime is not None:
                            enriched.runtime = avg_runtime

                return enriched
            except Exception as exc:
                logger.warning(
                    "get_tv_batch_enrichment: Failed for tmdb_id=%s error=%s",
                    tmdb_id,
                    exc,
                )
                return MCTvBatchEnrichmentItem.from_error(item, str(exc))

        tasks = [enrich_item(item) for item in items]
        results = await self._batch_process(
            tasks, batch_size=batch_size, delay_between_batches=0.05
        )

        processed: list[MCTvBatchEnrichmentItem] = []
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                processed.append(MCTvBatchEnrichmentItem.from_error(items[index], str(result)))
            elif isinstance(result, MCTvBatchEnrichmentItem):
                processed.append(result)
            else:
                processed.append(
                    MCTvBatchEnrichmentItem.from_error(items[index], "Unknown enrichment error")
                )
        return processed

    @staticmethod
    def _coerce_positive_runtime(value: int | None) -> int | None:
        if isinstance(value, int) and value > 0:
            return value
        return None

    def _compute_season_runtime_metrics(
        self,
        runtimes: list[int | None],
    ) -> tuple[int | None, int]:
        """
        Compute season avg/cume while ignoring nulls for avg and backfilling null episodes with avg.
        """
        known_runtimes = [runtime for runtime in runtimes if runtime is not None]
        if not known_runtimes:
            return None, 0

        avg_runtime = round(sum(known_runtimes) / len(known_runtimes))
        missing_count = len(runtimes) - len(known_runtimes)
        cume_runtime = sum(known_runtimes) + (missing_count * avg_runtime)
        return avg_runtime, cume_runtime

    async def _get_series_runtime_aggregate(
        self,
        tmdb_id: int,
        num_seasons: int,
        **kwargs: Any,
    ) -> tuple[int | None, int]:
        """
        Aggregate runtime across all seasons:
        - avg_runtime: average of non-null season averages
        - cume_runtime: sum of season cumulative runtimes
        """
        if num_seasons <= 0:
            return None, 0

        tasks = [
            self.get_tv_season_runtime(tmdb_id=tmdb_id, season_number=season_number, **kwargs)
            for season_number in range(1, num_seasons + 1)
        ]
        season_results = await self._batch_process(tasks, batch_size=5, delay_between_batches=0.05)

        season_averages: list[int] = []
        total_cume = 0
        for result in season_results:
            if isinstance(result, Exception) or not isinstance(result, MCTvSeasonRuntimeResponse):
                continue
            if result.status_code != 200:
                continue
            if result.avg_runtime is not None:
                season_averages.append(result.avg_runtime)
            total_cume += result.cume_runtime

        aggregate_avg = (
            round(sum(season_averages) / len(season_averages)) if season_averages else None
        )
        return aggregate_avg, total_cume

    async def get_tv_season_runtime(
        self,
        tmdb_id: int,
        season_number: int,
        **kwargs: Any,
    ) -> MCTvSeasonRuntimeResponse:
        """Get runtime rollup for all episodes in a TV season."""
        if tmdb_id <= 0:
            return MCTvSeasonRuntimeResponse(
                tmdb_id=tmdb_id,
                season_number=season_number,
                error="tmdb_id must be a positive integer",
                status_code=400,
            )
        if season_number < 0:
            return MCTvSeasonRuntimeResponse(
                tmdb_id=tmdb_id,
                season_number=season_number,
                error="season_number must be zero or greater",
                status_code=400,
            )

        try:
            payload = await self._make_request(
                f"tv/{tmdb_id}/season/{season_number}",
                {"language": "en-US"},
                **kwargs,
            )
            if not payload:
                return MCTvSeasonRuntimeResponse(
                    tmdb_id=tmdb_id,
                    season_number=season_number,
                    error="Season details not found",
                    status_code=404,
                )

            details = TMDBSeasonDetailsResult.model_validate(payload)
            episodes: list[MCTvSeasonRuntimeEpisode] = []
            runtime_values: list[int | None] = []

            for episode in details.episodes:
                runtime = self._coerce_positive_runtime(episode.runtime)
                runtime_values.append(runtime)
                image_url = None
                if episode.still_path and self.image_base_url:
                    image_url = f"{self.image_base_url}w300{episode.still_path}"

                episodes.append(
                    MCTvSeasonRuntimeEpisode(
                        episode_id=episode.id,
                        episode_number=episode.episode_number,
                        name=episode.name,
                        overview=episode.overview,
                        image=image_url,
                        air_date=episode.air_date,
                        runtime=runtime,
                    )
                )

            avg_runtime, cume_runtime = self._compute_season_runtime_metrics(runtime_values)
            return MCTvSeasonRuntimeResponse(
                tmdb_id=tmdb_id,
                season_number=season_number,
                num_episodes=len(episodes),
                avg_runtime=avg_runtime,
                cume_runtime=cume_runtime,
                episodes=episodes,
            )
        except Exception as exc:
            logger.error(
                "get_tv_season_runtime: failed tmdb_id=%s season_number=%s error=%s",
                tmdb_id,
                season_number,
                exc,
            )
            return MCTvSeasonRuntimeResponse(
                tmdb_id=tmdb_id,
                season_number=season_number,
                error=str(exc),
                status_code=500,
            )

    """
    Legacy Unified Search Support
    """

    async def search_multi_raw(
        self, query: str, page: int = 1, limit: int = 20
    ) -> TMDBRawMultiSearchRawResponse:
        """Search for movies, TV shows, and people using TMDB's multi endpoint.

        Args:
            query: Search query
            page: Page number
            limit: Maximum results per page

        Returns:
            Raw TMDB search response with typed results
        """
        endpoint = "search/multi"
        params = {"query": query, "language": "en-US", "page": page}

        data = await self._make_request(endpoint, params)
        if not data:
            return TMDBRawMultiSearchRawResponse(
                results=[], total_results=0, total_pages=0, page=page
            )

        return TMDBRawMultiSearchRawResponse(
            results=data.get("results", []),
            total_results=data.get("total_results", 0),
            total_pages=data.get("total_pages", 0),
            page=data.get("page", page),
        )

    """
    Multi Search Endpoint
    """

    async def search_multi(
        self, query: str, page: int = 1, limit: int = 20, **kwargs
    ) -> MCSearchResponse:
        """Search for movies and TV shows.

        Args:
            query: Search query
            page: Page number
            limit: Maximum results per page
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            MCSearchResponse with processed results (movies and TV shows, no people)
        """
        endpoint = "search/multi"
        params = {"query": query, "language": "en-US", "page": page}

        data = await self._make_request(endpoint, params)
        if not data:
            return MCSearchResponse(
                results=[],
                total_results=0,
                page=page,
                query=query,
                data_type=MCType.MIXED,
                data_source="TMDB Multi Search",
                error=None,
            )

        # Process results and collect items to enhance
        items_to_enhance: list[MCBaseMediaItem] = []

        for item in data.get("results", []):
            item_media_type = item.get("media_type")
            # Skip person results (TMDB returns "person" as string)
            if item_media_type == MCType.PERSON.value:
                continue
            # Normalize TMDB media_type to MCType enum for type-safe comparison
            item_mc_type = _normalize_media_type_to_mctype(item_media_type)
            if item_mc_type == MCType.TV_SERIES:
                item = MCTvItem.from_tv_search(
                    TMDBSearchTv.model_validate(item), image_base_url=self.image_base_url
                )
                is_valid = self.is_vaild_tv(item)
            else:
                item = MCMovieItem.from_movie_search(
                    TMDBSearchMovie.model_validate(item), image_base_url=self.image_base_url
                )
                is_valid = self.is_vaild_movie(item)

            if not is_valid:
                continue

            items_to_enhance.append(item)

        # Enhance first 20 results with detailed information in parallel
        processed_results: list[MCBaseMediaItem] = []
        if items_to_enhance:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in items_to_enhance[:limit]
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            for idx, enhanced_item in enumerate(enhanced_results):
                if isinstance(enhanced_item, Exception):
                    logger.warning(
                        f"Error enhancing item {items_to_enhance[idx].tmdb_id}: {enhanced_item}"
                    )
                    # Only append if it's a valid MCBaseMediaItem
                    if isinstance(items_to_enhance[idx], MCBaseMediaItem):
                        processed_results.append(items_to_enhance[idx])
                elif enhanced_item is not None and isinstance(enhanced_item, MCBaseMediaItem):
                    processed_results.append(enhanced_item)
        else:
            processed_results = items_to_enhance[:limit]

        # Apply enhanced sorting
        try:
            query_lower = query.lower()

            def safe_sort_key(
                item: MCBaseMediaItem,
            ) -> tuple[bool, bool, bool, float, float, float]:
                try:
                    name = str(item.name or item.title or "")
                    status = str(item.status or "")
                    popularity = float(item.popularity or 0)
                    vote_average = float(item.vote_average or 0)

                    return (
                        # 1. Exact name matches first
                        name.lower() != query_lower,
                        # 2. Names that start with query
                        not name.lower().startswith(query_lower),
                        # 3. TV shows with continuing/returning status
                        not (
                            item.content_type == MCType.TV_SERIES.value
                            and status.lower() in ["continuing", "returning"]
                        ),
                        # 4. More recent content first (by release date)
                        self._get_sort_date(item),
                        # 5. Higher popularity first
                        -popularity,
                        # 6. Higher rating first
                        -vote_average,
                    )
                except Exception as e:
                    logger.warning(f"Error in sort key for item {item.tmdb_id}: {e}")
                    return (True, True, True, 0, 0, 0)

            processed_results.sort(key=safe_sort_key)
        except Exception as e:
            logger.error(f"Error sorting search results: {e}")

        # Apply content-type specific filters to all results
        filtered_results = []
        for item in processed_results:
            # Use MCType enum for type-safe content_type comparison
            if item.content_type == MCType.MOVIE.value:
                if self.is_vaild_movie(item):
                    filtered_results.append(item)
            elif item.content_type == MCType.TV_SERIES.value:
                if self.is_vaild_tv(item):
                    filtered_results.append(item)
            else:
                # Unknown content type, include it
                filtered_results.append(item)

        return MCSearchResponse(
            results=cast(list[MCBaseItem], filtered_results),  # type: ignore[arg-type]
            total_results=len(filtered_results),
            page=data.get("page", page),
            query=query,
            data_type=MCType.MIXED,
            data_source="TMDB Multi Search",
            error=None,
        )

    async def search_tv_shows(
        self,
        query: str,
        page: int = 1,
        limit: int = 50,
        enrich: bool = False,
        num_to_enrich: int = 20,
        no_cache: bool = False,
    ) -> MCSearchResponse:
        """
        Search for TV shows only with weighted sorting by relevancy and recency.

        Args:
            query: Search query string
            page: Page number for pagination (default: 1)
            limit: Maximum number of results (default: 50)
            enrich: If True, enrich results with provider data (default: False)
            num_to_enrich: Number of results to enrich with full details (default: 20)
            no_cache: If True, bypass cache (default: False)

        Returns:
            MCSearchResponse with weighted sorted TV show results
        """
        search_results = await self._search(
            query,
            page,
            limit,
            media_type=MCType.TV_SERIES,
            no_cache=no_cache,
            num_to_enrich=num_to_enrich,
        )
        result = cast(list[MCTvItem], search_results)
        if enrich:
            result = cast(
                list[MCTvItem],
                await self._enrich_with_providers(cast(list[MCBaseMediaItem], result)),
            )
        return MCSearchResponse(
            results=cast(list[MCBaseItem], result),  # type: ignore[arg-type]
            total_results=len(result),
            page=page,
            query=query,
            data_type=MCType.TV_SERIES,
            data_source="TMDB TV Search (weighted by relevancy + recency)",
            error=None,
        )

    async def search_movies(
        self,
        query: str,
        page: int = 1,
        limit: int = 50,
        num_with_extended: int = 0,
        num_with_full: int = 20,
        **kwargs: Any,
    ) -> MCSearchResponse:
        """
        Search for movies only with weighted sorting by relevancy and recency.

        Args:
            query: Search query string (required)
            page: Page number for pagination (default: 1)
            limit: Maximum number of results (default: 50)
            num_with_extended: Number of results to enrich with extended data (default: 0)
            num_with_full: Number of results to enrich with full details (default: 20)
            no_cache: If True, bypass cache (default: False)

        Returns:
            MCSearchResponse with weighted sorted movie results
        """
        search_results = await self._search(
            query,
            page,
            limit,
            media_type=MCType.MOVIE,
            num_to_enrich=num_with_full,
            **kwargs,
        )
        result = cast(list[MCMovieItem], search_results)

        if num_with_extended > 0:
            result = cast(
                list[MCMovieItem],
                await self._enrich_with_providers(cast(list[MCBaseMediaItem], result)),
            )
        return MCSearchResponse(
            results=cast(list[MCBaseItem], result),  # type: ignore[arg-type]
            total_results=len(result),
            page=page,
            query=query,
            data_type=MCType.MOVIE,
            data_source="TMDB Movie Search (weighted by relevancy + recency)",
            error=None,
        )

    def _apply_weighted_sorting(
        self, results: list[MCBaseMediaItem], query: str
    ) -> list[MCBaseMediaItem]:
        """
        Apply weighted sorting algorithm to prioritize exact title matches.

        This method ensures exact or near-exact title matches are ranked first,
        even if TMDB's API returns them in a different order (e.g., due to recency).

        Weighting:
        - 60% title similarity (Levenshtein distance-based)
        - 25% TMDB relevance (original API order)
        - 15% recency (exponential decay with 5-year half-life)

        Args:
            results: List of media items to sort
            query: Original search query for title comparison

        Returns:
            Sorted list with exact matches first
        """
        if not results:
            return results

        # Use timezone-aware UTC date per best practices.
        today = datetime.now(UTC).date()
        n = len(results)

        # Normalize query for title matching
        query_normalized = query.lower().strip() if query else ""

        # Helper function to normalize title (remove leading articles)
        def normalize_title(text: str) -> str:
            if not text:
                return ""
            text_lower = text.lower().strip()
            for article in ["the ", "a ", "an "]:
                if text_lower.startswith(article):
                    return text_lower[len(article) :].strip()
            return text_lower

        for i, r in enumerate(results):
            # Title similarity using Levenshtein edit distance
            title_similarity = 0.0
            if query_normalized:
                # Use title or name field (both should be set)
                title_text = r.title or r.name
                if title_text:
                    title_normalized = normalize_title(title_text)
                    query_norm = normalize_title(query_normalized)

                    if query_norm and title_normalized:
                        # Calculate Levenshtein distance
                        distance = _levenshtein_distance(query_norm, title_normalized)

                        # Convert distance to similarity score (0.0 to 1.0)
                        # Lower distance = higher similarity
                        max_len = max(len(query_norm), len(title_normalized))
                        if max_len > 0:
                            # Normalize: 0 distance = 1.0 similarity, max distance = 0.0 similarity
                            title_similarity = 1.0 - (distance / max_len)
                        else:
                            title_similarity = 0.0

                        # Boost for exact matches
                        if query_norm == title_normalized:
                            title_similarity = 1.0
                        # Boost for substring matches
                        elif query_norm in title_normalized or title_normalized in query_norm:
                            title_similarity = max(title_similarity, 0.9)

            # Recency calculation - use appropriate date field based on media type
            days = None
            date_str = None
            if r.media_type == "movie":
                # Movies use release_date
                date_str = getattr(r, "release_date", None)
            else:
                # TV shows use first_air_date
                date_str = getattr(r, "first_air_date", None)

            if date_str:
                try:
                    release_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    days = (today - release_date).days
                    recency = math.exp(-days / 1825)  # 5-year half-life
                except (ValueError, TypeError):
                    recency = 0
                    days = None
            else:
                recency = 0

            # Relevance from TMDB order
            relevance = 1 - i / (n - 1) if n > 1 else 1

            # Weighted final score: 60% title similarity (heavily weighted), 25% relevance, 15% recency
            r.final_score = 0.60 * title_similarity + 0.25 * relevance + 0.15 * recency

            # Add debugging information
            r.relevancy_debug = {
                "original_tmdb_position": i,
                "title_similarity_score": round(title_similarity, 4),
                "relevance_score": round(relevance, 4),
                "recency_score": round(recency, 4),
                "final_weighted_score": round(r.final_score, 4),
                "release_date": date_str if r.media_type == "movie" else None,
                "first_air_date": date_str if r.media_type != "movie" else None,
                "days_since_release": days,
            }

        return sorted(results, key=lambda x: x.final_score or 0.0, reverse=True)

    async def _search(
        self,
        query: str,
        page: int = 1,
        limit: int = 50,
        media_type: MCType = MCType.TV_SERIES,
        num_to_enrich: int = 20,
        **kwargs: Any,
    ) -> list[MCTvItem | MCMovieItem]:
        """Cached TV show search implementation.

        Args:
            query: Search query
            page: Page number
            limit: Maximum results
            **kwargs: Additional arguments (including no_cache flag)

        Returns:
            TMDBSearchResponse with weighted sorted results
        """
        endpoint = f"search/{media_type.value}"
        params = {"query": query, "language": "en-US", "page": page}

        data = await self._make_request(endpoint, params)
        if not data:
            return []

        # Process results and collect items to enhance
        items_to_enhance: list[MCTvItem | MCMovieItem] = []
        basic_items: list[MCTvItem | MCMovieItem] = []

        # IMPORTANT: Always process at least MIN_RESULTS_FOR_SORTING items from TMDB
        # before applying weighted sorting, even if caller requests fewer results.
        # This ensures exact title matches are properly ranked above partial matches.
        # Example: "Succession" query with limit=1 should return HBO's "Succession"
        # not "Game of Succession" even if TMDB returns the latter first.
        MIN_RESULTS_FOR_SORTING = 20
        effective_num_to_process = max(num_to_enrich, MIN_RESULTS_FOR_SORTING)

        # Normalize media_type to MCType enum for type-safe comparison
        mc_type = _normalize_media_type_to_mctype(media_type)
        for i, item in enumerate(data.get("results", [])):
            if mc_type == MCType.TV_SERIES:
                mc_item: MCTvItem | MCMovieItem = MCTvItem.from_tv_search(
                    TMDBSearchTv.model_validate(item), image_base_url=self.image_base_url
                )
                is_valid = self.is_vaild_tv(mc_item)
            else:
                mc_item = MCMovieItem.from_movie_search(
                    TMDBSearchMovie.model_validate(item), image_base_url=self.image_base_url
                )
                is_valid = self.is_vaild_movie(mc_item)

            # Filter out items with empty overviews
            if not is_valid:
                continue

            # Collect first effective_num_to_process results for enhancement, rest as basic
            if i < effective_num_to_process:
                items_to_enhance.append(mc_item)
            else:
                basic_items.append(mc_item)

        # FIRST: Apply weighted sorting to ALL collected items (before enhancement)
        # This ensures exact title matches are ranked correctly regardless of TMDB order
        all_items_for_sorting = items_to_enhance + basic_items
        if all_items_for_sorting:
            all_items_for_sorting = cast(
                list[MCTvItem | MCMovieItem],
                self._apply_weighted_sorting(
                    cast(list[MCBaseMediaItem], all_items_for_sorting), query
                ),
            )

        # NOW: Take only the top 'limit' items after sorting for enhancement
        items_to_enhance = all_items_for_sorting[:limit]
        basic_items = []  # Clear - we've already sorted and selected the best items

        # Enhance results with detailed information in parallel
        processed_results: list[MCTvItem | MCMovieItem] = []
        # Calculate how many items to actually enhance (don't exceed the actual num_to_enrich requested)
        num_to_actually_enhance = min(len(items_to_enhance), num_to_enrich, limit)
        if items_to_enhance and num_to_actually_enhance > 0:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in items_to_enhance[:num_to_actually_enhance]
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            for idx, enhanced_item in enumerate(enhanced_results):
                if isinstance(enhanced_item, Exception):
                    logger.warning(
                        f"Error enhancing item {items_to_enhance[idx].tmdb_id}: {enhanced_item}"
                    )
                    # Fall back to basic item if enhancement failed
                    if isinstance(items_to_enhance[idx], (MCTvItem, MCMovieItem)):
                        processed_results.append(items_to_enhance[idx])
                elif enhanced_item is not None and isinstance(
                    enhanced_item, (MCTvItem, MCMovieItem)
                ):
                    processed_results.append(enhanced_item)
                else:
                    # Enhancement returned None, fall back to basic item
                    processed_results.append(items_to_enhance[idx])

            # Add remaining items that weren't enhanced (if any)
            if len(items_to_enhance) > num_to_actually_enhance:
                processed_results.extend(items_to_enhance[num_to_actually_enhance:])
        elif items_to_enhance:
            # If num_to_actually_enhance is 0 but we have items, just use them as-is
            processed_results.extend(items_to_enhance[:limit])

        # Note: Sorting was already applied before enhancement via _apply_weighted_sorting
        # to ensure exact title matches are ranked correctly regardless of TMDB's order
        return processed_results[:limit]

    async def search_movie_by_genre(
        self,
        genre_ids: str,
        page: int = 1,
        limit: int = 50,
        include_details: bool = True,
        **kwargs,
    ) -> MCSearchResponse:
        """Search movies by genre IDs.

        Args:
            genre_ids: Comma-separated genre IDs
            page: Page number
            limit: Maximum results
            include_details: Whether to include watch providers and cast data

        Returns:
            MCSearchResponse with MCMovieItem results
        """
        if not genre_ids:
            return MCSearchResponse(
                results=[],
                total_results=0,
                page=page,
                query=f"genre ids: {genre_ids}",
                data_type=MCType.MOVIE,
                data_source="TMDB Search Movies by Genres",
                error=None,
            )

        # Search movies
        movies_endpoint = "discover/movie"
        movies_params = {
            "with_genres": genre_ids,
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc",
        }

        # Fetch the requested page
        page_data = await self._make_request(movies_endpoint, movies_params)
        if isinstance(page_data, Exception):
            logger.warning(f"Error fetching movie page data: {page_data}")
            return MCSearchResponse(
                results=[],
                total_results=0,
                page=page,
                query=f"genre ids: {genre_ids}",
                data_type=MCType.MOVIE,
                data_source="TMDB Search Movies by Genres",
                error=str(page_data),
            )

        # Parse movie results
        movies: list[MCMovieItem] = []
        if page_data and isinstance(page_data, dict) and page_data.get("results"):
            for movie_dict in page_data["results"]:
                mc_item = MCMovieItem.from_movie_search(
                    TMDBSearchMovie.model_validate(movie_dict),
                    image_base_url=self.image_base_url,
                )
                if mc_item.tmdb_id == 0:
                    continue
                movies.append(mc_item)

        # Sort movies with enhanced sorting
        def sort_key(item: MCMovieItem) -> float:
            try:
                popularity = float(item.popularity or 0)
                vote_average = float(item.vote_average or 0)
                vote_count = int(item.vote_count or 0)

                # Get release date for recency boost
                release_date = item.release_date
                recency_boost = 0
                if release_date:
                    try:
                        release_year = int(release_date.split("-")[0]) if release_date else 0
                        current_year = datetime.now().year
                        years_old = current_year - release_year

                        # Heavy boost for recent content
                        if years_old <= 1:
                            recency_boost = int(popularity * 3.0)
                        elif years_old <= 3:
                            recency_boost = int(popularity * 2.0)
                        elif years_old <= 5:
                            recency_boost = int(popularity * 1.0)
                        elif years_old <= 10:
                            recency_boost = int(popularity * 0.5)
                    except (ValueError, IndexError):
                        recency_boost = 0

                # Quality boost
                quality_boost = 0
                if vote_average >= 7.0 and vote_count >= 100:
                    quality_boost = int(popularity * 0.3)
                elif vote_average >= 6.0 and vote_count >= 50:
                    quality_boost = int(popularity * 0.15)

                total_score = popularity + recency_boost + quality_boost
                return total_score
            except Exception as e:
                logger.warning(f"Error in sort key for movie {item.name or 'unknown'}: {e}")
                return float(item.popularity or 0)

        # Sort movies
        try:
            movies.sort(key=sort_key, reverse=True)
        except Exception as e:
            logger.warning(f"Error sorting movie results: {e}")
            movies.sort(key=lambda x: float(x.popularity or 0), reverse=True)

        # Limit results
        limited_movies: list[MCMovieItem] = movies[:limit]

        # Add detailed information if requested
        if include_details:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in limited_movies
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            results: list[MCMovieItem] = []
            for idx, detailed_item in enumerate(enhanced_results):
                if isinstance(detailed_item, Exception):
                    logger.warning(
                        f"Error enhancing movie {limited_movies[idx].tmdb_id}: {detailed_item}"
                    )
                    if isinstance(limited_movies[idx], MCMovieItem):
                        results.append(limited_movies[idx])
                elif detailed_item is not None and isinstance(detailed_item, MCMovieItem):
                    results.append(detailed_item)
        else:
            results = limited_movies

        # Filter valid movies
        filtered_movies: list[MCMovieItem] = [
            item
            for item in results
            if item.content_type == MCType.MOVIE.value
            and isinstance(item, MCMovieItem)
            and self.is_vaild_movie(item)
        ]

        logger.info(f"Retrieved {len(filtered_movies)} movie genre discovery results")
        return MCSearchResponse(
            results=cast(list[MCBaseItem], filtered_movies),  # type: ignore[arg-type]
            total_results=len(filtered_movies),
            page=page,
            query=f"genre ids: {genre_ids}",
            data_type=MCType.MOVIE,
            data_source="TMDB Search Movies by Genres",
            error=None,
        )

    async def search_tv_by_genre(
        self,
        genre_ids: str,
        page: int = 1,
        limit: int = 50,
        include_details: bool = True,
        **kwargs,
    ) -> MCSearchResponse:
        """Search TV shows by genre IDs.

        Args:
            genre_ids: Comma-separated genre IDs
            page: Page number
            limit: Maximum results
            include_details: Whether to include watch providers and cast data

        Returns:
            MCSearchResponse with MCTvItem results
        """
        if not genre_ids:
            return MCSearchResponse(
                results=[],
                total_results=0,
                page=page,
                query=f"genre ids: {genre_ids}",
                data_type=MCType.TV_SERIES,
                data_source="TMDB Search TV by Genres",
                error=None,
            )

        # Search TV shows
        tv_endpoint = "discover/tv"
        tv_params = {
            "with_genres": genre_ids,
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc",
        }

        # Fetch the requested page
        page_data = await self._make_request(tv_endpoint, tv_params)
        if isinstance(page_data, Exception):
            logger.warning(f"Error fetching TV page data: {page_data}")
            return MCSearchResponse(
                results=[],
                total_results=0,
                page=page,
                query=f"genre ids: {genre_ids}",
                data_type=MCType.TV_SERIES,
                data_source="TMDB Search TV by Genres",
                error=str(page_data),
            )

        # Parse TV results
        tv: list[MCTvItem] = []
        if page_data and isinstance(page_data, dict) and page_data.get("results"):
            for tv_dict in page_data["results"]:
                tv_item: MCTvItem = MCTvItem.from_tv_search(
                    TMDBSearchTv.model_validate(tv_dict), image_base_url=self.image_base_url
                )
                if tv_item.tmdb_id == 0:
                    continue
                tv.append(tv_item)

        # Sort TV shows with enhanced sorting
        def sort_key(item: MCTvItem) -> float:
            try:
                popularity = float(item.popularity or 0)
                vote_average = float(item.vote_average or 0)
                vote_count = int(item.vote_count or 0)

                # Get first air date for recency boost
                release_date = item.first_air_date
                recency_boost = 0
                if release_date:
                    try:
                        release_year = int(release_date.split("-")[0]) if release_date else 0
                        current_year = datetime.now().year
                        years_old = current_year - release_year

                        # Heavy boost for recent content
                        if years_old <= 1:
                            recency_boost = int(popularity * 3.0)
                        elif years_old <= 3:
                            recency_boost = int(popularity * 2.0)
                        elif years_old <= 5:
                            recency_boost = int(popularity * 1.0)
                        elif years_old <= 10:
                            recency_boost = int(popularity * 0.5)
                    except (ValueError, IndexError):
                        recency_boost = 0

                # Quality boost
                quality_boost = 0
                if vote_average >= 7.0 and vote_count >= 100:
                    quality_boost = int(popularity * 0.3)
                elif vote_average >= 6.0 and vote_count >= 50:
                    quality_boost = int(popularity * 0.15)

                total_score = popularity + recency_boost + quality_boost
                return total_score
            except Exception as e:
                logger.warning(f"Error in sort key for TV {item.name or 'unknown'}: {e}")
                return float(item.popularity or 0)

        # Sort TV shows
        try:
            tv.sort(key=sort_key, reverse=True)
        except Exception as e:
            logger.warning(f"Error sorting TV results: {e}")
            tv.sort(key=lambda x: float(x.popularity or 0), reverse=True)

        # Limit results
        limited_tv_shows: list[MCTvItem] = tv[:limit]

        # Add detailed information if requested
        if include_details:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in limited_tv_shows
            ]
            enhanced_results = await asyncio.gather(*enhance_tasks, return_exceptions=True)

            results: list[MCTvItem] = []
            for idx, detailed_item in enumerate(enhanced_results):
                if isinstance(detailed_item, Exception):
                    logger.warning(
                        f"Error enhancing TV {limited_tv_shows[idx].tmdb_id}: {detailed_item}"
                    )
                    if isinstance(limited_tv_shows[idx], MCTvItem):
                        results.append(limited_tv_shows[idx])
                elif detailed_item is not None and isinstance(detailed_item, MCTvItem):
                    results.append(detailed_item)
        else:
            results = limited_tv_shows

        # Filter valid TV shows
        filtered_tv_shows: list[MCTvItem] = [
            item
            for item in results
            if isinstance(item, MCTvItem)
            and item.content_type == MCType.TV_SERIES.value
            and self.is_vaild_tv(item)
        ]

        logger.info(f"Retrieved {len(filtered_tv_shows)} TV genre discovery results")
        return MCSearchResponse(
            results=cast(list[MCBaseItem], filtered_tv_shows),  # type: ignore[arg-type]
            total_results=len(filtered_tv_shows),
            page=page,
            query=f"genre ids: {genre_ids}",
            data_type=MCType.TV_SERIES,
            data_source="TMDB Search TV by Genres",
            error=None,
        )

    async def search_keywords(self, query: str, page: int = 1) -> MCKeywordSearchResponse:
        """Search for keywords by name.

        Args:
            query: Keyword search query
            page: Page number

        Returns:
            MCKeywordSearchResponse with typed keyword results
        """
        endpoint = "search/keyword"
        params = {"query": query, "page": page}

        data = await self._make_request(endpoint, params)
        if not data:
            return MCKeywordSearchResponse(
                results=[],
                total_results=0,
                total_pages=0,
                page=page,
                query=query,
                data_type=MCType.KEYWORD,
                data_source="TMDB Keyword Search",
                error=None,
            )

        # Parse results as TMDBKeyword objects using Pydantic 2.0
        keywords = [MCKeywordItem.model_validate(kw) for kw in data.get("results", [])]

        return MCKeywordSearchResponse(
            results=keywords,
            total_results=data.get("total_results", 0),
            total_pages=data.get("total_pages", 0),
            page=data.get("page", page),
            query=query,
        )

    async def search_by_keywords(
        self,
        keyword_ids: str,
        page: int = 1,
        limit: int = 50,
        include_details: bool = True,
        **kwargs,
    ) -> MCSearchResponse:
        """Search movies and TV shows by keyword IDs.

        Args:
            keyword_ids: Comma-separated keyword IDs (required)
            page: Page number for pagination (default: 1)
            limit: Maximum number of results (default: 50)
            include_details: Whether to include watch providers, cast, videos, and keywords (default: True)
            **kwargs: Additional arguments passed to media details enhancement

        Returns:
            MCSearchResponse with interleaved movies and TV shows sorted by popularity, recency, and quality
        """
        # Calculate limits per category
        movies_limit = 30
        tv_limit = 30

        # Search movies
        movies_endpoint = "discover/movie"
        movies_params = {
            "with_keywords": keyword_ids,
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc",
        }

        # Search TV shows
        tv_endpoint = "discover/tv"
        tv_params = {
            "with_keywords": keyword_ids,
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc",
        }

        # Fetch page 1 and page 2 for both movies and TV concurrently
        tasks = []
        num_pages = 2
        for page_num in range(1, num_pages + 1):
            tasks.append(self._make_request(movies_endpoint, {**movies_params, "page": page_num}))
            tasks.append(self._make_request(tv_endpoint, {**tv_params, "page": page_num}))
        data = await asyncio.gather(*tasks, return_exceptions=True)

        # Extract results: [movies_p1, tv_p1, movies_p2, tv_p2]
        # Filter out exceptions from gather results
        movies_data: list[dict[str, Any]] = []
        tv_data: list[dict[str, Any]] = []
        for idx, result in enumerate(data):
            if isinstance(result, Exception):
                logger.warning(f"Error fetching page data at index {idx}: {result}")
                continue
            if isinstance(result, dict):
                if idx % 2 == 0:  # Even indices are movies (0, 2)
                    movies_data.append(result)
                else:  # Odd indices are TV (1, 3)
                    tv_data.append(result)

        # Parse and combine movie results - FULLY TYPED using Pydantic 2.0
        movies: list[MCMovieItem] = []
        for page_data in movies_data:
            if page_data and isinstance(page_data, dict) and page_data.get("results"):
                for movie_dict in page_data["results"]:
                    try:
                        mc_item = MCMovieItem.from_movie_search(
                            TMDBSearchMovie.model_validate(movie_dict),
                            image_base_url=self.image_base_url,
                        )
                        if mc_item.tmdb_id == 0:
                            continue
                        movies.append(mc_item)
                    except Exception as e:
                        # Skip items that fail validation
                        logger.warning(f"Skipping invalid movie item {movie_dict.get('id')}: {e}")
                        continue

        # Parse and combine TV results - FULLY TYPED using Pydantic 2.0
        tv: list[MCTvItem] = []
        for page_data in tv_data:
            if page_data and isinstance(page_data, dict) and page_data.get("results"):
                for tv_dict in page_data["results"]:
                    try:
                        tv_item: MCTvItem = MCTvItem.from_tv_search(
                            TMDBSearchTv.model_validate(tv_dict), image_base_url=self.image_base_url
                        )
                        if tv_item.tmdb_id == 0:
                            continue
                        tv.append(tv_item)
                    except Exception as e:
                        # Skip items that fail validation
                        logger.warning(f"Skipping invalid TV item {tv_dict.get('id')}: {e}")
                        continue

        # Sort movies and TV shows with enhanced sorting
        def sort_key(item: MCBaseMediaItem) -> float:
            try:
                popularity = float(item.popularity or 0)
                vote_average = float(item.vote_average or 0)
                vote_count = int(item.vote_count or 0)

                # Get release/first air date for recency boost
                release_date = None
                # Use MCType enum for type-safe content_type comparison
                if item.content_type == MCType.MOVIE.value and isinstance(item, MCMovieItem):
                    release_date = item.release_date
                elif isinstance(item, MCTvItem):
                    release_date = item.first_air_date

                # Calculate recency boost
                recency_boost = 0
                if release_date:
                    try:
                        release_year = int(release_date.split("-")[0]) if release_date else 0
                        current_year = datetime.now().year
                        years_old = current_year - release_year

                        # Heavy boost for recent content
                        if years_old <= 1:
                            recency_boost = int(popularity * 3.0)
                        elif years_old <= 3:
                            recency_boost = int(popularity * 2.0)
                        elif years_old <= 5:
                            recency_boost = int(popularity * 1.0)
                        elif years_old <= 10:
                            recency_boost = int(popularity * 0.5)
                    except (ValueError, IndexError):
                        recency_boost = 0

                # Quality boost
                quality_boost = 0
                if vote_average >= 7.0 and vote_count >= 100:
                    quality_boost = int(popularity * 0.3)
                elif vote_average >= 6.0 and vote_count >= 50:
                    quality_boost = int(popularity * 0.15)

                total_score = popularity + recency_boost + quality_boost
                return total_score
            except Exception as e:
                logger.warning(f"Error in sort key for item {item.name or 'unknown'}: {e}")
                return float(popularity)

        # Sort movies and TV shows separately
        try:
            movies.sort(key=sort_key, reverse=True)
            tv.sort(key=sort_key, reverse=True)
        except Exception as e:
            logger.warning(f"Error sorting keyword results: {e}")
            movies.sort(key=lambda x: float(x.popularity or 0), reverse=True)
            tv.sort(key=lambda x: float(x.popularity or 0), reverse=True)

        # Limit each category
        limited_movies: list[MCMovieItem] = movies[:movies_limit]
        limited_tv_shows: list[MCTvItem] = tv[:tv_limit]

        # Interleave movies and TV shows
        combined_results: list[MCMovieItem | MCTvItem] = []
        max_items = max(len(limited_movies), len(limited_tv_shows))

        for i in range(max_items):
            if i < len(limited_movies):
                combined_results.append(limited_movies[i])
            if i < len(limited_tv_shows):
                combined_results.append(limited_tv_shows[i])

        # Limit to requested amount
        combined_results = combined_results[:limit]

        # Add detailed information if requested - process in batches to avoid rate limits
        if include_details:
            enhance_tasks = [
                self.get_media_details(
                    item.tmdb_id,
                    item.mc_type,
                    include_cast=True,
                    include_videos=True,
                    include_watch_providers=True,
                    include_keywords=True,
                    **kwargs,
                )
                for item in combined_results
            ]
            # Batch process to avoid overwhelming rate limits
            # Each get_media_details makes ~5 requests (1 basic + 4 enrichment)
            # Processing 5 items = ~25 requests per batch, safely under 35 req/sec limit
            enhanced_results = await self._batch_process(
                enhance_tasks, batch_size=10, delay_between_batches=0.2
            )

            results: list[MCMovieItem | MCTvItem] = []
            for idx, detailed_item in enumerate(enhanced_results):
                if isinstance(detailed_item, Exception):
                    logger.warning(
                        f"Error enhancing item {combined_results[idx].tmdb_id}: {detailed_item}"
                    )
                    if isinstance(combined_results[idx], (MCMovieItem, MCTvItem)):
                        results.append(combined_results[idx])
                elif detailed_item is not None and isinstance(
                    detailed_item, (MCMovieItem, MCTvItem)
                ):
                    results.append(detailed_item)
        else:
            # Ensure results is properly typed - use different variable name to avoid redefinition
            filtered_results_for_keywords: list[MCMovieItem | MCTvItem] = [
                x for x in combined_results if isinstance(x, (MCMovieItem, MCTvItem))
            ]
            results = filtered_results_for_keywords

        # Use the enhanced results (or combined_results if details not requested)
        filtered_movies_for_keywords: list[MCMovieItem] = cast(
            list[MCMovieItem],
            [
                item
                for item in results
                if item.content_type == MCType.MOVIE.value
                and isinstance(item, MCMovieItem)
                and self.is_vaild_movie(item)
            ],
        )
        tv_shows: list[MCTvItem] = [
            item
            for item in results
            if isinstance(item, MCTvItem)
            and item.content_type == MCType.TV_SERIES.value
            and self.is_vaild_tv(item)
        ]

        logger.info(
            f"Retrieved {len(results)} keyword search results "
            f"({len(filtered_movies_for_keywords)} movies, {len(tv_shows)} TV shows)"
        )
        combined_results = filtered_movies_for_keywords + tv_shows
        return MCSearchResponse(
            results=cast(list[MCBaseItem], combined_results),  # type: ignore[arg-type]
            total_results=len(combined_results),
            page=page,
            query=f"keyword ids: {keyword_ids}",
            data_type=MCType.MIXED,
            data_source="TMDB Search by Keywords",
            error=None,
        )

    async def _search_with_keywords(
        self, query: str, page: int = 1, limit: int = 20, include_details: bool = True
    ) -> MCSearchResponse:
        """Internal helper: Search with support for keyword syntax (keyword: "name").

        Args:
            query: Search query string (may contain keyword: syntax, e.g., 'keyword: "space opera"')
            page: Page number for pagination (default: 1)
            limit: Maximum number of results (default: 20)
            include_details: Whether to include watch providers, cast, videos, and keywords (default: True)

        Returns:
            MCSearchResponse with search results (keyword-based discovery or regular multi-search)
        """
        # Check if query contains keyword syntax
        keyword_pattern = r'keyword:\s*(?:["\']([^"\']+)["\']|(\w+(?:\s+\w+)*))'
        keyword_match = re.search(keyword_pattern, query, re.IGNORECASE)

        if keyword_match:
            # Extract the keyword
            keyword_name = (keyword_match.group(1) or keyword_match.group(2)).strip()
            logger.info(f"Detected keyword search for: '{keyword_name}'")

            # Search for the keyword to get its ID
            keyword_results = await self.search_keywords(keyword_name, 1)

            if keyword_results.results:
                # Use the first matching keyword
                keyword_id = str(keyword_results.results[0].id)
                logger.info(f"Found keyword ID {keyword_id} for '{keyword_name}'")

                # Use search_by_keywords endpoint - returns MCSearchResponse
                return await self.search_by_keywords(
                    keyword_id, page, limit, include_details=include_details
                )
            else:
                # No keyword found - fall back to regular search
                logger.info(
                    f"No keyword found for '{keyword_name}', falling back to regular search"
                )
                return await self.search_multi(query, page, limit)

        else:
            # Regular search
            return await self.search_multi(query, page, limit)

    async def _enrich_with_providers(
        self, results: list[MCBaseMediaItem], expiry: int = 60 * 60 * 24, **kwargs
    ) -> list[MCBaseMediaItem]:
        """
        Step 2: Enrich results with watch provider data from TMDB.

        Args:
            results: List of media items to enrich

        Returns:
            List of enriched items with watch provider data
        """
        if not results:
            return []

        tmdb_service = TMDBSearchService()
        enriched = []

        # Create tasks for concurrent provider fetching
        tasks: list[tuple[MCBaseMediaItem, Awaitable[dict]]] = []
        for item in results:
            tmdb_id = item.tmdb_id
            content_type = item.mc_type

            if item.watch_providers and len(item.watch_providers) > 0:
                enriched.append(item)
                continue
            if tmdb_id and content_type:
                task = tmdb_service._get_watch_providers(tmdb_id, content_type)
                tasks.append((item, task))
            else:
                # Skip enrichment for items without TMDB ID
                enriched.append(item)

        # Execute all provider requests concurrently
        if tasks:
            provider_results = await asyncio.gather(
                *[task for _, task in tasks], return_exceptions=True
            )

            # Merge provider data with original items
            for (item, _), provider_data in zip(tasks, provider_results, strict=False):
                if isinstance(provider_data, Exception):
                    item_name = (
                        getattr(item, "name", None) or getattr(item, "title", None) or "Unknown"
                    )
                    logger.warning(f"Failed to get providers for {item_name}: {provider_data}")
                    # Keep item without provider data
                    enriched.append(item)
                elif provider_data is not None and isinstance(provider_data, dict):
                    # Merge provider data (provider_data is a dict)
                    watch_providers_val = provider_data.get("watch_providers")
                    streaming_platform_val = provider_data.get("streaming_platform")
                    if watch_providers_val is not None:
                        item.watch_providers = watch_providers_val
                    if streaming_platform_val is not None:
                        item.streaming_platform = streaming_platform_val
                    enriched.append(item)

        return enriched


tmdb_search_service = TMDBSearchService()
