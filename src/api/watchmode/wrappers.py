"""
Watchmode Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.
"""

import os
from datetime import UTC, datetime
from typing import Literal, cast

from api.tmdb.core import TMDBService
from api.tmdb.models import (
    MCBaseMediaItem,
    MCMovieItem,
    MCTvItem,
    TMDBMovieDetailsResult,
    TMDBTvDetailsResult,
)
from api.watchmode.auth import watchmode_auth
from api.watchmode.core import WatchmodeService
from api.watchmode.models import (
    WatchmodeSearchResponse,
    WatchmodeTitleDetailsResponse,
    WatchmodeWhatsNewResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache for wrapper class methods
WatchmodeWrapperCache = RedisCache(
    defaultTTL=6 * 60 * 60,  # 6 hours
    prefix="watchmode_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
    version="4.1.0",  # Incremented for results field migration (was releases)
)


class WatchmodeWrapper:
    def __init__(self):
        """Initialize Watchmode wrapper. Services are created per-request."""

    @RedisCache.use_cache(WatchmodeWrapperCache, prefix="get_whats_new_wrapper")
    async def get_whats_new(
        self,
        region: str = "US",
        limit: int = 50,
        **kwargs,
    ) -> WatchmodeWhatsNewResponse:
        """
        Async wrapper function to get what's new this week.

        Uses Watchmode ONLY for the list of new tmdb_ids, then gets ALL data from TMDB.

        Returns:
            WatchmodeWhatsNewResponse: MCBaseItem derivative containing what's new data or error information
        """
        try:
            # Get API keys from auth service
            api_key = watchmode_auth.watchmode_api_key
            tmdb_token = watchmode_auth.tmdb_read_token

            if not api_key:
                return WatchmodeWhatsNewResponse(
                    results=[],
                    total_results=0,
                    region=region,
                    generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                    data_source="watchmode_list + tmdb_complete",
                    error="Watchmode API key is required",
                    status_code=400,
                )

            if not tmdb_token:
                return WatchmodeWhatsNewResponse(
                    results=[],
                    total_results=0,
                    region=region,
                    generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                    data_source="watchmode_list + tmdb_complete",
                    error="TMDB token is required for enhanced data",
                    status_code=400,
                )

            # Initialize services
            watchmode_service = WatchmodeService(api_key)

            # Initialize TMDB service and set token manually
            # TMDBService loads token from SecretParam/env, so we set env temporarily
            original_token = os.environ.get("TMDB_READ_TOKEN")
            os.environ["TMDB_READ_TOKEN"] = tmdb_token
            try:
                tmdb_service = TMDBService()
            finally:
                # Restore original token if it existed
                if original_token is not None:
                    os.environ["TMDB_READ_TOKEN"] = original_token
                elif "TMDB_READ_TOKEN" in os.environ:
                    del os.environ["TMDB_READ_TOKEN"]

            # Get new releases from Watchmode (ONLY to get the list of tmdb_ids)
            releases = await watchmode_service.get_new_releases(region=region, limit=limit)

            if not releases:
                return WatchmodeWhatsNewResponse(
                    results=[],
                    total_results=0,
                    region=region,
                    generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                    data_source="watchmode_list + tmdb_complete",
                    error="Failed to fetch new releases",
                    status_code=500,
                )

            # Extract tmdb_ids, watchmode_ids and content types from Watchmode releases
            class TrendingItem:
                def __init__(
                    self, tmdb_id: int | None, content_type: str, watchmode_id: int | None
                ):
                    self.tmdb_id = tmdb_id
                    self.content_type = content_type
                    self.watchmode_id = watchmode_id

            tmdb_items: list[TrendingItem] = []
            for release in releases.get("releases", []):
                tmdb_id = release.get("tmdb_id")
                watchmode_id = release.get("id")
                content_type_raw = release.get("type", "")
                content_type: Literal["tv", "movie"]
                if "tv" in content_type_raw:
                    content_type = "tv"
                else:
                    content_type = "movie"
                if tmdb_id:  # Only include items with valid TMDB IDs
                    item = TrendingItem(
                        tmdb_id=tmdb_id, content_type=content_type, watchmode_id=watchmode_id
                    )
                    tmdb_items.append(item)

            logger.info(f"Found {len(tmdb_items)} releases with TMDB IDs")

            # Fetch COMPLETE data from TMDB for each item
            enhanced_releases: list[MCMovieItem | MCTvItem] = []

            for item_info in tmdb_items:
                tmdb_id = item_info.tmdb_id
                content_type = cast(Literal["tv", "movie"], item_info.content_type)

                try:
                    # Get full TMDB details (basic + enhanced)
                    # Try the original content_type first
                    basic_details_endpoint = f"{content_type}/{tmdb_id}"
                    basic_details = await tmdb_service._make_request(
                        basic_details_endpoint, {"language": "en-US"}
                    )

                    # If not found, try the other content_type as fallback
                    # (Watchmode sometimes has incorrect content_type)
                    if not basic_details:
                        fallback_content_type: Literal["tv", "movie"] = (
                            "movie" if content_type == "tv" else "tv"
                        )
                        logger.debug(
                            f"No TMDB data for {content_type} {tmdb_id}, trying fallback {fallback_content_type}"
                        )
                        fallback_endpoint = f"{fallback_content_type}/{tmdb_id}"
                        basic_details = await tmdb_service._make_request(
                            fallback_endpoint, {"language": "en-US"}
                        )
                        if basic_details:
                            # Update content_type to the successful fallback
                            content_type = fallback_content_type
                            logger.debug(
                                f"Found TMDB data for {tmdb_id} as {content_type} (was originally {item_info.content_type})"
                            )

                    if not basic_details:
                        # Both attempts failed - TMDB ID doesn't exist for either type
                        logger.debug(
                            f"No TMDB data for {tmdb_id} as {item_info.content_type} or fallback type"
                        )
                        continue

                    # Process basic details using TMDB model class methods
                    # Get image_base_url from service for proper image URLs
                    image_base_url = tmdb_service.image_base_url

                    trending_response: MCBaseMediaItem
                    if content_type == "tv":
                        tv_details = TMDBTvDetailsResult.model_validate(basic_details)
                        trending_response = MCTvItem.from_tv_details(tv_details, image_base_url)
                    else:
                        movie_details = TMDBMovieDetailsResult.model_validate(basic_details)
                        trending_response = MCMovieItem.from_movie_details(
                            movie_details, image_base_url
                        )

                    if trending_response.tmdb_id == 0:
                        continue

                    # Only include items with poster images
                    if trending_response.poster_path:
                        # Enhance with additional details (cast, videos, keywords, watch providers)
                        enhanced_base = await tmdb_service.enhance_media_item(trending_response)

                        # Convert back to specific type (enhance_media_item returns MCBaseMediaItem)
                        enhanced_dict = enhanced_base.model_dump()
                        if item_info.watchmode_id:
                            # Add watchmode_id to external_ids
                            if "external_ids" not in enhanced_dict:
                                enhanced_dict["external_ids"] = {}
                            enhanced_dict["external_ids"]["watchmode_id"] = item_info.watchmode_id

                        # Reconstruct the specific type
                        enhanced_item: MCMovieItem | MCTvItem
                        if content_type == "tv":
                            enhanced_item = MCTvItem.model_validate(enhanced_dict)
                        else:
                            enhanced_item = MCMovieItem.model_validate(enhanced_dict)

                        enhanced_releases.append(enhanced_item)

                except Exception as e:
                    logger.warning(f"Error fetching TMDB data for {content_type} {tmdb_id}: {e}")
                    continue

            logger.info(
                f"Enhanced What's New: {len(tmdb_items)} → {len(enhanced_releases)} items with full TMDB data"
            )

            return WatchmodeWhatsNewResponse(
                results=enhanced_releases,
                total_results=len(enhanced_releases),
                region=region,
                generated_at=datetime.now(UTC).isoformat(),
                data_source="watchmode_list + tmdb_complete",
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_whats_new: {e}")
            return WatchmodeWhatsNewResponse(
                results=[],
                total_results=0,
                region=region,
                generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                data_source="watchmode_list + tmdb_complete",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(WatchmodeWrapperCache, prefix="get_watchmode_title_details_wrapper")
    async def get_watchmode_title_details(
        self, watchmode_id: int, **kwargs
    ) -> WatchmodeTitleDetailsResponse:
        """
        Async wrapper function to get title details from Watchmode.

        Returns:
            WatchmodeTitleDetailsResponse: MCBaseItem derivative containing title details or error information
        """
        try:
            # Get API key from auth service
            api_key = watchmode_auth.watchmode_api_key

            if not api_key:
                return WatchmodeTitleDetailsResponse(
                    id=watchmode_id,
                    title="",
                    type="",
                    error="Watchmode API key is required",
                    status_code=400,
                )

            service = WatchmodeService(api_key)
            details = await service.get_title_details(watchmode_id)

            if not details:
                return WatchmodeTitleDetailsResponse(
                    id=watchmode_id,
                    title="",
                    type="",
                    error=f"Title with ID {watchmode_id} not found",
                    status_code=404,
                )

            # Also get streaming sources
            sources = await service.get_title_streaming_sources(watchmode_id)

            # Combine the data
            # Note: Watchmode API returns sources as a list directly, not wrapped in a dict
            if isinstance(sources, list):
                streaming_sources = sources
            elif isinstance(sources, dict):
                streaming_sources = sources.get("sources", [])
            else:
                streaming_sources = []

            # Build response from details dict
            # Handle None values for list fields
            networks = details.get("networks")
            if networks is None:
                networks = []
            network_names = details.get("network_names")
            if network_names is None:
                network_names = []
            genre_names = details.get("genre_names")
            if genre_names is None:
                genre_names = []
            similar_titles = details.get("similar_titles")
            if similar_titles is None:
                similar_titles = []

            return WatchmodeTitleDetailsResponse(
                id=details.get("id", watchmode_id),
                title=details.get("title", ""),
                original_title=details.get("original_title"),
                plot_overview=details.get("plot_overview"),
                type=details.get("type", ""),
                runtime_minutes=details.get("runtime_minutes"),
                year=details.get("year"),
                end_year=details.get("end_year"),
                release_date=details.get("release_date"),
                imdb_id=details.get("imdb_id"),
                tmdb_id=details.get("tmdb_id"),
                tmdb_type=details.get("tmdb_type"),
                user_rating=details.get("user_rating"),
                critic_score=details.get("critic_score"),
                us_rating=details.get("us_rating"),
                poster=details.get("poster"),
                backdrop=details.get("backdrop"),
                original_language=details.get("original_language"),
                genre_names=genre_names,
                similar_titles=similar_titles,
                networks=networks,
                network_names=network_names,
                streaming_sources=streaming_sources,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_watchmode_title_details: {e}")
            return WatchmodeTitleDetailsResponse(
                id=watchmode_id,
                title="",
                type="",
                error=str(e),
                status_code=500,
            )

    @RedisCache.use_cache(WatchmodeWrapperCache, prefix="search_titles_wrapper")
    async def search_titles(
        self, query: str, types: str = "movie,tv", **kwargs
    ) -> "WatchmodeSearchResponse":
        """
        Async wrapper function to search for titles by name.

        Args:
            query: Search query string
            types: Content types to search (default: movie,tv)
                   Valid values: "movie", "tv", "person" or comma-separated combinations

        Returns:
            WatchmodeSearchResponse: Search results with watchmode IDs or error information
        """
        from api.watchmode.models import WatchmodeSearchResponse, WatchmodeSearchResult

        try:
            # Get API key from auth service
            api_key = watchmode_auth.watchmode_api_key

            if not api_key:
                return WatchmodeSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error="Watchmode API key is required",
                    status_code=400,
                )

            if not query or not query.strip():
                return WatchmodeSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error="Search query is required",
                    status_code=400,
                )

            service = WatchmodeService(api_key)
            search_results = await service.search_titles(query=query, types=types)

            if not search_results:
                return WatchmodeSearchResponse(
                    results=[],
                    total_results=0,
                    query=query,
                    error="No results found",
                    status_code=404,
                )

            # Parse the search results
            # Watchmode API returns: {"title_results": [...], "people_results": [...]}
            title_results = search_results.get("title_results", [])

            # Convert to WatchmodeSearchResult models
            results: list[WatchmodeSearchResult] = []
            for item in title_results:
                try:
                    # Map API response to our model
                    result = WatchmodeSearchResult(
                        id=item.get("id"),
                        name=item.get("name", ""),
                        title=item.get("title"),  # Some results have title instead of name
                        type=item.get("type", ""),
                        year=item.get("year"),
                        result_type="title",
                        tmdb_id=item.get("tmdb_id"),
                        tmdb_type=item.get("tmdb_type"),
                        image_url=item.get("image_url"),
                    )
                    results.append(result)
                except Exception as e:
                    logger.warning(f"Error parsing search result item: {e}")
                    continue

            return WatchmodeSearchResponse(
                results=results,
                total_results=len(results),
                query=query,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in search_titles: {e}")
            return WatchmodeSearchResponse(
                results=[],
                total_results=0,
                query=query,
                error=str(e),
                status_code=500,
            )


watchmode_wrapper = WatchmodeWrapper()
