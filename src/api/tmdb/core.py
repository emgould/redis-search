"""
TMDB Core Service - Base service for TMDB API operations
Handles core API communication, details, and media enhancement.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, cast

from api.subapi.comscore import BoxOfficeData, comscore_wrapper
from api.tmdb.auth import Auth
from api.tmdb.models import MCBaseMediaItem, MCMovieItem, MCTvItem
from api.tmdb.tmdb_models import (
    TMDBGenre,
    TMDBKeyword,
    TMDBMovieDetailsResult,
    TMDBProvidersResponse,
    TMDBTvDetailsResult,
)
from contracts.models import MCType
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 7 days for most data
CacheExpiration = 7 * 24 * 60 * 60  # 7 days

# Request cache - separate from other caches, independent refresh
TMDBRequestCache = RedisCache(
    defaultTTL=7 * 24 * 60 * 60,  # 7 days - raw API responses cache longer
    prefix="tmdb_request",
    verbose=False,
    isClassMethod=True,
)

# Cache for standalone async functions (not class methods)
# Moved here to avoid circular imports with wrappers.py
TMDBFunctionCache = RedisCache(
    defaultTTL=24 * 60 * 60,  # 24 hours - matches FlixPatrol daily updates
    prefix="tmdb_func",
    verbose=False,
    isClassMethod=False,  # For standalone functions
)
TMDBCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="tmdb",
    verbose=False,
)


logger = get_logger(__name__)


class TMDBService(Auth, BaseAPIClient):
    """
    Core TMDB service for API communication and media details.
    Handles basic TMDB operations, details fetching, and media enhancement.
    """

    # Rate limiter configuration: TMDB current limits (2024/2025)
    # TMDB allows 40 requests per second for free API keys
    # Using 35 requests per second to stay safely under the limit
    _rate_limit_max = 25
    _rate_limit_period = 1

    def __init__(self):
        """Initialize TMDB service with API token."""
        super().__init__()

    @staticmethod
    async def _batch_process(
        tasks: list, batch_size: int = 10, delay_between_batches: float = 0.1
    ) -> list:
        """Process async tasks in batches to avoid overwhelming rate limits.

        Args:
            tasks: List of async tasks (coroutines) to execute
            batch_size: Number of tasks to process concurrently per batch (default: 10)
            delay_between_batches: Delay in seconds between batches (default: 0.1s)

        Returns:
            List of results in the same order as tasks
        """
        results = []
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            results.extend(batch_results)

            # Add delay between batches (except after the last batch)
            if i + batch_size < len(tasks):
                await asyncio.sleep(delay_between_batches)

        return results

    @RedisCache.use_cache(TMDBRequestCache, prefix="tmdb_api")
    async def _make_request(
        self, endpoint: str, params: dict[str, Any] | None = None, max_retries: int = 3
    ) -> dict[str, Any] | None:
        """Make async HTTP request to TMDB API.

        This method brokers the call to _core_async_request with TMDB-specific config.
        The cache decorator ensures responses are cached independently.

        Args:
            endpoint: API endpoint (e.g., 'movie/123')
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            JSON response dict or None on error

        Raises:
            Exception: If the request fails after all retries
        """
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth_headers()

        result = await self._core_async_request(
            url=url,
            params=params,
            headers=headers,
            timeout=60,
            max_retries=max_retries,
            rate_limit_max=self._rate_limit_max,
            rate_limit_period=self._rate_limit_period,
        )
        # Cast to expected type since return_status_code=False
        return cast(dict[str, Any] | None, result)

    def _get_sort_date(self, item: MCBaseMediaItem) -> float:
        """
        Helper method to get a sortable date for content (more recent = better sorting).

        Args:
            item: MCBaseMediaItem (MCMovieItem has release_date, MCTvItem has first_air_date)

        Returns:
            Negative timestamp (more recent dates sort first when negated)
        """
        try:
            # Get date string, handling None and empty cases
            # Use getattr to safely access attributes that may not exist on all item types
            release_date = getattr(item, "release_date", None)
            first_air_date = getattr(item, "first_air_date", None)
            date_str = release_date or first_air_date

            if not date_str or not isinstance(date_str, str):
                logger.debug(
                    f"No valid date found for item {item.tmdb_id} - release_date: {release_date}, first_air_date: {first_air_date}"
                )
                return 0  # Default for missing/invalid dates

            # Remove any whitespace and handle empty strings
            date_str = date_str.strip()
            if not date_str:
                return 0

            # Try to parse the date
            from datetime import datetime

            # Handle different date formats
            date_formats = ["%Y-%m-%d", "%Y-%m", "%Y"]

            for date_format in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, date_format)
                    return -date_obj.timestamp()  # Negative for reverse chronological
                except ValueError:
                    continue

            # If none of the formats worked, log it and return default
            logger.debug(f"Could not parse date '{date_str}' for item {item.tmdb_id}")
            return 0

        except Exception as e:
            logger.warning(f"Unexpected error parsing date for item {item.tmdb_id}: {e}")
            return 0  # Safe default

    def is_vaild_movie(self, item: MCBaseMediaItem) -> bool:
        """Filter out movies with empty overviews or no streaming availability.

        For basic search results (without watch provider data), only validates
        overview, poster_path, and title. For enriched items, also checks watch providers.
        """
        if item.media_type != "movie":
            return True

        # Basic validation: title, overview and poster are required
        if not item.title or len(item.title) == 0:
            return False
        if not item.overview or len(item.overview) == 0 or item.poster_path is None:
            return False

        # If watch_providers dict exists and has content, validate streaming availability
        # If watch_providers is empty/None, assume item hasn't been enriched yet and allow it
        if item.watch_providers and len(item.watch_providers) > 0:
            # Item has been enriched, check for streaming availability
            has_availability = (
                item.streaming_platform == "In Theaters"
                or (item.watch_providers.get("flatrate", None) is not None)
                or (item.watch_providers.get("buy", None) is not None)
                or (item.watch_providers.get("rent", None) is not None)
            )
            return has_availability

        # Basic search result without enrichment - allow it
        return True

    def is_vaild_tv(self, item: MCBaseMediaItem) -> bool:
        """Filter out TV shows with empty overviews or no streaming availability.

        For basic search results (without watch provider data), only validates
        name/title, overview and poster_path. For enriched items, also checks watch providers.
        """
        if item.media_type != "tv":
            return True

        item_name = item.name or item.title or "Unknown"

        # Basic validation: name/title, overview and poster are required
        if (not item.name or len(item.name) == 0) and (not item.title or len(item.title) == 0):
            logger.debug("is_vaild_tv: Filtered '%s' - no name/title", item_name)
            return False
        if not item.overview or len(item.overview) == 0 or item.poster_path is None:
            logger.debug("is_vaild_tv: Filtered '%s' - missing overview or poster", item_name)
            return False

        # If watch_providers dict exists and has content, validate streaming availability
        # If watch_providers is empty/None, assume item hasn't been enriched yet and allow it
        if item.watch_providers and len(item.watch_providers) > 0:
            # Item has been enriched, check for streaming availability
            has_availability = (
                item.streaming_platform == "In Theaters"
                or (item.watch_providers.get("flatrate", None) is not None)
                or (item.watch_providers.get("buy", None) is not None)
                or (item.watch_providers.get("rent", None) is not None)
            )
            if not has_availability:
                logger.debug("is_vaild_tv: Filtered '%s' - no US streaming availability", item_name)
            return has_availability

        # Basic search result without enrichment - allow it
        return True

    @RedisCache.use_cache(TMDBCache, prefix="media_details")
    async def get_media_details(
        self,
        tmdb_id: int,
        media_type: MCType,
        include_cast: bool = True,
        include_videos: bool = True,
        include_watch_providers: bool = True,
        include_keywords: bool = True,
        include_release_dates: bool = True,
        cast_limit: int = 5,
        no_cache: bool = False,
        **kwargs: Any,
    ) -> MCBaseMediaItem:
        """
        Get detailed information for a movie or TV show.

        Args:
            tmdb_id: TMDB ID
            media_type: MCType.MOVIE or MCType.TV_SERIES
            include_cast: Include cast and crew information
            include_videos: Include trailers and videos
            include_watch_providers: Include streaming providers
            include_keywords: Include keywords information

        Returns:
            Detailed media dict or None if not found
        """
        # Get basic details
        if media_type == MCType.TV_SERIES:
            endpoint = f"tv/{tmdb_id}"
        else:
            endpoint = f"movie/{tmdb_id}"

        params = {"language": "en-US"}

        details_data = await self._make_request(endpoint, params)
        if not details_data:
            return MCBaseMediaItem(
                mc_type=media_type,
                tmdb_id=tmdb_id,
                error=f"Content with ID {tmdb_id} not found",
                status_code=404,
            )

        try:
            if media_type == MCType.TV_SERIES:
                details: MCBaseMediaItem = MCTvItem.from_tv_details(
                    TMDBTvDetailsResult.model_validate(details_data), self.image_base_url
                )
            else:
                details = MCMovieItem.from_movie_details(
                    TMDBMovieDetailsResult.model_validate(details_data), self.image_base_url
                )
        except Exception as e:
            logger.warning(f"Validation error for {media_type} {tmdb_id}: {e}")
            return MCBaseMediaItem(
                mc_type=media_type,
                tmdb_id=tmdb_id,
                error=f"Validation error: {e}",
                status_code=500,
            )
        # Process basic information
        if details.source_id == "0" or not details.source_id:
            details.error = f"Content with ID {tmdb_id} not found"
            details.status_code = 404
            return details

        # Add additional details in parallel
        tasks = []

        if include_cast:
            tasks.append(("cast", self._get_cast_and_crew(tmdb_id, media_type, limit=cast_limit)))

        if include_videos:
            tasks.append(("videos", self._get_videos(tmdb_id, media_type)))

        if include_watch_providers:
            tasks.append(
                (
                    "watch_providers",
                    self._get_watch_providers(tmdb_id, media_type, no_cache=no_cache),
                )
            )

        if include_keywords:
            tasks.append(("keywords", self._get_keywords(tmdb_id, media_type)))

        # Only fetch release_dates for movies (TV shows use different air date structure)
        if include_release_dates and media_type == MCType.MOVIE:
            tasks.append(("release_dates", self._get_release_dates(tmdb_id)))

        if tasks:
            results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)

            # Track enrichment failures — any failure means incomplete data
            enrichment_failures: list[str] = []

            # Process results
            for task_name, task_result in zip(tasks, results, strict=True):
                if task_name[0] == "cast":
                    cast_result = task_result
                    if isinstance(cast_result, Exception):
                        enrichment_failures.append(f"cast: {cast_result}")
                    elif isinstance(cast_result, dict):
                        details.tmdb_cast = cast_result.get("tmdb_cast", {})
                        details.main_cast = cast_result.get("main_cast", [])
                elif task_name[0] == "videos":
                    video_result = task_result
                    if isinstance(video_result, Exception):
                        enrichment_failures.append(f"videos: {video_result}")
                    elif isinstance(video_result, dict):
                        details.tmdb_videos = video_result.get("tmdb_videos", {})
                        details.primary_trailer = video_result.get("primary_trailer", {})
                        details.trailers = video_result.get("trailers", [])
                        details.clips = video_result.get("clips", [])
                elif task_name[0] == "watch_providers":
                    provider_result = task_result
                    if isinstance(provider_result, Exception):
                        enrichment_failures.append(f"watch_providers: {provider_result}")
                    elif isinstance(provider_result, dict):
                        details.watch_providers = provider_result.get("watch_providers", {})
                        details.streaming_platform = provider_result.get("streaming_platform")
                elif task_name[0] == "keywords":
                    keywords_result = task_result
                    if isinstance(keywords_result, Exception):
                        enrichment_failures.append(f"keywords: {keywords_result}")
                    elif isinstance(keywords_result, dict):
                        details.keywords = keywords_result.get("keywords", [])
                        details.keywords_count = keywords_result.get("keywords_count", 0)
                elif task_name[0] == "release_dates":
                    release_dates_result = task_result
                    if isinstance(release_dates_result, Exception):
                        enrichment_failures.append(f"release_dates: {release_dates_result}")
                    elif isinstance(release_dates_result, dict) and isinstance(
                        details, MCMovieItem
                    ):
                        details.release_dates = release_dates_result.get("release_dates", {})

            # If any enrichment step failed, flag the result as an error.
            # This prevents caching (RedisCache skips entries with .error set)
            # and surfaces the failure to callers like the ETL.
            if enrichment_failures:
                failed_steps = ", ".join(enrichment_failures)
                logger.warning(f"Enrichment failed for {media_type} {tmdb_id}: {failed_steps}")
                details.error = f"Partial enrichment failure: {failed_steps}"
                details.status_code = 500

        return details

    @RedisCache.use_cache(TMDBCache, prefix="enhance_media_item")
    async def enhance_media_item(self, media_item: MCBaseMediaItem) -> MCBaseMediaItem:
        """Add detailed information to a basic media item.

        Args:
            media_item: Basic media item to enhance

        Returns:
            Enhanced media dict with cast, videos, providers, keywords
        """
        tmdb_id = media_item.tmdb_id
        media_type_str = media_item.media_type
        item = media_item.model_dump()

        if not tmdb_id or not media_type_str:
            return media_item

        # Convert string media_type to MCType enum
        if media_type_str == "tv":
            mc_type = MCType.TV_SERIES
        elif media_type_str == "movie":
            mc_type = MCType.MOVIE
        else:
            # Default to movie for unknown types
            mc_type = MCType.MOVIE

        # Get additional details - DO NOT fetch basic details here to avoid timeout
        tasks = [
            self._get_watch_providers(tmdb_id, mc_type),
            self._get_cast_and_crew(tmdb_id, mc_type, limit=5),  # Top 5 cast only
            self._get_videos(tmdb_id, mc_type),
            self._get_keywords(tmdb_id, mc_type),
        ]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Add watch providers
            if (
                not isinstance(results[0], Exception)
                and isinstance(results[0], dict)
                and results[0]
            ):
                item.update(results[0])

            # Add cast data
            if (
                not isinstance(results[1], Exception)
                and isinstance(results[1], dict)
                and results[1]
            ):
                item.update(results[1])

            # Add video data
            if (
                not isinstance(results[2], Exception)
                and isinstance(results[2], dict)
                and results[2]
            ):
                item.update(results[2])

            # Add keywords data
            if (
                not isinstance(results[3], Exception)
                and isinstance(results[3], dict)
                and results[3]
            ):
                item.update(results[3])

        except Exception as e:
            logger.warning(f"Error enhancing media item {tmdb_id}: {e}")

        # Convert back to MCBaseMediaItem before returning
        return MCBaseMediaItem.model_validate(item)

    async def _get_cast_and_crew(
        self, tmdb_id: int, media_type: MCType, limit: int | None = None
    ) -> dict[str, Any]:
        """Get cast and crew information.

        Args:
            tmdb_id: TMDB ID
            media_type: 'movie' or 'tv'
            limit: Optional limit on number of cast members to return

        Returns:
            Dict with cast and crew data
        """
        if media_type == MCType.MOVIE:
            path = "movie"
        elif media_type == MCType.TV_SERIES:
            path = "tv"
        else:
            raise ValueError(f"Invalid media type: {media_type}")

        endpoint = f"{path}/{tmdb_id}/credits"

        data = await self._make_request(endpoint)
        if not data:
            return {}

        cast_data = data.get("cast", [])
        crew_data = data.get("crew", [])

        # Process cast (limit if specified)
        processed_cast = []
        cast_limit = limit or len(cast_data)

        for actor in sorted(cast_data, key=lambda x: x.get("order", 999))[:cast_limit]:
            profile_path = actor.get("profile_path")
            cast_member = {
                "id": actor.get("id"),
                "name": actor.get("name"),
                "character": actor.get("character"),
                "order": actor.get("order", 999),
                "gender": actor.get("gender"),
                "profile_path": profile_path,
            }

            # Add profile image URLs
            if profile_path:
                cast_member["profile_images"] = {
                    "small": f"{self.image_base_url}w45{profile_path}",
                    "medium": f"{self.image_base_url}w185{profile_path}",
                    "large": f"{self.image_base_url}h632{profile_path}",
                    "original": f"{self.image_base_url}original{profile_path}",
                }
                # Legacy compatibility
                cast_member["profile_image_url"] = f"{self.image_base_url}w185{profile_path}"
                cast_member["image_url"] = f"{self.image_base_url}w185{profile_path}"
                cast_member["has_image"] = True
            else:
                cast_member["profile_images"] = None
                cast_member["profile_image_url"] = None
                cast_member["image_url"] = None
                cast_member["has_image"] = False

            processed_cast.append(cast_member)

        # Find director
        director = None
        cast_limit = limit or len(crew_data)
        for crew_member in sorted(crew_data, key=lambda x: x.get("order", 999))[:cast_limit]:
            if crew_member.get("job") == "Director":
                profile_path = crew_member.get("profile_path")
                director = {
                    "id": crew_member.get("id"),
                    "name": crew_member.get("name"),
                    "job": "Director",
                }

                if profile_path:
                    director["profile_images"] = {
                        "small": f"{self.image_base_url}w45{profile_path}",
                        "medium": f"{self.image_base_url}w185{profile_path}",
                        "large": f"{self.image_base_url}h632{profile_path}",
                        "original": f"{self.image_base_url}original{profile_path}",
                    }
                break

        result = {
            "tmdb_cast": {
                "cast": processed_cast,
                "total_cast": len(cast_data),
                "cast_count": len(processed_cast),
            },
            "main_cast": processed_cast[:5],  # Top 5 for compatibility
        }

        if director:
            result["tmdb_cast"]["director"] = director  # type: ignore[index]
            result["director"] = director

        return result

    async def _get_videos(self, tmdb_id: int, media_type: MCType, **kwargs: Any) -> dict[str, Any]:
        """Get videos/trailers for media.

        Args:
            tmdb_id: TMDB ID
            media_type: MCType.MOVIE or MCType.TV_SERIES

        Returns:
            Dict with video data categorized by type
        """
        if media_type == MCType.MOVIE:
            path = "movie"
        elif media_type == MCType.TV_SERIES:
            path = "tv"
        else:
            raise ValueError(f"Invalid media type: {media_type}")

        endpoint = f"{path}/{tmdb_id}/videos"

        data = await self._make_request(endpoint)
        if not data:
            return {}

        videos = data.get("results", [])

        # Categorize videos
        trailers = []
        teasers = []
        clips = []
        behind_the_scenes = []
        other_videos = []

        for video in videos:
            video_type = video.get("type", "").lower()
            site = video.get("site", "")

            video_info = {
                "id": video.get("id"),
                "key": video.get("key"),
                "name": video.get("name"),
                "site": site,
                "type": video.get("type"),
                "official": video.get("official", False),
                "published_at": video.get("published_at"),
                "size": video.get("size", 1080),
                "iso_639_1": video.get("iso_639_1", "en"),
                "iso_3166_1": video.get("iso_3166_1", "US"),
            }

            # Generate URLs
            if site.lower() == "youtube":
                video_info["url"] = f"https://www.youtube.com/watch?v={video.get('key')}"
                video_info["embed_url"] = f"https://www.youtube.com/embed/{video.get('key')}"
                video_info["thumbnail_url"] = (
                    f"https://img.youtube.com/vi/{video.get('key')}/maxresdefault.jpg"
                )
            elif site.lower() == "vimeo":
                video_info["url"] = f"https://vimeo.com/{video.get('key')}"
                video_info["embed_url"] = f"https://player.vimeo.com/video/{video.get('key')}"

            # Categorize by type
            if video_type == "trailer":
                trailers.append(video_info)
            elif video_type == "teaser":
                teasers.append(video_info)
            elif video_type == "clip":
                clips.append(video_info)
            elif video_type in ["behind the scenes", "making of"]:
                behind_the_scenes.append(video_info)
            else:
                other_videos.append(video_info)

        # Sort by official status and publication date
        def sort_videos(video_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return sorted(
                video_list,
                key=lambda x: (not x.get("official", False), x.get("published_at", "")),
                reverse=True,
            )

        sorted_trailers = sort_videos(trailers)
        sorted_teasers = sort_videos(teasers)
        sorted_clips = sort_videos(clips)
        sorted_behind_the_scenes = sort_videos(behind_the_scenes)
        sorted_other = sort_videos(other_videos)

        result = {
            "tmdb_videos": {
                "trailers": sorted_trailers,
                "teasers": sorted_teasers,
                "clips": sorted_clips,
                "behind_the_scenes": sorted_behind_the_scenes,
                "other": sorted_other,
                "total_videos": len(videos),
                "video_categories": {
                    "trailers_count": len(sorted_trailers),
                    "teasers_count": len(sorted_teasers),
                    "clips_count": len(sorted_clips),
                    "behind_the_scenes_count": len(sorted_behind_the_scenes),
                    "other_count": len(sorted_other),
                },
            }
        }

        # Add primary trailer (prefer trailers over teasers)
        all_promotional = sorted_trailers + sorted_teasers
        if all_promotional:
            result["primary_trailer"] = all_promotional[0]
            result["tmdb_videos"]["primary_trailer"] = all_promotional[0]

        # Add legacy compatibility fields
        if sorted_trailers:
            result["trailers"] = sorted_trailers  # type: ignore[assignment]

        if sorted_clips:
            result["clips"] = sorted_clips  # type: ignore[assignment]

        return result

    @RedisCache.use_cache(TMDBCache, prefix="watch_providers")
    async def _get_watch_providers(
        self, tmdb_id: int, media_type: MCType, region: str = "US", **kwargs
    ) -> dict[str, Any]:
        """Get watch providers for media.

        Args:
            tmdb_id: TMDB ID
            media_type: MCType.MOVIE or MCType.TV_SERIES
            region: Region code (e.g., 'US', 'CA')

        Returns:
            Dict with watch provider data
        """
        if media_type == MCType.MOVIE:
            path = "movie"
        elif media_type == MCType.TV_SERIES:
            path = "tv"
        else:
            raise ValueError(f"Invalid media type: {media_type}")

        endpoint = f"{path}/{tmdb_id}/watch/providers"

        data = await self._make_request(endpoint)
        if not data:
            return {}

        results = data.get("results", {})
        region_data = results.get(region, {})

        if not region_data:
            return {}

        # Helper function to sort providers by display_priority
        def sort_providers(providers: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return sorted(providers, key=lambda x: x.get("display_priority", 999))

        response_data: dict[str, Any] = {
            "watch_providers": {
                "region": region,
                "link": region_data.get("link"),
                "tmdb_id": tmdb_id,
                "content_type": media_type.value,
            }
        }

        # Process different provider types
        if region_data.get("flatrate"):
            response_data["watch_providers"]["flatrate"] = sort_providers(region_data["flatrate"])

        if region_data.get("buy"):
            response_data["watch_providers"]["buy"] = sort_providers(region_data["buy"])

        if region_data.get("rent"):
            response_data["watch_providers"]["rent"] = sort_providers(region_data["rent"])

        # Add primary provider
        if response_data["watch_providers"].get("flatrate"):
            primary = response_data["watch_providers"]["flatrate"][0]
            response_data["watch_providers"]["primary_provider"] = {
                "provider_name": primary.get("provider_name"),
                "provider_id": primary.get("provider_id"),
                "logo_path": primary.get("logo_path"),
                "display_priority": primary.get("display_priority"),
                "type": "flatrate",
            }
            # Legacy compatibility
            response_data["streaming_platform"] = primary.get("provider_name")

        return response_data

    @RedisCache.use_cache(TMDBCache, prefix="tv_providers")
    async def get_providers(
        self, media_type: MCType, region: str = "US", **kwargs
    ) -> TMDBProvidersResponse:
        """
        Get list of available TV streaming providers from TMDB.

        Args:
            tmdb_token (str): TMDB API bearer token
            region (str): Region code (default "US")

        Returns:
            list[TMDBWatchProvider]: list of TV providers sorted by display_priority
        """

        try:
            params = {"watch_region": region}
            if media_type == MCType.MOVIE:
                list_type = "movie"
            elif media_type == MCType.TV_SERIES:
                list_type = "tv"
            else:
                raise ValueError(f"Invalid media type: {media_type}")

            endpoint = f"watch/providers/{list_type}"

            data = await self._make_request(endpoint, params)
            if not data:
                return TMDBProvidersResponse(
                    list_type=list_type,  # type: ignore[arg-type]
                    results=[],
                    mc_type=MCType.PROVIDERS_LIST,
                    error=f"No data returned for {list_type} providers",
                    status_code=404,
                )

            data_with_type = {**data, "list_type": list_type}
            response = TMDBProvidersResponse.model_validate(data_with_type)
            providers = response.results

            if not providers:
                logger.warning(f"No TV providers found for region {region}")
                return TMDBProvidersResponse(
                    list_type=list_type,  # type: ignore[arg-type]
                    results=[],
                    mc_type=MCType.PROVIDERS_LIST,
                    error=f"No {list_type} providers found for region {region}",
                    status_code=404,
                )

            # Filter out providers with "Channel" in their name
            filtered_providers = [p for p in providers if "channel" not in p.provider_name.lower()]

            # Sort by display_priority
            response.results = sorted(filtered_providers, key=lambda x: x.display_priority)

            return response

        except Exception as e:
            logger.error(f"Error getting providers for region {region}: {e}")
            return TMDBProvidersResponse(
                list_type=list_type,  # type: ignore[arg-type]
                results=[],
                mc_type=MCType.PROVIDERS_LIST,
                error=str(e),
                status_code=500,
            )

    async def _get_keywords(self, tmdb_id: int, media_type: MCType) -> dict[str, Any]:
        """Get keywords for a movie or TV show.

        Args:
            tmdb_id: TMDB ID
            media_type: MCType.MOVIE or MCType.TV_SERIES

        Returns:
            Dict with keywords data
        """
        if media_type == MCType.MOVIE:
            path = "movie"
        elif media_type == MCType.TV_SERIES:
            path = "tv"
        else:
            raise ValueError(f"Invalid media type: {media_type}")

        endpoint = f"{path}/{tmdb_id}/keywords"

        data = await self._make_request(endpoint)
        if not data:
            return {}

        # Process keywords data - NOTE: TV shows use 'results', movies use 'keywords'
        keywords_raw = (
            data.get("results", []) if media_type == MCType.TV_SERIES else data.get("keywords", [])
        )

        processed_keywords = []
        for keyword in keywords_raw:
            processed_keywords.append({"id": keyword.get("id"), "name": keyword.get("name")})

        return {"keywords": processed_keywords, "keywords_count": len(processed_keywords)}

    async def _get_release_dates(self, tmdb_id: int) -> dict[str, Any]:
        """Get release dates and certifications for a movie.

        This endpoint provides release dates by country, including release type:
        - Type 1: Premiere
        - Type 2: Theatrical (limited)
        - Type 3: Theatrical
        - Type 4: Digital
        - Type 5: Physical
        - Type 6: TV

        Args:
            tmdb_id: TMDB movie ID

        Returns:
            Dict with release dates data in TMDB format
        """
        endpoint = f"movie/{tmdb_id}/release_dates"

        data = await self._make_request(endpoint)
        if not data:
            return {}

        return {"release_dates": data}

    @RedisCache.use_cache(TMDBCache, prefix="keyword_search")
    async def find_keywords_async(self, query: str) -> list[TMDBKeyword]:
        """Search for keywords by name.

        Args:
            query: Keyword search query
            page: Page number

        Returns:
            list of TMDBKeywords with typed keyword results
        """
        endpoint = "search/keyword"
        params = {"query": query, "page": 1}

        data = await self._make_request(endpoint, params)
        if not data:
            return []

        return [TMDBKeyword.model_validate(kw) for kw in data.get("results", [])]

    @RedisCache.use_cache(TMDBCache, prefix="genres_search")
    async def find_genres_async(self, language: str = "en-US") -> list[TMDBGenre]:
        """Search for genres by name.

        Args:
            language: Language code
            page: Page number

        Returns:
            list of TMDBGenres with typed genre results
        """
        movie_endpoint = "genres/movie/list"
        tv_endpoint = "genres/tv/list"
        params = {"language": language}

        data = await self._make_request(movie_endpoint, params)
        tv_data = await self._make_request(tv_endpoint, params)
        if not data or not tv_data:
            return []

        # Parse results as TMDBKeyword objects using Pydantic 2.0
        movie_genres = [TMDBGenre.model_validate(g) for g in data.get("genres", [])]
        tv_genres = [TMDBGenre.model_validate(g) for g in tv_data.get("genres", [])]

        return movie_genres + tv_genres

    def sort_movies_by_box_office(
        self, movies: list[MCMovieItem], box_office_data: BoxOfficeData
    ) -> list[dict[str, Any]]:
        """
        Sort a list of movies by their box office ranking.

        Movies with box office rankings are placed first in rank order,
        followed by movies without rankings in their original order.

        Args:
            movies: List of MCMovieItem objects
            box_office_data: Box office rankings data

        Returns:
            Sorted list of movie dicts with box office metadata when available
        """
        try:
            if not movies:
                logger.debug("No movies to sort")
                return []

            if not box_office_data or not box_office_data.rankings:
                logger.debug("No box office data to sort by, converting movies to dicts")
                # Convert all movies to dicts and sort by streaming platform
                movies_dicts = [m.model_dump() for m in movies]

                def streaming_platform_sort_key(x: dict[str, Any]) -> tuple[int, float]:
                    platform = (x.get("streaming_platform") or "").lower()
                    platform_priority = 0
                    if platform == "in theaters":
                        platform_priority = 0
                    elif platform == "on demand":
                        platform_priority = 1
                    else:
                        platform_priority = 2

                    release_timestamp = 0.0
                    release_date = x.get("release_date", "2025-01-01")
                    if release_date:
                        try:
                            release_timestamp = -datetime.fromisoformat(release_date).timestamp()
                        except (ValueError, TypeError):
                            release_timestamp = 0.0

                    return (platform_priority, release_timestamp)

                movies_dicts.sort(key=streaming_platform_sort_key)
                return movies_dicts

            # Separate movies with and without box office rankings
            movies_with_ranking: list[dict[str, Any]] = []
            movies_without_ranking: list[MCMovieItem] = []

            for movie in movies:
                # Get movie title from various possible fields
                movie_title = movie.title or movie.name

                if movie_title:
                    matching_ranking = comscore_wrapper.match_movie_to_ranking(
                        movie_title, box_office_data.rankings
                    )

                    # Check if match was successful (no error and rank > 0)
                    if (
                        matching_ranking
                        and not matching_ranking.error
                        and matching_ranking.rank > 0
                    ):
                        # Add ranking info to movie
                        movie_with_rank = movie.model_dump()
                        movie_with_rank["box_office_rank"] = matching_ranking.rank
                        movie_with_rank["box_office_estimate"] = matching_ranking.weekend_estimate
                        movie_with_rank["box_office_distributor"] = matching_ranking.dom_distributor
                        movies_with_ranking.append(movie_with_rank)
                    else:
                        movies_without_ranking.append(movie)
                else:
                    movies_without_ranking.append(movie)

            # Sort movies with rankings by their box office rank (lower rank number = higher position)
            movies_with_ranking.sort(key=lambda x: x.get("box_office_rank", 999))

            # Convert unranked movies to dicts and sort by streaming platform and release date
            movies_without_ranking_dicts = [m.model_dump() for m in movies_without_ranking]

            def streaming_platform_sort_key(x: dict[str, Any]) -> tuple[int, float]:
                platform = (x.get("streaming_platform") or "").lower()
                platform_priority = 0
                if platform == "in theaters":
                    platform_priority = 0
                elif platform == "on demand":
                    platform_priority = 1
                else:
                    platform_priority = 2

                release_timestamp = 0.0
                release_date = x.get("release_date", "2025-01-01")
                if release_date:
                    try:
                        release_timestamp = -datetime.fromisoformat(release_date).timestamp()
                    except (ValueError, TypeError):
                        release_timestamp = 0.0

                return (platform_priority, release_timestamp)

            movies_without_ranking_dicts.sort(key=streaming_platform_sort_key)

            sorted_movies = movies_with_ranking + movies_without_ranking_dicts

            logger.info(
                f"Sorted {len(movies)} movies: {len(movies_with_ranking)} with box office rankings, {len(movies_without_ranking_dicts)} without"
            )

            return sorted_movies

        except Exception as e:
            logger.error(f"Error sorting movies by box office: {e}")
            # Return original movies as dicts if sorting fails
            return [m.model_dump() for m in movies]


tmdb_service = TMDBService()
