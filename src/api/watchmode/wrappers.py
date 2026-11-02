"""
Watchmode Async Wrappers - Firebase Functions compatible async wrapper functions.
Provides async wrappers for Firebase Functions integration using ApiWrapperResponse pattern.

Includes fallback to Streaming Availability API when Watchmode fails.
"""

import asyncio
import os
from datetime import UTC, datetime
from typing import Literal, cast

from api.streaming_availability.auth import streaming_availability_auth
from api.streaming_availability.core import StreamingAvailabilityService
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
    WatchmodeSportsEvent,
    WatchmodeSportsResponse,
    WatchmodeTitleDetailsResponse,
    WatchmodeWhatsNewResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Fast timeout for API calls (5 seconds each) - same as deep_links pattern
API_TIMEOUT_SECONDS = 5

# Cache for wrapper class methods
WatchmodeWrapperCache = RedisCache(
    defaultTTL=6 * 60 * 60,  # 6 hours
    prefix="watchmode_wrapper",
    verbose=False,
    isClassMethod=True,  # Required for class methods
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
            releases = None
            data_source = "watchmode_list + tmdb_complete"
            watchmode_failed = False

            # Try Watchmode first with fast timeout (max_retries=1 for fast-fail)
            if api_key:
                try:
                    releases = await asyncio.wait_for(
                        watchmode_service.get_new_releases(
                            region=region, limit=limit, max_retries=1
                        ),
                        timeout=API_TIMEOUT_SECONDS,
                    )
                    if not releases or not releases.get("releases"):
                        logger.warning("Watchmode returned no releases, will try fallback")
                        watchmode_failed = True
                except TimeoutError:
                    logger.warning(
                        f"Watchmode timeout after {API_TIMEOUT_SECONDS}s, will try fallback"
                    )
                    watchmode_failed = True
                except Exception as wm_error:
                    logger.warning(f"Watchmode API failed: {wm_error}, will try fallback")
                    watchmode_failed = True
            else:
                watchmode_failed = True

            # Helper class for trending items
            class TrendingItem:
                def __init__(
                    self, tmdb_id: int | None, content_type: str, watchmode_id: int | None
                ):
                    self.tmdb_id = tmdb_id
                    self.content_type = content_type
                    self.watchmode_id = watchmode_id

            tmdb_items: list[TrendingItem] = []

            # If Watchmode succeeded, extract from releases
            if releases and releases.get("releases"):
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
                logger.info(f"Watchmode: Found {len(tmdb_items)} releases with TMDB IDs")

            # Fallback to Streaming Availability API if Watchmode failed or returned nothing
            if watchmode_failed or not tmdb_items:
                if streaming_availability_auth.has_rapidapi_keys():
                    try:
                        logger.info("Falling back to Streaming Availability API for new releases")
                        sa_service = StreamingAvailabilityService()

                        # Map region to country code (Watchmode uses "US", SA uses "us")
                        country_code = region.lower()

                        # Get recent changes (new additions) with fast timeout
                        changes = await asyncio.wait_for(
                            sa_service.get_changes(
                                country=country_code,
                                change_type="new",
                                item_type="show",  # Both movies and series
                            ),
                            timeout=API_TIMEOUT_SECONDS,
                        )

                        if changes and changes.get("changes") and changes.get("shows"):
                            data_source = "streaming_availability + tmdb_complete"
                            shows_dict = changes.get("shows", {})
                            seen_tmdb_ids: set[int] = set()

                            for change in changes.get("changes", []):
                                # Look up show in the shows dict by showId
                                show_id = change.get("showId")
                                if not show_id:
                                    continue

                                show = shows_dict.get(show_id, {})
                                if not show:
                                    continue

                                # Extract TMDB ID from tmdbId field (format: "movie/238" or "tv/1234")
                                tmdb_id_str = show.get("tmdbId", "")
                                if "/" in tmdb_id_str:
                                    parts = tmdb_id_str.split("/")
                                    if len(parts) == 2:
                                        try:
                                            tmdb_id = int(parts[1])
                                            # Deduplicate - same show can appear multiple times
                                            if tmdb_id in seen_tmdb_ids:
                                                continue
                                            seen_tmdb_ids.add(tmdb_id)

                                            content_type_sa: Literal["tv", "movie"] = (
                                                "tv" if parts[0] == "tv" else "movie"
                                            )
                                            item = TrendingItem(
                                                tmdb_id=tmdb_id,
                                                content_type=content_type_sa,
                                                watchmode_id=None,
                                            )
                                            tmdb_items.append(item)
                                        except ValueError:
                                            pass

                            logger.info(
                                f"Streaming Availability fallback: Found {len(tmdb_items)} unique items"
                            )
                    except TimeoutError:
                        logger.warning(
                            f"Streaming Availability timeout after {API_TIMEOUT_SECONDS}s"
                        )
                    except Exception as sa_error:
                        logger.error(f"Streaming Availability fallback failed: {sa_error}")
                else:
                    logger.warning("No RapidAPI key available for Streaming Availability fallback")

            # If still no items, return error
            if not tmdb_items:
                return WatchmodeWhatsNewResponse(
                    results=[],
                    total_results=0,
                    region=region,
                    generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                    data_source=data_source,
                    error="Failed to fetch new releases from any source",
                    status_code=500,
                )

            logger.info(f"Total: Found {len(tmdb_items)} releases with TMDB IDs")

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
                data_source=data_source,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error in get_whats_new: {e}")
            return WatchmodeWhatsNewResponse(
                results=[],
                total_results=0,
                region=region,
                generated_at=datetime.now(UTC).strftime("%Y-%m-%d"),
                data_source="error",
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

    async def get_sports(
        self,
        action: str = "events",
        region: str = "US",
        date: str | None = None,
        timezone_name: str = "America/New_York",
        mode: str = "all",
        league_ids: str = "",
        team_ids: str = "",
        followed_league_ids: str = "",
        followed_team_ids: str = "",
        search: str = "",
        page: int = 1,
        limit: int = 25,
        version: str | None = None,
        event_id: int | None = None,
        league_id: int | None = None,
        dma: str | None = None,
        team_league_hints: str = "",
    ) -> WatchmodeSportsResponse:
        """Sports frontend broker using the application's existing Redis caches.

        No wrapper-level caching: preferences are applied after shared core reads,
        and link deadlines must be checked on each response. Warm core caches from
        the existing scheduler to minimize user-triggered cache misses.
        """
        from copy import deepcopy
        from datetime import timedelta
        from hashlib import sha256
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        from pydantic import ValidationError

        from api.watchmode.core import (
            SPORTS_SECTION_ORDER,
            WatchmodeLiveError,
            new_york_dates_for_viewer_day,
            sports_feed_section,
        )
        from api.watchmode.team_alias_groups import mapping_records
        from utils.redis_cache import RedisUnavailableError

        try:
            if action not in {"events", "event", "sources", "leagues", "teams", "coverage"}:
                raise ValueError("Unsupported sports action")
            region = region.upper()
            if region not in {"US", "GB", "CA", "AU"}:
                raise ValueError("Unsupported sports region")
            if mode not in {"all", "for_you"} or page < 1 or not 1 <= limit <= 100:
                raise ValueError("Invalid mode, page or limit")
            if len(search) > 120:
                raise ValueError("search must be at most 120 characters")
            selected = []
            for value in (league_ids, team_ids, followed_league_ids, followed_team_ids):
                values = {int(v.strip()) for v in value.split(",") if v.strip()}
                if len(values) > 100 or any(v < 1 for v in values):
                    raise ValueError("Filters accept at most 100 positive IDs")
                selected.append(values)
            leagues, teams, follow_leagues, follow_teams = selected
            if dma is not None:
                raise ValueError("Sports viewing options are country-level")
            if action in {"event", "sources"} and (event_id is None or event_id < 1):
                raise ValueError("event_id is required and must be positive")
            teams_search = search.strip()
            if action == "teams":
                has_single_league = league_id is not None and league_id >= 1
                has_multi_league = bool(leagues) and len(teams_search) >= 2
                if not has_single_league and not has_multi_league:
                    raise ValueError(
                        "league_id is required for teams, or league_ids with search "
                        "of at least 2 characters"
                    )
            zone = ZoneInfo(timezone_name)
            day = datetime.strptime(
                date or datetime.now(zone).date().isoformat(), "%Y-%m-%d"
            ).replace(tzinfo=zone)
            date = day.date().isoformat()
            if action == "events":
                # Bound public cache-key cardinality. History is not part of this frontend.
                delta = (day.date() - datetime.now(zone).date()).days
                if not -1 <= delta <= 6:
                    raise ValueError("Sports date must be yesterday through six days ahead")
                # All timezones for supported regions can be enabled explicitly by host config.
                allowed_zones = os.getenv(
                    "WATCHMODE_SPORTS_TIMEZONES",
                    "America/New_York,America/Chicago,America/Denver,America/Los_Angeles,"
                    "America/Anchorage,Pacific/Honolulu,Europe/London,America/Toronto,"
                    "America/Vancouver,Australia/Sydney,Australia/Perth,UTC",
                ).split(",")
                if timezone_name not in allowed_zones:
                    raise ValueError("Timezone is not enabled for sports")
            api_key = watchmode_auth.watchmode_api_key
            if not api_key:
                return WatchmodeSportsResponse(
                    status_code=503, error="Watchmode unavailable", code="NOT_CONFIGURED"
                )
            service = WatchmodeService(api_key)
            if action == "coverage":
                return WatchmodeSportsResponse(
                    region=region, coverage=await service.get_live_coverage()
                )
            if action == "leagues":
                data = await service.get_live_leagues()
                items = [
                    {k: v for k, v in item.items() if "logo" not in k.lower()}
                    for item in data.get("leagues", [])
                ]
                if search:
                    items = [
                        item
                        for item in items
                        if search.casefold() in item.get("name", "").casefold()
                    ]
                return WatchmodeSportsResponse(
                    region=region, leagues=items, total_results=len(items)
                )
            if action == "teams":
                activity_loader = getattr(service, "get_live_team_activity", None)
                activity_available = activity_loader is not None
                activity = (
                    await activity_loader(region)
                    if activity_loader is not None
                    else {"active_team_ids": []}
                )
                active_team_ids = {
                    team_id
                    for team_id in activity.get("active_team_ids", [])
                    if isinstance(team_id, int) and team_id > 0
                }

                def annotate_activity(team: dict[str, object]) -> dict[str, object]:
                    if not activity_available:
                        return team
                    team_id = team.get("id")
                    team["is_active"] = (
                        isinstance(team_id, int) and team_id > 0 and team_id in active_team_ids
                    )
                    return team

                def roster_team(raw: dict[str, object], roster_league_id: int) -> dict[str, object]:
                    cleaned = strip_team_logos(raw)
                    league_value = cleaned.get("league_id")
                    if not isinstance(league_value, int) or league_value < 1:
                        cleaned["league_id"] = roster_league_id
                    return cleaned

                def strip_team_logos(item: dict[str, object]) -> dict[str, object]:
                    return {k: v for k, v in item.items() if "logo" not in k.lower()}

                def team_matches_search(item: dict[str, object]) -> bool:
                    if not teams_search:
                        return True
                    folded = teams_search.casefold()
                    for key in ("name", "short_name", "city"):
                        value = item.get(key)
                        if isinstance(value, str) and folded in value.casefold():
                            return True
                    return False

                if league_id is not None and league_id >= 1:
                    data = await service.get_live_teams(league_id)
                    raw_teams = mapping_records(data["teams"])
                    single_league_teams = []
                    for raw in raw_teams:
                        team = roster_team(raw, league_id)
                        if team_matches_search(team):
                            single_league_teams.append(annotate_activity(team))
                    return WatchmodeSportsResponse(
                        region=region,
                        teams=single_league_teams[(page - 1) * limit : page * limit],
                        total_results=len(single_league_teams),
                        page=page,
                        has_more=page * limit < len(single_league_teams),
                    )

                sem = asyncio.Semaphore(8)

                async def teams_for_league(lid: int) -> list[dict[str, object]]:
                    async with sem:
                        payload = await service.get_live_teams(lid)
                        raw_teams = mapping_records(payload.get("teams"))
                        matched: list[dict[str, object]] = []
                        for raw in raw_teams:
                            team = roster_team(raw, lid)
                            if team_matches_search(team):
                                matched.append(annotate_activity(team))
                        return matched

                batches = await asyncio.gather(
                    *[teams_for_league(lid) for lid in sorted(leagues)]
                )
                by_team_id: dict[int, dict[str, object]] = {}
                for batch in batches:
                    for item in batch:
                        team_id = item.get("id")
                        if isinstance(team_id, int) and team_id not in by_team_id:
                            by_team_id[team_id] = item
                def team_sort_key(team: dict[str, object]) -> tuple[str, int]:
                    league_val = team.get("league_id")
                    league_num = league_val if isinstance(league_val, int) else 0
                    return (str(team.get("name") or "").casefold(), league_num)

                merged_team_items = sorted(by_team_id.values(), key=team_sort_key)
                return WatchmodeSportsResponse(
                    region=region,
                    teams=merged_team_items[(page - 1) * limit : page * limit],
                    total_results=len(merged_team_items),
                    page=page,
                    has_more=page * limit < len(merged_team_items),
                )
            if action == "sources":
                data = await service.get_live_event_sources(event_id, region)
                if data.get("event_type") != "sport":
                    return WatchmodeSportsResponse(status_code=404, error="Sports event not found")
                projected = WatchmodeSportsEvent(
                    **{**data, "sources_fetched_at": data["fetched_at"]}
                )
                return WatchmodeSportsResponse(
                    region=region,
                    sources=projected.sources,
                    event_destinations=projected.event_destinations,
                    sources_state=projected.sources_state,
                    viewing_links_need_refresh=projected.viewing_links_need_refresh,
                    fetched_at=data["fetched_at"],
                    market=data.get("market"),
                )
            if action == "event":
                data = await service.get_live_event(event_id, region)
                if data.get("event_type") != "sport":
                    return WatchmodeSportsResponse(status_code=404, error="Sports event not found")
                return WatchmodeSportsResponse(region=region, event=WatchmodeSportsEvent(**data))

            ny_dates = new_york_dates_for_viewer_day(day)
            snapshots = [await service.get_live_events_day(ny_date, region) for ny_date in ny_dates]
            ncaam_start_times: dict[tuple[int, int], str] = {}
            if action == "events" and region in {"US", "CA"}:
                from api.watchmode.espn_ncaam_scoreboard import (
                    apply_ncaam_espn_start_time,
                    build_ncaam_start_times_by_matchup,
                )

                get_scoreboard = getattr(service, "get_ncaam_scoreboard", None)
                if callable(get_scoreboard):
                    espn_day = date.replace("-", "")
                    try:
                        scoreboard = await get_scoreboard(espn_day)
                        ncaam_start_times = build_ncaam_start_times_by_matchup(scoreboard)
                    except Exception:
                        logger.exception(
                            "ESPN NCAAM scoreboard fetch failed for date=%s", espn_day
                        )
            # Work on a copy: never mutate a cached object with one user's filters.
            # Several New York dates can overlap one viewer day. Status stays on
            # those snapshots; there is no second download per viewer timezone.
            index = {}
            for snapshot in snapshots:
                for raw_event in snapshot["events"]:
                    index[raw_event["id"]] = deepcopy(raw_event)
            freshness = min(snapshot["fetched_at"] for snapshot in snapshots)
            for event in index.values():
                if event.get("sources_state") != "not_requested":
                    event["sources_fetched_at"] = freshness
                event["status_fetched_at"] = freshness
            # Client restarts pagination on 409 rather than skipping or duplicating events.
            current_version = sha256(
                "|".join(snapshot["fetched_at"] for snapshot in snapshots).encode()
            ).hexdigest()[:24]
            if (version and version != current_version) or (page > 1 and not version):
                return WatchmodeSportsResponse(
                    status_code=409,
                    error="Restart pagination at page 1",
                    code="SNAPSHOT_CHANGED",
                    version=current_version,
                )
            results = []
            college_team_info_by_league: dict[int, dict[int, tuple[str, str | None]]] = {}
            espn_ranks_by_path: dict[str, tuple[dict[str, int], dict[str, int]]] = {}
            espn_paths_by_league_id = {
                471: "football/college-football",
                472: "basketball/mens-college-basketball",
                473: "basketball/womens-college-basketball",
                415073: "volleyball/womens-college-volleyball",
                413998: "soccer/usa.ncaa.w.1",
                413997: "soccer/usa.ncaa.m.1",
            }
            for raw in index.values():
                if ncaam_start_times:
                    apply_ncaam_espn_start_time(raw, ncaam_start_times)
                league_obj = raw.get("league") or {}
                league = league_obj.get("id")
                participants = {
                    (raw.get(side) or {}).get("id") for side in ("home_team", "away_team")
                }
                if (
                    leagues
                    and league not in leagues
                    or teams
                    and not teams.intersection(participants)
                ):
                    continue
                if mode == "for_you" and not (
                    league in follow_leagues or follow_teams.intersection(participants)
                ):
                    continue
                text = " ".join(
                    [
                        raw.get("name", ""),
                        league_obj.get("name") or "",
                        (raw.get("home_team") or {}).get("name") or "",
                        (raw.get("away_team") or {}).get("name") or "",
                    ]
                )
                if search and search.casefold() not in text.casefold():
                    continue
                # A rescheduled event can move out of the requested local day.
                when = datetime.fromisoformat(raw["start_time"].replace("Z", "+00:00"))
                if when.tzinfo is None or not day <= when < day + timedelta(days=1):
                    continue
                league_name_lower = str(league_obj.get("name") or "").lower()
                is_college = (
                    league_obj.get("level") == "college"
                    or "ncaa" in league_name_lower
                    or "college" in league_name_lower
                )
                if is_college and isinstance(league, int):
                    if league not in college_team_info_by_league:
                        try:
                            teams_payload = await service.get_live_teams(league)
                            info_map: dict[int, tuple[str, str | None]] = {}
                            for t in teams_payload.get("teams", []):
                                if not isinstance(t, dict) or not isinstance(t.get("id"), int):
                                    continue
                                city = str(t.get("city") or "").strip()
                                logo = str(t.get("logo_url") or "")
                                espn_id: str | None = None
                                if "/ncaa/" in logo and logo.endswith(".png"):
                                    tail = logo.rsplit("/", 1)[-1][:-4]
                                    if tail.isdigit():
                                        espn_id = tail
                                if city or espn_id:
                                    info_map[int(t["id"])] = (city, espn_id)
                            college_team_info_by_league[league] = info_map
                        except Exception:
                            college_team_info_by_league[league] = {}

                    espn_path = espn_paths_by_league_id.get(league)
                    if not espn_path:
                        sport_key = str(league_obj.get("sport") or "").lower()
                        is_women = (
                            str(league_obj.get("gender") or "").lower() == "women"
                            or "women" in league_name_lower
                        )
                        if sport_key == "football":
                            espn_path = "football/college-football"
                        elif sport_key == "basketball":
                            espn_path = (
                                "basketball/womens-college-basketball"
                                if is_women
                                else "basketball/mens-college-basketball"
                            )
                        elif sport_key == "volleyball":
                            espn_path = (
                                "volleyball/womens-college-volleyball"
                                if is_women
                                else "volleyball/mens-college-volleyball"
                            )
                        elif sport_key == "soccer":
                            espn_path = "soccer/usa.ncaa.w.1" if is_women else "soccer/usa.ncaa.m.1"
                        elif sport_key in {"hockey", "ice_hockey", "ice-hockey"}:
                            espn_path = (
                                "hockey/womens-college-hockey"
                                if is_women
                                else "hockey/mens-college-hockey"
                            )
                        elif sport_key == "lacrosse":
                            espn_path = (
                                "lacrosse/womens-college-lacrosse"
                                if is_women
                                else "lacrosse/mens-college-lacrosse"
                            )

                    rank_by_id: dict[str, int] = {}
                    rank_by_name: dict[str, int] = {}
                    get_rankings_fn = getattr(service, "get_college_rankings", None)
                    if espn_path and callable(get_rankings_fn):
                        if espn_path not in espn_ranks_by_path:
                            try:
                                r_payload = await get_rankings_fn(espn_path)
                                r_list = (
                                    r_payload.get("rankings", [])
                                    if isinstance(r_payload, dict)
                                    else []
                                )
                                chosen = next(
                                    (
                                        rk
                                        for rk in r_list
                                        if isinstance(rk, dict)
                                        and "ap top 25" in str(rk.get("name") or "").lower()
                                    ),
                                    r_list[0] if r_list and isinstance(r_list[0], dict) else None,
                                )
                                by_id: dict[str, int] = {}
                                by_nm: dict[str, int] = {}
                                if isinstance(chosen, dict):
                                    for entry in chosen.get("ranks", []):
                                        if not isinstance(entry, dict):
                                            continue
                                        cur = entry.get("current")
                                        tm = entry.get("team")
                                        if (
                                            not isinstance(cur, int)
                                            or isinstance(cur, bool)
                                            or cur < 1
                                            or not isinstance(tm, dict)
                                        ):
                                            continue
                                        tid = tm.get("id")
                                        if tid is not None and str(tid).strip():
                                            by_id[str(tid).strip()] = cur
                                        loc = str(tm.get("location") or "").strip()
                                        nm = str(tm.get("name") or "").strip()
                                        nick = str(tm.get("nickname") or "").strip()
                                        for cand in (loc, nick, f"{loc} {nm}".strip()):
                                            norm_c = "".join(
                                                ch for ch in cand.lower() if ch.isalnum()
                                            )
                                            if norm_c and norm_c not in by_nm:
                                                by_nm[norm_c] = cur
                                espn_ranks_by_path[espn_path] = (by_id, by_nm)
                            except Exception:
                                espn_ranks_by_path[espn_path] = ({}, {})
                        rank_by_id, rank_by_name = espn_ranks_by_path[espn_path]

                    school_map = college_team_info_by_league[league]
                    for side in ("away_team", "home_team"):
                        side_obj = raw.get(side)
                        if not isinstance(side_obj, dict):
                            continue
                        raw_side_name = str(side_obj.get("name") or "").strip()
                        side_logo = str(side_obj.get("logo_url") or "")
                        side_espn_id: str | None = None
                        if "/ncaa/" in side_logo and side_logo.endswith(".png"):
                            tail = side_logo.rsplit("/", 1)[-1][:-4]
                            if tail.isdigit():
                                side_espn_id = tail
                        school = raw_side_name
                        tid_val = side_obj.get("id")
                        if isinstance(tid_val, int) and tid_val in school_map:
                            mapped_city, mapped_espn_id = school_map[tid_val]
                            if mapped_city:
                                school = mapped_city
                            if not side_espn_id and mapped_espn_id:
                                side_espn_id = mapped_espn_id
                        rank: int | None = None
                        if side_espn_id and side_espn_id in rank_by_id:
                            rank = rank_by_id[side_espn_id]
                        else:
                            for cand in (school, raw_side_name):
                                norm_c = "".join(ch for ch in cand.lower() if ch.isalnum())
                                if norm_c and norm_c in rank_by_name:
                                    rank = rank_by_name[norm_c]
                                    break
                        if school:
                            side_obj["name"] = f"{school}({rank})" if rank else school
                    away_name = str((raw.get("away_team") or {}).get("name") or "").strip()
                    home_name = str((raw.get("home_team") or {}).get("name") or "").strip()
                    if away_name and home_name:
                        raw["name"] = f"{away_name} at {home_name}"
                raw["section"] = sports_feed_section(
                    str(raw.get("status") or "unknown"),
                    viewer_day=day.date(),
                    today=datetime.now(zone).date(),
                )
                results.append(WatchmodeSportsEvent(**raw))
            results.sort(
                key=lambda event: (
                    SPORTS_SECTION_ORDER[event.section],
                    datetime.fromisoformat(event.start_time.replace("Z", "+00:00")),
                    event.id,
                )
            )
            return WatchmodeSportsResponse(
                region=region,
                date=date,
                timezone=timezone_name,
                fetched_at=freshness,
                status_fetched_at=freshness,
                version=current_version,
                events=results[(page - 1) * limit : page * limit],
                total_results=len(results),
                page=page,
                has_more=page * limit < len(results),
                preferences_required=mode == "for_you" and not (follow_leagues or follow_teams),
                date_range_limited=any(snapshot["date_range_limited"] for snapshot in snapshots),
                effective_date_from=snapshots[0]["effective_date_from"],
                effective_date_to=snapshots[-1]["effective_date_to"],
            )
        except WatchmodeLiveError as exc:
            if exc.status_code == 404:
                return WatchmodeSportsResponse(
                    status_code=404, error="Sports event not found", code="NOT_FOUND"
                )
            logger.exception("Watchmode sports upstream failed")
            return WatchmodeSportsResponse(
                status_code=503, error="Sports temporarily unavailable", code="SPORTS_UNAVAILABLE"
            )
        except ValidationError:
            logger.exception("Invalid Watchmode sports response")
            return WatchmodeSportsResponse(
                status_code=503, error="Sports data unavailable", code="INVALID_UPSTREAM_DATA"
            )
        except (ValueError, ZoneInfoNotFoundError) as exc:
            return WatchmodeSportsResponse(status_code=400, error=str(exc), code="INVALID_QUERY")
        except RedisUnavailableError as exc:
            logger.error("Sports request blocked: %s", exc)
            return WatchmodeSportsResponse(
                status_code=503,
                error=str(exc),
                code="REDIS_UNAVAILABLE",
            )
        except Exception:
            logger.exception("Watchmode sports operation failed")
            return WatchmodeSportsResponse(
                status_code=503, error="Sports temporarily unavailable", code="SPORTS_UNAVAILABLE"
            )


watchmode_wrapper = WatchmodeWrapper()
