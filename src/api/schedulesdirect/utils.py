"""
Utility functions for converting SchedulesDirect data to MediaCircle models.
"""

from __future__ import annotations

import unicodedata
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from api.schedulesdirect.channel_filters import channel_name_map
from api.schedulesdirect.models import SDProgramMetadata
from api.tmdb import tmdb_wrapper
from api.tmdb.models import MCMovieItem, MCTvItem
from contracts.models import MCBaseItem, MCType
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

SDFunctionCache = RedisCache(
    defaultTTL=-1,  # Never expires
    prefix="schedulesdirect_func",
    verbose=False,
    isClassMethod=False,  # search_tmdb is a standalone function, not a class method
)

movie_title_cache: dict[str, str] = {}
tv_title_cache: dict[str, str] = {}


def extract_matching_properties(program_data: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract matching properties from program data.
    """
    # Extract program metadata
    program_details_raw = program_data.get("program_details")
    if not program_details_raw:
        logger.warning("No program_details found in program_data")
        return None

    # Convert to SDProgramMetadata model
    try:
        program_details = SDProgramMetadata.model_validate(program_details_raw)
    except Exception as e:
        logger.error("Failed to validate program_details: %s", e)
        return None

    # Extract title from the validated model
    title = _extract_title(program_details)
    if not title:
        logger.warning("No title found for program %s", program_data.get("programID"))
        return None

    # Determine if this is a movie or TV show
    is_movie = _is_movie(program_details)
    media_type = MCType.MOVIE if is_movie else MCType.TV_SERIES

    # Extract year for better matching
    release_year = _extract_year(program_details.originalAirDate)

    # Extract and normalize channel name for network matching
    channel_details = program_data.get("channel_details")
    channel_name_raw = None
    if channel_details:
        if hasattr(channel_details, "name"):
            channel_name_raw = channel_details.name
        elif isinstance(channel_details, dict):
            channel_name_raw = channel_details.get("name")

    # Cleanse and map the channel name
    channel_name = None
    if channel_name_raw:
        # First, split on "-" to handle variants like "ABC-DT"
        channel_name_split = channel_name_raw.split("-")[0]
        # Strip common suffixes like " National Feed", " HD", etc.
        channel_name_clean = channel_name_split.upper().strip()
        for suffix in [" NATIONAL FEED", " HD", " DT", " TV"]:
            if channel_name_clean.endswith(suffix):
                channel_name_clean = channel_name_clean[: -len(suffix)].strip()
        # Lookup in channel_name_map
        channel_name = channel_name_map.get(channel_name_clean)

    return {
        "program_details": program_details,
        "channel_details": channel_details,
        "title": title,
        "year": release_year,
        "channel_name": channel_name,
        "media_type": media_type,
    }


@RedisCache.use_cache(SDFunctionCache, prefix="search_tmdb")
async def search_tmdb(
    matching_properties: dict[str, Any], enrich: bool = True, **kwargs
) -> MCTvItem | MCMovieItem | None:
    """
    Search TMDB for the content.
    """
    title = matching_properties["title"]
    media_type = matching_properties["media_type"]

    # Search TMDB for the content
    try:
        # Note: The wrapper automatically handles num_to_enrich based on limit
        response: Any = None
        if media_type == MCType.MOVIE:
            cached_response = movie_title_cache.get(title)
            if cached_response and hasattr(cached_response, "results"):
                response = cached_response
            else:
                response = await tmdb_wrapper.search_movies_async(
                    query=title,
                    limit=5,
                    enrich=enrich,
                )
                movie_title_cache[title] = response
        else:
            cached_response = tv_title_cache.get(title)
            if cached_response and hasattr(cached_response, "results"):
                response = cached_response
            else:
                response = await tmdb_wrapper.search_tv_shows_async(
                    query=title,
                    limit=5,
                    enrich=enrich,
                )
                tv_title_cache[title] = response

        # Type check: response should be a MCSearchResponse, not a string
        if not response or not hasattr(response, "results") or not response.results:
            logger.warning(
                "search_tmdb: TMDB returned no results for '%s' (%s)",
                title,
                media_type.value,
            )
            return None

        logger.debug(
            "search_tmdb: TMDB returned %d results for '%s' (%s)",
            len(response.results),
            title,
            media_type.value,
        )

        # Select best match (pass channel_name for network matching)
        return _select_best_match(
            response.results,
            title,
            matching_properties["year"],
            media_type,
            channel_name=matching_properties["channel_name"],
        )

    except Exception as e:
        logger.error("Error searching TMDB for '%s': %s", matching_properties["title"], e)
        return None


async def create_mc_item_from_schedule(
    program_data: dict[str, Any],
    enrich: bool = True,
    filter_mc_type: MCType | None = None,
) -> MCBaseItem | None:
    """
    Create an MCTvItem or MCMovieItem from a program dictionary returned by get_schedules_for_lineup.

    Automatically detects whether the program is a TV show or movie based on SchedulesDirect
    metadata (showType and entityType fields).

    Args:
        program_data: Dictionary containing:
            - programID: SchedulesDirect program ID
            - airDateTime: ISO 8601 UTC air date/time
            - duration: Duration in seconds
            - program_details: SDProgramMetadata object or dict
            - channel_details: LineupStation object or dict
            - channel_number: Channel number string
        enrich: If True, fetches full TMDB details (default: True)
        filter_mc_type: If provided, only returns items matching this type (optimization)

    Returns:
        MCTvItem or MCMovieItem with schedule metadata in metrics["schedule"],
        or None if no TMDB match found or type doesn't match filter

    Example:
        ```python
        from api.schedulesdirect.core import SchedulesDirectService
        from api.schedulesdirect.utils import create_mc_item_from_schedule

        service = SchedulesDirectService()
        await service.init()
        schedules = await service.get_schedules_for_lineup()

        # Convert first program from first station
        for station_id, programs in schedules.items():
            if programs:
                item = await create_mc_item_from_schedule(programs[0])
                if item:
                    print(f"Found {item.mc_type}: {item.title}")
                    print(f"Airs: {item.metrics['schedule']['air_datetime_utc']}")
                break
        ```
    """
    # Extract program metadata
    matching_properties = extract_matching_properties(program_data)
    if not matching_properties:
        logger.warning("No matching properties found for program %s", program_data.get("programID"))
        return None

    # Optimization: Check if media_type matches filter before TMDB search
    if filter_mc_type and matching_properties["media_type"] != filter_mc_type:
        return None

    matched_item = await search_tmdb(matching_properties, enrich=enrich)
    if not matched_item:
        logger.warning(
            "No TMDB results found for '%s' (%s)",
            matching_properties["title"],
            matching_properties["media_type"].value,
        )
        return None
    return enrich_mc_item(matched_item, program_data, matching_properties)


def enrich_mc_item(
    matched_item: MCTvItem | MCMovieItem,
    program_data: dict[str, Any],
    matching_properties: dict[str, Any],
) -> MCBaseItem | None:
    # If channel_name is not in map, skip this program (should have been caught earlier, but double-check)
    if not matching_properties["channel_name"]:
        logger.debug(
            f"Skipping program due to unmapped channel name: {matching_properties['channel_name']}"
        )
        return None

    program_details = matching_properties["program_details"]
    channel_details = matching_properties["channel_details"]
    # Build schedule metadata block
    # Extract episode information (for TV shows)
    season_number, episode_number = _extract_season_episode(matching_properties["program_details"])
    episode_title = (
        matching_properties["program_details"].episodeTitle150
        if matching_properties["program_details"]
        else None
    )

    # Build schedule metadata block
    station_id = None
    if channel_details:
        if isinstance(channel_details, dict):
            station_id = channel_details.get("stationID")
        else:
            station_id = (
                channel_details.stationID if hasattr(channel_details, "stationID") else None
            )

    network_logo = None
    network_logo_obj = None
    if channel_details:
        # Handle both dict and object cases
        if isinstance(channel_details, dict):
            network_logo_obj = channel_details.get("logo")
            if network_logo_obj:
                # logo can be a dict or an object - we need the full object, not just URL
                if isinstance(network_logo_obj, dict):
                    # Extract full logo object with all properties
                    network_logo = {
                        "URL": network_logo_obj.get("URL"),
                        "width": network_logo_obj.get("width"),
                        "height": network_logo_obj.get("height"),
                        "md5": network_logo_obj.get("md5"),
                        "hash": network_logo_obj.get("hash"),
                        "source": network_logo_obj.get("source"),
                        "category": network_logo_obj.get("category"),
                    }
                    # Only set if URL exists
                    if not network_logo.get("URL"):
                        network_logo = None
                elif hasattr(network_logo_obj, "URL"):
                    # It's an object with attributes
                    network_logo = {
                        "URL": network_logo_obj.URL,
                        "width": getattr(network_logo_obj, "width", None),
                        "height": getattr(network_logo_obj, "height", None),
                        "md5": getattr(network_logo_obj, "md5", None),
                        "hash": getattr(network_logo_obj, "hash", None),
                        "source": getattr(network_logo_obj, "source", None),
                        "category": getattr(network_logo_obj, "category", None),
                    }
                    if not network_logo.get("URL"):
                        network_logo = None
        else:
            # channel_details is an object
            if hasattr(channel_details, "logo") and channel_details.logo:
                if isinstance(channel_details.logo, dict):
                    network_logo = {
                        "URL": channel_details.logo.get("URL"),
                        "width": channel_details.logo.get("width"),
                        "height": channel_details.logo.get("height"),
                        "md5": channel_details.logo.get("md5"),
                        "hash": channel_details.logo.get("hash"),
                        "source": channel_details.logo.get("source"),
                        "category": channel_details.logo.get("category"),
                    }
                    if not network_logo.get("URL"):
                        network_logo = None
                elif hasattr(channel_details.logo, "URL"):
                    network_logo = {
                        "URL": channel_details.logo.URL,
                        "width": getattr(channel_details.logo, "width", None),
                        "height": getattr(channel_details.logo, "height", None),
                        "md5": getattr(channel_details.logo, "md5", None),
                        "hash": getattr(channel_details.logo, "hash", None),
                        "source": getattr(channel_details.logo, "source", None),
                        "category": getattr(channel_details.logo, "category", None),
                    }
                    if not network_logo.get("URL"):
                        network_logo = None

    duration = program_data.get("duration")
    duration_minutes = None
    if duration is not None:
        duration_minutes = duration // 60
    media_type = matching_properties["media_type"]
    program_id = program_data.get("programID")
    channel_name = matching_properties["channel_name"]
    channel_number = program_data.get("channel_number")
    air_datetime_utc = program_data.get("airDateTime")
    schedule_block = {
        "media_type": media_type.value,
        "program_id": program_id,
        "station_id": station_id,
        "channel_name": channel_name,
        "channel_number": channel_number,
        "air_datetime_utc": air_datetime_utc,
        "duration_seconds": duration,
        "duration_minutes": duration_minutes,
        "episode_title": episode_title if media_type != MCType.MOVIE else None,
        "season_number": season_number if media_type != MCType.MOVIE else None,
        "episode_number": episode_number if media_type != MCType.MOVIE else None,
        "new_episode": program_details.originalAirDate
        == datetime.now(ZoneInfo("America/New_York")).date().isoformat(),
        "original_air_date": program_details.originalAirDate,
        "genres": program_details.genres,
        "description": _extract_description(program_details),
        "network_logo": network_logo,
        "show_type": program_details.showType,
        "entity_type": program_details.entityType,
    }

    # Create enriched copy with schedule metadata
    enriched_item = matched_item.model_copy(deep=True)
    enriched_item.metrics = enriched_item.metrics or {}
    enriched_item.metrics["schedule"] = schedule_block

    # Add external IDs
    enriched_item.external_ids = enriched_item.external_ids or {}
    enriched_item.external_ids["schedules_direct_program_id"] = program_data.get("programID")
    if channel_details:
        station_id = (
            channel_details.stationID
            if hasattr(channel_details, "stationID")
            else channel_details.get("stationID")
        )
        if station_id:
            enriched_item.external_ids["schedules_direct_station_id"] = station_id

    # Generate unique mc_id for schedule items by incorporating air datetime and station
    # This ensures each airing is unique even for:
    # - Episodes of the same series at different times
    # - Same show simulcast on multiple channels at the same time
    air_datetime = program_data.get("airDateTime")
    schedule_station_id = schedule_block.get("station_id", "")
    if air_datetime and hasattr(matched_item, "tmdb_id") and matched_item.tmdb_id:
        # Clean the air datetime for use in ID (remove special chars)
        air_datetime_clean = air_datetime.replace(":", "").replace("-", "").replace("T", "_")
        air_datetime_clean = air_datetime_clean.replace("Z", "").replace("+", "").replace(".", "")
        enriched_item.mc_id = (
            f"tmdb_{matched_item.tmdb_id}_{air_datetime_clean}_{schedule_station_id}"
        )

    return enriched_item


# Helper functions (matching the pattern from wrappers.py)
def _is_movie(metadata: SDProgramMetadata) -> bool:
    """
    Determine if program is a movie based on SchedulesDirect metadata.

    Checks showType and entityType fields:
    - showType: 'Movie', 'Feature Film', etc. indicates movie
    - entityType: 'Movie' indicates movie
    - If neither field is set, assumes TV show

    Args:
        metadata: SDProgramMetadata object

    Returns:
        True if movie, False if TV show
    """
    if not metadata:
        return False

    # Check showType field
    if metadata.showType:
        show_type_lower = metadata.showType.lower()
        if "movie" in show_type_lower or "feature" in show_type_lower:
            return True

    # Check entityType field
    if metadata.entityType:
        entity_type_lower = metadata.entityType.lower()
        if "movie" in entity_type_lower:
            return True

    # Default to TV show (most common case)
    return False


def _extract_title(metadata: SDProgramMetadata) -> str | None:
    """
    Extract title from program metadata.

    Uses the model's title property which returns titles[0].title120.
    """
    if not metadata:
        return None

    # Use the title property from the model
    return metadata.title


def _extract_year(date_str: str | None) -> str | None:
    """Extract 4-digit year from YYYY-MM-DD date string."""
    if not date_str or len(date_str) < 4:
        return None
    return date_str[:4]


def _extract_season_episode(metadata: SDProgramMetadata) -> tuple[int | None, int | None]:
    """
    Extract season and episode numbers from program metadata.

    Uses the model's season_number and episode_number properties
    which extract from Gracenote metadata.

    Returns:
        Tuple of (season_number, episode_number), either or both may be None
    """
    if not metadata:
        return None, None

    return metadata.season_number, metadata.episode_number


def _extract_description(metadata: SDProgramMetadata) -> str | None:
    """
    Extract best available description from program metadata.

    Uses the model's description property.
    """
    if not metadata:
        return None

    return metadata.description


def _select_best_match(
    results: list[MCTvItem | MCMovieItem],
    target_title: str,
    release_year: str | None,
    media_type: MCType,
    channel_name: str | None = None,
) -> MCTvItem | MCMovieItem | None:
    """
    Select the best TMDB match based on title similarity, year, network, origin country, language, and popularity.

    Prioritizes US shows, English language content, and shows that match the network/channel
    for SchedulesDirect data, which is US-focused.

    Args:
        results: List of MCTvItem or MCMovieItem candidates from TMDB search
        target_title: Original title from SchedulesDirect
        release_year: Original air year (YYYY format)
        media_type: MCType.TV_SERIES or MCType.MOVIE
        channel_name: Network/channel name (e.g., "ABC", "CBS", "NBC") for network matching

    Returns:
        Best matching item, or None if no suitable match
    """
    if not results:
        logger.debug("_select_best_match: No results provided")
        return None

    # Normalize using NFKC to handle different Unicode representations
    # (e.g., "í" as single char vs "i" + combining acute accent)
    normalized_target = unicodedata.normalize("NFKC", target_title.lower().strip())
    best_score = -1.0
    best_item: MCTvItem | MCMovieItem | None = None

    # Normalize channel name for matching
    normalized_channel = None
    if channel_name:
        normalized_channel = channel_name.upper().strip()

    logger.debug(
        "_select_best_match: Searching for '%s' (%s) among %d results",
        target_title,
        media_type.value,
        len(results),
    )

    for item in results:
        raw_title = (item.name or item.title or "").lower().strip()
        title = unicodedata.normalize("NFKC", raw_title)
        score = 0.0

        # Exact title match is strongest signal
        if title == normalized_target:
            score += 10  # Increased from 5 to prioritize exact matches
        elif normalized_target in title:
            # SchedulesDirect title is substring of TMDB title
            score += 3
        elif title in normalized_target:
            # TMDB title is substring of SchedulesDirect title
            # This handles cases like "Hard Knocks: In Season With the NFC East" matching "Hard Knocks"
            # Score based on how much of the target title is covered
            coverage = len(title) / len(normalized_target) if normalized_target else 0
            score += 3 + (coverage * 4)  # Base 3 + up to 4 more for better coverage
        else:
            # If title doesn't match at all, skip this item
            continue

        # Network matching - very strong signal for TV shows
        if media_type == MCType.TV_SERIES and normalized_channel and hasattr(item, "network"):
            item_network = item.network
            if item_network:
                item_network_normalized = item_network.upper().strip()
                # Exact network match
                if item_network_normalized == normalized_channel:
                    score += 8  # Very strong preference for matching network
                # Partial match (e.g., "ABC" matches "ABC Network" or "American Broadcasting Company")
                elif (
                    normalized_channel in item_network_normalized
                    or item_network_normalized in normalized_channel
                ):
                    score += 5  # Good preference for partial network match
                else:
                    # Penalize shows on different networks
                    score -= 3

        # For TV shows, prioritize US origin country (SchedulesDirect is US-focused)
        if media_type == MCType.TV_SERIES and hasattr(item, "origin_country"):
            origin_countries = item.origin_country if isinstance(item.origin_country, list) else []
            if "US" in origin_countries:
                score += 5  # Strong preference for US shows
            elif origin_countries:
                # Penalize non-US shows slightly
                score -= 2

        # Prioritize English language content (SchedulesDirect is US-focused)
        is_english = False
        # Check original_language first (TMDB uses ISO 639-1 codes, "en" is English)
        if (
            hasattr(item, "original_language")
            and item.original_language
            and item.original_language.lower() == "en"
        ):
            is_english = True
            score += 4  # Strong preference for English language
        # Also check spoken_languages as fallback
        elif hasattr(item, "spoken_languages"):
            spoken_langs = item.spoken_languages if isinstance(item.spoken_languages, list) else []
            if any(
                lang.lower() in ["english", "en"] for lang in spoken_langs if isinstance(lang, str)
            ):
                is_english = True
                score += 3  # Good preference if English is in spoken languages

        if not is_english:
            # Penalize non-English content
            score -= 2

        # Year match is important for content with same name
        if media_type == MCType.TV_SERIES:
            item_year = (
                _extract_year(item.first_air_date) if hasattr(item, "first_air_date") else None
            )
        else:
            item_year = _extract_year(item.release_date) if hasattr(item, "release_date") else None

        if release_year and item_year:
            if item_year == release_year:
                score += 4  # Increased from 3
            elif abs(int(item_year) - int(release_year)) <= 1:
                # Within 1 year is still good
                score += 2
            else:
                # Penalize shows that are far off in year
                year_diff = abs(int(item_year) - int(release_year))
                score -= min(year_diff / 5, 3)  # Max penalty of 3

        # Popularity helps break ties (but less weight than origin country or network)
        popularity = item.metrics.get("popularity") if item.metrics else None
        if isinstance(popularity, (int, float)):
            score += min(popularity / 200, 1)  # Reduced from 2 to 1, divided by 200 instead of 100

        if score > best_score:
            best_score = score
            best_item = item

    if best_item:
        matched_title = best_item.name or best_item.title or "Unknown"
        logger.debug(
            "_select_best_match: Matched '%s' -> '%s' (score: %.2f)",
            target_title,
            matched_title,
            best_score,
        )
    else:
        # Log all candidate titles when no match is found
        candidate_titles = [item.name or item.title or "Unknown" for item in results[:5]]
        logger.warning(
            "_select_best_match: No match for '%s' (%s). TMDB candidates: %s",
            target_title,
            media_type.value,
            candidate_titles,
        )

    return best_item
