"""
Async wrappers that convert SchedulesDirect data into TMDB-backed MC items.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from api.schedulesdirect.channel_filters import (
    ChannelType,
    channel_name_map,
    get_channel_type,
    get_schedule_sort_key,
)
from api.schedulesdirect.core import SchedulesDirectService
from api.schedulesdirect.models import (
    DEFAULT_PRIMETIME_TIMEZONE,
    DEFAULT_PRIMETIME_WINDOW,
    SchedulesDirectPrimetimeResponse,
    SchedulesDirectServiceUnavailableError,
)
from api.schedulesdirect.utils import create_mc_item_from_schedule
from api.tmdb.models import MCTvItem
from contracts.models import MCType
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

SchedulesDirectCache = RedisCache(
    defaultTTL=30 * 60,
    prefix="schedulesdirect_wrapper",
    verbose=False,
    isClassMethod=True,
)


class SchedulesDirectWrapper:
    """High-level orchestrator that enriches SchedulesDirect data with TMDB items."""

    def __init__(self):
        self.service = SchedulesDirectService()
        self._tmdb_cache: dict[tuple[str, str | None], MCTvItem] = {}
        self._station_cache: dict[str, str] = {}  # network_name -> station_id
        self._stations_loaded = False

    @RedisCache.use_cache(SchedulesDirectCache, prefix="primetime")
    async def get_primetime_schedule(
        self,
        start_time: str = DEFAULT_PRIMETIME_WINDOW["start"],
        end_time: str = DEFAULT_PRIMETIME_WINDOW["end"],
        mc_type: MCType | None = None,
        channel_type: ChannelType | None = None,
    ) -> SchedulesDirectPrimetimeResponse:
        print(
            f"🔥 DEBUG: get_primetime_schedule called: start={start_time}, end={end_time}, mc_type={mc_type}, channel_type={channel_type}"
        )
        # Normalize mc_type to enum if it's a string (can happen with cached calls)
        if mc_type is not None and isinstance(mc_type, str):
            mc_type_lower = mc_type.lower()
            if mc_type_lower == "tv":
                mc_type = MCType.TV_SERIES
            elif mc_type_lower == "movie":
                mc_type = MCType.MOVIE
            else:
                mc_type = None

        # Normalize channel_type to enum if it's a string (can happen with cached calls)
        if channel_type is not None and isinstance(channel_type, str):
            channel_type_lower = channel_type.lower()
            if channel_type_lower == "broadcast":
                channel_type = ChannelType.BROADCAST
            elif channel_type_lower == "premium-cable":
                channel_type = ChannelType.PREMIUM_CABLE
            elif channel_type_lower == "non-premium-cable":
                channel_type = ChannelType.NON_PREMIUM_CABLE
            else:
                channel_type = None

        logger.info(
            "get_primetime_schedule called: start=%s, end=%s, mc_type=%s, channel_type=%s",
            start_time,
            end_time,
            mc_type,
            channel_type,
        )

        # Get schedules for default lineup for mediacircle account
        # Wrap in try/catch to handle service unavailability gracefully
        try:
            # Optimization: If user specifically requests BROADCAST, we can ask the service
            # to filter down to just those 6 networks immediately, saving huge processing time.
            use_broadcast_filter = channel_type == ChannelType.BROADCAST

            prime_schedules = await self.service.get_schedules_for_lineup(
                channel_map=None,
                num_days=1,
                broadcast_only=use_broadcast_filter,
                start_time=start_time,
                end_time=end_time,
            )
        except SchedulesDirectServiceUnavailableError as e:
            logger.warning(
                "SchedulesDirect service unavailable, returning empty results: %s", str(e)
            )
            # Return empty response instead of propagating the error
            target_date = datetime.now().date()
            window_start_dt = datetime.combine(
                target_date,
                datetime.strptime(start_time, "%H:%M").time(),
            )
            window_end_dt = datetime.combine(
                target_date,
                datetime.strptime(end_time, "%H:%M").time(),
            )
            return SchedulesDirectPrimetimeResponse(
                results=[],
                data_type=mc_type if mc_type else MCType.MIXED,
                data_source="SchedulesDirect national primetime + TMDB",
                query=f"primetime:{start_time} to {end_time}",
                requested_date=target_date.isoformat(),
                timezone=DEFAULT_PRIMETIME_TIMEZONE,
                window_start=window_start_dt.isoformat(),
                window_end=window_end_dt.isoformat(),
                total_results=0,
                metadata={"service_unavailable": True, "error": str(e)},
            )

        logger.info(
            "Retrieved %d networks for primetime lineup",
            len(prime_schedules.keys()),
        )

        # Debug: Log sample channel names
        if prime_schedules:
            sample_channels = []
            for sid, progs in list(prime_schedules.items())[:5]:
                if progs:
                    ch_name = progs[0].get("channel_details", {}).get("name", "Unknown")
                    sample_channels.append(f"{sid}:{ch_name}")
            logger.info("Sample channels: %s", sample_channels)

        # Filter to only today's programs (in Eastern timezone)
        eastern_tz = ZoneInfo("America/New_York")
        today_eastern = datetime.now(eastern_tz).date()
        today_programs: dict[str, list[dict[str, Any]]] = {}

        for station_id, programs in prime_schedules.items():
            today_station_programs = []
            for program in programs:
                air_datetime_str = program.get("airDateTime")
                if air_datetime_str:
                    try:
                        # Parse the datetime
                        dt_str = (
                            air_datetime_str.replace("Z", "+00:00")
                            if air_datetime_str.endswith("Z")
                            else air_datetime_str
                        )
                        dt = datetime.fromisoformat(dt_str)

                        # Convert to Eastern timezone
                        if dt.tzinfo is None:
                            dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
                            dt_eastern = dt_utc.astimezone(eastern_tz)
                        else:
                            dt_eastern = dt.astimezone(eastern_tz)

                        # Check if program airs today
                        if dt_eastern.date() == today_eastern:
                            today_station_programs.append(program)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse airDateTime {air_datetime_str}: {e}")
                        continue

            if today_station_programs:
                today_programs[station_id] = today_station_programs

        logger.info(
            "After date filter (today only): %d/%d stations have programs airing today",
            len(today_programs),
            len(prime_schedules),
        )

        # Now convert schedule blocks to MCTvItem objects
        # Use semaphore to limit concurrent TMDB API calls and prevent cache lock contention
        # Each TMDB enrichment makes 4 parallel calls (providers, credits, videos, keywords)
        # So 5 concurrent operations × 4 calls = 20 max concurrent TMDB requests
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent TMDB operations

        logger.info(
            "Starting conversion of %d stations with mc_type filter: %s",
            len(today_programs),
            mc_type,
        )

        async def bounded_conversion(program: dict) -> Any:
            async with semaphore:
                # Pass mc_type to avoid unnecessary TMDB lookups
                return await create_mc_item_from_schedule(program, filter_mc_type=mc_type)

        conversion_tasks = []
        for _station_id, schedule in today_programs.items():
            for program in schedule:
                conversion_tasks.append(bounded_conversion(program))

        results_with_none = await asyncio.gather(*conversion_tasks)
        logger.info("Conversion completed. Processed %d tasks", len(conversion_tasks))

        # Filter out None values (programs that didn't match TMDB)
        results = [item for item in results_with_none if item is not None]
        logger.info(
            "After TMDB conversion: %d/%d items matched",
            len(results),
            len(results_with_none),
        )

        # Filter by mc_type if specified (allows frontend to request TV or movies only)
        if mc_type is not None:
            pre_filter_count = len(results)
            results = [item for item in results if item.mc_type == mc_type]
            mc_type_str = mc_type.value if hasattr(mc_type, "value") else str(mc_type)
            logger.info(
                "After mc_type filter (%s): %d/%d items remain",
                mc_type_str,
                len(results),
                pre_filter_count,
            )

        # Filter by channel_type if specified (broadcast, premium-cable, non-premium-cable)
        if channel_type is not None:
            pre_filter_count = len(results)
            results = [
                item
                for item in results
                if get_channel_type(item.metrics.get("schedule", {}).get("channel_name", ""))
                == channel_type
            ]
            for item in results:
                try:
                    item.metrics["schedule"]["channel_name"] = channel_name_map[
                        item.metrics["schedule"]["channel_name"]
                    ]
                except KeyError:
                    pass

            channel_type_str = (
                channel_type.value if hasattr(channel_type, "value") else str(channel_type)
            )
            logger.info(
                "After channel_type filter (%s): %d/%d items remain",
                channel_type_str,
                len(results),
                pre_filter_count,
            )

        # Deduplicate by mc_id to prevent duplicate items
        # For broadcast networks (ABC, CBS, NBC, FOX, CW, PBS), also deduplicate by
        # tmdb_id + air_datetime + channel_name to handle multiple affiliate stations
        # (e.g., WABC and KABC showing the same show)
        seen_mc_ids: set[str] = set()
        seen_broadcast_keys: set[tuple[str, str, str]] = (
            set()
        )  # (tmdb_id, air_datetime, channel_name)
        deduplicated_results = []
        duplicates_count = 0
        duplicate_details: list[dict[str, Any]] = []

        for item in results:
            schedule_dict: dict[str, Any] = {}
            if item.metrics:
                metrics = item.metrics
                if isinstance(metrics, dict):
                    schedule_raw = metrics.get("schedule")
                    if schedule_raw and isinstance(schedule_raw, dict):
                        schedule_dict = schedule_raw
            channel_name_raw: Any = schedule_dict.get("channel_name")
            air_datetime_raw: Any = schedule_dict.get("air_datetime_utc")
            channel_name: str = str(channel_name_raw) if channel_name_raw else ""
            air_datetime: str = str(air_datetime_raw) if air_datetime_raw else ""
            tmdb_id = getattr(item, "tmdb_id", None)

            # Check if this is a broadcast network show that might have affiliate duplicates
            is_broadcast = channel_name and channel_name.upper() in [
                "ABC",
                "CBS",
                "NBC",
                "FOX",
                "CW",
                "PBS",
            ]

            # Get item name/title safely
            item_name = getattr(item, "name", None) or getattr(item, "title", None) or "Unknown"

            # For broadcast networks, also check by tmdb_id + air_datetime + channel_name
            if is_broadcast and tmdb_id and air_datetime:
                broadcast_key = (str(tmdb_id), air_datetime, channel_name.upper())
                if broadcast_key in seen_broadcast_keys:
                    duplicates_count += 1
                    duplicate_details.append(
                        {
                            "mc_id": item.mc_id,
                            "name": item_name,
                            "channel_name": channel_name,
                            "air_datetime": air_datetime,
                            "station_id": schedule_dict.get("station_id"),
                            "reason": "broadcast_affiliate_duplicate",
                        }
                    )
                    continue
                seen_broadcast_keys.add(broadcast_key)

            # Standard deduplication by mc_id
            if item.mc_id and item.mc_id not in seen_mc_ids:
                seen_mc_ids.add(item.mc_id)
                deduplicated_results.append(item)
            else:
                duplicates_count += 1
                # Log details about duplicates for debugging
                duplicate_details.append(
                    {
                        "mc_id": item.mc_id,
                        "name": item_name,
                        "channel_name": channel_name,
                        "air_datetime": air_datetime,
                        "station_id": schedule_dict.get("station_id"),
                        "reason": "same_mc_id",
                    }
                )

        results = deduplicated_results

        if duplicates_count > 0:
            logger.warning(
                "Removed %d duplicate items. Remaining: %d items",
                duplicates_count,
                len(results),
            )

        # Sort results by:
        # 1. Air time (ascending) - 8pm before 8:30pm
        # 2. Channel type: Broadcast (ABC, CBS, NBC, FOX, CW, PBS) first
        # 3. Then cable (non-premium) alphabetically
        # 4. Premium channels last
        results.sort(key=get_schedule_sort_key)

        mc_type_str = (
            mc_type.value
            if mc_type and hasattr(mc_type, "value")
            else (str(mc_type) if mc_type else "none")
        )
        channel_type_str = (
            channel_type.value
            if channel_type and hasattr(channel_type, "value")
            else (str(channel_type) if channel_type else "none")
        )
        logger.info(
            "Successfully converted %d/%d programs to MC items (mc_type: %s, channel_type: %s)",
            len(results),
            len(conversion_tasks),
            mc_type_str,
            channel_type_str,
        )

        # Calculate window times for response
        target_date = datetime.now().date()
        window_start_dt = datetime.combine(
            target_date,
            datetime.strptime(start_time, "%H:%M").time(),
        )
        window_end_dt = datetime.combine(
            target_date,
            datetime.strptime(end_time, "%H:%M").time(),
        )

        # Determine data_type for response based on filter or mixed if no filter
        response_data_type = mc_type if mc_type else MCType.MIXED
        print(
            f"🔥 DEBUG: get_primetime_schedule Completed: returned{len(results)} items, mc_type={mc_type}, channel_type={channel_type}"
        )
        return SchedulesDirectPrimetimeResponse(
            results=results,
            data_type=response_data_type,
            data_source="SchedulesDirect national primetime + TMDB",
            query=f"primetime:{start_time} to {end_time}",
            requested_date=target_date.isoformat(),
            timezone=DEFAULT_PRIMETIME_TIMEZONE,
            window_start=window_start_dt.isoformat(),
            window_end=window_end_dt.isoformat(),
            total_results=len(results),
        )


schedules_direct_wrapper = SchedulesDirectWrapper()
