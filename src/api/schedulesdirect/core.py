"""
Core service for interacting with the SchedulesDirect JSON API.
This version is rewritten to match the ACTUAL working endpoints and
NDJSON streaming behavior confirmed via curl.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from api.schedulesdirect.auth import SchedulesDirectAuth
from api.schedulesdirect.channel_filters import (
    convert_airdatetime_to_est,
    filter_channels,
    filter_out_past_programs,
    filter_programs_by_time_of_day,
    is_broadcast_network,
    is_news_channel,
    is_premium_channel,
    is_sports_channel,
)
from api.schedulesdirect.models import (
    HeadendsSearchResponse,
    LineupChannelsResponse,
    SDProgramMetadata,
)
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

logger = get_logger(__name__)

# Cache configuration constants
CACHE_NUM_DAYS = 2
CACHE_START_TIME = "18:00"  # 6pm
CACHE_END_TIME = "23:59"  # 11:59pm
CACHE_TTL_SECONDS = CACHE_NUM_DAYS * 24 * 60 * 60  # CACHE_NUM_DAYS days in seconds

# Schedule cache instance - uses Redis
ScheduleCache = RedisCache(
    prefix="sd_schedule",
    defaultTTL=CACHE_TTL_SECONDS - 60 * 60,
    verbose=False,
)


class SchedulesDirectService(BaseAPIClient):
    """
    High-level SchedulesDirect API service.

    ✔ Correct endpoint behavior
    ✔ NDJSON support
    ✔ Proper lineup add/remove
    ✔ Search lineups by ZIP
    ✔ Compatible with wrapper expectations
    """

    def __init__(self, auth: SchedulesDirectAuth | None = None):
        self.base_url = "https://json.schedulesdirect.org/20141201"
        self.auth = auth or SchedulesDirectAuth()
        self.account_status = None
        self.active_lineup: str | None = None
        self.channels: LineupChannelsResponse | None = None

    async def init(self) -> None:
        """Initialize the service."""
        if not self.auth.account_status:
            await self.auth.get_account_status()

        if self.auth.account_status and self.auth.account_status.lineups:
            self.active_lineup = self.auth.account_status.lineups[0].lineup
            self.channels = await self.get_channels_for_lineup(self.active_lineup)

    # ----------------------------------------------------------
    # Schedules
    # ----------------------------------------------------------
    async def get_station_schedules(
        self,
        station_ids: list[str],
        date_str: str,
    ) -> list[dict[str, Any]]:
        """POST /schedules (works)"""

        if not station_ids:
            logger.warning("get_station_schedules: empty station list")
            return []
        # SchedulesDirect expects a list of station IDs and a list of dates
        next_day = (
            (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).isoformat()
        ).split("T")[0]
        payload = [{"stationID": sid, "date": [date_str, next_day]} for sid in station_ids]

        response, status = await self.auth.sd_request(
            method="POST",
            endpoint="/schedules",
            json_body=payload,
        )

        if status != 200:
            raise RuntimeError(f"SchedulesDirect schedule error {status}: {json.dumps(response)}")

        if not isinstance(response, list):
            raise RuntimeError(f"/schedules expected list, got {type(response)}")

        return response

    # ----------------------------------------------------------
    # lineup utils
    # ----------------------------------------------------------
    async def add_lineup(self, lineup_id: str) -> bool:
        """
        ✔ Correct method: PUT /lineups/<LINEUP_ID>
        NOT POST /lineups/add (that’s deprecated/invisible).
        """

        response, status = await self.auth.sd_request(
            method="PUT",
            endpoint=f"/lineups/{lineup_id}",
        )

        if status != 200:
            logger.error("PUT /lineups/%s failed %d: %s", lineup_id, status, response)
            return False

        if isinstance(response, dict) and response.get("response") == "OK":
            return True

        logger.error("Unexpected add_lineup response: %s", response)
        return False

    async def get_lineup_stations(self, lineup_id: str) -> list[dict[str, Any]]:
        """
        Get stations for a given lineup.
        Returns a list of station dictionaries.
        """
        channels_response = await self.get_channels_for_lineup(lineup_id)
        if not channels_response:
            return []
        return [station.model_dump() for station in channels_response.stations]

    async def search_lineups_by_zip(self, zip_code: str) -> list[dict[str, Any]]:
        """
        Search for available lineups by ZIP code.
        Returns a list of lineup dictionaries.
        """
        headends = await self.find_headends_by_zip(zip_code)
        lineups: list[dict[str, Any]] = []
        for headend in headends.headends:
            for lineup in headend.lineups:
                lineups.append(
                    {
                        "lineup": lineup.lineup,
                        "name": lineup.name,
                        "uri": lineup.uri,
                        "headend": headend.headend,
                        "transport": headend.transport,
                        "location": headend.location,
                    }
                )
        return lineups

    async def remove_lineup(self, lineup_id: str) -> bool:
        """
        Helper to remove a lineup from the account.
        """

        response, status = await self.auth.sd_request(
            method="DELETE",
            endpoint=f"/lineups/{lineup_id}",
        )

        if status != 200:
            raise RuntimeError(
                f"SchedulesDirect remove lineup error {status}: {json.dumps(response)}"
            )

        return bool(isinstance(response, dict) and response.get("response") == "OK")

    # ----------------------------------------------------------
    # Program Metadata
    # ----------------------------------------------------------

    # SchedulesDirect API limit is 5000 program IDs per request
    # Using 500 to stay well under the limit and avoid timeouts
    PROGRAM_CHUNK_SIZE = 500

    async def get_program_metadata_for_lineup(
        self, program_ids: list[str]
    ) -> dict[str, SDProgramMetadata]:
        """
        Fetches program metadata for a lineup.
        SchedulesDirect /programs expects a plain JSON array of program IDs.
        Automatically chunks requests to stay under API limits.
        """
        if not program_ids:
            return {}

        # Chunk program IDs to stay under API limit (max 5000)
        chunks = self._chunk(program_ids, self.PROGRAM_CHUNK_SIZE)
        all_metadata: dict[str, SDProgramMetadata] = {}

        logger.info(
            f"Fetching program metadata: {len(program_ids)} programs in {len(chunks)} chunks"
        )

        for i, chunk in enumerate(chunks):
            t0 = time.time()
            logger.info(f"Fetching program metadata chunk {i + 1}/{len(chunks)}")
            program_metadata, status = await self.auth.sd_request(
                method="POST",
                endpoint="/programs",
                json_body=chunk,
            )
            if status != 200:
                raise RuntimeError(f"/programs error: {json.dumps(program_metadata)}")
            t1 = time.time()
            logger.info(
                f"Fetching program metadata chunk {i + 1}/{len(chunks)} completed in {t1 - t0:.2f}s"
            )
            for program in program_metadata:
                all_metadata[program["programID"]] = SDProgramMetadata.model_validate(program)

        return all_metadata

    """
    Schedules Direct Flow
    """

    # ----------------------------------------------------------
    # 1 Get HeadendID: Search by ZIP (ACTUAL working version)
    #  THinks of this is what cable/broadcast providers are available in a given ZIP code.
    #  And a provider can have different packages or "lineups".
    # ----------------------------------------------------------
    async def find_headends_by_zip(self, zip_code: str) -> HeadendsSearchResponse:
        """
        GET /headends?country= USA&postalcode={
        """

        response, status = await self.auth.sd_request(
            method="GET",
            endpoint=f"/headends?country=USA&postalcode={zip_code}",
        )

        if status != 200:
            logger.error("Lineup search failed for zip %s: %s", zip_code, response)
            return HeadendsSearchResponse(headends=[])

        if not isinstance(response, list):
            logger.error("Expected list from /lineups/USA/<ZIP>, got %s", type(response))
            return HeadendsSearchResponse(headends=[])

        return HeadendsSearchResponse.model_validate({"headends": response})

    # ----------------------------------------------------------
    # 2. Now if we have lineups, we get the chanles for the lineup(if desired)
    # ----------------------------------------------------------
    async def get_channels_for_lineup(
        self, lineup_id: str | None = None
    ) -> LineupChannelsResponse | None:
        """
        ✔ Correct method: GET /lineups/<LINEUP_ID>
        """
        # If no lineup_id is provided, use the default lineup
        if not lineup_id:
            if not self.auth.account_status:
                await self.auth.get_account_status()

            if self.auth.account_status and self.auth.account_status.lineups:
                lineup_id = self.auth.account_status.lineups[0].lineup
            else:
                logger.error("No lineups available in account")
                return None

        # Check if lineup is in the account
        if self.auth.account_status and not self.auth.account_status.get_lineup_by_id(lineup_id):
            # Do we have remove to add lineup to account(max 4 per account)
            if self.auth.account_status.total_lineups >= 4:
                # Remove the oldest lineup
                oldest_lineup = min(self.auth.account_status.lineups, key=lambda x: x.modified)
                logger.info(
                    "Over account limit of 4 lineups. Removing oldest lineup: %s",
                    oldest_lineup.lineup,
                )
                await self.remove_lineup(oldest_lineup.lineup)
            else:
                # Add the lineup to the account
                await self.add_lineup(lineup_id)
                logger.warning("Added lineup: %s", lineup_id)

        response, status = await self.auth.sd_request(
            method="GET",
            endpoint=f"/lineups/{lineup_id}",
        )

        if status != 200:
            logger.error("GET /lineups/%s failed %d: %s", lineup_id, status, response)
            return None

        return LineupChannelsResponse.model_validate(response)

    # ----------------------------------------------------------
    # 3. Schedule Fetching (with caching via decorator)
    # ----------------------------------------------------------
    @RedisCache.use_cache(ScheduleCache, prefix="lineup_schedule")
    async def _fetch_lineup_schedule(
        self,
        lineup_id: str,
        channel_map_dict: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Fetch 2 days of primetime schedule data (6pm-11:59pm).
        Cached automatically via @RedisCache.use_cache decorator.

        **IMPORTANT**
        Currently limits to broadcast and premium cable channels only.
        This could easily be expanded to include other channels types if needed.

        Args:
            lineup_id: The lineup identifier (used as cache key)
            channel_map_dict: Serialized channel map (for cache key generation)

        Returns:
            Merged schedule data keyed by station ID
        """
        # Reconstruct channel_map from dict
        channel_map = LineupChannelsResponse.model_validate(channel_map_dict)

        station_map = {c.stationID: c for c in channel_map.stations}
        channel_number_map = {c.stationID: c.channel for c in channel_map.map}

        # DEBUG: Log all stations before filtering
        logger.info(f"📊 Total stations in lineup: {len(channel_map.stations)}")
        for c in channel_map.stations[:20]:  # First 20
            logger.info(f"  Station: {c.stationID} | {c.name} | {c.callsign}")

        valid_channels = filter_channels(channel_map.stations)
        logger.info(f"📊 After filter_channels: {len(valid_channels)} valid channels")
        for c in valid_channels[:20]:
            logger.info(f"  Valid: {c.stationID} | {c.name} | {c.callsign}")

        news_channels = [c for c in valid_channels if is_news_channel(c)]
        sports_channels = [c for c in valid_channels if is_sports_channel(c)]
        logger.info(
            f"📊 News channels: {len(news_channels)}, Sports channels: {len(sports_channels)}"
        )
        logger.info("Aggressive filtering now, limiting to broadcase and premium cable channels")
        filtered_channels = [
            c for c in valid_channels if is_broadcast_network(c) or is_premium_channel(c.name)
        ]
        station_ids = [
            c.stationID for c in filtered_channels if c not in news_channels + sports_channels
        ]
        logger.info(f"📊 Final station_ids (broadcast and premium cable only): {len(station_ids)}")

        # Build schedule payload for 7 days
        start_date = datetime.now().date().isoformat()
        end_date = (datetime.now().date() + timedelta(days=CACHE_NUM_DAYS)).isoformat()

        dates = self.generate_date_list(start_date, end_date)
        schedule_payload = [{"stationID": sid, "date": dates} for sid in station_ids]
        logger.info(f"📊 Example Schedule payload: {schedule_payload[0]}")

        raw_schedule_results, status = await self.auth.sd_request(
            method="POST",
            endpoint="/schedules",
            json_body=schedule_payload,
        )

        if status != 200:
            raise RuntimeError(
                f"SchedulesDirect schedule error {status}: {json.dumps(raw_schedule_results)}"
            )

        if raw_schedule_results is None:
            logger.error(
                "SchedulesDirect returned None response with status 200. "
                "This may indicate an empty response body or JSON parsing failure. "
                f"Payload sent: {len(schedule_payload)} stations, dates: {dates[:3]}..."
            )
            raise RuntimeError(
                "SchedulesDirect API returned empty response (None) with status 200. "
                "This may indicate a temporary API issue or invalid request."
            )

        if not isinstance(raw_schedule_results, list):
            logger.error(
                f"SchedulesDirect returned unexpected type {type(raw_schedule_results)}: {raw_schedule_results}"
            )
            raise RuntimeError(
                f"SchedulesDirect API returned unexpected response type {type(raw_schedule_results)}. "
                f"Expected list, got: {raw_schedule_results}"
            )

        est_adjusted_schedule_results = convert_airdatetime_to_est(raw_schedule_results)
        schedule_results = filter_out_past_programs(est_adjusted_schedule_results)
        # SchedulesDirect data is in Eastern time, so filter using Eastern timezone
        schedule_results = filter_programs_by_time_of_day(
            schedule_results,
            start_time=CACHE_START_TIME,
            end_time=CACHE_END_TIME,
        )

        program_ids_set: set[str] = set()
        for st in schedule_results:
            for p in st.get("programs", []):
                program_ids_set.add(p["programID"])

        logger.info(f"Running get_program_metadata_for_lineup for {len(program_ids_set)} programs")
        program_metadata = await self.get_program_metadata_for_lineup(list(program_ids_set))
        logger.info(
            f"Running get_program_metadata_for_lineup for {len(program_ids_set)} programs DONE"
        )
        # Merge with slim details
        merged: dict[str, list[dict[str, Any]]] = {}
        for st in schedule_results:
            sid = st["stationID"]
            merged.setdefault(sid, [])
            station = station_map.get(sid)

            for p in st.get("programs", []):
                pid = p["programID"]
                meta = program_metadata.get(pid)

                # Store full model data so it can be properly validated later
                program_details_dict = meta.model_dump() if meta else None

                channel_details_dict = None
                if station:
                    channel_details_dict = {
                        "stationID": station.stationID,
                        "name": station.name,
                        "callsign": station.callsign,
                        "logo": station.logo,
                    }

                merged[sid].append(
                    {
                        "programID": pid,
                        "airDateTime": p.get("airDateTime"),
                        "duration": p.get("duration"),
                        "program_details": program_details_dict,
                        "channel_details": channel_details_dict,
                        "channel_number": channel_number_map.get(sid),
                    }
                )

        logger.info(
            f"Fetched schedule | lineup: {lineup_id} | "
            f"stations: {len(merged)} | programs: {sum(len(v) for v in merged.values())}"
        )
        return merged

    def _filter_schedule_data(
        self,
        data: dict[str, list[dict[str, Any]]],
        channel_map: LineupChannelsResponse,
        broadcast_only: bool,
        start_time: str | None,
        end_time: str | None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Filter schedule data by request parameters."""
        logger.info(
            f"🔍 _filter_schedule_data: broadcast_only={broadcast_only}, data has {len(data)} stations"
        )

        if broadcast_only:
            valid_channels = filter_channels(channel_map.stations)
            broadcast_channels = [c for c in valid_channels if is_broadcast_network(c)]
            broadcast_ids = {c.stationID for c in broadcast_channels}

            logger.info(f"🔍 Broadcast filter: {len(broadcast_channels)} broadcast channels found")
            for c in broadcast_channels:
                logger.info(f"  Broadcast: {c.stationID} | {c.name} | {c.callsign}")

            logger.info(f"🔍 Data station IDs: {list(data.keys())[:20]}")
            logger.info(f"🔍 Broadcast IDs: {broadcast_ids}")

            data = {sid: progs for sid, progs in data.items() if sid in broadcast_ids}
            logger.info(f"🔍 After broadcast filter: {len(data)} stations remain")

        if start_time or end_time:
            schedule_format = [
                {"stationID": sid, "programs": list(progs)} for sid, progs in data.items()
            ]
            filtered = filter_programs_by_time_of_day(
                schedule_format, start_time=start_time, end_time=end_time
            )
            data = {st["stationID"]: st.get("programs", []) for st in filtered}

        # Filter past programs
        from datetime import UTC
        from zoneinfo import ZoneInfo

        now_utc = datetime.now(UTC)
        result: dict[str, list[dict[str, Any]]] = {}
        for sid, progs in data.items():
            future = []
            for p in progs:
                adt = p.get("airDateTime")
                if adt:
                    try:
                        # Parse datetime - handle both UTC (Z) and EST-aware formats
                        dt_str = adt.replace("Z", "+00:00") if adt.endswith("Z") else adt
                        dt = datetime.fromisoformat(dt_str)

                        # Convert to UTC for comparison
                        if dt.tzinfo is None:
                            # Naive datetime - assume UTC
                            dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
                        else:
                            # Timezone-aware - convert to UTC
                            dt_utc = dt.astimezone(UTC)

                        if dt_utc >= now_utc:
                            future.append(p)
                    except (ValueError, TypeError):
                        continue
            if future:
                result[sid] = future
        return result

    # ----------------------------------------------------------
    # 4. Get Schedules (public API)
    # ----------------------------------------------------------
    async def get_schedules_for_lineup(
        self,
        channel_map: LineupChannelsResponse | None = None,
        num_days: int = 1,
        broadcast_only: bool = False,
        start_time: str | None = None,
        end_time: str | None = None,
        default_lineup: str = "USA-YTBE501-X",
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Fetches schedules for a lineup with caching and filtering.

        Args:
            channel_map: Optional LineupChannelsResponse. If None, uses default lineup.
            num_days: Number of days (default: 1, max: 7 from cache).
            broadcast_only: If True, only return broadcast network channels.
            start_time: Optional start time filter in HH:MM format.
            end_time: Optional end time filter in HH:MM format.
            default_lineup: Default lineup ID.

        Returns:
            Dictionary mapping station IDs to lists of program dictionaries.
        """
        lineup_id: str = default_lineup

        if not channel_map:
            if not self.auth.account_status:
                await self.auth.get_account_status()

            if (
                self.auth.account_status
                and self.auth.account_status.lineups
                and default_lineup not in [ln.lineup for ln in self.auth.account_status.lineups]
            ):
                raise RuntimeError(f"Lineup {default_lineup} not found in account")

            channel_map = await self.get_channels_for_lineup(lineup_id)
            if not channel_map:
                logger.error("Unable to get channel map for lineup")
                return {}
        else:
            lineup_id = channel_map.metadata.lineup

        # Fetch cached schedule (decorator handles caching)
        schedule_data = await self._fetch_lineup_schedule(
            lineup_id=lineup_id,
            channel_map_dict=channel_map.model_dump(),
        )

        # Filter and return
        return self._filter_schedule_data(
            data=schedule_data,
            channel_map=channel_map,
            broadcast_only=broadcast_only,
            start_time=start_time,
            end_time=end_time,
        )

    # ----------------------------------------------------------
    # Utilities
    # ----------------------------------------------------------

    def generate_date_list(self, start_date, end_date) -> str | list[str]:
        """Returns a list of YYYY-MM-DD strings inclusive."""
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        current = start
        days = []
        while current <= end:
            days.append(current.isoformat())
            current += timedelta(days=1)
        return days

    @staticmethod
    def _chunk(items: Iterable[str], size: int) -> list[list[str]]:
        chunk = []
        out = []
        for item in items:
            chunk.append(item)
            if len(chunk) == size:
                out.append(chunk)
                chunk = []
        if chunk:
            out.append(chunk)
        return out
