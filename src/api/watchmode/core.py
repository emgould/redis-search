"""
Watchmode Core Service - Base service for Watchmode API operations.
Handles new releases, title details, and streaming availability.
"""

from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from api.watchmode.espn_ncaam_scoreboard import ESPN_NCAAM_SCOREBOARD_PATH
from utils.base_api_client import BaseAPIClient
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 6 hours for new releases data
CacheExpiration = 6 * 60 * 60  # 6 hours

# Request cache - separate from other caches, independent refresh
WatchmodeRequestCache = RedisCache(
    defaultTTL=4 * 60 * 60,  # 4 hours - streaming data changes frequently
    prefix="watchmode_request",
    verbose=False,
    isClassMethod=True,
)

WatchmodeCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="watchmode",
    verbose=False,
    isClassMethod=True,
)

# Sports caches are deliberately independent of the catalog request cache.
WatchmodeSportsDirectoryCache = RedisCache(
    defaultTTL=24 * 60 * 60,
    prefix="watchmode_sports_directory",
    verbose=False,
    isClassMethod=True,
)
# Full league team rosters change rarely; one Watchmode download per league per week.
WATCHMODE_LIVE_TEAMS_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
WatchmodeSportsTeamDirectoryCache = RedisCache(
    defaultTTL=WATCHMODE_LIVE_TEAMS_CACHE_TTL_SECONDS,
    prefix="watchmode_sports_directory",
    verbose=False,
    isClassMethod=True,
)
WatchmodeSportsScheduleCache = RedisCache(
    defaultTTL=24 * 60 * 60,
    prefix="watchmode_sports_schedule",
    verbose=False,
    isClassMethod=True,
)
WatchmodeSportsActivityCache = RedisCache(
    defaultTTL=-1,
    prefix="watchmode_sports_activity",
    verbose=False,
    isClassMethod=True,
)
WatchmodeSportsSourceCache = RedisCache(
    defaultTTL=24 * 60 * 60,
    prefix="watchmode_sports_sources",
    verbose=False,
    isClassMethod=True,
)

logger = get_logger(__name__)

# One schedule window for every viewer zone. The wrapper buckets start_time
# into the viewer's local day. London "yesterday" reaches two New York days
# back; Hawaii six days ahead reaches seven New York days forward.
SPORTS_CANONICAL_TIMEZONE = "America/New_York"
SPORTS_WARM_DAYS_BEFORE = 2
SPORTS_WARM_DAYS_AFTER = 7


SPORTS_SECTION_ORDER = {"live": 0, "upcoming": 1, "status": 2, "earlier": 3}


def sports_feed_section(status: str, *, viewer_day: date, today: date) -> str:
    """Group a reported status for the viewer's selected day.

    Live is only its own section on today. Future days stay Upcoming.
    Past days that are not postponed, cancelled, or unknown are Earlier.
    """
    if status in {"postponed", "cancelled", "unknown"}:
        return "status"
    if status == "final":
        return "earlier"
    if status == "live" and viewer_day == today:
        return "live"
    if viewer_day < today:
        return "earlier"
    return "upcoming"


def new_york_dates_for_viewer_day(viewer_day_start: datetime) -> list[str]:
    """New York calendar dates that overlap one viewer-local day.

    viewer_day_start is midnight in the viewer's zone. A day behind New York
    also overlaps the next New York date; a day ahead overlaps the previous one.
    """
    if viewer_day_start.tzinfo is None:
        raise ValueError("Viewer day must include a timezone")
    schedule_zone = ZoneInfo(SPORTS_CANONICAL_TIMEZONE)
    local_end = viewer_day_start + timedelta(days=1)
    first = viewer_day_start.astimezone(schedule_zone).date()
    last = (local_end.astimezone(schedule_zone) - timedelta(microseconds=1)).date()
    dates: list[str] = []
    cursor = first
    while cursor <= last:
        dates.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return dates


class WatchmodeLiveError(RuntimeError):
    """Upstream live-sports failure that still carries the HTTP status."""

    def __init__(self, status_code: int, code: str) -> None:
        self.status_code = status_code
        self.code = code
        super().__init__(f"Watchmode Live request failed ({status_code} {code})")


class WatchmodeService(BaseAPIClient):
    """
    Base Watchmode service for all Watchmode API operations.
    Provides core utilities for fetching streaming data.
    """

    # Default rate limit (no specific limit documented, using conservative defaults)
    _rate_limit_max = 10
    _rate_limit_period = 1

    def __init__(self, api_key: str):
        """Initialize Watchmode service with API key.

        Args:
            api_key: Watchmode API key (required)

        Raises:
            ValueError: If API key is not provided
        """
        if not api_key:
            raise ValueError("Watchmode API key is required")

        self.api_key = api_key
        self.base_url = "https://api.watchmode.com/v1"

    @RedisCache.use_cache(WatchmodeRequestCache, prefix="watchmode_api")
    async def _make_request(
        self, endpoint: str, params: dict | None = None, max_retries: int = 3
    ) -> Any:
        """Make an HTTP request to the Watchmode API with retry logic.

        This method brokers the call to _core_async_request with Watchmode-specific config.

        Args:
            endpoint: API endpoint (e.g., '/releases/')
            params: Optional query parameters
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            JSON response (dict or list) or None on error
        """
        if params is None:
            params = {}

        # Add API key to params
        params["apiKey"] = self.api_key

        url = f"{self.base_url}{endpoint}"

        result = await self._core_async_request(
            url=url,
            params=params,
            headers=None,
            timeout=30,
            max_retries=max_retries,
            rate_limit_max=self._rate_limit_max,
            rate_limit_period=self._rate_limit_period,
        )
        # Cast to expected type since return_status_code=False
        return result

    @RedisCache.use_cache(WatchmodeCache, prefix="new_releases")
    async def get_new_releases(
        self,
        region: str = "US",
        types: str = "movie,tv",
        limit: int = 50,
        start_date: str | None = None,
        end_date: str | None = None,
        max_retries: int = 3,
    ) -> dict | None:
        """Get new releases from streaming api.

        Args:
            region: Country code (default: US)
            types: Content types (default: movie,tv)
                   Note: The /releases/ endpoint uses different type names than /search/
                   Valid values: "movie", "tv", "tv_special", "tv_miniseries", "short_film"
            limit: Number of results (default: 50)
            max_retries: Maximum retry attempts (default: 3, use 1 for fast-fail)

        Returns:
            Dict with releases data or None on error
        """
        from datetime import datetime, timedelta

        # Calculate date range for "this week" - last 7 days
        end_datetime = datetime.now()
        start_datetime = end_datetime - timedelta(days=7)

        params = {
            "start_date": start_date or start_datetime.strftime("%Y%m%d"),
            "end_date": end_date or end_datetime.strftime("%Y%m%d"),
            "regions": region,
            "types": types,
            "limit": limit,
        }

        result = await self._make_request("/releases/", params, max_retries=max_retries)
        return cast(dict[Any, Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="title_details")
    async def get_title_details(self, watchmode_id: int) -> dict | None:
        """Get detailed information for a specific title.

        Args:
            watchmode_id: Watchmode title ID

        Returns:
            Dict with title details or None on error
        """
        result = await self._make_request(f"/title/{watchmode_id}/details/")
        return cast(dict[Any, Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="title_sources")
    async def get_title_streaming_sources(
        self, watchmode_id: int, region: str = "US", max_retries: int = 3
    ) -> list | None:
        """Get streaming sources for a specific title.

        Args:
            watchmode_id: Watchmode title ID
            region: Country code (default: US)
            max_retries: Maximum retry attempts (default: 3, use 1 for fast-fail)

        Returns:
            List of streaming sources or None on error.
            Note: The Watchmode API always returns a list for this endpoint.
        """
        params = {"regions": region}
        result = await self._make_request(
            f"/title/{watchmode_id}/sources/", params, max_retries=max_retries
        )
        # The sources endpoint always returns a list
        return cast(list[Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="search")
    async def search_titles(self, query: str, types: str = "movie,tv") -> dict | None:
        """Search for titles by name.

        Args:
            query: Search query string
            types: Content types to search (default: movie,tv)
                   Valid values: "movie", "tv", "person" or comma-separated combinations

        Returns:
            Dict with search results or None on error
        """
        params = {"search_field": "name", "search_value": query, "types": types}
        result = await self._make_request("/search/", params)
        return cast(dict[Any, Any] | None, result)

    @RedisCache.use_cache(WatchmodeCache, prefix="title_by_tmdb")
    async def get_title_by_tmdb_id(
        self, tmdb_id: int, content_type: str = "movie", max_retries: int = 3
    ) -> dict | None:
        """Get title details using TMDB ID.

        Watchmode supports looking up titles by external IDs using colon format:
        /title/tmdb:{tmdb_id}/details/

        Note: Watchmode uses the same format for both movies and TV shows.

        Args:
            tmdb_id: TMDB ID of the title
            content_type: "movie" or "tv" (currently not used, kept for API compatibility)
            max_retries: Maximum retry attempts (default: 3, use 1 for fast-fail)

        Returns:
            Dict with title details or None if not found
        """
        search_field = "tmdb_tv_id" if content_type == "tv" else "tmdb_movie_id"
        result = await self._make_request(
            f"/search?search_field={search_field}&search_value={tmdb_id}",
            max_retries=max_retries,
        )
        return cast(dict[Any, Any] | None, result)

    async def _make_live_request(self, endpoint: str, params: dict | None = None) -> dict:
        """Live transport using the existing client, bypassing the catalog's 4h cache.

        Cache complete operations at the public-method level below. In particular,
        never cache a continuation page or an error. No automatic paid retries.
        """
        result = await self._core_async_request(
            url=f"{self.base_url}{endpoint}",
            params={
                k: str(v).lower() if isinstance(v, bool) else v for k, v in (params or {}).items()
            },
            headers={"X-API-Key": self.api_key},
            timeout=30,
            max_retries=1,
            rate_limit_max=120,
            rate_limit_period=60,
            return_status_code=True,
        )
        if not isinstance(result, tuple) or len(result) != 2:
            raise WatchmodeLiveError(503, "LIVE_UNAVAILABLE")
        body, status = result
        status_code = status if isinstance(status, int) else 503
        if not isinstance(body, dict) or body.get("success") is False or status_code >= 400:
            code = "NOT_FOUND" if status_code == 404 else "LIVE_UNAVAILABLE"
            if isinstance(body, dict) and body.get("code"):
                code = str(body["code"])
            if status_code < 400:
                status_code = 502
            raise WatchmodeLiveError(status_code, code)
        return body

    @RedisCache.use_cache(WatchmodeSportsDirectoryCache, prefix="live_coverage_v1")
    async def get_live_coverage(self) -> dict:
        return await self._make_live_request("/live-coverage/")

    @RedisCache.use_cache(WatchmodeSportsDirectoryCache, prefix="live_leagues_v1")
    async def get_live_leagues(self) -> dict:
        return await self._make_live_request("/live-leagues/", {"types": "sport"})

    @RedisCache.use_cache(WatchmodeSportsTeamDirectoryCache, prefix="live_teams_v1")
    async def get_live_teams(self, league_id: int) -> dict:
        """Cache the entire league directory, not per-user search variants.

        Redis entry TTL is one week per league. Watchmode Live is called only on
        a miss or expiry. Bust with the shared cache-version registry if rosters
        must refresh sooner.
        """
        if league_id < 1:
            raise ValueError("league_id must be positive")
        teams = []
        for page in range(1, 101):
            data = await self._make_live_request(
                "/live-teams/",
                {
                    "league_id": league_id,
                    "page": page,
                    "limit": 100,
                },
            )
            if not isinstance(data.get("teams"), list) or "total_pages" not in data:
                raise RuntimeError("Incomplete Watchmode team directory")
            teams.extend(data["teams"])
            if page >= data["total_pages"]:
                return {"teams": teams, "total_results": len(teams)}
        raise RuntimeError("Watchmode team directory exceeded 100 pages")

    @RedisCache.use_cache(WatchmodeSportsScheduleCache, prefix="live_day_v1")
    async def get_live_events_day(self, date: str, region: str = "US") -> dict:
        """Shared one-day snapshot for one America/New_York date.

        Startup pages do not request viewing options. Those stay on the
        per-event sources call. Business bulk pages include options because
        that endpoint has no separate flag. The date is a New York calendar
        date. Viewer timezones are not a cache dimension; the wrapper selects
        these dates and then keeps start times that fall in the viewer's local
        day. The cache dimensions are New York date and country.
        """
        if region not in {"US", "GB", "CA", "AU"}:
            raise ValueError("Unsupported sports region")
        start = datetime.strptime(date, "%Y-%m-%d").replace(
            tzinfo=ZoneInfo(SPORTS_CANONICAL_TIMEZONE)
        )
        end = start + timedelta(days=1)
        params = {"region": region, "date_from": start.isoformat(), "date_to": end.isoformat()}
        coverage = await self.get_live_coverage()
        access = coverage.get("access_level")
        if access not in {"startup", "business"}:
            raise RuntimeError("Sports requires Startup or Business access")
        if access == "business":
            data = await self.get_live_schedule(**params)
            events = data["events"]
            effective_start, effective_end = params["date_from"], params["date_to"]
            limited = False
        else:
            events = []
            effective_start, effective_end, limited = params["date_from"], params["date_to"], False
            for page in range(1, 101):
                data = await self._make_live_request(
                    "/live-events/",
                    {
                        **params,
                        "types": "sport",
                        "include_sources": False,
                        "limit": 100,
                        "page": page,
                    },
                )
                if not isinstance(data.get("events"), list) or "total_pages" not in data:
                    raise RuntimeError("Incomplete Watchmode event page")
                events.extend(data["events"])
                limited = limited or data.get("date_range_limited", False)
                effective_start = data.get("effective_date_from", effective_start)
                effective_end = data.get("effective_date_to", effective_end)
                if page >= data["total_pages"]:
                    break
            else:
                raise RuntimeError("Watchmode event list exceeded 100 pages")
        fetched_at = datetime.now(UTC).isoformat()
        return {
            "events": [e for e in events if e.get("event_type") == "sport"],
            "fetched_at": fetched_at,
            "region": region,
            "date": date,
            "timezone": SPORTS_CANONICAL_TIMEZONE,
            "date_range_limited": limited,
            "effective_date_from": effective_start,
            "effective_date_to": effective_end,
        }

    @RedisCache.use_cache(WatchmodeSportsActivityCache, prefix="live_activity_v1")
    async def get_live_team_activity(
        self, region: str = "US", *, no_cache: bool = False
    ) -> dict[str, object]:
        """Return team ids seen in the refreshed seven-day regional window."""
        today = datetime.now(ZoneInfo(SPORTS_CANONICAL_TIMEZONE)).date()
        active_ids: set[int] = set()
        for offset in range(7):
            date = (today + timedelta(days=offset)).isoformat()
            snapshot = await self.get_live_events_day(date, region, no_cache=no_cache)
            for event in snapshot.get("events", []):
                if not isinstance(event, dict):
                    continue
                for side in ("home_team", "away_team"):
                    team = event.get(side)
                    if isinstance(team, dict):
                        team_id = team.get("id")
                        if isinstance(team_id, int) and team_id > 0:
                            active_ids.add(team_id)
        return {
            "region": region,
            "refreshed_at": datetime.now(UTC).isoformat(),
            "active_team_ids": sorted(active_ids),
        }

    async def get_live_schedule(
        self, *, region: str, date_from: str, date_to: str, dma: str | None = None
    ) -> dict:
        """Internal bulk download; caller caches ONLY after all pages succeed.

        Never expose continuation tokens to the frontend. No per-page cache.
        """
        from datetime import datetime
        from time import monotonic

        start, end = datetime.fromisoformat(date_from), datetime.fromisoformat(date_to)
        if (
            start.tzinfo is None
            or end.tzinfo is None
            or not 0 < (end - start).total_seconds() <= 604800
        ):
            raise ValueError("Bulk window must have offsets and span at most seven days")
        if region not in {"US", "GB", "CA", "AU"}:
            raise ValueError("Unsupported sports region")
        if dma is not None and (region != "US" or len(dma) != 3 or not dma.isdigit()):
            raise ValueError("dma must be a three-digit US market code")
        params = {"region": region, "date_from": date_from, "date_to": date_to}
        if dma:
            params["dma"] = dma
        events, seen, snapshot_id = [], set(), None
        deadline = monotonic() + 540  # finish before Watchmode's 10-minute cursor expiry
        for _ in range(100):
            if monotonic() >= deadline:
                raise RuntimeError("Watchmode bulk download timed out")
            data = await self._make_live_request("/live-schedule/", params)
            if not isinstance(data.get("events"), list) or not data.get("snapshot_id"):
                raise RuntimeError("Incomplete Watchmode bulk page")
            if snapshot_id is not None and snapshot_id != data["snapshot_id"]:
                raise RuntimeError("Watchmode bulk snapshot changed")
            snapshot_id = data["snapshot_id"]
            events.extend(data["events"])
            cursor = data.get("next_cursor")
            if not cursor:
                if data.get("has_more"):
                    raise RuntimeError("Missing Watchmode continuation")
                return {"events": events, "snapshot_id": snapshot_id}
            if cursor in seen:
                raise RuntimeError("Repeated Watchmode continuation")
            seen.add(cursor)
            params = {"continuation": cursor}
        raise RuntimeError("Watchmode bulk download exceeded 100 pages")

    @RedisCache.use_cache(WatchmodeSportsScheduleCache, prefix="live_event_v1")
    async def get_live_event(self, event_id: int, region: str = "US") -> dict:
        if event_id < 1 or region not in {"US", "GB", "CA", "AU"}:
            raise ValueError("Invalid event_id or region")
        return await self._make_live_request(f"/live-event/{event_id}/", {"region": region})

    @RedisCache.use_cache(WatchmodeSportsSourceCache, prefix="live_sources_v1")
    async def get_live_event_sources(self, event_id: int, region: str = "US") -> dict:
        """Shared one-day viewing-option cache per event and country.

        Schedules are not a score feed, so this matches the day snapshot instead
        of refreshing links every few hours. Market filters are not accepted:
        one country shares one cache entry. Wrappers suppress links past
        refresh_after even while this cache is valid.
        """
        if event_id < 1 or region not in {"US", "GB", "CA", "AU"}:
            raise ValueError("Invalid event_id or region")
        result = await self._make_live_request(
            f"/live-event/{event_id}/sources/", {"regions": region}
        )
        return {**result, "fetched_at": datetime.now(UTC).isoformat()}

    @RedisCache.use_cache(WatchmodeSportsScheduleCache, prefix="espn_ncaam_scoreboard_v1")
    async def get_ncaam_scoreboard(self, date_yyyymmdd: str) -> dict[str, object]:
        """Fetch ESPN Division I men's college basketball scoreboard for one calendar day."""
        if len(date_yyyymmdd) != 8 or not date_yyyymmdd.isdigit():
            raise ValueError("date_yyyymmdd must be YYYYMMDD")
        url = (
            f"{ESPN_NCAAM_SCOREBOARD_PATH}?dates={date_yyyymmdd}&groups=50&limit=1000"
        )
        result = await self._core_async_request(
            url=url,
            timeout=15,
            max_retries=2,
            rate_limit_max=30,
            rate_limit_period=60,
            return_status_code=True,
        )
        if not isinstance(result, tuple) or len(result) != 2:
            return {"events": []}
        body, status = result
        if not isinstance(body, dict) or (isinstance(status, int) and status >= 400):
            return {"events": []}
        return body

    @RedisCache.use_cache(WatchmodeSportsScheduleCache, prefix="espn_rankings_v1")
    async def get_college_rankings(self, espn_path: str) -> dict:
        """Fetch and cache ESPN college rankings (24h TTL via WatchmodeSportsScheduleCache)."""
        url = f"https://site.api.espn.com/apis/site/v2/sports/{espn_path}/rankings"
        result = await self._core_async_request(
            url=url,
            timeout=15,
            max_retries=2,
            rate_limit_max=30,
            rate_limit_period=60,
            return_status_code=True,
        )
        if not isinstance(result, tuple) or len(result) != 2:
            return {"rankings": []}
        body, status = result
        if not isinstance(body, dict) or (isinstance(status, int) and status >= 400):
            return {"rankings": []}
        return body

    async def warm_sports_cache(
        self,
        region: str = "US",
        *,
        days_before: int = SPORTS_WARM_DAYS_BEFORE,
        days_after: int = SPORTS_WARM_DAYS_AFTER,
        force_refresh: bool = False,
    ) -> dict:
        """Fill New York schedule dates that cover every enabled viewer zone.

        days_before and days_after are relative to today in America/New_York.
        The schedule cache lasts a day, so a second fill in that window is a
        Redis hit. Does not download a second copy per viewer timezone. Does
        not expose a public refresh endpoint.
        """
        if (
            not 0 <= days_before <= SPORTS_WARM_DAYS_BEFORE
            or not 0 <= days_after <= SPORTS_WARM_DAYS_AFTER
        ):
            raise ValueError("Schedule warm window exceeds the New York dates viewers can reach")
        today = datetime.now(ZoneInfo(SPORTS_CANONICAL_TIMEZONE)).date()
        completed, failed = [], []
        await self.get_live_leagues()
        for offset in range(-days_before, days_after + 1):
            date = (today + timedelta(days=offset)).isoformat()
            try:
                await self.get_live_events_day(date, region, no_cache=force_refresh)
                completed.append(date)
            except RuntimeError:
                logger.warning("Sports cache warming failed for %s / %s", region, date)
                failed.append(date)
        return {"region": region, "completed_dates": completed, "failed_dates": failed}
