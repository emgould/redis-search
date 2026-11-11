"""Live Watchmode sports checks.

Requires WATCHMODE_API_KEY. Coverage is free. Business then loads one
America/New_York date for the US and one for Canada through the shared cache.
Startup walks unsourced event pages. The day snapshot uses the same flag.
"""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from api.watchmode.core import SPORTS_CANONICAL_TIMEZONE, SPORTS_SECTION_ORDER, WatchmodeService
from api.watchmode.wrappers import watchmode_wrapper

pytestmark = pytest.mark.integration

WATCHMODE_API_KEY = os.getenv("WATCHMODE_API_KEY")


def _new_york_today() -> str:
    return datetime.now(ZoneInfo(SPORTS_CANONICAL_TIMEZONE)).date().isoformat()


async def _assert_startup_day_shapes(day: str) -> None:
    """One unsourced page-walk per country. Stops once both shapes appear."""
    key = WATCHMODE_API_KEY
    assert key is not None
    service = WatchmodeService(key)
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=ZoneInfo(SPORTS_CANONICAL_TIMEZONE))
    end = start + timedelta(days=1)
    for region in ("US", "CA"):
        matchups = 0
        sessions = 0
        for page in range(1, 9):
            data = await service._make_live_request("/live-events/", {
                "region": region,
                "date_from": start.isoformat(),
                "date_to": end.isoformat(),
                "types": "sport",
                "include_sources": False,
                "limit": 100,
                "page": page,
            })
            events = data.get("events")
            total_pages = data.get("total_pages")
            assert isinstance(events, list)
            assert isinstance(total_pages, int)
            for raw in events:
                assert raw.get("event_type") == "sport"
                if raw.get("home_team") and raw.get("away_team"):
                    matchups += 1
                else:
                    sessions += 1
            if page >= total_pages or (matchups > 0 and sessions > 0):
                break
        assert matchups > 0, f"{region} {day} returned no team matchup"
        assert sessions > 0, f"{region} {day} returned no tournament or session"


@pytest.mark.skipif(not WATCHMODE_API_KEY, reason="WATCHMODE_API_KEY not set")
@pytest.mark.asyncio
async def test_live_sports_coverage_and_country_days() -> None:
    coverage = await watchmode_wrapper.get_sports(action="coverage", region="US")
    assert coverage.status_code == 200
    assert coverage.coverage is not None
    level = coverage.coverage.get("access_level")
    assert level in {"free", "startup", "business"}
    if level == "free":
        pytest.skip("access_level=free; sports event pages are not on this plan")

    day = _new_york_today()
    if level == "startup":
        await _assert_startup_day_shapes(day)
        return

    for region in ("US", "CA"):
        matchups = 0
        sessions = 0
        version: str | None = None
        page = 1
        while page < 20:
            feed = await watchmode_wrapper.get_sports(
                action="events",
                region=region,
                date=day,
                timezone_name=SPORTS_CANONICAL_TIMEZONE,
                limit=100,
                page=page,
                version=version,
            )
            assert feed.status_code == 200, feed.error
            assert feed.date == day
            assert feed.timezone == SPORTS_CANONICAL_TIMEZONE
            version = feed.version
            for event in feed.events:
                assert event.event_type == "sport"
                assert event.section in SPORTS_SECTION_ORDER
                if event.home_team is not None and event.away_team is not None:
                    matchups += 1
                else:
                    sessions += 1
            if not feed.has_more:
                break
            page += 1
        assert matchups > 0, f"{region} {day} returned no team matchup"
        assert sessions > 0, f"{region} {day} returned no tournament or session"
