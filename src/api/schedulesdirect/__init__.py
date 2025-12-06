"""
SchedulesDirect integration package.

Provides:
- Auth helpers for credentials stored in SecretParam/env
- Core service for interacting with the SchedulesDirect JSON API
- Wrapper that enriches SchedulesDirect schedule data with TMDB MCTvItems
- HTTP handlers exposed via Firebase Functions
"""

from api.schedulesdirect.auth import SchedulesDirectAuth, schedules_direct_auth
from api.schedulesdirect.core import SchedulesDirectService
from api.schedulesdirect.handlers import SchedulesDirectHandler, schedules_direct_handler
from api.schedulesdirect.models import (
    DEFAULT_PRIMETIME_NETWORKS,
    DEFAULT_PRIMETIME_TIMEZONE,
    DEFAULT_PRIMETIME_WINDOW,
    PRIMETIME_STATION_LOOKUP,
    PrimetimeAiring,
    SchedulesDirectPrimetimeResponse,
)
from api.schedulesdirect.wrappers import schedules_direct_wrapper

__all__ = [
    "SchedulesDirectAuth",
    "schedules_direct_auth",
    "SchedulesDirectService",
    "SchedulesDirectHandler",
    "schedules_direct_handler",
    "SchedulesDirectPrimetimeResponse",
    "PrimetimeAiring",
    "DEFAULT_PRIMETIME_NETWORKS",
    "DEFAULT_PRIMETIME_TIMEZONE",
    "DEFAULT_PRIMETIME_WINDOW",
    "PRIMETIME_STATION_LOOKUP",
    "schedules_direct_wrapper",
]
