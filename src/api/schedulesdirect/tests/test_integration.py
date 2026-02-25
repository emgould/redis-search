"""Integration tests for SchedulesDirect API - these hit the real API."""

# CRITICAL: Set environment variables FIRST, before any other imports
import os

from api.tmdb.models import MCMovieItem, MCTvItem
from contracts.models import MCType

os.environ.setdefault("FIRESTORE_EMULATOR_HOST", "localhost:8080")
os.environ.setdefault("FIREBASE_AUTH_EMULATOR_HOST", "localhost:9099")

from datetime import UTC, datetime, timedelta

import firebase_admin
import pytest
from google.auth.credentials import AnonymousCredentials

from utils.pytest_utils import write_snapshot

# Default project should match what emulator uses (media-circle from dev-start.sh)
project_id = os.environ.get("GCLOUD_PROJECT", "media-circle")

# Initialize Firebase Admin SDK for emulator BEFORE importing any modules that use it
if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credential=AnonymousCredentials(), options={"projectId": project_id}
    )

from api.schedulesdirect.core import SchedulesDirectService
from api.schedulesdirect.models import (
    DEFAULT_PRIMETIME_NETWORKS,
    DEFAULT_PRIMETIME_TIMEZONE,
    AccountStatusResponse,
    HeadendsSearchResponse,
    LineupChannelsResponse,
    LineupInfo,
    LineupStation,
    SchedulesDirectPrimetimeResponse,
    SDProgramMetadata,
)
from api.schedulesdirect.sd_token_client import get_schedulesdirect_token
from api.schedulesdirect.wrappers import SchedulesDirectWrapper

# Skip integration tests if credentials are not available
pytestmark = pytest.mark.skipif(
    not os.getenv("SCHEDULES_DIRECT_USERNAME") or not os.getenv("SCHEDULES_DIRECT_PASSWORD"),
    reason="SchedulesDirect credentials not available",
)


@pytest.fixture
def schedules_direct_service():
    """Create service instance using real credentials from environment."""
    return SchedulesDirectService()


@pytest.fixture
def schedules_direct_wrapper():
    """Create wrapper instance."""
    return SchedulesDirectWrapper()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_account_status_integration(schedules_direct_service: SchedulesDirectService):
    """Test account status retrieval against real SchedulesDirect API."""

    try:
        response: AccountStatusResponse = (
            await schedules_direct_service.auth.get_account_status()
        )  # Skip if auth failed

        assert response is not None
        expires_dt = datetime.fromisoformat(response.account.expires.replace("Z", "+00:00"))
        assert expires_dt > datetime.now(UTC)
        assert response.account.accountExpiration > datetime.now().timestamp()
        assert len(response.lineups) > 0
        assert response.is_account_active
        assert response.is_token_valid

        # Write token info to snapshot
        write_snapshot(response.model_dump(), "get_token_result.json")
    except RuntimeError as e:
        if "Exceeded maximum number of logins" in str(e):
            pytest.skip(
                "SchedulesDirect account has exceeded maximum logins in 24 hours. "
                "This is expected during development/testing. Please try again later."
            )
        raise


# @pytest.mark.asyncio
# @pytest.mark.integration
# async def test_find_headends_by_zip(schedules_direct_service: SchedulesDirectService):
#     """Test schedule retrieval against real SchedulesDirect API."""

#     test_zip = "07417"
#     try:
#         # Use today's date
#         response = await schedules_direct_service.find_headends_by_zip(zip_code=test_zip)

#         assert isinstance(response, HeadendsSearchResponse)

#         assert response.total_lineups > 0
#         assert len(response.headends) > 0
#         assert len(response.headends[0].lineups) > 0

#         write_snapshot(response.model_dump(), "find_headends_by_zip_result.json")

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# @pytest.mark.asyncio
# @pytest.mark.integration
# async def test_adding_and_removing_lineup_from_account(
#     schedules_direct_service: SchedulesDirectService,
# ):
#     """Test removing a lineup from the account"""

#     try:
#         """
#         Can only be run once a day.
#         """
#         # Add the lineup to the account
#         bypass = True
#         if not bypass:
#             test_lineup = "USA-DITV-X"
#             response = await schedules_direct_service.add_lineup(test_lineup)
#             assert response
#             await schedules_direct_service.auth.get_account_status()
#             assert test_lineup in [
#                 a.lineup for a in schedules_direct_service.auth.account_status.lineups
#             ]

#             # Remove the default lineup
#             response = await schedules_direct_service.remove_lineup(test_lineup)
#             assert response
#             await schedules_direct_service.auth.get_account_status()
#             assert test_lineup not in [
#                 a.lineup for a in schedules_direct_service.auth.account_status.lineups
#             ]

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# @pytest.mark.asyncio
# @pytest.mark.integration
# async def test_get_channels_for_lineup(schedules_direct_service: SchedulesDirectService):
#     """Test getting channels for the default lineup"""

#     try:
#         # Get the default lineup
#         response = await schedules_direct_service.get_channels_for_lineup()

#         assert isinstance(response, LineupChannelsResponse)
#         assert response.total_channels > 0
#         assert response.total_stations > 0

#         write_snapshot(response.model_dump(), "get_channels_for_lineup_result.json")

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_schedule_for_lineup_default(schedules_direct_service: SchedulesDirectService):
    """Test program metadata retrieval against real SchedulesDirect API."""
    try:
        schedules = await schedules_direct_service.get_schedules_for_lineup(
            num_days=1, broadcast_only=True, start_time="20:00", end_time="23:00"
        )

        assert isinstance(schedules, dict)
        for sid, programs in schedules.items():
            assert isinstance(sid, str)
            assert isinstance(programs, list)
            assert len(programs) > 0
            for program in programs:
                assert isinstance(program, dict)
                assert "programID" in program
                assert "airDateTime" in program
                assert "duration" in program
                assert "program_details" in program
                assert SDProgramMetadata.model_validate(program["program_details"])
                assert "channel_details" in program
                assert "channel_number" in program
                assert LineupStation.model_validate(program["channel_details"])
                assert isinstance(program["channel_number"], str)
        assert isinstance(schedules, dict)
        assert len(schedules) > 0

        write_snapshot(schedules, "get_schedules_for_lineup.json")
    except RuntimeError as e:
        error_msg = str(e)
        if (
            "Exceeded maximum number of logins" in error_msg
            or "Failed to authenticate" in error_msg
            or "Token required" in error_msg
        ):
            pytest.skip(
                "SchedulesDirect authentication failed (likely rate limited). "
                "This is expected during development/testing. Please try again later."
            )
        raise


# @pytest.mark.asyncio
# @pytest.mark.integration
# async def test_get_schedule_with_time_filtering(schedules_direct_service: SchedulesDirectService):
#     """Test time-of-day filtering in get_schedules_for_lineup."""
#     try:
#         # Get schedules for primetime (8 PM - 11 PM ET)
#         # Note: Use num_days=2 to ensure we have evening programs available
#         schedules_primetime = await schedules_direct_service.get_schedules_for_lineup(
#             num_days=1,
#             broadcast_only=True,
#             start_time="20:00",
#             end_time="23:00",
#             timezone="America/New_York",
#         )
#         total_primetime = sum(len(programs) for programs in schedules_primetime.values())

#         # Verify filtering worked
#         assert isinstance(schedules_primetime, dict)
#         assert total_primetime > 0, "Should have some primetime programs"

#         # Verify all programs are within the time window
#         from datetime import datetime
#         from zoneinfo import ZoneInfo

#         for programs in schedules_primetime.values():
#             for program in programs:
#                 air_time_str = program["airDateTime"]
#                 dt = datetime.fromisoformat(air_time_str.replace("Z", "+00:00"))
#                 hour = dt.hour

#                 # Should be between 20:00 and 23:00 (exclusive of 23:00)
#                 assert 20 <= hour < 23, f"Program at {dt.isoformat()} is outside primetime window"

#         print(f"✓ Time filtering working: found {total_primetime} primetime programs (8-11 PM ET)")
#         write_snapshot(schedules_primetime, "get_schedules_for_lineup_with_time_filtering.json")
#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# """
# Wrapper tests
# """


# @pytest.mark.asyncio
# @pytest.mark.integration
# async def test_wrapper_get_primetime_schedule(schedules_direct_wrapper: SchedulesDirectWrapper):
#     """Test the wrapper's get_primetime_schedule method with default parameters."""
#     try:
#         # Test with default primetime window (8 PM - 11 PM ET)
#         response = await schedules_direct_wrapper.get_primetime_schedule()

#         # Verify response structure
#         assert isinstance(response, SchedulesDirectPrimetimeResponse)
#         assert response.data_type == "tv"
#         assert response.data_source == "SchedulesDirect national primetime + TMDB"
#         assert response.requested_date is not None
#         assert response.timezone == DEFAULT_PRIMETIME_TIMEZONE

#         # Verify results
#         assert isinstance(response.results, list)
#         assert len(response.results) > 0, "Should have at least some primetime programs"

#         check_movie_detection(response)
#         check_tv_show_detection(response)
#         check_metadata_completeness(response, response.results[0])

#         # Verify each result is an MCTvItem with schedule metadata
#         for item in response.results:  # Verify schedule metadata is present
#             schedule = item.metrics["schedule"]
#             assert "program_id" in schedule
#             assert "station_id" in schedule
#             assert "channel_name" in schedule
#             assert "air_datetime_utc" in schedule
#             assert "duration_minutes" in schedule

#             # Verify external IDs
#             assert "schedules_direct_program_id" in item.external_ids
#             assert "schedules_direct_station_id" in item.external_ids

#         print(f"✓ Wrapper test passed: {len(response.results)} primetime shows found")
#         write_snapshot(
#             [item.model_dump(exclude_none=True) for item in response.results],
#             "wrapper_get_primetime_schedule.json",
#         )

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# def check_movie_detection(response: SchedulesDirectPrimetimeResponse):
#     """Test that the wrapper correctly handles movies in the schedule."""
#     try:
#         movies = [item for item in response.results if item.mc_type == MCType.MOVIE]
#         print(f"✓ Content type detection: {len(movies)} movies")

#         # At minimum, we should have TV shows
#         assert len(movies) > 0, "Should have at least some TV shows"

#         # If we found movies, verify they have proper movie metadata
#         if movies:
#             for item in movies:
#                 assert isinstance(item, MCMovieItem)
#                 assert item.mc_id is not None
#                 assert item.tmdb_id is not None
#                 assert item.title is not None
#                 assert hasattr(item, "release_date")
#                 schedule = item.metrics.get("schedule", {})
#                 assert schedule.get("media_type") == "movie"

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# def check_tv_show_detection(response: SchedulesDirectPrimetimeResponse):
#     """Test that the wrapper correctly handles movies in the schedule."""
#     try:
#         tv_shows = [item for item in response.results if item.mc_type == MCType.TV_SERIES]

#         print(f"✓ Content type detection: {len(tv_shows)} TV shows")

#         # At minimum, we should have TV shows
#         assert len(tv_shows) > 0, "Should have at least some TV shows"

#         # If we found movies, verify they have proper movie metadata
#         if tv_shows:
#             # Verify each result is an MCTvItem with schedule metadata
#             for item in tv_shows:
#                 assert isinstance(item, MCTvItem)
#                 assert item.mc_id is not None
#                 assert item.tmdb_id is not None
#                 assert item.title is not None
#                 assert hasattr(item, "last_air_date")
#                 schedule = item.metrics.get("schedule", {})
#                 assert schedule.get("media_type") == "tv"
#                 print(f"  Found TV show: {item.title}")

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise


# async def check_metadata_completeness(
#     response: SchedulesDirectPrimetimeResponse,
#     item: MCTvItem,
# ):
#     """Test that schedule metadata is complete and properly formatted."""
#     try:
#         # Check first result in detail
#         item = response.results[0]
#         schedule = item.metrics["schedule"]

#         # Required fields
#         required_fields = [
#             "media_type",
#             "program_id",
#             "station_id",
#             "channel_name",
#             "air_datetime_utc",
#             "duration_minutes",
#         ]

#         for field in required_fields:
#             assert field in schedule, f"Missing required field: {field}"
#             assert schedule[field] is not None, f"Field {field} should not be None"

#         # Verify data types
#         assert isinstance(schedule["duration_minutes"], int)
#         assert schedule["duration_minutes"] > 0

#         # Verify ISO format for datetime
#         from datetime import datetime

#         dt = datetime.fromisoformat(schedule["air_datetime_utc"].replace("Z", "+00:00"))
#         assert dt is not None

#         # For TV shows, check episode metadata
#         from contracts.models import MCType

#         if item.mc_type == MCType.TV_SERIES:
#             # These may be None for some shows, but the fields should exist
#             assert "season_number" in schedule
#             assert "episode_number" in schedule
#             assert "episode_title" in schedule

#         print(f"✓ Schedule metadata completeness verified for: {item.title}")
#         print(f"  Channel: {schedule['channel_name']}")
#         print(f"  Air time: {schedule['air_datetime_utc']}")
#         print(f"  Duration: {schedule['duration_minutes']} minutes")

#     except RuntimeError as e:
#         error_msg = str(e)
#         if (
#             "Exceeded maximum number of logins" in error_msg
#             or "Failed to authenticate" in error_msg
#             or "Token required" in error_msg
#         ):
#             pytest.skip(
#                 "SchedulesDirect authentication failed (likely rate limited). "
#                 "This is expected during development/testing. Please try again later."
#             )
#         raise
