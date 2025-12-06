"""Unit tests for SchedulesDirectService core functionality."""

# CRITICAL: Set environment variables FIRST, before any other imports
import os
from typing import Any

os.environ.setdefault("FIRESTORE_EMULATOR_HOST", "localhost:8080")
os.environ.setdefault("FIREBASE_AUTH_EMULATOR_HOST", "localhost:9099")

from datetime import UTC, datetime

import firebase_admin
import pytest
from google.auth.credentials import AnonymousCredentials

# Default project should match what emulator uses
project_id = os.environ.get("GCLOUD_PROJECT", "media-circle")

# Initialize Firebase Admin SDK for emulator BEFORE importing any modules that use it
if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credential=AnonymousCredentials(), options={"projectId": project_id}
    )

from api.schedulesdirect.core import SchedulesDirectService
from api.schedulesdirect.models import AccountStatusResponse, HeadendsSearchResponse, SDStations
from utils.pytest_utils import write_snapshot

# -------------------------------------------------------------------
# SCHEDULES
# -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_account_status():
    """Fetch schedules for first station today."""
    service = SchedulesDirectService()
    await service.auth.get_account_status()
    assert isinstance(service.auth.account_status, AccountStatusResponse)
    assert service.auth.account_status.is_account_active
    write_snapshot(service.auth.account_status, "auth_account_status.json")


# -------------------------------------------------------------------
# GET STATION SCHEDULES
# -------------------------------------------------------------------
# @pytest.mark.asyncio
# async def test_add_lineup():
#     """Fetch schedules for first station today."""
#     service = SchedulesDirectService()
#     status = await service.auth.get_account_status()
#     if not status.get_lineup_by_id("USA-YTBE501-X"):
#         result = await service.add_lineup("USA-YTBE501-X")
#         assert result
#     else:
#         print("Lineup already exists")
#     assert status.get_lineup_by_id("USA-YTBE501-X")


# @pytest.mark.asyncio
# async def test_get_lineups_by_zip():
#     service = SchedulesDirectService()
#     await service.auth.get_account_status()
#     # Headends are service providers
#     zip_response = await service.find_headends_by_zip(zip_code="07417")
#     assert isinstance(zip_response, HeadendsSearchResponse)
#     assert len(zip_response.headends) > 0

#     lineups = [
#         {
#             "provider": headend.headend,
#             "provider_type": headend.transport,
#             "name": headend.lineups[0].name,
#             "lineup": headend.lineups[0].lineup,
#         }
#         for headend in zip_response.headends
#     ]
#     write_snapshot(lineups, "find_headends_by_zip.json")

#     assert isinstance(lineups, list) and len(lineups) > 0
#     lineup_id = "USA-YTBE501-X"
#     stations = await service.get_lineup_stations(lineup_id)
#     assert isinstance(stations, list) and len(stations) > 0
#     station_ids = [s["stationID"] for s in stations]
#     assert len(station_ids) > 0
#     write_snapshot(station_ids, "get_lineup_stations.json")

#     today = datetime.now(UTC).strftime("%Y-%m-%d")
#     schedules = await service.get_station_schedules([station_ids[0:2]], today)
#     assert isinstance(schedules, list)
#     write_snapshot(schedules, "get_station_schedules_result.json")

#     program_ids = []
#     for sched in schedules:
#         for prog in sched.get("programs", []):
#             pid = prog.get("programID")
#             if pid:
#                 program_ids.append(pid)
#     program_ids = list(set(program_ids))
#     metadata = await service.get_program_metadata(program_ids)
#     assert isinstance(metadata, dict)
#     assert len(metadata) > 0
#     write_snapshot(metadata, "get_program_metadata_result.json")


# # -------------------------------------------------------------------
# # CHUNK UTILITY
# # -------------------------------------------------------------------


# def test_chunk():
#     service = SchedulesDirectService()
#     out = service._chunk(["a", "b", "c", "d", "e"], 2)

#     assert out == [["a", "b"], ["c", "d"], ["e"]]


# # -------------------------------------------------------------------
# # LINEUPS
# # -------------------------------------------------------------------


# @pytest.mark.asyncio
# async def test_get_lineups_live():
#     """GET /lineups returns list."""
#     service = SchedulesDirectService()
#     lineups = await service.get_lineups()
#     assert isinstance(lineups, list)
#     write_snapshot(lineups, "get_lineups_result.json")


# # -------------------------------------------------------------------
# # LINEUP STATIONS (NDJSON)
# # -------------------------------------------------------------------


# @pytest.mark.asyncio
# async def test_get_lineup_stations_live():
#     """NDJSON response for lineup stations returns list with items."""
#     service = SchedulesDirectService()
#     await service.add_lineup("USA-DITV-X")
#     stations = await service.get_lineup_stations("USA-DITV-X")
#     assert isinstance(stations, list)
#     assert len(stations) > 0
#     write_snapshot(stations, "get_lineup_stations_result.json")
