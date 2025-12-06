#!/usr/bin/env python3
"""
Test script to verify time-of-day filtering in get_schedules_for_lineup.

This script demonstrates the new start_time and end_time parameters
which filter by time of day (e.g., "20:00" to "23:00") regardless of date.
"""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
config_path = (
    Path(__file__).parent.parent.parent.parent.parent.parent / "config" / "api.dev.env"
)
if config_path.exists():
    load_dotenv(config_path, override=True)
    print(f"Loaded config from: {config_path}")
else:
    print(f"Warning: Config file not found at {config_path}")

# Map SCHEDULES_DIRECT_USER to SCHEDULES_DIRECT_USERNAME if needed
if os.getenv("SCHEDULES_DIRECT_USER") and not os.getenv("SCHEDULES_DIRECT_USERNAME"):
    user = os.getenv("SCHEDULES_DIRECT_USER")
    if user:
        os.environ["SCHEDULES_DIRECT_USERNAME"] = user

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from api.schedulesdirect.auth import SchedulesDirectAuth
from api.schedulesdirect.core import SchedulesDirectService


async def test_time_filtering():
    """Test the time-of-day filtering functionality."""
    print("Testing get_schedules_for_lineup with time-of-day filtering...")
    print("=" * 80)

    # Check for required environment variables
    if not os.getenv("SCHEDULES_DIRECT_USERNAME") or not os.getenv("SCHEDULES_DIRECT_PASSWORD"):
        print("ERROR: Missing required environment variables:")
        print("  - SCHEDULES_DIRECT_USERNAME")
        print("  - SCHEDULES_DIRECT_PASSWORD")
        return

    # Initialize service (without Firebase - uses env vars directly)
    auth = SchedulesDirectAuth()
    service = SchedulesDirectService(auth=auth)

    # Get account status directly (bypasses Firebase token storage)
    await auth.get_account_status()

    # Test 1: Default behavior (no time filtering)
    print("\n1. Default behavior (no time filtering):")
    schedules_default = await service.get_schedules_for_lineup(num_days=1, broadcast_only=True)
    total_programs_default = sum(len(programs) for programs in schedules_default.values())
    print(f"   Total programs: {total_programs_default}")

    # Test 2: Primetime window (8 PM to 11 PM Eastern Time)
    print("\n2. Primetime window (8 PM - 11 PM ET):")
    print("   Time filter: 20:00 to 23:00 in America/New_York timezone")
    schedules_primetime = await service.get_schedules_for_lineup(
        num_days=1,
        broadcast_only=True,
        start_time="20:00",
        end_time="23:00",
    )
    total_programs_primetime = sum(len(programs) for programs in schedules_primetime.values())
    print(f"   Total programs: {total_programs_primetime}")

    # Test 3: Morning shows (6 AM to 10 AM Pacific Time)
    print("\n3. Morning shows (6 AM - 10 AM PT):")
    print("   Time filter: 06:00 to 10:00 in America/Los_Angeles timezone")
    schedules_morning = await service.get_schedules_for_lineup(
        num_days=1,
        broadcast_only=True,
        start_time="06:00",
        end_time="10:00",
    )
    total_programs_morning = sum(len(programs) for programs in schedules_morning.values())
    print(f"   Total programs: {total_programs_morning}")

    # Test 4: Late night (11 PM to 2 AM - wraps around midnight)
    print("\n4. Late night (11 PM - 2 AM ET, wraps midnight):")
    print("   Time filter: 23:00 to 02:00 in America/New_York timezone")
    schedules_latenight = await service.get_schedules_for_lineup(
        num_days=1,
        broadcast_only=True,
        start_time="23:00",
        end_time="02:00",
    )
    total_programs_latenight = sum(len(programs) for programs in schedules_latenight.values())
    print(f"   Total programs: {total_programs_latenight}")

    # Test 5: Only start_time (from 6 PM onwards)
    print("\n5. Only start_time (from 6 PM onwards):")
    print("   Time filter: 18:00 to end of day in America/New_York timezone")
    schedules_evening = await service.get_schedules_for_lineup(
        num_days=1, broadcast_only=True, start_time="18:00"
    )
    total_programs_evening = sum(len(programs) for programs in schedules_evening.values())
    print(f"   Total programs: {total_programs_evening}")

    # Test 6: Only end_time (until 12 PM noon)
    print("\n6. Only end_time (until 12 PM noon):")
    print("   Time filter: start of day to 12:00 in America/New_York timezone")
    schedules_morning_only = await service.get_schedules_for_lineup(
        num_days=1, broadcast_only=True, end_time="12:00"
    )
    total_programs_morning_only = sum(len(programs) for programs in schedules_morning_only.values())
    print(f"   Total programs: {total_programs_morning_only}")

    print("\n" + "=" * 80)
    print("✅ All tests completed successfully!")

    # Show sample programs from primetime
    if schedules_primetime:
        print("\n📺 Sample primetime programs (8-11 PM ET):")
        for station_id, programs in list(schedules_primetime.items())[:2]:
            print(f"\n   Station: {station_id}")
            for program in programs[:3]:
                title = (
                    program["program_details"].titles[0].title120
                    if program["program_details"] and program["program_details"].titles
                    else "Unknown"
                )
                air_time = program["airDateTime"]
                duration = program["duration"]
                print(f"   - {title}")
                print(f"     Air time: {air_time}, Duration: {duration} min")


if __name__ == "__main__":
    asyncio.run(test_time_filtering())
