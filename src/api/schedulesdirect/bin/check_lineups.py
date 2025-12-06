#!/usr/bin/env python3
"""
Quick script to check SchedulesDirect lineups and available stations.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv

from api.schedulesdirect.core import SchedulesDirectService

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)


async def main():
    service = SchedulesDirectService()

    print("=" * 60)
    print("SchedulesDirect Account Information")
    print("=" * 60)

    # 1. Always add a default lineup first (Option A)
    zip_code = "07054"  # default ZIP for lineup search
    print("\n1. Initializing JSON API lineup (forced add)...")

    # Search for available lineups by ZIP
    headends = await service.find_headends_by_zip(zip_code)
    available: list[dict[str, str]] = []
    for headend in headends.headends:
        for lineup in headend.lineups:
            available.append(
                {
                    "lineup": lineup.lineup,
                    "name": lineup.name,
                }
            )
    if not available:
        print(f"   ✗ No available lineups found for ZIP {zip_code}. Cannot continue.")
        return

    first_lineup_id = available[0].get("lineup")
    if not first_lineup_id:
        print("   ✗ First lineup has no ID. Cannot continue.")
        return
    print(f"   Attempting to add lineup: {first_lineup_id} ({available[0].get('name')})")

    added = await service.add_lineup(first_lineup_id)
    if added:
        print("   ✓ Lineup added or already exists.")
    else:
        print("   ⚠ add_lineup() returned a non-success code — continuing anyway.")

    # Now fetch lineups AFTER we've attempted to add one
    await service.auth.get_account_status()
    if service.auth.account_status and service.auth.account_status.lineups:
        lineups = [lineup.model_dump() for lineup in service.auth.account_status.lineups]
    else:
        lineups = []

    print("\n2. Fetching JSON API lineups...")
    if not lineups:
        print("   ✗ No lineups found after adding.")
        return

    print(f"   ✓ Found {len(lineups)} lineup(s):")
    for lineup_dict in lineups:
        print(f"     - {lineup_dict.get('lineup')}: {lineup_dict.get('name')}")

    # Get stations from first lineup
    if lineups:
        lineup_id_to_fetch = lineups[0].get("lineup")
        print(f"\n3. Fetching stations from lineup: {lineup_id_to_fetch}")
        channels_response = await service.get_channels_for_lineup(lineup_id_to_fetch)
        stations = [s.model_dump() for s in channels_response.stations] if channels_response else []

        if not stations:
            print("   ✗ No stations found")
            return

        print(f"   ✓ Found {len(stations)} station(s)")

        # Show major network stations
        print("\n4. Major Network Stations:")
        networks = ["CBS", "NBC", "ABC", "FOX", "PBS", "CW"]
        for station in stations:
            station_id = station.get("stationID")
            name = station.get("name", "")
            callsign = station.get("callsign", "")
            affiliate = station.get("affiliate", "")

            # Check if this is a major network
            for network in networks:
                if (
                    network.upper() in name.upper()
                    or network.upper() in callsign.upper()
                    or network.upper() in affiliate.upper()
                ):
                    print(f"   {network:6s} -> {station_id:10s} {callsign:10s} {name}")
                    break

        # Test schedule fetch with first station
        if stations:
            test_station = stations[0]
            test_station_id = test_station.get("stationID")
            print(
                f"\n5. Testing schedule fetch for {test_station_id} ({test_station.get('name')})..."
            )

            from datetime import datetime

            today = datetime.now().date().isoformat()

            if not test_station_id:
                print("   ✗ Station has no ID. Cannot continue.")
                return

            try:
                schedules = await service.get_station_schedules([test_station_id], today)
                if schedules:
                    print(
                        f"   ✓ Got schedule with {len(schedules[0].get('programs', []))} programs"
                    )
                else:
                    print("   ✗ Empty schedule returned")
            except Exception as e:
                print(f"   ✗ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
