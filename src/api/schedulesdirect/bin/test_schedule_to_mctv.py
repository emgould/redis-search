#!/usr/bin/env python3
"""
Test script for converting SchedulesDirect schedule data to MCTvItem objects.

Usage:
    cd firebase/python_functions
    source venv/bin/activate
    python -m api.schedulesdirect.bin.test_schedule_to_mctv
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv

# Load environment variables FIRST
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Set emulator environment variables if not already set
os.environ.setdefault("FIRESTORE_EMULATOR_HOST", "localhost:8080")
os.environ.setdefault("FIREBASE_AUTH_EMULATOR_HOST", "localhost:9099")

# Initialize Firebase Admin SDK for emulator
import firebase_admin
from google.auth.credentials import AnonymousCredentials

project_id = os.environ.get("GCLOUD_PROJECT", "media-circle")

if not firebase_admin._apps:
    firebase_admin.initialize_app(
        credential=AnonymousCredentials(), options={"projectId": project_id}
    )

# Now import modules that depend on Firebase
from api.schedulesdirect.core import SchedulesDirectService
from api.schedulesdirect.utils import (
    create_mc_item_from_schedule,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)


async def test_single_program():
    """Test converting a single program to MC item (TV or Movie)."""
    logger.info("=" * 80)
    logger.info("TEST 1: Convert single program to MC item")
    logger.info("=" * 80)

    # Initialize service
    service = SchedulesDirectService()
    await service.init()

    # Get schedules for primetime
    logger.info("Fetching primetime schedules...")
    schedules = await service.get_schedules_for_lineup(
        broadcast_only=True,
        start_time="20:00",
        end_time="23:00",
        num_days=1,
    )

    if not schedules:
        logger.error("No schedules returned")
        return

    # Get first program from first station
    for station_id, programs in schedules.items():
        if programs:
            program = programs[0]
            logger.info("\nConverting program:")
            logger.info("  Station ID: %s", station_id)
            logger.info("  Program ID: %s", program.get("programID"))
            logger.info("  Air Time: %s", program.get("airDateTime"))

            # Convert to MC item
            item = await create_mc_item_from_schedule(program, enrich=True)

            if item:
                logger.info("\n✅ Successfully created %s:", item.mc_type.value.upper())
                # Access title based on item type
                title = getattr(item, "title", None) or getattr(item, "name", "N/A")
                tmdb_id = getattr(item, "tmdb_id", "N/A")
                overview = getattr(item, "overview", None)
                logger.info("  Title: %s", title)
                logger.info("  TMDB ID: %s", tmdb_id)
                logger.info("  MC ID: %s", item.mc_id)
                logger.info("  Overview: %s", overview[:100] if overview else "N/A")

                # Show schedule metadata
                schedule = item.metrics.get("schedule", {})
                logger.info("\n  Schedule Metadata:")
                logger.info("    Media Type: %s", schedule.get("media_type"))
                logger.info("    Show Type: %s", schedule.get("show_type"))
                logger.info("    Entity Type: %s", schedule.get("entity_type"))
                logger.info(
                    "    Channel: %s (%s)",
                    schedule.get("channel_name"),
                    schedule.get("channel_number"),
                )
                logger.info("    Air Time: %s", schedule.get("air_datetime_utc"))
                logger.info("    Duration: %s minutes", schedule.get("duration_minutes"))

                # Show episode info only for TV shows
                if item.mc_type.value == "tv":
                    logger.info(
                        "    Episode: S%sE%s - %s",
                        schedule.get("season_number", "?"),
                        schedule.get("episode_number", "?"),
                        schedule.get("episode_title", "N/A"),
                    )

                # Show external IDs
                logger.info("\n  External IDs:")
                for key, value in item.external_ids.items():
                    logger.info("    %s: %s", key, value)

                return item
            else:
                logger.warning("❌ Failed to create MC item for this program")
                continue

    logger.error("No programs found in schedules")
    return None


async def main():
    """Run all tests."""
    try:
        # Test 1: Single program
        single_item = await test_single_program()

        logger.info("\n" + "=" * 80)
        logger.info("ALL TESTS COMPLETED")
        logger.info("=" * 80)
        logger.info("Single item test: %s", "✅ PASS" if single_item else "❌ FAIL")

    except Exception as e:
        logger.error("Test failed with error: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
