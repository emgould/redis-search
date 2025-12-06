#!/usr/bin/env python3
"""
Generate mock data from real FlixPatrol responses.

This utility fetches real data from FlixPatrol and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data
    python generate_mock_data.py --core       # Generate core method results

Requirements:
    - Internet connection to access FlixPatrol
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

# Set environment to test mode FIRST to disable caching
os.environ["ENVIRONMENT"] = "test"

# Add python_functions directory to path to import services
python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

from api.subapi.flixpatrol.core import FlixPatrolService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

disable_cache()


class FlixPatrolMockDataGenerator:
    """Generate mock data from real FlixPatrol responses."""

    def __init__(self):
        """Initialize generator."""
        # Set up output directories
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)
        self.core_dir = Path.cwd() / "fixtures/core"
        self.core_dir.mkdir(parents=True, exist_ok=True)

    def _save_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file.

        Args:
            filename: Output filename
            data: Data to save
            directory: Target directory (defaults to make_requests_dir)
        """
        if directory is None:
            directory = self.make_requests_dir

        output_path = directory / filename

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_all_endpoints(self) -> None:
        """Generate mock data for all test endpoints."""
        print("\n" + "=" * 60)
        print("FlixPatrol Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        success_count = 0
        error_count = 0
        errors = []

        service = FlixPatrolService()

        # Test 1: Fetch raw HTML
        try:
            print("[1/3] Fetching FlixPatrol HTML...")
            html = await service.fetch_flixpatrol_data()
            self._save_json("flixpatrol_html.json", {"html": html})
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("fetch_html", str(e)))

        # Test 2: Parse HTML
        try:
            print("[2/3] Parsing FlixPatrol HTML...")
            html = await service.fetch_flixpatrol_data()
            parsed = service.parse_flixpatrol_html(html)
            self._save_json("flixpatrol_parsed.json", parsed)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("parse_html", str(e)))

        # Test 3: Get full data
        try:
            print("[3/3] Getting FlixPatrol data...")
            response = await service.get_flixpatrol_data()
            self._save_json("flixpatrol_data.json", response.model_dump())
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("get_data", str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/3")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/3")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} endpoint mocks")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing FlixPatrolService core methods."""
        print("\n" + "=" * 60)
        print("FlixPatrol Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        service = FlixPatrolService()

        success_count = 0
        error_count = 0
        errors = []

        # Test parse_flixpatrol_html
        try:
            print("[1/2] Testing parse_flixpatrol_html...")
            html = await service.fetch_flixpatrol_data()
            parsed = service.parse_flixpatrol_html(html)
            self._save_json("parse_flixpatrol_html.json", parsed, directory=self.core_dir)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("parse_flixpatrol_html", str(e)))

        # Test get_flixpatrol_data
        try:
            print("[2/2] Testing get_flixpatrol_data...")
            response = await service.get_flixpatrol_data()
            self._save_json(
                "get_flixpatrol_data.json", response.model_dump(), directory=self.core_dir
            )
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("get_flixpatrol_data", str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/2")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/2")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.core_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} core method mocks")

    async def generate_all(self) -> None:
        """Generate all mock data files (basic set)."""
        print("\n" + "=" * 60)
        print("FlixPatrol Mock Data Generator - Basic Set")
        print("=" * 60 + "\n")

        service = FlixPatrolService()

        print("Fetching basic data...")

        # Get data
        response = await service.get_flixpatrol_data()

        print("\nSaving mock data files...")

        # Save data
        self._save_json("mock_flixpatrol_data.json", response.model_dump())

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real FlixPatrol responses"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate mock data for all test endpoints",
    )
    parser.add_argument(
        "--endpoints",
        action="store_true",
        help="Generate mock data for all test endpoints (make_requests/)",
    )
    parser.add_argument(
        "--core",
        action="store_true",
        help="Generate mock data by executing core service methods",
    )
    args = parser.parse_args()

    try:
        generator = FlixPatrolMockDataGenerator()

        if args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.endpoints or args.all:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        else:
            # Generate basic mock data
            await generator.generate_all()

        return 0
    except Exception as e:
        print(f"\n❌ Error generating mock data: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
