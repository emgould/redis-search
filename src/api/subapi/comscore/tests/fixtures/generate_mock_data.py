#!/usr/bin/env python3
"""
Generate mock data from real Comscore API responses.

This utility fetches real data from Comscore API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate all mock data
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --models     # Generate model fixtures (models/)

Requirements:
    - Internet connection to access Comscore API
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

from dotenv import find_dotenv, load_dotenv  # noqa: E402

from api.subapi.comscore.core import ComscoreService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)


class ComscoreMockDataGenerator:
    """Generate mock data from real Comscore API responses."""

    def __init__(self):
        """Initialize generator."""
        # Set directories relative to current working directory
        self.fixtures_dir = Path.cwd() / "fixtures"
        self.make_requests_dir = self.fixtures_dir / "make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir = self.fixtures_dir / "models"
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.core_dir = self.fixtures_dir / "core"
        self.core_dir.mkdir(parents=True, exist_ok=True)

        disable_cache()

    def _save_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file in specified directory.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if it's a Pydantic model)
            directory: Target directory (defaults to make_requests_dir)
        """
        if directory is None:
            directory = self.make_requests_dir

        output_path = directory / filename

        # Convert Pydantic models to dict if needed
        if hasattr(data, "model_dump"):
            data = data.model_dump()

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_api_responses(self) -> None:
        """Generate mock data from actual Comscore API responses."""
        print("\n" + "=" * 60)
        print("Comscore API Responses Mock Data Generator")
        print("=" * 60 + "\n")

        service = ComscoreService()

        try:
            print("[1/1] Fetching domestic box office rankings...")

            # Make actual API request
            response_data = await service._make_request()

            if not response_data:
                raise Exception("Failed to fetch data from Comscore API")

            # Save raw API response
            filename = "domestic_rankings.json"
            self._save_json(filename, response_data, directory=self.make_requests_dir)

            print("  ✓ Success\n")

            print("=" * 60)
            print("Generation Summary")
            print("=" * 60)
            print("✓ Successful: 1/1")
            print(f"\n✓ Files saved to: {self.make_requests_dir}")
            print("=" * 60 + "\n")

        except Exception as e:
            print(f"  ❌ Error: {str(e)}\n")
            raise

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing ComscoreService core methods."""
        print("\n" + "=" * 60)
        print("Comscore Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        service = ComscoreService()

        success_count = 0
        error_count = 0

        # Test: get_domestic_rankings
        try:
            print("[1/1] Processing: get_domestic_rankings")

            result = await service.get_domestic_rankings()

            if not result:
                raise Exception("get_domestic_rankings returned None")

            # Save processed data
            filename = "domestic_rankings_processed.json"
            self._save_json(filename, result, directory=self.core_dir)

            success_count += 1
            print("  ✓ Success\n")

        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/1")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/1")

        print(f"\n✓ Files saved to: {self.core_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} core method mocks")

    async def generate_model_fixtures(self) -> None:
        """Generate model fixtures from actual API data."""
        print("\n" + "=" * 60)
        print("Comscore Model Fixtures Generator")
        print("=" * 60 + "\n")

        service = ComscoreService()

        try:
            print("[1/2] Fetching box office data...")

            box_office_data = await service.get_domestic_rankings()

            if not box_office_data or not box_office_data.rankings:
                raise Exception("No box office rankings available")

            # Save single ranking (first one)
            print("[2/2] Generating model fixtures...")
            ranking = box_office_data.rankings[0]
            self._save_json("box_office_ranking.json", ranking, directory=self.models_dir)

            # Save complete box office data (first 3 rankings)
            limited_data = {
                "rankings": [r.model_dump() for r in box_office_data.rankings[:3]],
                "exhibition_week": box_office_data.exhibition_week,
                "fetched_at": box_office_data.fetched_at,
            }
            self._save_json("box_office_data.json", limited_data, directory=self.models_dir)

            print("  ✓ Success\n")

            print("=" * 60)
            print("Generation Summary")
            print("=" * 60)
            print("✓ Successful: 2/2")
            print(f"\n✓ Files saved to: {self.models_dir}")
            print("=" * 60 + "\n")

        except Exception as e:
            print(f"  ❌ Error: {str(e)}\n")
            raise

    async def generate_all(self) -> None:
        """Generate all mock data files."""
        print("\n" + "=" * 60)
        print("Comscore Mock Data Generator - ALL")
        print("=" * 60 + "\n")

        try:
            # Generate API responses
            await self.generate_api_responses()

            # Generate core method results
            await self.generate_core_methods()

            # Generate model fixtures
            await self.generate_model_fixtures()

            print("\n" + "=" * 60)
            print("✓ All mock data generation complete!")
            print("=" * 60 + "\n")

        except Exception as e:
            print(f"\n❌ Error during generation: {e}")
            raise


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real Comscore API responses"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all mock data (default)",
    )
    parser.add_argument(
        "--api",
        action="store_true",
        help="Generate mock data from API responses (make_requests/)",
    )
    parser.add_argument(
        "--core",
        action="store_true",
        help="Generate mock data by executing core service methods",
    )
    parser.add_argument(
        "--models",
        action="store_true",
        help="Generate model fixtures (models/)",
    )
    args = parser.parse_args()

    try:
        generator = ComscoreMockDataGenerator()

        if args.api:
            # Generate mock data from API responses
            await generator.generate_api_responses()
        elif args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.models:
            # Generate model fixtures
            await generator.generate_model_fixtures()
        else:
            # Generate all mock data (default)
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
