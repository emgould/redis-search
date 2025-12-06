#!/usr/bin/env python3
"""
Generate mock data from real TVDB API responses.

This utility fetches real data from TVDB API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data (make_requests/)
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --models     # Generate model validation data (models/)

Requirements:
    - TVDB_API_KEY environment variable must be set
    - TMDB_API_TOKEN environment variable must be set (for complete tests)
    - Internet connection to access TVDB API
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

from api.subapi._tvdb.core import TVDBService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# Test data configuration - well-known, stable content
TEST_DATA_CONFIG = {
    "search_query": "The Office",
    "tvdb_id": 73244,  # The Office (US)
    "external_id": "tt0386676",  # IMDB ID for The Office
    "tmdb_search_query": "Breaking Bad",
    "language": "eng",
    "max_results": 5,
}

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_CORE = [
    [
        "search",
        {
            "query": TEST_DATA_CONFIG["search_query"],
            "limit": TEST_DATA_CONFIG["max_results"],
        },
        "search_shows",
    ],
    [
        "get_show_details",
        {
            "tvdb_id": TEST_DATA_CONFIG["tvdb_id"],
            "extended": False,
        },
        "get_show_details_basic",
    ],
    [
        "get_show_details",
        {
            "tvdb_id": TEST_DATA_CONFIG["tvdb_id"],
            "extended": True,
        },
        "get_show_details_extended",
    ],
    [
        "get_show_images",
        {
            "query": "",
            "tvdb_id": TEST_DATA_CONFIG["tvdb_id"],
            "lang": TEST_DATA_CONFIG["language"],
            "image_types": ["poster", "logo"],
        },
        "get_show_images",
    ],
    [
        "get_all_images",
        {
            "tvdb_id": TEST_DATA_CONFIG["tvdb_id"],
            "lang": TEST_DATA_CONFIG["language"],
        },
        "get_all_images",
    ],
    [
        "get_show_complete",
        {
            "tvdb_id": TEST_DATA_CONFIG["tvdb_id"],
            "lang": TEST_DATA_CONFIG["language"],
        },
        "get_show_complete",
    ],
    [
        "search_by_external_id",
        {
            "external_id": TEST_DATA_CONFIG["external_id"],
            "source": "imdb",
        },
        "search_by_external_id",
    ],
]


class TVDBMockDataGenerator:
    """Generate mock data from real TVDB API responses."""

    def __init__(self, api_key: str, tmdb_token: str | None = None):
        """Initialize generator with API credentials.

        Args:
            api_key: TVDB API key
            tmdb_token: TMDB API token (optional)
        """
        self.api_key = api_key
        self.tmdb_token = tmdb_token

        # Set up output directories
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)
        self.core_dir = Path.cwd() / "fixtures/core"
        self.core_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir = Path.cwd() / "fixtures/models"
        self.models_dir.mkdir(parents=True, exist_ok=True)

        disable_cache()

    def _save_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if it's a Pydantic model)
            directory: Target directory (defaults to make_requests_dir)
        """
        if directory is None:
            directory = self.make_requests_dir

        output_path = directory / filename

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_all_endpoints(self) -> None:
        """Generate mock data for all test endpoints.

        This method executes all TVDB API endpoints and saves the responses
        as mock data files.
        """
        print("\n" + "=" * 60)
        print("TVDB Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        success_count = 0
        error_count = 0
        errors = []

        # Create service instance
        service = TVDBService(self.api_key)

        # Test 1: Search shows
        try:
            print("[1/7] Searching shows...")
            results = service.search(
                TEST_DATA_CONFIG["search_query"], limit=TEST_DATA_CONFIG["max_results"]
            )
            self._save_json("tvdb_search_shows.json", results)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("search_shows", str(e)))

        # Test 2: Get show details (basic)
        try:
            print("[2/7] Getting show details (basic)...")
            result = service.get_show_details(TEST_DATA_CONFIG["tvdb_id"], extended=False)
            self._save_json("tvdb_show_details_basic.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("show_details_basic", str(e)))

        # Test 3: Get show details (extended)
        try:
            print("[3/7] Getting show details (extended)...")
            result = service.get_show_details(TEST_DATA_CONFIG["tvdb_id"], extended=True)
            self._save_json("tvdb_show_details_extended.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("show_details_extended", str(e)))

        # Test 4: Get show images
        try:
            print("[4/7] Getting show images...")
            result = service.get_show_images(
                query="",
                tvdb_id=TEST_DATA_CONFIG["tvdb_id"],
                lang=TEST_DATA_CONFIG["language"],
                image_types=["poster", "logo"],
            )
            self._save_json("tvdb_show_images.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("show_images", str(e)))

        # Test 5: Get all images
        try:
            print("[5/7] Getting all images...")
            result = service.get_all_images(
                TEST_DATA_CONFIG["tvdb_id"], lang=TEST_DATA_CONFIG["language"]
            )
            self._save_json("tvdb_all_images.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("all_images", str(e)))

        # Test 6: Get complete show data
        try:
            print("[6/7] Getting complete show data...")
            result = service.get_show_complete(
                TEST_DATA_CONFIG["tvdb_id"], lang=TEST_DATA_CONFIG["language"]
            )
            self._save_json("tvdb_show_complete.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("show_complete", str(e)))

        # Test 7: Search by external ID
        try:
            print("[7/7] Searching by external ID...")
            result = service.search_by_external_id(TEST_DATA_CONFIG["external_id"], source="imdb")
            self._save_json("tvdb_search_by_external_id.json", result)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("search_by_external_id", str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/7")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/7")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} endpoint mocks")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing TVDBService core methods.

        This method calls the actual TVDBService methods defined in TEST_CORE
        and saves the results as mock data files in the core directory.
        """
        print("\n" + "=" * 60)
        print("TVDB Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_CORE)}\n")

        # Create TVDBService instance
        service = TVDBService(self.api_key)

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_CORE, 1):
            # Unpack the method configuration
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_CORE)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.core_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((method_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_CORE)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_CORE)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.core_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} core method mocks")

    async def generate_models(self) -> None:
        """Generate mock data for model validation.

        This creates sample data structures that can be used to test Pydantic models.
        """
        print("\n" + "=" * 60)
        print("TVDB Models Mock Data Generator")
        print("=" * 60 + "\n")

        # Fetch real data to use for model validation
        print("Fetching real data for model validation...")

        service = TVDBService(self.api_key)

        # Get search result
        search_results = service.search(TEST_DATA_CONFIG["search_query"], limit=1)
        search_result = search_results[0] if search_results else {}

        # Get show details
        show_details = service.get_show_details(TEST_DATA_CONFIG["tvdb_id"], extended=True)

        # Get images
        images = service.get_show_images(
            query="",
            tvdb_id=TEST_DATA_CONFIG["tvdb_id"],
            lang=TEST_DATA_CONFIG["language"],
            image_types=["poster", "logo"],
        )

        print("\nSaving model validation data...")

        # Save all model data
        self._save_json("tvdb_search_result.json", search_result, directory=self.models_dir)
        self._save_json("tvdb_show.json", show_details, directory=self.models_dir)
        self._save_json("tvdb_image_data.json", images, directory=self.models_dir)
        self._save_json(
            "tvdb_search_response.json",
            {
                "shows": search_results,
                "total_count": len(search_results),
                "query": TEST_DATA_CONFIG["search_query"],
            },
            directory=self.models_dir,
        )

        print("\n" + "=" * 60)
        print("✓ Model validation data generation complete!")
        print(f"✓ Files saved to: {self.models_dir}")
        print("=" * 60 + "\n")

    async def generate_all(self) -> None:
        """Generate all mock data files (basic set)."""
        print("\n" + "=" * 60)
        print("TVDB Mock Data Generator - Basic Set")
        print("=" * 60 + "\n")

        # Fetch basic data
        print("Fetching basic data...")

        service = TVDBService(self.api_key)

        # Get search results
        search_results = service.search(
            TEST_DATA_CONFIG["search_query"], limit=TEST_DATA_CONFIG["max_results"]
        )

        # Get show details
        show_details = service.get_show_details(TEST_DATA_CONFIG["tvdb_id"], extended=True)

        # Get images
        images = service.get_show_images(
            query="",
            tvdb_id=TEST_DATA_CONFIG["tvdb_id"],
            lang=TEST_DATA_CONFIG["language"],
            image_types=["poster", "logo", "banner"],
        )

        print("\nSaving mock data files...")

        # Save all data
        self._save_json("mock_search_results.json", search_results)
        self._save_json("mock_show_details.json", show_details)
        self._save_json("mock_show_images.json", images)

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate mock data from real TVDB API responses")
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
    parser.add_argument(
        "--models",
        action="store_true",
        help="Generate mock data for model validation",
    )
    args = parser.parse_args()

    # Get API credentials from environment
    tvdb_api_key = os.getenv("TVDB_API_KEY")
    tmdb_api_token = os.getenv("TMDB_API_TOKEN")

    if not tvdb_api_key:
        print("ERROR: Missing required environment variable:")
        print("  - TVDB_API_KEY")
        print("\nPlease set the required environment variable:")
        print("  export TVDB_API_KEY='your_key_here'")
        return 1

    try:
        generator = TVDBMockDataGenerator(tvdb_api_key, tmdb_api_token)

        if args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.endpoints or args.all:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        elif args.models:
            # Generate mock data for model validation
            await generator.generate_models()
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
