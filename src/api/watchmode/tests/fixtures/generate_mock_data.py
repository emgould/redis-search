#!/usr/bin/env python3
"""
Generate mock data from real Watchmode API responses.

This utility fetches real data from Watchmode API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data
    python generate_mock_data.py --core       # Generate core method results

Requirements:
    - WATCHMODE_API_KEY environment variable must be set
    - TMDB_READ_TOKEN environment variable must be set (for whats_new)
    - Internet connection to access Watchmode API
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

import aiohttp  # noqa: E402
from dotenv import find_dotenv, load_dotenv  # noqa: E402

from api.watchmode.core import WatchmodeService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# Watchmode API Configuration
WATCHMODE_BASE_URL = "https://api.watchmode.com/v1"

# Test data configuration - Use stable, well-known titles
# These IDs are for Watchmode's database (not TMDB!)
# We'll get these from the new releases endpoint first
TEST_DATA_CONFIG = {
    "movie": {
        "watchmode_id": None,  # Will be populated from API
        "name": "Fight Club",
    },
    "tv": {
        "watchmode_id": None,  # Will be populated from API
        "name": "Breaking Bad",
    },
}

# These will be populated after fetching new releases
MOVIE_TEST_ID = None
TV_TEST_ID = None
SEARCH_QUERY = "Breaking Bad"

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
# Note: Will be populated dynamically with valid IDs
TEST_CORE = []

# API endpoints to generate mock data for
# Format: [endpoint_template, test_data, function_name, mock_name]
# Note: These will be populated dynamically after getting IDs from new releases
TEST_ENDPOINTS = []


class WatchmodeMockDataGenerator:
    """Generate mock data from real Watchmode API responses."""

    def __init__(self, api_key: str):
        """Initialize generator with Watchmode API key.

        Args:
            api_key: Watchmode API key
        """
        self.api_key = api_key
        # Set make_requests_dir to fixtures/make_requests
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(exist_ok=True)
        self.core_dir = Path.cwd() / "fixtures/core"
        self.core_dir.mkdir(exist_ok=True)

        disable_cache()

    async def _make_request(self, endpoint: str) -> dict[str, Any]:
        """Make async HTTP request to Watchmode API.

        Args:
            endpoint: API endpoint with query params (e.g., 'releases/?limit=10')

        Returns:
            JSON response dict

        Raises:
            Exception: If request fails
        """
        # Add API key to endpoint
        separator = "&" if "?" in endpoint else "?"
        url = f"{WATCHMODE_BASE_URL}/{endpoint}{separator}apiKey={self.api_key}"
        timeout = aiohttp.ClientTimeout(total=30)

        async with (
            aiohttp.ClientSession() as session,
            session.get(url, timeout=timeout) as response,
        ):
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Watchmode API returned status {response.status}: {error_text}")

            return await response.json()

    def _save_json(self, filename: str, data: dict[str, Any]) -> None:
        """Save data to JSON file.

        Args:
            filename: Output filename
            data: Data to save
        """
        output_path = self.make_requests_dir / filename
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  → Saved to {output_path}")

    def _save_core_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file in specified directory.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if needed)
            directory: Target directory (defaults to core_dir)
        """
        if directory is None:
            directory = self.core_dir

        output_path = directory / filename

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def _get_test_ids_from_releases(self) -> tuple[int | None, int | None]:
        """Get valid Watchmode IDs from new releases.

        Returns:
            Tuple of (movie_id, tv_id) or (None, None) if not found
        """
        from datetime import datetime, timedelta

        # Get releases from the past 30 days to have better chance of finding content
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        endpoint = f"releases/?start_date={start_date.strftime('%Y%m%d')}&end_date={end_date.strftime('%Y%m%d')}&regions=US&types=movie,tv_series&limit=50"

        try:
            data = await self._make_request(endpoint)
            releases = data.get("releases", [])

            movie_id = None
            tv_id = None

            for release in releases:
                if movie_id is None and release.get("type") == "movie":
                    movie_id = release.get("id")
                if tv_id is None and release.get("type") in ["tv_series", "tv_special"]:
                    tv_id = release.get("id")

                if movie_id and tv_id:
                    break

            return movie_id, tv_id
        except Exception as e:
            print(f"  Warning: Could not fetch test IDs from releases: {e}")
            return None, None

    async def generate_all_endpoints(self) -> None:
        """Generate mock data for all test endpoints."""
        print("\n" + "=" * 60)
        print("Watchmode Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        # First, fetch new releases to get valid IDs and save it
        print("Step 1: Fetching new releases to get valid Watchmode IDs...")
        from datetime import datetime, timedelta

        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        releases_endpoint = f"releases/?start_date={start_date.strftime('%Y%m%d')}&end_date={end_date.strftime('%Y%m%d')}&regions=US&types=movie,tv_series&limit=20"

        try:
            releases_data = await self._make_request(releases_endpoint)
            self._save_json("get_new_releases.json", releases_data)
            print("  ✓ Saved new releases\n")

            # Extract IDs
            movie_id, tv_id = await self._get_test_ids_from_releases()

            if not movie_id or not tv_id:
                print("  ⚠️  Warning: Could not find both movie and TV IDs from releases")
                print("  Skipping title details and streaming sources endpoints\n")
                endpoints_to_test = [
                    [
                        "search/?search_field=name&search_value={parameter}&types=movie,tv",
                        SEARCH_QUERY,
                        "search_titles",
                        "search_titles",
                    ],
                ]
            else:
                print(f"  ✓ Found movie ID: {movie_id}")
                print(f"  ✓ Found TV ID: {tv_id}\n")

                endpoints_to_test = [
                    [
                        "title/{parameter}/details/",
                        movie_id,
                        "get_title_details",
                        "get_title_details_movie",
                    ],
                    [
                        "title/{parameter}/details/",
                        tv_id,
                        "get_title_details",
                        "get_title_details_tv",
                    ],
                    [
                        "title/{parameter}/sources/?regions=US",
                        movie_id,
                        "get_title_streaming_sources",
                        "get_streaming_sources_movie",
                    ],
                    [
                        "title/{parameter}/sources/?regions=US",
                        tv_id,
                        "get_title_streaming_sources",
                        "get_streaming_sources_tv",
                    ],
                    [
                        "search/?search_field=name&search_value={parameter}&types=movie,tv",
                        SEARCH_QUERY,
                        "search_titles",
                        "search_titles",
                    ],
                ]
        except Exception as e:
            print(f"  ❌ Error fetching new releases: {e}")
            print("  Cannot continue without valid IDs\n")
            raise

        print(f"Step 2: Processing {len(endpoints_to_test)} additional endpoints\n")

        success_count = 1  # Already saved new_releases
        error_count = 0
        errors = []

        for idx, endpoint_config in enumerate(endpoints_to_test, 1):
            endpoint_template = endpoint_config[0]
            test_data = endpoint_config[1]
            function_name = endpoint_config[2]
            mock_name = endpoint_config[3]

            try:
                print(f"[{idx}/{len(endpoints_to_test)}] Processing: {function_name}")
                print(f"  Endpoint template: {endpoint_template}")

                # Replace {parameter} with test_data if present
                if test_data is not None:
                    endpoint = endpoint_template.replace("{parameter}", str(test_data))
                else:
                    endpoint = endpoint_template

                print(f"  Final endpoint: {endpoint}")

                # Make the request
                data = await self._make_request(endpoint)

                # Generate filename and save
                filename = mock_name + ".json"
                self._save_json(filename, data)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((endpoint_template, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        total_endpoints = len(endpoints_to_test) + 1  # +1 for new_releases
        print(f"✓ Successful: {success_count}/{total_endpoints}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{total_endpoints}")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        # Don't raise exception if we got at least some data
        if success_count == 0:
            raise Exception("Failed to generate any endpoint mocks")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing WatchmodeService core methods."""
        print("\n" + "=" * 60)
        print("Watchmode Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        # Create WatchmodeService instance
        service = WatchmodeService(self.api_key)

        # First, get new releases to extract valid IDs
        print("Step 1: Fetching new releases to get valid Watchmode IDs...")
        new_releases = await service.get_new_releases(region="US", limit=20)

        if not new_releases:
            print("  ❌ Error: Could not fetch new releases")
            raise Exception("Cannot continue without new releases data")

        self._save_core_json("new_releases.json", new_releases)
        print("  ✓ Saved new releases\n")

        # Extract IDs
        movie_id, tv_id = await self._get_test_ids_from_releases()

        # Build test methods list dynamically
        core_methods = []

        if movie_id and tv_id:
            print(f"  ✓ Found movie ID: {movie_id}")
            print(f"  ✓ Found TV ID: {tv_id}\n")

            core_methods = [
                ["get_title_details", {"watchmode_id": movie_id}, "title_details_movie"],
                ["get_title_details", {"watchmode_id": tv_id}, "title_details_tv"],
                [
                    "get_title_streaming_sources",
                    {"watchmode_id": movie_id, "region": "US"},
                    "streaming_sources_movie",
                ],
                [
                    "get_title_streaming_sources",
                    {"watchmode_id": tv_id, "region": "US"},
                    "streaming_sources_tv",
                ],
            ]
        else:
            print("  ⚠️  Warning: Could not find both movie and TV IDs from releases")
            print("  Skipping title details and streaming sources\n")

        # Always add search
        core_methods.append(
            ["search_titles", {"query": SEARCH_QUERY, "types": "movie,tv"}, "search_titles"]
        )

        print(f"Step 2: Processing {len(core_methods)} additional methods\n")

        success_count = 1  # Already saved new_releases
        error_count = 0
        errors = []

        for idx, method_config in enumerate(core_methods, 1):
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(core_methods)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = await method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_core_json(filename, result)

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
        total_methods = len(core_methods) + 1  # +1 for new_releases
        print(f"✓ Successful: {success_count}/{total_methods}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{total_methods}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.core_dir}")
        print("=" * 60 + "\n")

        # Don't raise exception if we got at least some data
        if success_count == 0:
            raise Exception("Failed to generate any core method mocks")

    async def generate_all(self) -> None:
        """Generate basic mock data files."""
        print("\n" + "=" * 60)
        print("Watchmode Mock Data Generator - Basic")
        print("=" * 60 + "\n")

        # Generate a subset of data
        service = WatchmodeService(self.api_key)

        # Get new releases
        print("Fetching new releases...")
        releases = await service.get_new_releases(limit=10)
        self._save_json("mock_new_releases.json", releases or {})

        # Extract IDs from releases
        movie_id, tv_id = await self._get_test_ids_from_releases()

        if movie_id:
            # Get title details
            print(f"Fetching title details for movie ID {movie_id}...")
            movie_details = await service.get_title_details(movie_id)
            self._save_json("mock_title_details_movie.json", movie_details or {})

            # Get streaming sources
            print(f"Fetching streaming sources for movie ID {movie_id}...")
            movie_sources = await service.get_title_streaming_sources(movie_id)
            self._save_json("mock_streaming_sources_movie.json", movie_sources or {})
        else:
            print("  ⚠️  Warning: No movie ID found, skipping movie-related endpoints")

        if tv_id:
            print(f"Fetching title details for TV ID {tv_id}...")
            tv_details = await service.get_title_details(tv_id)
            self._save_json("mock_title_details_tv.json", tv_details or {})
        else:
            print("  ⚠️  Warning: No TV ID found, skipping TV-related endpoints")

        # Search
        print(f"Searching for '{SEARCH_QUERY}'...")
        search_results = await service.search_titles(SEARCH_QUERY, types="movie,tv")
        self._save_json("mock_search_results.json", search_results or {})

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real Watchmode API responses"
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

    # Get Watchmode API key from environment
    api_key = os.getenv("WATCHMODE_API_KEY")
    if not api_key:
        print("ERROR: WATCHMODE_API_KEY environment variable not set")
        print("\nPlease set your Watchmode API key:")
        print("  export WATCHMODE_API_KEY='your_key_here'")
        return 1

    try:
        generator = WatchmodeMockDataGenerator(api_key)

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
