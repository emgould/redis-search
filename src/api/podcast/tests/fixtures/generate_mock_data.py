#!/usr/bin/env python3
"""
Generate mock data from real PodcastIndex API responses.

This utility fetches real data from PodcastIndex API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data
    python generate_mock_data.py --search     # Generate search method results

Requirements:
    - PODCASTINDEX_API_KEY environment variable must be set
    - PODCASTINDEX_API_SECRET environment variable must be set
    - Internet connection to access PodcastIndex API
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

from api.podcast.search import PodcastSearchService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# Test data configuration - IDs for well-known, stable content
TEST_DATA_CONFIG = {
    "podcast": {
        "feed_id": 360084,  # Joe Rogan Experience - stable, well-known podcast
        "name": "The Joe Rogan Experience",
    },
    "search_query": "true crime",
    "trending_lang": "en",
}

# Search service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_SEARCH = [
    [
        "get_trending_podcasts",
        {"max_results": 10, "lang": "en"},
        "trending_podcasts",
    ],
    [
        "search_podcasts",
        {"query": "true crime", "max_results": 20},
        "search_podcasts",
    ],
    [
        "get_podcast_by_id",
        {"feed_id": 360084},
        "podcast_by_id",
    ],
    [
        "get_podcast_episodes",
        {"feed_id": 360084, "max_results": 10},
        "podcast_episodes",
    ],
    [
        "get_podcast_with_latest_episode",
        {"feed_id": 360084},
        "podcast_with_latest_episode",
    ],
]


class PodcastMockDataGenerator:
    """Generate mock data from real PodcastIndex API responses."""

    def __init__(self, api_key: str, api_secret: str):
        """Initialize generator with PodcastIndex API credentials.

        Args:
            api_key: PodcastIndex API key
            api_secret: PodcastIndex API secret
        """
        self.api_key = api_key
        self.api_secret = api_secret

        # Set directories
        # Note: We don't create make_requests/ because the podcastindex library
        # abstracts away raw API calls. We only save processed search results.
        self.search_dir = Path.cwd() / "search"
        self.search_dir.mkdir(exist_ok=True)

        disable_cache()

    def _save_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file in specified directory.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if it's a Pydantic model)
            directory: Target directory (defaults to search_dir)
        """
        if directory is None:
            directory = self.search_dir

        output_path = directory / filename

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_search_methods(self) -> None:
        """Generate mock data by executing PodcastSearchService methods.

        This method calls the actual PodcastSearchService methods defined in TEST_SEARCH
        and saves the results as mock data files in the search directory.
        """
        print("\n" + "=" * 60)
        print("Podcast Search Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_SEARCH)}\n")

        # Set environment variables for Auth to use
        import os
        os.environ["PODCASTINDEX_API_KEY"] = self.api_key
        os.environ["PODCASTINDEX_API_SECRET"] = self.api_secret

        # Create PodcastSearchService instance
        service = PodcastSearchService()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_SEARCH, 1):
            # Unpack the method configuration
            # Format: [method_name, kwargs, output_filename]
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_SEARCH)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = await method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.search_dir)

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
        print(f"✓ Successful: {success_count}/{len(TEST_SEARCH)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_SEARCH)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.search_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} search method mocks")

    async def generate_all(self) -> None:
        """Generate all mock data files."""
        print("\n" + "=" * 60)
        print("Podcast Mock Data Generator")
        print("=" * 60 + "\n")

        await self.generate_search_methods()

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.search_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real PodcastIndex API responses"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all mock data",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Generate mock data by executing search service methods",
    )
    args = parser.parse_args()

    # Get PodcastIndex credentials from environment
    api_key = os.getenv("PODCASTINDEX_API_KEY")
    api_secret = os.getenv("PODCASTINDEX_API_SECRET")

    if not api_key:
        print("ERROR: PODCASTINDEX_API_KEY environment variable not set")
        print("\nPlease set your PodcastIndex API key:")
        print("  export PODCASTINDEX_API_KEY='your_key_here'")
        return 1

    if not api_secret:
        print("ERROR: PODCASTINDEX_API_SECRET environment variable not set")
        print("\nPlease set your PodcastIndex API secret:")
        print("  export PODCASTINDEX_API_SECRET='your_secret_here'")
        return 1

    try:
        generator = PodcastMockDataGenerator(api_key, api_secret)

        if args.search or args.all:
            # Generate mock data by executing search service methods
            await generator.generate_search_methods()
        else:
            # Generate all mock data
            await generator.generate_all()

        return 0
    except Exception as e:
        print(f"\n❌ Error generating mock data: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys

    # Check if "search" is in sys.argv and add if not present
    if "search" not in sys.argv and "--search" not in sys.argv:
        sys.argv.append("--search")

    exit_code = asyncio.run(main())
    exit(exit_code)
