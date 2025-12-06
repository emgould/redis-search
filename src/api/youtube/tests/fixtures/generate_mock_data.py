#!/usr/bin/env python3
"""
Generate mock data from real YouTube Data API responses.

This utility fetches real data from YouTube Data API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data (make_requests/)
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --models     # Generate model validation data (models/)

Requirements:
    - YOUTUBE_API_KEY environment variable must be set
    - Internet connection to access YouTube Data API
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
# __file__ gives absolute path, go up 4 levels: fixtures -> tests -> youtube -> services -> python_functions
python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

from dotenv import find_dotenv, load_dotenv  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from api.youtube.core import YouTubeService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# Test data configuration - well-known, stable content
TEST_DATA_CONFIG = {
    "search_query": "Python programming",
    "trending_region": "US",
    "trending_language": "en",
    "max_results": 5,
    "category_id": "28",  # Science & Technology
}

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_CORE = [
    [
        "_process_video_item",
        {
            "video_data": {
                "id": "test_video_id",
                "snippet": {
                    "title": "Test Video",
                    "description": "Test Description",
                    "channelTitle": "Test Channel",
                    "channelId": "test_channel_id",
                    "publishedAt": "2024-01-01T00:00:00Z",
                    "thumbnails": {
                        "high": {"url": "https://example.com/thumbnail.jpg"},
                    },
                    "tags": ["test", "video"],
                    "categoryId": "28",
                    "defaultLanguage": "en",
                },
                "statistics": {
                    "viewCount": "1000",
                    "likeCount": "100",
                    "commentCount": "10",
                },
                "contentDetails": {
                    "duration": "PT5M30S",
                },
            }
        },
        "process_video_item",
    ],
]


class YouTubeMockDataGenerator:
    """Generate mock data from real YouTube Data API responses."""

    def __init__(self, api_key: str):
        """Initialize generator with API credentials.

        Args:
            api_key: YouTube Data API key
        """
        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)

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

        This method executes all YouTube API endpoints and saves the responses
        as mock data files.
        """
        print("\n" + "=" * 60)
        print("YouTube Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        success_count = 0
        error_count = 0
        errors = []

        # Test 1: Get trending videos
        try:
            print("[1/4] Fetching trending videos...")
            request = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode=TEST_DATA_CONFIG["trending_region"],
                hl=TEST_DATA_CONFIG["trending_language"],
                maxResults=TEST_DATA_CONFIG["max_results"],
            )
            response = request.execute()
            self._save_json("youtube_trending_videos.json", response)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("trending_videos", str(e)))

        # Test 2: Search videos
        try:
            print("[2/4] Searching videos...")
            search_request = self.youtube.search().list(
                part="snippet",
                q=TEST_DATA_CONFIG["search_query"],
                type="video",
                maxResults=TEST_DATA_CONFIG["max_results"],
                order="relevance",
            )
            search_response = search_request.execute()
            self._save_json("youtube_search_videos.json", search_response)

            # Get detailed video info
            video_ids = []
            for item in search_response.get("items", []):
                item_id = item.get("id", {})
                if isinstance(item_id, dict):
                    video_id = item_id.get("videoId")
                else:
                    video_id = item_id
                if video_id:
                    video_ids.append(video_id)

            if video_ids:
                videos_request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails", id=",".join(video_ids)
                )
                videos_response = videos_request.execute()
                self._save_json("youtube_video_details.json", videos_response)

            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("search_videos", str(e)))

        # Test 3: Get video categories
        try:
            print("[3/4] Fetching video categories...")
            request = self.youtube.videoCategories().list(
                part="snippet",
                regionCode=TEST_DATA_CONFIG["trending_region"],
                hl=TEST_DATA_CONFIG["trending_language"],
            )
            response = request.execute()
            self._save_json("youtube_categories.json", response)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("video_categories", str(e)))

        # Test 4: Get trending with category filter
        try:
            print("[4/4] Fetching trending videos with category filter...")
            request = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode=TEST_DATA_CONFIG["trending_region"],
                hl=TEST_DATA_CONFIG["trending_language"],
                videoCategoryId=TEST_DATA_CONFIG["category_id"],
                maxResults=TEST_DATA_CONFIG["max_results"],
            )
            response = request.execute()
            self._save_json("youtube_trending_by_category.json", response)
            success_count += 1
            print("  ✓ Success\n")
        except Exception as e:
            error_count += 1
            print(f"  ❌ Error: {str(e)}\n")
            errors.append(("trending_by_category", str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/4")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/4")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} endpoint mocks")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing YouTubeService core methods.

        This method calls the actual YouTubeService methods defined in TEST_CORE
        and saves the results as mock data files in the core directory.
        """
        print("\n" + "=" * 60)
        print("YouTube Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_CORE)}\n")

        # Create YouTubeService instance
        service = YouTubeService(self.api_key)

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
        print("YouTube Models Mock Data Generator")
        print("=" * 60 + "\n")

        # Fetch real data to use for model validation
        print("Fetching real data for model validation...")

        # Get trending video
        trending_request = self.youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=TEST_DATA_CONFIG["trending_region"],
            maxResults=1,
        )
        trending_response = trending_request.execute()
        trending_video = trending_response.get("items", [{}])[0]

        # Get video categories
        categories_request = self.youtube.videoCategories().list(
            part="snippet",
            regionCode=TEST_DATA_CONFIG["trending_region"],
        )
        categories_response = categories_request.execute()
        category = categories_response.get("items", [{}])[0]

        print("\nSaving model validation data...")

        # Save all model data
        self._save_json("youtube_video.json", trending_video, directory=self.models_dir)
        self._save_json("youtube_category.json", category, directory=self.models_dir)
        self._save_json(
            "youtube_trending_response.json", trending_response, directory=self.models_dir
        )
        self._save_json(
            "youtube_categories_response.json", categories_response, directory=self.models_dir
        )

        print("\n" + "=" * 60)
        print("✓ Model validation data generation complete!")
        print(f"✓ Files saved to: {self.models_dir}")
        print("=" * 60 + "\n")

    async def generate_all(self) -> None:
        """Generate all mock data files (basic set)."""
        print("\n" + "=" * 60)
        print("YouTube Mock Data Generator - Basic Set")
        print("=" * 60 + "\n")

        # Fetch basic data
        print("Fetching basic data...")

        # Get trending videos
        trending_request = self.youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=TEST_DATA_CONFIG["trending_region"],
            hl=TEST_DATA_CONFIG["trending_language"],
            maxResults=TEST_DATA_CONFIG["max_results"],
        )
        trending_response = trending_request.execute()

        # Search videos
        search_request = self.youtube.search().list(
            part="snippet",
            q=TEST_DATA_CONFIG["search_query"],
            type="video",
            maxResults=TEST_DATA_CONFIG["max_results"],
        )
        search_response = search_request.execute()

        # Get video categories
        categories_request = self.youtube.videoCategories().list(
            part="snippet",
            regionCode=TEST_DATA_CONFIG["trending_region"],
        )
        categories_response = categories_request.execute()

        print("\nSaving mock data files...")

        # Save all data
        self._save_json("mock_trending_videos.json", trending_response)
        self._save_json("mock_search_videos.json", search_response)
        self._save_json("mock_categories.json", categories_response)

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real YouTube Data API responses"
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
    parser.add_argument(
        "--models",
        action="store_true",
        help="Generate mock data for model validation",
    )
    args = parser.parse_args()

    # Get API credentials from environment
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")

    if not youtube_api_key:
        print("ERROR: Missing required environment variable:")
        print("  - YOUTUBE_API_KEY")
        print("\nPlease set the required environment variable:")
        print("  export YOUTUBE_API_KEY='your_key_here'")
        return 1

    try:
        generator = YouTubeMockDataGenerator(youtube_api_key)

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
