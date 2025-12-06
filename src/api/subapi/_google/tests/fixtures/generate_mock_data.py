#!/usr/bin/env python3
"""
Generate mock data from real Google Books API responses.

This utility fetches real data from Google Books API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data (make_requests/)
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --search     # Generate search method results (search/)

Requirements:
    - GOOGLE_BOOK_API_KEY environment variable (optional but recommended)
    - Internet connection to access Google Books API
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

from api.subapi._google.core import GoogleBooksService  # noqa: E402
from api.subapi._google.search import GoogleBooksSearchService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# Google Books API Configuration
GOOGLE_BOOKS_BASE_URL = "https://www.googleapis.com/books/v1"

# Test data configuration - Well-known, stable books
TEST_DATA_CONFIG = {
    "book_query": "Harry Potter and the Philosopher's Stone",
    "author_query": "J.K. Rowling",
    "isbn": "9780439708180",  # Harry Potter and the Sorcerer's Stone
    "volume_id": "wrOQLV6xB-wC",  # Harry Potter and the Sorcerer's Stone
    "general_query": "python programming",
}

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_CORE = [
    [
        "get_volume_by_id",
        {"volume_id": TEST_DATA_CONFIG["volume_id"]},
        "volume_by_id",
    ],
    [
        "get_volume_by_isbn",
        {"isbn": TEST_DATA_CONFIG["isbn"]},
        "volume_by_isbn",
    ],
]

# Search service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_SEARCH = [
    [
        "search_books",
        {"query": TEST_DATA_CONFIG["book_query"], "max_results": 10},
        "search_books_by_title",
    ],
    [
        "search_books",
        {"query": TEST_DATA_CONFIG["general_query"], "max_results": 10},
        "search_books_general",
    ],
    [
        "search_by_isbn",
        {"isbn": TEST_DATA_CONFIG["isbn"]},
        "search_by_isbn",
    ],
    [
        "search_by_title_and_author",
        {
            "title": TEST_DATA_CONFIG["book_query"],
            "author": TEST_DATA_CONFIG["author_query"],
            "max_results": 10,
        },
        "search_by_title_and_author",
    ],
    [
        "search_direct",
        {"query": TEST_DATA_CONFIG["book_query"], "max_results": 5},
        "search_direct",
    ],
]

# API endpoints to test directly
# Format: [endpoint_template, test_data, function_name, mock_name]
TEST_ENDPOINTS = [
    # Core Service (core.py)
    [
        "volumes/{parameter}",
        TEST_DATA_CONFIG["volume_id"],
        "get_volume_by_id",
        "get_volume_by_id",
    ],
    [
        "volumes?q=isbn:{parameter}",
        TEST_DATA_CONFIG["isbn"],
        "get_volume_by_isbn",
        "get_volume_by_isbn",
    ],
    # Search Service (search.py)
    [
        "volumes?q={parameter}&maxResults=10&orderBy=relevance&printType=books&projection=full",
        TEST_DATA_CONFIG["book_query"],
        "search_books",
        "search_books_by_title",
    ],
    [
        "volumes?q={parameter}&maxResults=10&orderBy=relevance&printType=books&projection=full",
        TEST_DATA_CONFIG["general_query"],
        "search_books",
        "search_books_general",
    ],
    [
        'volumes?q=intitle:"{parameter1}"+inauthor:"{parameter2}"&maxResults=10&orderBy=relevance&printType=books&projection=full',
        (TEST_DATA_CONFIG["book_query"], TEST_DATA_CONFIG["author_query"]),
        "search_by_title_and_author",
        "search_by_title_and_author",
    ],
]


class GoogleBooksMockDataGenerator:
    """Generate mock data from real Google Books API responses."""

    def __init__(self, api_key: str | None = None):
        """Initialize generator with Google Books API key.

        Args:
            api_key: Google Books API key (optional but recommended)
        """
        self.api_key = api_key
        self.headers = {"Content-Type": "application/json"}

        # Set up directories
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)
        self.core_dir = Path.cwd() / "fixtures/core"
        self.core_dir.mkdir(parents=True, exist_ok=True)
        self.search_dir = Path.cwd() / "fixtures/search"
        self.search_dir.mkdir(parents=True, exist_ok=True)

        disable_cache()

    async def _make_request(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make async HTTP request to Google Books API.

        Args:
            endpoint: API endpoint (e.g., 'volumes/wrOQLV6xB-wC')
            params: Optional query parameters

        Returns:
            JSON response dict

        Raises:
            Exception: If request fails
        """
        url = f"{GOOGLE_BOOKS_BASE_URL}/{endpoint}"

        # Add API key if available
        if self.api_key:
            if params is None:
                params = {}
            params["key"] = self.api_key

        timeout = aiohttp.ClientTimeout(total=30)

        async with (
            aiohttp.ClientSession() as session,
            session.get(url, headers=self.headers, params=params, timeout=timeout) as response,
        ):
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Google Books API returned status {response.status}: {error_text}")

            return await response.json()

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

        This method executes all endpoints defined in TEST_ENDPOINTS using the
        test data provided in each endpoint configuration and saves the responses
        as mock data files.
        """
        print("\n" + "=" * 60)
        print("Google Books Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        print(f"Total endpoints to process: {len(TEST_ENDPOINTS)}\n")

        success_count = 0
        error_count = 0
        errors = []

        for idx, endpoint_config in enumerate(TEST_ENDPOINTS, 1):
            # Unpack the endpoint configuration
            endpoint_template = endpoint_config[0]
            test_data = endpoint_config[1]
            function_name = endpoint_config[2]
            mock_name = endpoint_config[3]

            try:
                print(f"[{idx}/{len(TEST_ENDPOINTS)}] Processing: {function_name}")
                print(f"  Endpoint template: {endpoint_template}")

                # Handle multiple parameters
                if isinstance(test_data, tuple):
                    endpoint = endpoint_template.replace("{parameter1}", test_data[0]).replace(
                        "{parameter2}", test_data[1]
                    )
                elif test_data is not None:
                    endpoint = endpoint_template.replace("{parameter}", str(test_data))
                else:
                    endpoint = endpoint_template

                print(f"  Final endpoint: {endpoint}")

                # Make the request
                data = await self._make_request(endpoint, None)

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
        print(f"✓ Successful: {success_count}/{len(TEST_ENDPOINTS)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_ENDPOINTS)}")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} endpoint mocks")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing GoogleBooksService core methods.

        This method calls the actual GoogleBooksService methods defined in TEST_CORE
        and saves the results as mock data files in the core directory.
        """
        print("\n" + "=" * 60)
        print("Google Books Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_CORE)}\n")

        # Create GoogleBooksService instance
        service = GoogleBooksService(api_key=self.api_key)

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
                result, error = await method(**kwargs)

                if error:
                    raise Exception(f"Method returned error: {result}")

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

    async def generate_search_methods(self) -> None:
        """Generate mock data by executing GoogleBooksSearchService methods.

        This method calls the actual GoogleBooksSearchService methods defined in TEST_SEARCH
        and saves the results as mock data files in the search directory.
        """
        print("\n" + "=" * 60)
        print("Google Books Search Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_SEARCH)}\n")

        # Create GoogleBooksSearchService instance
        service = GoogleBooksSearchService(api_key=self.api_key)

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_SEARCH, 1):
            # Unpack the method configuration
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

                # Handle tuple returns (result, error)
                if isinstance(result, tuple):
                    result, error = result
                    if error:
                        raise Exception(f"Method returned error: {result}")

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
        print("Google Books Mock Data Generator")
        print("=" * 60 + "\n")

        # Generate all types
        await self.generate_all_endpoints()
        await self.generate_core_methods()
        await self.generate_search_methods()

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Endpoint files saved to: {self.make_requests_dir}")
        print(f"✓ Core method files saved to: {self.core_dir}")
        print(f"✓ Search method files saved to: {self.search_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real Google Books API responses"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all mock data (endpoints, core, search)",
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
        "--search",
        action="store_true",
        help="Generate mock data by executing search service methods",
    )
    args = parser.parse_args()

    # Get API key from environment (optional)
    api_key = os.getenv("GOOGLE_BOOK_API_KEY")
    if not api_key:
        print("WARNING: GOOGLE_BOOK_API_KEY environment variable not set")
        print("API requests may be rate limited without an API key")
        print("\nTo set API key:")
        print("  export GOOGLE_BOOK_API_KEY='your_key_here'")
        print("\nContinuing without API key...\n")

    try:
        generator = GoogleBooksMockDataGenerator(api_key)

        if args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.endpoints:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        elif args.search:
            # Generate mock data by executing search service methods
            await generator.generate_search_methods()
        elif args.all:
            # Generate all mock data
            await generator.generate_all()
        else:
            # Default: generate search methods (most commonly used)
            await generator.generate_search_methods()

        return 0
    except Exception as e:
        print(f"\n❌ Error generating mock data: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
