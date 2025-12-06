#!/usr/bin/env python3
"""
Generate mock data from real TMDB API responses.

This utility fetches real data from TMDB API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --all        # Generate all endpoint mock data (make_requests/)
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --person     # Generate person method results (person/)

Requirements:
    - TMDB_READ_TOKEN environment variable must be set
    - Internet connection to access TMDB API
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
# __file__ gives absolute path, go up 5 levels: fixtures -> tests -> tmdb -> services -> python_functions
python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

import aiohttp  # noqa: E402
from dotenv import find_dotenv, load_dotenv  # noqa: E402

from api.tmdb.core import TMDBService  # noqa: E402
from api.tmdb.person import TMDBPersonService  # noqa: E402
from api.tmdb.search import TMDBSearchService  # noqa: E402
from api.tmdb.trending import get_trending_movies, get_trending_tv_shows  # noqa: E402
from contracts.models import MCType  # noqa: E402

python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# TMDB API Configuration
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# Test data configuration - IDs for well-known, stable content
TEST_DATA_CONFIG = {
    "movie": {
        "tmdb_id": 550,  # Fight Club - stable, well-known movie
        "name": "Fight Club",
    },
    "tv": {
        "tmdb_id": 1396,  # Breaking Bad - stable, well-known TV show
        "name": "Breaking Bad",
    },
    "person": {
        "person_id": 287,  # Brad Pitt - stable, well-known actor
        "name": "Brad Pitt",
    },
}
TV_TEST_ID = 1396  # Breaking Bad - stable, well-known TV show
MOVIE_TEST_ID = 550  # Fight Club - stable, well-known movie
PERSON_TEST_ID = 287  # Brad Pitt - stable, well-known actor
MULTI_SEARCH_QUERY = "Breaking Bad"
MULTI_SEARCH_KEYWORD_QUERY = "keyword: space"
TV_SEARCH_QUERY = "Breaking Bad"
MOVIE_SEARCH_QUERY = "Fight Club"
KEYWORD_SEARCH_QUERY = "space"
PERSON_SEARCH_QUERY = "Brad Pitt"
DISCOVER_KEYWORD_QUERY = "851"
MOVIE_KEYWORD_ID = 14964
TV_KEYWORD_ID = 14964

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_CORE = [
    [
        "get_media_details",
        {"tmdb_id": MOVIE_TEST_ID, "media_type": "movie"},
        "tmdb_media_item_movie",
    ],
    ["get_media_details", {"tmdb_id": TV_TEST_ID, "media_type": "tv"}, "tmdb_media_item_tv"],
    [
        "_get_cast_and_crew",
        {"tmdb_id": MOVIE_TEST_ID, "media_type": "movie"},
        "cast_and_crew_movie",
    ],
    ["_get_cast_and_crew", {"tmdb_id": TV_TEST_ID, "media_type": "tv"}, "cast_and_crew_tv"],
    ["_get_videos", {"tmdb_id": MOVIE_TEST_ID, "media_type": "movie"}, "videos_movie"],
    ["_get_videos", {"tmdb_id": TV_TEST_ID, "media_type": "tv"}, "videos_tv"],
    [
        "_get_watch_providers",
        {"tmdb_id": MOVIE_TEST_ID, "media_type": "movie"},
        "watch_providers_movie",
    ],
    ["_get_watch_providers", {"tmdb_id": TV_TEST_ID, "media_type": "tv"}, "watch_providers_tv"],
    ["_get_keywords", {"tmdb_id": MOVIE_TEST_ID, "media_type": "movie"}, "keywords_movie"],
    ["_get_keywords", {"tmdb_id": TV_TEST_ID, "media_type": "tv"}, "keywords_tv"],
]

# Person service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_PERSON = [
    ["get_person_details", {"person_id": PERSON_TEST_ID}, "person_details"],
    [
        "get_person_movie_credits",
        {"person_id": PERSON_TEST_ID, "limit": 50},
        "person_movie_credits",
    ],
    ["get_person_tv_credits", {"person_id": PERSON_TEST_ID, "limit": 50}, "person_tv_credits"],
    [
        "get_person_credits",
        {"person_id": PERSON_TEST_ID, "limit": 50},
        "person_credits",
    ],
    ["search_people", {"query": PERSON_SEARCH_QUERY, "page": 1, "limit": 20}, "search_people"],
]

# Search service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_SEARCH = [
    ["get_trending", {"media_type": "movie", "time_window": "week", "limit": 20}, "trending_movie"],
    ["get_trending", {"media_type": "tv", "time_window": "week", "limit": 20}, "trending_tv"],
    ["get_now_playing", {"limit": 20}, "now_playing"],
    ["get_popular_tv", {"limit": 20}, "popular_tv"],
    ["search_multi", {"query": MULTI_SEARCH_QUERY, "page": 1, "limit": 20}, "search_multi"],
    ["search_tv_shows", {"query": TV_SEARCH_QUERY, "page": 1, "limit": 20}, "search_tv_shows"],
    ["search_movies", {"query": MOVIE_SEARCH_QUERY, "page": 1, "limit": 20}, "search_movies"],
    ["search_keywords", {"query": KEYWORD_SEARCH_QUERY, "page": 1}, "search_keywords"],
    ["search_multi", {"query": MULTI_SEARCH_KEYWORD_QUERY, "page": 1, "limit": 20}, "search_multi"],
    [
        "search_by_keywords",
        {"keyword_ids": str(MOVIE_KEYWORD_ID), "page": 1, "limit": 50},
        "search_by_keywords",
    ],
    [
        "search_by_genre",
        {"genre_ids": "18,80", "page": 1, "limit": 50},
        "search_by_genre",
    ],
]
TEST_ENDPOINTS = [
    # Core Service (core.py)
    [
        "movie/{parameter}",
        MOVIE_TEST_ID,
        "get_media_details",
        "get_media_details_movie",
    ],
    ["tv/{parameter}", TV_TEST_ID, "get_media_details", "get_media_details_tv"],
    [
        "movie/{parameter}/credits",
        MOVIE_TEST_ID,
        "_get_cast_and_crew",
        "get_cast_and_crew_movie",
    ],
    [
        "tv/{parameter}/credits",
        TV_TEST_ID,
        "_get_cast_and_crew",
        "get_cast_and_crew_tv",
    ],
    ["movie/{parameter}/videos", MOVIE_TEST_ID, "_get_videos", "get_videos_movie"],
    ["tv/{parameter}/videos", TV_TEST_ID, "_get_videos", "get_videos_tv"],
    [
        "movie/{parameter}/watch/providers",
        MOVIE_TEST_ID,
        "_get_watch_providers",
        "get_watch_providers_movie",
    ],
    [
        "tv/{parameter}/watch/providers",
        TV_TEST_ID,
        "_get_watch_providers",
        "get_watch_providers_tv",
    ],
    [
        "movie/{parameter}/keywords",
        MOVIE_TEST_ID,
        "_get_keywords",
        "get_keywords_movie",
    ],
    ["tv/{parameter}/keywords", TV_TEST_ID, "_get_keywords", "get_keywords_tv"],
    # Search Service (search.py)
    ["trending/{parameter}/week", "tv", "get_trending", "get_trending_tv"],
    [
        "trending/{parameter}/week?language=en-US",
        "movie",
        "get_trending",
        "get_trending_movie",
    ],
    ["movie/now_playing", None, "get_now_playing", "get_now_playing_movie"],
    ["tv/popular?language=en-US", None, "get_popular_tv", "get_popular_tv"],
    [
        "search/multi?query={parameter}&include_adult=false&language=en-US&page=1",
        MULTI_SEARCH_QUERY,
        "search_multi",
        "search_multi",
    ],
    [
        "search/tv?query={parameter}&include_adult=false&language=en-US&page=1",
        TV_SEARCH_QUERY,
        "search_tv_shows",
        "search_tv_shows",
    ],
    [
        "search/movie?query={parameter}&include_adult=false&language=en-US&page=1",
        MOVIE_SEARCH_QUERY,
        "search_movies",
        "search_movies",
    ],
    [
        "search/keyword?query={parameter}&page=1",
        KEYWORD_SEARCH_QUERY,
        "search_keywords",
        "search_keywords",
    ],
    [
        "genre/movie/list?language=en-US",
        None,
        "search_by_genre",
        "get_genres_movie",
    ],
    [
        "genre/tv/list?language=en-US",
        None,
        "search_by_genre",
        "get_genres_tv",
    ],
    [
        "discover/movie?include_adult=false&include_video=false&language=en-US&page=1&sort_by=popularity.desc&with_keywords={parameter}",
        MOVIE_KEYWORD_ID,
        "search_by_keywords",
        "search_by_keywords_movie",
    ],
    [
        "discover/tv?include_adult=false&include_video=false&language=en-US&page=1&sort_by=popularity.desc&with_keywords={parameter}",
        TV_KEYWORD_ID,
        "search_by_keywords",
        "search_by_keywords_tv",
    ],
    [
        "discover/movie?language=en-US&page=1&sort_by=popularity.desc&with_genres=18,80",
        None,
        "search_by_genre",
        "search_by_genre_movie",
    ],
    [
        "discover/tv?language=en-US&page=1&sort_by=popularity.desc&with_genres=18,80",
        None,
        "search_by_genre",
        "search_by_genre_tv",
    ],
    # Person Service (person.py)
    ["person/{parameter}", PERSON_TEST_ID, "get_person_details", "get_person_details"],
    [
        "person/{parameter}/movie_credits",
        PERSON_TEST_ID,
        "get_person_movie_credits",
        "get_person_movie_credits",
    ],
    [
        "person/{parameter}/tv_credits",
        PERSON_TEST_ID,
        "get_person_tv_credits",
        "get_person_tv_credits",
    ],
    [
        "search/person?query={parameter}&include_adult=false&language=en-US&page=1",
        PERSON_SEARCH_QUERY,
        "search_people",
        "search_people",
    ],
]


class TMDBMockDataGenerator:
    """Generate mock data from real TMDB API responses."""

    def __init__(self, tmdb_token: str):
        """Initialize generator with TMDB API token.

        Args:
            tmdb_token: TMDB API bearer token
        """
        self.tmdb_token = tmdb_token
        self.headers = {
            "Authorization": f"Bearer {tmdb_token}",
            "Content-Type": "application/json",
        }
        # Set make_requests_dir to the current working directory (the directory for which this script exists)
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(exist_ok=True)
        self.code_dir = Path.cwd() / "fixtures/core"
        self.code_dir.mkdir(exist_ok=True)
        self.person_dir = Path.cwd() / "fixtures/person"
        self.person_dir.mkdir(exist_ok=True)
        self.search_dir = Path.cwd() / "fixtures/search"
        self.search_dir.mkdir(exist_ok=True)

        disable_cache()

    async def _make_request(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make async HTTP request to TMDB API.

        Args:
            endpoint: API endpoint (e.g., 'movie/550')
            params: Optional query parameters

        Returns:
            JSON response dict

        Raises:
            Exception: If request fails
        """
        url = f"{TMDB_BASE_URL}/{endpoint}"
        timeout = aiohttp.ClientTimeout(total=30)

        async with (
            aiohttp.ClientSession() as session,
            session.get(url, headers=self.headers, params=params, timeout=timeout) as response,
        ):
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"TMDB API returned status {response.status}: {error_text}")

            return await response.json()

    async def fetch_movie_data(self) -> dict[str, Any]:
        """Fetch comprehensive movie data.

        Returns:
            Movie data with all details
        """
        movie_id = TEST_DATA_CONFIG["movie"]["id"]
        print(f"Fetching movie data for ID {movie_id} ({TEST_DATA_CONFIG['movie']['name']})...")

        # Fetch movie details with append_to_response for efficiency
        data = await self._make_request(
            f"movie/{movie_id}",
            params={"append_to_response": "credits,videos,watch/providers,keywords"},
        )

        print(f"✓ Fetched movie: {data.get('title')}")
        return data

    async def fetch_tv_data(self) -> dict[str, Any]:
        """Fetch comprehensive TV show data.

        Returns:
            TV show data with all details
        """
        tv_id = TEST_DATA_CONFIG["tv"]["id"]
        print(f"Fetching TV data for ID {tv_id} ({TEST_DATA_CONFIG['tv']['name']})...")

        # Fetch TV details with append_to_response
        data = await self._make_request(
            f"tv/{tv_id}",
            params={"append_to_response": "credits,videos,watch/providers,keywords"},
        )

        print(f"✓ Fetched TV show: {data.get('name')}")
        return data

    async def fetch_person_data(self) -> dict[str, Any]:
        """Fetch person data.

        Returns:
            Person data with details
        """
        person_id = TEST_DATA_CONFIG["person"]["id"]
        print(f"Fetching person data for ID {person_id} ({TEST_DATA_CONFIG['person']['name']})...")

        data = await self._make_request(f"person/{person_id}")

        print(f"✓ Fetched person: {data.get('name')}")
        return data

    async def fetch_search_results(self) -> dict[str, Any]:
        """Fetch search results.

        Returns:
            Multi-search results
        """
        query = TEST_DATA_CONFIG["tv"]["name"]
        print(f"Fetching search results for query: {query}...")

        data = await self._make_request("search/multi", params={"query": query, "page": 1})

        print(f"✓ Fetched {len(data.get('results', []))} search results")
        return data

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

    def _extract_cast_data(self, full_data: dict[str, Any]) -> dict[str, Any]:
        """Extract cast/credits data from full response.

        Args:
            full_data: Full movie/TV response with credits

        Returns:
            Just the credits portion with director and main cast
        """
        crew = full_data.get("credits", {}).get("crew", [])

        # Find director (important for tests)
        director = next((c for c in crew if c.get("job") == "Director"), None)

        # Include director plus a few other crew members
        crew_to_include = []
        if director:
            crew_to_include.append(director)
        # Add a couple more crew members (non-directors)
        crew_to_include.extend([c for c in crew if c.get("job") != "Director"][:2])

        return {
            "cast": full_data.get("credits", {}).get("cast", [])[:3],  # First 3 cast members
            "crew": crew_to_include,
        }

    def _extract_videos_data(self, full_data: dict[str, Any]) -> dict[str, Any]:
        """Extract videos data from full response.

        Args:
            full_data: Full movie/TV response with videos

        Returns:
            Just the videos portion
        """
        videos = full_data.get("videos", {}).get("results", [])
        # Get first trailer
        trailer = next(
            (v for v in videos if v.get("type") == "Trailer"), videos[0] if videos else None
        )
        return {"results": [trailer] if trailer else []}

    def _extract_watch_providers_data(self, full_data: dict[str, Any]) -> dict[str, Any]:
        """Extract watch providers data from full response.

        Args:
            full_data: Full movie/TV response with watch providers

        Returns:
            Just the watch providers portion
        """
        return {"results": full_data.get("watch/providers", {}).get("results", {})}

    def _extract_keywords_data(self, full_data: dict[str, Any]) -> dict[str, Any]:
        """Extract keywords data from full response.

        Args:
            full_data: Full movie/TV response with keywords

        Returns:
            Just the keywords portion
        """
        keywords = full_data.get("keywords", {})
        # Handle both movie (keywords) and TV (results) formats
        keyword_list = keywords.get("keywords", keywords.get("results", []))
        return {"keywords": keyword_list[:3]}  # First 3 keywords

    def _generate_filename(self, mock_name: str) -> str:
        """Generate appropriate filename for mock data.

        Args:
            mock_name: Mock name from TEST_ENDPOINTS

        Returns:
            Filename for the mock data
        """
        # Add mock_ prefix if not already present
        if not mock_name.startswith("mock_"):
            filename = f"mock_{mock_name}"
        else:
            filename = mock_name

        # Ensure it ends with .json
        if not filename.endswith(".json"):
            filename += ".json"

        return filename

    async def generate_all_endpoints(self) -> None:
        """Generate mock data for all test endpoints.

        This method executes all endpoints defined in TEST_ENDPOINTS using the
        test data provided in each endpoint configuration and saves the responses
        as mock data files.
        """
        print("\n" + "=" * 60)
        print("TMDB Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        print(f"Total endpoints to process: {len(TEST_ENDPOINTS)}\n")

        success_count = 0
        error_count = 0
        errors = []

        for idx, endpoint_config in enumerate(TEST_ENDPOINTS, 1):
            # Unpack the endpoint configuration
            # Format: [endpoint_template, test_data, function_name, mock_name]
            endpoint_template = endpoint_config[0]
            test_data = endpoint_config[1]
            function_name = endpoint_config[2]
            mock_name = endpoint_config[3]

            try:
                print(f"[{idx}/{len(TEST_ENDPOINTS)}] Processing: {function_name}")
                print(f"  Endpoint template: {endpoint_template}")
                endpoint = (
                    endpoint_template.replace(
                        "{parameter}", aiohttp.helpers.quote(str(test_data), safe="")
                    )
                    if test_data is not None
                    else endpoint_template
                )
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
            raise Exception(f"❌ Failed: {error_count}/{len(TEST_ENDPOINTS)}")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

    def _save_code_json(self, filename: str, data: Any, directory: Path | None = None) -> None:
        """Save data to JSON file in specified directory.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if it's a Pydantic model)
            directory: Target directory (defaults to code_dir)
        """
        if directory is None:
            directory = self.code_dir

        output_path = directory / filename

        # Convert Pydantic models to dict

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_core_methods(self) -> None:
        """Generate mock data by executing TMDBService core methods.

        This method calls the actual TMDBService methods defined in TEST_CORE
        and saves the results as mock data files in the code directory.
        """
        print("\n" + "=" * 60)
        print("TMDB Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_CORE)}\n")

        # Set token in environment for Auth base class
        os.environ["TMDB_READ_TOKEN"] = self.tmdb_token

        # Create TMDBService instance (no longer takes token parameter - uses Auth)
        service = TMDBService()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_CORE, 1):
            # Unpack the method configuration
            # Format: [method_name, kwargs, output_filename]
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_CORE)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Convert media_type string to MCType enum if present
                if "media_type" in kwargs:
                    media_type_str = kwargs["media_type"]
                    if media_type_str == "movie":
                        kwargs["media_type"] = MCType.MOVIE
                    elif media_type_str == "tv":
                        kwargs["media_type"] = MCType.TV_SERIES

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = await method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_code_json(filename, result)

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

        print(f"\n✓ Files saved to: {self.code_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} core method mocks")

    async def generate_person_methods(self) -> None:
        """Generate mock data by executing TMDBPersonService methods.

        This method calls the actual TMDBPersonService methods defined in TEST_PERSON
        and saves the results as mock data files in the person directory.
        """
        print("\n" + "=" * 60)
        print("TMDB Person Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_PERSON)}\n")

        # Set token in environment for Auth base class
        os.environ["TMDB_READ_TOKEN"] = self.tmdb_token

        # Create TMDBPersonService instance (no longer takes token parameter - uses Auth)
        service = TMDBPersonService()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_PERSON, 1):
            # Unpack the method configuration
            # Format: [method_name, kwargs, output_filename]
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_PERSON)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = await method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_code_json(filename, result, directory=self.person_dir)

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
        print(f"✓ Successful: {success_count}/{len(TEST_PERSON)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_PERSON)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.person_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} person method mocks")

    async def generate_search_methods(self) -> None:
        """Generate mock data by executing TMDBSearchService methods.

        This method calls the actual TMDBSearchService methods defined in TEST_SEARCH
        and saves the results as mock data files in the search directory.
        """
        print("\n" + "=" * 60)
        print("TMDB Search Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_SEARCH)}\n")

        # Set token in environment for Auth base class
        os.environ["TMDB_READ_TOKEN"] = self.tmdb_token

        # Create TMDBSearchService instance (no longer takes token parameter - uses Auth)
        service = TMDBSearchService()

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

                # Handle special cases that don't exist on TMDBSearchService
                if method_name == "get_trending":
                    media_type = kwargs.get("media_type", "tv")
                    limit = kwargs.get("limit", 20)
                    if media_type == "movie":
                        result = await get_trending_movies(limit=limit)
                    else:
                        result = await get_trending_tv_shows(limit=limit)
                else:
                    # Get the method from the service
                    method = getattr(service, method_name)
                    # Call the method with kwargs
                    result = await method(**kwargs)

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_code_json(filename, result, directory=self.search_dir)

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
        print("TMDB Mock Data Generator")
        print("=" * 60 + "\n")

        # Fetch all data
        movie_data = await self.fetch_movie_data()
        tv_data = await self.fetch_tv_data()
        person_data = await self.fetch_person_data()
        search_data = await self.fetch_search_results()

        print("\nSaving mock data files...")

        # Save full data
        self._save_json("mock_movie_data.json", movie_data)
        self._save_json("mock_tv_data.json", tv_data)
        self._save_json("mock_person_data.json", person_data)
        self._save_json("mock_search_results.json", search_data)

        # Save extracted data
        self._save_json("mock_cast_data.json", self._extract_cast_data(movie_data))
        self._save_json("mock_videos_data.json", self._extract_videos_data(movie_data))
        self._save_json(
            "mock_watch_providers_data.json", self._extract_watch_providers_data(movie_data)
        )
        self._save_json("mock_keywords_data.json", self._extract_keywords_data(movie_data))

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate mock data from real TMDB API responses")
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
        "--person",
        action="store_true",
        help="Generate mock data by executing person service methods",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Generate mock data by executing search service methods",
    )
    args = parser.parse_args()

    # Get TMDB token from environment
    tmdb_token = os.getenv("TMDB_READ_TOKEN")
    if not tmdb_token:
        print("ERROR: TMDB_READ_TOKEN environment variable not set")
        print("\nPlease set your TMDB API token:")
        print("  export TMDB_READ_TOKEN='your_token_here'")
        return 1

    try:
        generator = TMDBMockDataGenerator(tmdb_token)

        if args.person:
            # Generate mock data by executing person service methods
            await generator.generate_person_methods()
        elif args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.endpoints:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        elif args.search:
            # Generate mock data by executing search service methods
            await generator.generate_search_methods()
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
    import sys

    # Check if "search" is in sys.argv and add if not present (helpful for interactive scripts)
    if "search" not in sys.argv and "--search" not in sys.argv:
        sys.argv.append("--search")

    exit_code = asyncio.run(main())
    exit(exit_code)
