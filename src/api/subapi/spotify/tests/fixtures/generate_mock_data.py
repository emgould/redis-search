#!/usr/bin/env python3
"""
Generate mock data from real Spotify API responses.

This utility fetches real data from Spotify API and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate basic mock data
    python generate_mock_data.py --search      # Generate search method results (search/)
    python generate_mock_data.py --models     # Generate model validation data (models/)
    python generate_mock_data.py --endpoints  # Generate API endpoint responses (make_requests/)

Requirements:
    - SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables must be set
    - Internet connection to access APIs
"""

import argparse
import asyncio
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any

# Set environment to test mode FIRST to disable caching
os.environ["ENVIRONMENT"] = "test"

# Add python_functions directory to path to import services
# __file__ gives absolute path, go up 4 levels: fixtures -> tests -> spotify -> python_functions
python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

import aiohttp  # noqa: E402
from dotenv import find_dotenv, load_dotenv  # noqa: E402

from api.subapi.spotify.search import SpotifySearchService  # noqa: E402
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

dotenv_path = find_dotenv(usecwd=True)
if not dotenv_path:
    print("Warning: .env file not found in expected locations")
else:
    load_dotenv(dotenv_path)

# API Configuration
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_BASE_URL = "https://api.spotify.com/v1"

# Test data configuration - well-known, stable content
TEST_DATA_CONFIG = {
    "album": {
        "name": "The Dark Side of the Moon",
        "artist": "Pink Floyd",
    },
    "artist": {
        "name": "Pink Floyd",
        "spotify_id": "0k17h0D3J5VfsdmQ1iZtE9",
    },
    "genre": "rock",
    "keyword": "progressive",
    "track": {
        "name": "wish you were here",
        "artist": "pink floyd",
    },
}

# Search service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_SEARCH = [
    ["search_albums", {"query": "Dark Side of the Moon", "limit": 20}, "search_albums"],
    ["search_artists", {"query": "Pink Floyd", "limit": 20}, "search_artists"],
    ["search_by_genre", {"genre": "rock", "limit": 20}, "search_by_genre"],
    ["search_by_keyword", {"keyword": "progressive", "limit": 20}, "search_by_keyword"],
]

# API endpoints to generate mock data for
# Format: [endpoint_template, params, function_name, mock_name]
TEST_ENDPOINTS = [
    # Spotify API endpoints
    [
        "search?q=album:{album}+artist:{artist}&type=album&limit=20",
        None,
        "search_albums",
        "spotify_album_search",
    ],
    [
        "search?q={artist}&type=artist&limit=20",
        None,
        "search_artists",
        "spotify_artist_search",
    ],
    ["search?q={genre}&type=artist&limit=20", None, "search_by_genre", "spotify_genre_search"],
    [
        "search?q={keyword}&type=artist,album,playlist&limit=20",
        None,
        "search_by_keyword",
        "spotify_keyword_search",
    ],
    [
        "search",
        {"q": "track:{track_name} artist:{track_artist}", "type": "track", "limit": "20"},
        "search_albums(type=track)",
        "mock_spotify_track_search",
    ],
    ["artists/{artist_id}/top-tracks?market=US", None, "get_top_track()", "spotify_top_tracks"],
]


class SpotifyMockDataGenerator:
    """Generate mock data from real Spotify API responses."""

    def __init__(self, spotify_client_id: str, spotify_client_secret: str):
        """Initialize generator with API credentials.

        Args:
            spotify_client_id: Spotify client ID
            spotify_client_secret: Spotify client secret
        """
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.spotify_token = None

        # Set up output directories
        self.make_requests_dir = Path.cwd() / "fixtures/make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)
        self.search_dir = Path.cwd() / "fixtures/search"
        self.search_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir = Path.cwd() / "fixtures/models"
        self.models_dir.mkdir(parents=True, exist_ok=True)

        disable_cache()

    async def _get_spotify_token(self) -> str:
        """Get Spotify access token.

        Returns:
            Access token string

        Raises:
            Exception: If token request fails
        """
        if self.spotify_token:
            return self.spotify_token

        # Encode credentials
        credentials = f"{self.spotify_client_id}:{self.spotify_client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"grant_type": "client_credentials"}

        async with (
            aiohttp.ClientSession() as session,
            session.post(
                SPOTIFY_TOKEN_URL,
                headers=headers,
                data=data,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response,
        ):
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Spotify token request failed: {error_text}")

            result = await response.json()
            self.spotify_token = result["access_token"]
            return self.spotify_token

    async def _spotify_request(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make request to Spotify API.

        Args:
            endpoint: API endpoint
            params: Optional query parameters

        Returns:
            JSON response

        Raises:
            Exception: If request fails
        """
        token = await self._get_spotify_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{SPOTIFY_BASE_URL}/{endpoint}" if not endpoint.startswith("http") else endpoint

        async with (
            aiohttp.ClientSession() as session,
            session.get(
                url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as response,
        ):
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Spotify API error: {error_text}")

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
        print("Spotify Mock Data Generator - All Endpoints")
        print("=" * 60 + "\n")

        print(f"Total endpoints to process: {len(TEST_ENDPOINTS)}\n")

        success_count = 0
        error_count = 0
        errors = []

        for idx, endpoint_config in enumerate(TEST_ENDPOINTS, 1):
            # Unpack the endpoint configuration
            endpoint_template = endpoint_config[0]
            params = endpoint_config[1]
            function_name = endpoint_config[2]
            mock_name = endpoint_config[3]

            try:
                print(f"[{idx}/{len(TEST_ENDPOINTS)}] Processing: {function_name}")
                print(f"  Endpoint template: {endpoint_template}")

                # Replace placeholders in endpoint and params
                if params:
                    processed_params = {}
                    for key, value in params.items():
                        if isinstance(value, str):
                            value = value.replace("{artist}", TEST_DATA_CONFIG["artist"]["name"])
                            value = value.replace("{album}", TEST_DATA_CONFIG["album"]["name"])
                            value = value.replace("{genre}", TEST_DATA_CONFIG["genre"])
                            value = value.replace("{keyword}", TEST_DATA_CONFIG["keyword"])
                            value = value.replace(
                                "{artist_id}", TEST_DATA_CONFIG["artist"]["spotify_id"]
                            )
                            value = value.replace("{track_name}", TEST_DATA_CONFIG["track"]["name"])
                            value = value.replace(
                                "{track_artist}", TEST_DATA_CONFIG["track"]["artist"]
                            )
                        processed_params[key] = value
                else:
                    processed_params = None

                # Replace placeholders in endpoint
                endpoint = endpoint_template.replace("{artist}", TEST_DATA_CONFIG["artist"]["name"])
                endpoint = endpoint.replace("{album}", TEST_DATA_CONFIG["album"]["name"])
                endpoint = endpoint.replace("{genre}", TEST_DATA_CONFIG["genre"])
                endpoint = endpoint.replace("{keyword}", TEST_DATA_CONFIG["keyword"])
                endpoint = endpoint.replace("{artist_id}", TEST_DATA_CONFIG["artist"]["spotify_id"])
                endpoint = endpoint.replace("{track_name}", TEST_DATA_CONFIG["track"]["name"])
                endpoint = endpoint.replace("{track_artist}", TEST_DATA_CONFIG["track"]["artist"])

                print(f"  Final endpoint: {endpoint}")
                print(f"  Params: {processed_params}")

                # Make the request
                data = await self._spotify_request(endpoint, processed_params)

                # Generate filename and save
                filename = f"{mock_name}.json"
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

    async def generate_search_methods(self) -> None:
        """Generate mock data by executing SpotifyService methods.

        This method calls the actual SpotifyService methods defined in TEST_SEARCH
        and saves the results as mock data files in the search directory.
        """
        print("\n" + "=" * 60)
        print("Spotify Search Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_SEARCH)}\n")

        # Create SpotifySearchService instance
        service = SpotifySearchService()

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

    async def generate_models(self) -> None:
        """Generate mock data for model validation.

        This creates sample data structures that can be used to test Pydantic models.
        """
        print("\n" + "=" * 60)
        print("Spotify Models Mock Data Generator")
        print("=" * 60 + "\n")

        # Fetch real data to use for model validation
        print("Fetching real data for model validation...")

        # Get Spotify artist data
        artist_data = await self._spotify_request(
            f"artists/{TEST_DATA_CONFIG['artist']['spotify_id']}"
        )

        # Get Spotify token
        token = await self._get_spotify_token()
        token_response = {"access_token": token, "token_type": "Bearer", "expires_in": 3600}

        # Get Spotify album search
        album_search = await self._spotify_request(
            "search",
            params={
                "q": f"album:{TEST_DATA_CONFIG['album']['name']} artist:{TEST_DATA_CONFIG['album']['artist']}",
                "type": "album",
                "limit": 1,
            },
        )

        # Get Spotify playlist search
        playlist_search = await self._spotify_request(
            "search",
            params={"q": "progressive rock", "type": "playlist", "limit": 1},
        )

        print("\nSaving model validation data...")

        # Save all model data
        self._save_json("spotify_artist.json", artist_data, directory=self.models_dir)
        self._save_json("spotify_token_response.json", token_response, directory=self.models_dir)
        self._save_json(
            "spotify_album.json",
            album_search.get("albums", {}).get("items", [{}])[0],
            directory=self.models_dir,
        )
        self._save_json(
            "spotify_playlist.json",
            playlist_search.get("playlists", {}).get("items", [{}])[0],
            directory=self.models_dir,
        )

        print("\n" + "=" * 60)
        print("✓ Model validation data generation complete!")
        print(f"✓ Files saved to: {self.models_dir}")
        print("=" * 60 + "\n")

    async def generate_all(self) -> None:
        """Generate all mock data files (basic set)."""
        print("\n" + "=" * 60)
        print("Spotify Mock Data Generator - Basic Set")
        print("=" * 60 + "\n")

        # Fetch basic data
        print("Fetching basic data...")

        artist_data = await self._spotify_request(
            f"artists/{TEST_DATA_CONFIG['artist']['spotify_id']}"
        )

        spotify_album_search = await self._spotify_request(
            "search",
            params={
                "q": f"album:{TEST_DATA_CONFIG['album']['name']} artist:{TEST_DATA_CONFIG['album']['artist']}",
                "type": "album",
                "limit": 1,
            },
        )

        spotify_artist_search = await self._spotify_request(
            "search", params={"q": TEST_DATA_CONFIG["artist"]["name"], "type": "artist", "limit": 1}
        )

        spotify_top_tracks = await self._spotify_request(
            f"artists/{TEST_DATA_CONFIG['artist']['spotify_id']}/top-tracks",
            params={"market": "US"},
        )

        token = await self._get_spotify_token()
        spotify_token = {"access_token": token, "token_type": "Bearer", "expires_in": 3600}

        print("\nSaving mock data files...")

        # Save all data
        self._save_json("mock_artist_data.json", artist_data)
        self._save_json("mock_spotify_album_search.json", spotify_album_search)
        self._save_json("mock_spotify_artist_search.json", spotify_artist_search)
        self._save_json("mock_spotify_top_tracks.json", spotify_top_tracks)
        self._save_json("mock_spotify_token_response.json", spotify_token)

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate mock data from real Spotify API responses"
    )
    parser.add_argument(
        "--endpoints",
        action="store_true",
        help="Generate mock data for all test endpoints (make_requests/)",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Generate mock data by executing search service methods",
    )
    parser.add_argument(
        "--models",
        action="store_true",
        help="Generate mock data for model validation",
    )
    args = parser.parse_args()

    # Get API credentials from environment
    spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
    spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    missing = []
    if not spotify_client_id:
        missing.append("SPOTIFY_CLIENT_ID")
    if not spotify_client_secret:
        missing.append("SPOTIFY_CLIENT_SECRET")

    if missing:
        print("ERROR: Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease set the required environment variables:")
        print("  export SPOTIFY_CLIENT_ID='your_id_here'")
        print("  export SPOTIFY_CLIENT_SECRET='your_secret_here'")
        return 1

    try:
        generator = SpotifyMockDataGenerator(spotify_client_id, spotify_client_secret)

        if args.endpoints:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        elif args.search:
            # Generate mock data by executing search service methods
            await generator.generate_search_methods()
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
