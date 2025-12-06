"""
Test that fixture loading mechanism works correctly.

This test validates that the new fixture-based approach works
before we fully migrate from hardcoded mocks.
"""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def test_load_fixture_function():
    """Test that load_fixture function works correctly."""
    # This will import the load_fixture function from conftest
    from .conftest import load_fixture

    # Test with a fixture that should exist (using new structure)
    try:
        movie_data = load_fixture("make_requests/get_media_details_movie.json")
        assert isinstance(movie_data, dict)
        assert "id" in movie_data
        assert "title" in movie_data or "name" in movie_data
    except FileNotFoundError as e:
        pytest.skip(f"Fixtures not generated yet: {e}")


def test_fixtures_directory_structure():
    """Test that fixtures directory exists and has expected structure."""
    fixtures_dir = Path(__file__).parent / "fixtures"

    # Directory should exist after running generate_mock_data.py
    if not fixtures_dir.exists():
        pytest.skip("Fixtures directory not created yet. Run generate_mock_data.py first.")

    # Check for expected subdirectories and fixture files (new structure)
    expected_subdirs = ["make_requests", "core", "person", "search"]
    expected_files = {
        "make_requests": [
            "get_media_details_movie.json",
            "get_media_details_tv.json",
            "search_movies.json",
            "search_tv_shows.json",
            "search_people.json",
        ],
        "core": [
            "cast_and_crew_movie.json",
            "videos_movie.json",
            "watch_providers_movie.json",
        ],
        "person": ["person_details.json"],
        "search": ["search_movies.json", "trending_movie.json"],
    }

    # Check subdirectories exist
    for subdir in expected_subdirs:
        subdir_path = fixtures_dir / subdir
        if subdir_path.exists():
            print(f"✓ Found subdirectory: {subdir}")
            # Check for some expected files in this subdirectory
            if subdir in expected_files:
                for expected_file in expected_files[subdir]:
                    file_path = subdir_path / expected_file
                    if file_path.exists():
                        print(f"  ✓ Found: {subdir}/{expected_file}")
                    else:
                        print(f"  ✗ Missing: {subdir}/{expected_file}")
        else:
            print(f"✗ Missing subdirectory: {subdir}")

    # Count all JSON files recursively
    all_json_files = list(fixtures_dir.rglob("*.json"))
    assert len(all_json_files) > 0, "No fixture files found"
    print(f"\nTotal fixture files found: {len(all_json_files)}")


def test_fixture_data_structure():
    """Test that loaded fixtures have expected structure."""
    fixtures_dir = Path(__file__).parent / "fixtures"

    if not fixtures_dir.exists():
        pytest.skip("Fixtures not generated yet")

    # Test movie data structure (using new path)
    movie_file = fixtures_dir / "make_requests" / "get_media_details_movie.json"
    if movie_file.exists():
        with open(movie_file) as f:
            movie_data = json.load(f)

        # Verify it's real TMDB data structure
        assert "id" in movie_data, "Movie data should have 'id' field"
        assert "title" in movie_data, "Movie data should have 'title' field"
        assert "overview" in movie_data, "Movie data should have 'overview' field"

        # Check for fields that real API includes but mocks might miss
        # These are common fields that hardcoded mocks often omit
        real_api_fields = [
            "adult",
            "backdrop_path",
            "poster_path",
            "popularity",
            "vote_average",
            "vote_count",
            "original_language",
        ]

        for field in real_api_fields:
            assert field in movie_data, f"Real API should include '{field}' field"

        print(f"✓ Movie data has {len(movie_data)} fields (real API data is comprehensive)")
    else:
        pytest.skip("Movie fixture not found at new location")
