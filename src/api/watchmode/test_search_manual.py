#!/usr/bin/env python3
"""
Manual test script for Watchmode search functionality.
Run this to test the search endpoint locally.

Usage:
    cd firebase/python_functions
    source venv/bin/activate
    python -m api.watchmode.test_search_manual
"""

import asyncio
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from api.watchmode.wrappers import watchmode_wrapper


async def test_search():
    """Test the search_titles wrapper."""
    print("Testing Watchmode Search Functionality")
    print("=" * 60)

    # Test 1: Search for a popular movie
    print("\n1. Searching for 'Inception'...")
    result = await watchmode_wrapper.search_titles(query="Inception", types="movie")
    print(f"   Status: {result.status_code}")
    print(f"   Total Results: {result.total_results}")
    if result.results:
        print("   First Result:")
        print(f"     - Watchmode ID: {result.results[0].id}")
        print(f"     - Name: {result.results[0].name}")
        print(f"     - Type: {result.results[0].type}")
        print(f"     - Year: {result.results[0].year}")
        print(f"     - TMDB ID: {result.results[0].tmdb_id}")

    # Test 2: Search for a TV show
    print("\n2. Searching for 'Breaking Bad'...")
    result = await watchmode_wrapper.search_titles(query="Breaking Bad", types="tv")
    print(f"   Status: {result.status_code}")
    print(f"   Total Results: {result.total_results}")
    if result.results:
        print("   First Result:")
        print(f"     - Watchmode ID: {result.results[0].id}")
        print(f"     - Name: {result.results[0].name}")
        print(f"     - Type: {result.results[0].type}")
        print(f"     - Year: {result.results[0].year}")
        print(f"     - TMDB ID: {result.results[0].tmdb_id}")

        # Test 3: Use the watchmode ID to get details
        watchmode_id = result.results[0].id
        print(f"\n3. Getting details for Watchmode ID {watchmode_id}...")
        details = await watchmode_wrapper.get_watchmode_title_details(watchmode_id=watchmode_id)
        print(f"   Status: {details.status_code}")
        print(f"   Title: {details.title}")
        print(f"   Type: {details.type}")
        print(f"   Year: {details.year}")
        print(f"   IMDB ID: {details.imdb_id}")
        print(f"   TMDB ID: {details.tmdb_id}")
        print(f"   Streaming Sources: {len(details.streaming_sources)}")

    # Test 4: Empty query
    print("\n4. Testing empty query...")
    result = await watchmode_wrapper.search_titles(query="")
    print(f"   Status: {result.status_code}")
    print(f"   Error: {result.error}")

    print("\n" + "=" * 60)
    print("Tests Complete!")


if __name__ == "__main__":
    asyncio.run(test_search())

