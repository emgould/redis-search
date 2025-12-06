#!/usr/bin/env python3
"""
Manual test script for NewsAI API.
Tests the actual Event Registry API with real requests.

Usage:
    cd firebase/python_functions
    source venv/bin/activate
    python api/newsai/bin/test_api.py
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from api.newsai.wrappers import newsai_wrapper


async def test_trending_news():
    """Test get_trending_news with default complex query."""
    print("\n" + "=" * 60)
    print("Testing get_trending_news (default complex query)")
    print("=" * 60)

    result = await newsai_wrapper.get_trending_news(
        country="us",
        page_size=3,  # Small page size for testing
    )

    print(f"\nStatus Code: {result.status_code}")
    print(f"Total Results: {result.total_results}")
    print(f"Error: {result.error}")
    print(f"Articles Returned: {len(result.results)}")

    if result.results:
        print("\nFirst Article:")
        article = result.results[0]
        print(f"  Title: {article.title[:80]}...")
        print(f"  URL: {article.url}")
        print(f"  Source: {article.news_source.name if article.news_source else 'N/A'}")
        print(f"  Published: {article.published_at}")
        print(f"  MC ID: {article.mc_id}")

    return result.status_code == 200


async def test_search_news():
    """Test search_news with simple query."""
    print("\n" + "=" * 60)
    print("Testing search_news (simple query)")
    print("=" * 60)

    result = await newsai_wrapper.search_news(
        query="artificial intelligence",
        language="en",
        page_size=3,
    )

    print(f"\nStatus Code: {result.status_code}")
    print(f"Total Results: {result.total_results}")
    print(f"Error: {result.error}")
    print(f"Articles Returned: {len(result.results)}")

    if result.results:
        print("\nFirst Article:")
        article = result.results[0]
        print(f"  Title: {article.title[:80]}...")
        print(f"  URL: {article.url}")
        print(f"  Source: {article.news_source.name if article.news_source else 'N/A'}")
        print(f"  Published: {article.published_at}")

    return result.status_code == 200


async def test_search_news_complex():
    """Test search_news with complex query."""
    print("\n" + "=" * 60)
    print("Testing search_news (complex query)")
    print("=" * 60)

    complex_query = {
        "$query": {
            "$and": [
                {"categoryUri": "dmoz/Arts/Television/Programs"},
                {"sourceUri": "variety.com"},
            ]
        }
    }

    result = await newsai_wrapper.search_news(
        query="television",
        complex_query=complex_query,
        page_size=3,
    )

    print(f"\nStatus Code: {result.status_code}")
    print(f"Total Results: {result.total_results}")
    print(f"Error: {result.error}")
    print(f"Articles Returned: {len(result.results)}")

    if result.results:
        print("\nFirst Article:")
        article = result.results[0]
        print(f"  Title: {article.title[:80]}...")
        print(f"  URL: {article.url}")
        print(f"  Source: {article.news_source.name if article.news_source else 'N/A'}")

    return result.status_code == 200


async def test_get_sources():
    """Test get_news_sources."""
    print("\n" + "=" * 60)
    print("Testing get_news_sources")
    print("=" * 60)

    result = await newsai_wrapper.get_news_sources()

    print(f"\nStatus Code: {result.status_code}")
    print(f"Total Sources: {result.total_results}")
    print(f"Error: {result.error}")
    print(f"Sources Returned: {len(result.results)}")

    if result.results:
        print("\nFirst 3 Sources:")
        for i, source in enumerate(result.results[:3]):
            print(f"  {i + 1}. {source.name} ({source.uri})")

    return result.status_code == 200


async def main():
    """Run all tests."""
    # Check for API key
    api_key = os.getenv("NEWSAI_API_KEY")
    if not api_key:
        print("ERROR: NEWSAI_API_KEY environment variable not set")
        print("Set it with: export NEWSAI_API_KEY=your-key-here")
        return False

    if api_key.startswith("test_"):
        print("ERROR: Test API key detected. Please use a real API key.")
        return False

    print("=" * 60)
    print("NewsAI API Integration Tests")
    print("=" * 60)
    print(f"API Key: {api_key[:10]}...{api_key[-4:]}")

    results = []

    # Run tests
    try:
        results.append(("Trending News (Default Query)", await test_trending_news()))
        results.append(("Search News (Simple)", await test_search_news()))
        results.append(("Search News (Complex)", await test_search_news_complex()))
        results.append(("Get Sources", await test_get_sources()))
    except Exception as e:
        print(f"\n\nERROR: Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Print summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")

    all_passed = all(passed for _, passed in results)
    print("\n" + ("=" * 60))
    if all_passed:
        print("✓ All tests passed!")
    else:
        print("✗ Some tests failed")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
