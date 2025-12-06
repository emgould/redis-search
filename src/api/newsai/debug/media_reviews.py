#!/usr/bin/env python3
"""
Interactive debug script for testing get_media_reviews functionality.
Allows you to enter titles and see the raw API response and processed results.
"""

import asyncio
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from contracts.models import MCType

from api.newsai.search import NewsAISearchService


async def test_media_reviews(title: str, media_type: str):
    """Test get_media_reviews with a given title and media type."""
    print(f"\n{'=' * 80}")
    print(f"Testing: {title} ({media_type})")
    print(f"{'=' * 80}\n")

    # Initialize service
    service = NewsAISearchService()

    # Convert media_type string to MCType enum
    if media_type.lower() == "movie":
        mc_type = MCType.MOVIE
    elif media_type.lower() in ["tv", "tv_series"]:
        mc_type = MCType.TV_SERIES
    else:
        print(f"❌ Invalid media_type: {media_type}")
        return

    print(f"📅 Date range: {date.today() - timedelta(days=30)} to {date.today()}")
    print(f'🔍 Search query will be: "{title}" {media_type} review')
    print(
        "📰 Searching sources: metacritic.com, rottentomatoes.com, nytimes.com, variety.com, etc."
    )
    print()

    # Call the service method
    try:
        result = await service.get_media_reviews(
            title=title, media_type=mc_type, page_size=20, page=1, no_cache=True
        )

        # Display results
        print(f"✅ Status: {result.status}")
        print(f"📊 Status Code: {result.status_code}")
        print(f"🔢 Total Results: {result.total_results}")
        print(f"📄 Results Returned: {len(result.results)}")

        if result.error:
            print(f"❌ Error: {result.error}")

        print()

        if len(result.results) > 0:
            print(f"{'=' * 80}")
            print("ARTICLES FOUND:")
            print(f"{'=' * 80}\n")

            for i, article in enumerate(result.results, 1):
                print(f"[{i}] {article.title}")
                print(
                    f"    Source: {article.news_source.name if article.news_source else 'Unknown'}"
                )
                print(f"    URL: {article.url}")
                print(f"    Published: {article.published_at or 'Unknown'}")
                if article.description:
                    desc = (
                        article.description[:150] + "..."
                        if len(article.description) > 150
                        else article.description
                    )
                    print(f"    Description: {desc}")
                print()
        else:
            print("⚠️  No articles found!")
            print("\nLet's debug the query structure...")

            # # Show what the actual query looks like
            # review_sources = [
            #     "metacritic.com",
            #     "editorial.rottentomatoes.com",
            #     "nytimes.com",
            #     "variety.com",
            #     "hollywoodreporter.com",
            #     "rogerebert.com",
            #     "slantmagazine.com",
            #     "ew.com",
            #     "buzzfeed.com",
            # ]

            # # Use exact keyword match without "review" suffix
            # keyword_query = f'"{title}"'

            # complex_query = {
            #     "$query": {
            #         "$and": [
            #             {"keyword": keyword_query, "keywordSearchMode": "exact"},
            #             {"$or": [{"sourceUri": s} for s in review_sources]},
            #         ]
            #     },
            #     "$filter": {
            #         "forceMaxDataTimeWindow": "31",
            #         "dataType": ["news", "blog"],
            #         "startSourceRankPercentile": 0,
            #         "endSourceRankPercentile": 30,
            #     },
            # }

            # print("\n📋 Query structure being sent to Event Registry:")
            # print(json.dumps(complex_query, indent=2))

            # # Try a simpler search without source restrictions
            # print("\n\n🔄 Trying broader search without source restrictions...")
            # simple_result = await service.search_news(
            #     query=keyword_query, language="en", page_size=5
            # )

            # print(f"✅ Broader search found: {simple_result.total_results} results")
            # if len(simple_result.results) > 0:
            #     print("\nFirst 3 results from broader search:")
            #     for i, article in enumerate(simple_result.results[:3], 1):
            #         print(f"  [{i}] {article.title}")
            #         print(
            #             f"      Source: {article.news_source.name if article.news_source else 'Unknown'}"
            #         )
            #         print(f"      URL: {article.url}")
            #         print()

    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback

        traceback.print_exc()


async def main():
    """Main interactive loop."""
    print("=" * 80)
    print("🎬 Media Reviews Debug Tool")
    print("=" * 80)
    print()
    print("This tool helps debug the get_media_reviews functionality.")
    print("It will show you the exact query being sent and the results returned.")
    print()

    # Check for API key
    if not os.getenv("NEWSAI_API_KEY"):
        print("⚠️  NEWSAI_API_KEY not found in environment")
        print("Attempting to load from .env file...")

        # Try to load from .env
        env_path = Path(__file__).parent.parent.parent / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    if line.startswith("NEWSAI_API_KEY="):
                        key = line.split("=", 1)[1].strip()
                        os.environ["NEWSAI_API_KEY"] = key
                        print("✅ Loaded NEWSAI_API_KEY from .env")
                        break

        if not os.getenv("NEWSAI_API_KEY"):
            print(
                "❌ Could not find NEWSAI_API_KEY. Please set it in your environment or .env file."
            )
            return
    else:
        print("✅ NEWSAI_API_KEY found in environment")

    print()

    while True:
        print("\n" + "=" * 80)
        title = input("Enter title to search (or 'quit' to exit): ").strip()

        if title.lower() in ["quit", "exit", "q"]:
            print("\n👋 Goodbye!")
            break

        if not title:
            print("⚠️  Please enter a title")
            continue

        media_type = input("Enter media type (movie/tv): ").strip().lower()

        if media_type not in ["movie", "tv", "tv_series"]:
            print("⚠️  Please enter 'movie' or 'tv'")
            continue

        await test_media_reviews(title, media_type)

        print("\n" + "=" * 80)
        another = input("Test another? (y/n): ").strip().lower()
        if another not in ["y", "yes"]:
            print("\n👋 Goodbye!")
            break


if __name__ == "__main__":
    asyncio.run(main())
