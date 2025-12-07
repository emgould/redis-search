#!/usr/bin/env python3
"""
Utility to generate a markdown summary of trending TV shows.
Fetches trending shows, their details, and media reviews, then creates a formatted document.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.newsai.wrappers import newsai_wrapper
from api.tmdb.wrappers import get_trending_async
from contracts.models import MCType
from utils.get_logger import get_logger

logger = get_logger(__name__)


def format_cast(cast_data: dict[str, Any]) -> str:
    """Format cast information for markdown output."""
    if not cast_data or "cast" not in cast_data:
        return "No cast information available"

    cast_list = cast_data.get("cast", [])
    if not cast_list:
        return "No cast information available"

    # Format top cast members
    formatted_cast = []
    for member in cast_list[:10]:  # Limit to top 10
        name = member.get("name", "Unknown")
        character = member.get("character", "")
        if character:
            formatted_cast.append(f"- {name} as {character}")
        else:
            formatted_cast.append(f"- {name}")

    return "\n".join(formatted_cast)


def format_streaming_providers(watch_providers: dict[str, Any]) -> str:
    """Format streaming provider information for markdown output."""
    if not watch_providers:
        return "No streaming information available"

    # The watch_providers dict already contains the region data at the root level
    # Structure: {"flatrate": [...], "buy": [...], "rent": [...], "region": "US", ...}
    providers_list = []

    # Check for different availability types
    if "flatrate" in watch_providers:
        flatrate = watch_providers["flatrate"]
        if flatrate:
            providers_list.append("**Streaming:**")
            for provider in flatrate:
                providers_list.append(f"- {provider.get('provider_name', 'Unknown')}")

    if "buy" in watch_providers:
        buy = watch_providers["buy"]
        if buy:
            providers_list.append("\n**Buy:**")
            for provider in buy:
                providers_list.append(f"- {provider.get('provider_name', 'Unknown')}")

    if "rent" in watch_providers:
        rent = watch_providers["rent"]
        if rent:
            providers_list.append("\n**Rent:**")
            for provider in rent:
                providers_list.append(f"- {provider.get('provider_name', 'Unknown')}")

    if not providers_list:
        return "No streaming information available"

    return "\n".join(providers_list)


def format_trailer_link(videos: dict[str, Any]) -> str:
    """Extract and format the primary trailer link."""
    if not videos:
        return "No trailer available"

    # Check for trailers
    trailers = videos.get("trailers", [])
    if not trailers:
        # Try teasers as fallback
        trailers = videos.get("teasers", [])

    if not trailers:
        return "No trailer available"

    # Get the first trailer (usually the main one)
    trailer = trailers[0]
    video_key = trailer.get("key")
    video_site = trailer.get("site", "YouTube")

    if not video_key:
        return "No trailer available"

    if video_site == "YouTube":
        return f"https://www.youtube.com/watch?v={video_key}"

    return f"Trailer available on {video_site}"


def format_article(article: Any, index: int) -> str:
    """Format a single article for markdown output."""
    title = article.title or "Untitled"
    url = article.url or "No URL"

    # Get body/content - prefer content over description
    body = article.content or article.description or "No content available"

    # Truncate body if too long (keep first 500 characters)
    if len(body) > 500:
        body = body[:500] + "..."

    # Format the article section
    article_md = f"\n#### [{index}] {title}\n\n"
    article_md += f"**Body:**\n{body}\n\n"
    article_md += f"**Web Link:** [{url}]({url})\n"

    if article.news_source and article.news_source.name:
        article_md += f"**Source:** {article.news_source.name}\n"

    if article.published_at:
        article_md += f"**Published:** {article.published_at}\n"

    return article_md


async def generate_trending_summary(limit: int = 10, output_path: str | None = None) -> str:
    """
    Generate a markdown summary of trending TV shows with details and reviews.

    Args:
        limit: Number of trending shows to include (default: 10)
        output_path: Optional custom output path. If None, uses default location.

    Returns:
        Path to the generated markdown file
    """
    logger.info(f"Starting trending summary generation for {limit} TV shows...")

    # Set default output path if not provided
    if output_path is None:
        base_dir = Path(__file__).parent.parent
        data_dir = base_dir / "data"
        data_dir.mkdir(exist_ok=True)
        output_path = str(data_dir / "trending_summary.md")

    # Initialize markdown content
    markdown_content = "# Trending TV Shows Summary\n\n"
    markdown_content += f"*Generated with top {limit} trending shows*\n\n"
    markdown_content += "---\n\n"

    try:
        # Step 1: Get trending TV shows
        logger.info("Fetching trending TV shows...")
        trending_result = await get_trending_async(limit=limit, media_type=MCType.TV_SERIES)

        if trending_result.status_code != 200 or trending_result.error:
            error_msg = trending_result.error or "Unknown error fetching trending shows"
            logger.error(f"Error fetching trending shows: {error_msg}")
            markdown_content += f"\n**Error:** {error_msg}\n"
            return output_path

        shows = trending_result.results
        logger.info(f"Found {len(shows)} trending shows")

        # Step 2: Process each show
        for idx, show in enumerate(shows[:limit], 1):
            logger.info(f"Processing show {idx}/{limit}: {show.title or show.name}")

            # Get show title
            show_title = show.title or show.name or "Unknown Title"
            markdown_content += f"## {idx}. {show_title}\n\n"

            try:
                # Step 3: Use details already in the trending result (no extra API call needed!)
                # The trending endpoint already includes cast, videos, and watch providers
                logger.info(f"Processing details for: {show_title}")

                # Add trailer link
                trailer_link = format_trailer_link(show.tmdb_videos)
                markdown_content += f"**Trailer:** {trailer_link}\n\n"

                # Add streaming providers
                providers = format_streaming_providers(show.watch_providers)
                markdown_content += f"**Streaming On:**\n{providers}\n\n"

                # Add cast
                cast = format_cast(show.tmdb_cast)
                markdown_content += f"**The Cast:**\n{cast}\n\n"

                # Step 4: Get media reviews
                logger.info(f"Fetching reviews for: {show_title}")
                reviews_result = await newsai_wrapper.get_media_reviews(
                    title=show_title,
                    media_type="tv",
                    page_size=5,  # Limit to 5 articles per show
                    page=1
                )

                if reviews_result.status_code == 200 and reviews_result.results:
                    markdown_content += f"### Articles ({len(reviews_result.results)} found):\n"

                    for article_idx, article in enumerate(reviews_result.results, 1):
                        article_md = format_article(article, article_idx)
                        markdown_content += article_md
                else:
                    markdown_content += "### Articles:\n\n*No articles found*\n"

                markdown_content += "\n---\n\n"

            except Exception as show_error:
                logger.error(f"Error processing show {show_title}: {show_error}")
                markdown_content += f"\n*Error processing this show: {show_error}*\n\n"
                markdown_content += "---\n\n"
                continue

        # Step 5: Write to file
        logger.info(f"Writing summary to: {output_path}")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        logger.info(f"✅ Successfully generated trending summary at: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Fatal error generating trending summary: {e}")
        markdown_content += f"\n**Fatal Error:** {e}\n"

        # Still try to write what we have
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        raise


async def main():
    """Main entry point for command-line execution."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate a markdown summary of trending TV shows with reviews"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of trending shows to include (default: 10)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path (default: data/trending_summary.md)"
    )

    args = parser.parse_args()

    # Check for required API keys
    if not os.getenv("TMDB_API_KEY"):
        logger.error("❌ TMDB_API_KEY not found in environment")
        sys.exit(1)

    if not os.getenv("NEWSAI_API_KEY"):
        logger.error("❌ NEWSAI_API_KEY not found in environment")
        sys.exit(1)

    try:
        output_path = await generate_trending_summary(
            limit=args.limit,
            output_path=args.output
        )
        print("\n✅ Summary generated successfully!")
        print(f"📄 Output: {output_path}")
    except Exception as e:
        logger.error(f"❌ Failed to generate summary: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

