#!/usr/bin/env python3
"""
Interactive test script for RottenTomatoes API wrappers.
Tests search functionality for movies, TV shows, and people.

Usage:
    cd firebase/python_functions
    source venv/bin/activate
    python -m api.rottentomatoes.bin.interactive_test

Commands:
    - Type a query to search (searches content by default)
    - Type 'mode content' to search movies/TV only
    - Type 'mode people' to search people only
    - Type 'mode all' to search both content and people
    - Type 'metrics <title>' to get RT scores (simple lookup)
    - Type 'metrics <title> year=1999' to filter by year
    - Type 'metrics <title> star=Brad Pitt' to filter by cast member
    - Type 'filter movie' to filter results to movies only
    - Type 'filter tv' to filter results to TV shows only
    - Type 'filter none' to remove filter
    - Type 'limit N' to set number of results (e.g., 'limit 20')
    - Type 'json' to toggle JSON output mode
    - Type 'help' to see this help message
    - Type 'exit' or 'quit' to exit
"""

import asyncio
import re
import sys
from pathlib import Path

# Add parent directories to path for imports
script_dir = Path(__file__).parent.absolute()
api_dir = script_dir.parent.parent.parent  # firebase/python_functions
sys.path.insert(0, str(api_dir))

from contracts.models import MCType  # noqa: E402

from api.rottentomatoes.models import MCRottenTomatoesItem, MCRottenTomatoesPersonItem  # noqa: E402
from api.rottentomatoes.wrappers import (  # noqa: E402
    get_rt_metrics,
    search_all_async,
    search_content_async,
    search_people_async,
)


class InteractiveTest:
    """Interactive test runner for RottenTomatoes API."""

    def __init__(self):
        self.mode = "content"  # content, people, or all
        self.media_filter: MCType | None = None
        self.limit = 10
        self.json_output = False

    def print_help(self):
        """Print help message."""
        print("\n" + "=" * 60)
        print("RottenTomatoes Interactive Test")
        print("=" * 60)
        print("\nSearch Commands:")
        print("  <query>       - Search with current mode and filter")
        print("  mode content  - Search movies/TV only")
        print("  mode people   - Search people only")
        print("  mode all      - Search both content and people")
        print("\nMetrics Lookup (quick score check):")
        print("  metrics <title>                    - Get RT scores")
        print("  metrics <title> year=1999          - Filter by year")
        print("  metrics <title> star=Brad Pitt     - Filter by cast")
        print("  metrics <title> year=1999 star=Tom - Both filters")
        print("\nFilters & Settings:")
        print("  filter movie  - Filter content to movies only")
        print("  filter tv     - Filter content to TV shows only")
        print("  filter none   - Remove media type filter")
        print("  limit N       - Set number of results (e.g., 'limit 20')")
        print("  json          - Toggle JSON output mode")
        print("  help          - Show this help message")
        print("  exit/quit     - Exit the test")
        print("\nCurrent settings:")
        print(f"  Mode: {self.mode}")
        print(f"  Filter: {self.media_filter or 'none'}")
        print(f"  Limit: {self.limit}")
        print(f"  JSON output: {self.json_output}")
        print("=" * 60 + "\n")

    def print_content_item(self, item: MCRottenTomatoesItem, index: int):
        """Pretty print a content item."""
        print(f"\n{'─' * 50}")
        print(f"[{index}] {item.title} ({item.release_year or 'N/A'})")
        print(f"    Type: {item.mc_type.value.upper()}")
        print(f"    RT URL: {item.rt_url or 'N/A'}")

        # Scores
        scores = []
        if item.critics_score is not None:
            sentiment = "🍅" if item.critics_sentiment in ("fresh", "certified_fresh") else "🤢"
            scores.append(f"Critics: {sentiment} {item.critics_score}%")
        if item.audience_score is not None:
            sentiment = "🍿" if item.audience_sentiment == "positive" else "😐"
            scores.append(f"Audience: {sentiment} {item.audience_score}%")
        if scores:
            print(f"    Scores: {' | '.join(scores)}")

        # Badges
        badges = []
        if item.certified_fresh:
            badges.append("🏆 Certified Fresh")
        if item.verified_hot:
            badges.append("🔥 Verified Hot")
        if badges:
            print(f"    Badges: {' | '.join(badges)}")

        # Details
        if item.rating:
            print(f"    Rating: {item.rating}")
        if item.genres:
            print(f"    Genres: {', '.join(item.genres[:5])}")
        if item.runtime:
            print(f"    Runtime: {item.runtime} min")

        # Cast
        if item.cast_names:
            print(f"    Cast: {', '.join(item.cast_names[:5])}")
        if item.director:
            print(f"    Director: {item.director}")

        # Description
        if item.description:
            desc = (
                item.description[:200] + "..." if len(item.description) > 200 else item.description
            )
            print(f"    Description: {desc}")

        # Popularity
        if item.popularity:
            print(f"    Popularity: {item.popularity:,} page views")

    def print_person_item(self, item: MCRottenTomatoesPersonItem, index: int):
        """Pretty print a person item."""
        print(f"\n{'─' * 50}")
        print(f"[{index}] {item.name}")

        # IDs for debugging
        ids = []
        if item.person_id:
            ids.append(f"personId: {item.person_id}")
        if item.ems_id:
            ids.append(f"emsId: {item.ems_id}")
        if ids:
            print(f"    IDs: {' | '.join(ids)}")

        # Image URL
        if item.image_url:
            print(f"    Photo: {item.image_url}")

        # Known for / filmography
        if item.known_for:
            print(f"    Known for: {', '.join(item.known_for[:5])}")

        # Dates
        if item.birth_date:
            print(f"    Born: {item.birth_date}")
        if item.death_date:
            print(f"    Died: {item.death_date}")

        # Gender
        if item.gender:
            print(f"    Gender: {item.gender}")

        # Bio
        if item.biography:
            bio = item.biography[:200] + "..." if len(item.biography) > 200 else item.biography
            print(f"    Bio: {bio}")

        # Aliases
        if item.aliases:
            print(f"    Also known as: {', '.join(item.aliases[:3])}")

        # Popularity
        if item.popularity:
            print(f"    Popularity: {item.popularity:,}")

        # Show mc_id for reference
        if item.mc_id:
            print(f"    MC ID: {item.mc_id}")

    async def search_content(self, query: str):
        """Search for content."""
        filter_str = self.media_filter or "none"
        print(f"\n🔍 Searching content for: '{query}' (limit: {self.limit}, filter: {filter_str})")

        result = await search_content_async(
            query=query,
            limit=self.limit,
            media_type=self.media_filter,
        )

        if result.error:
            print(f"\n❌ Error: {result.error}")
            return

        print(f"\n✅ Found {result.total_results} results (content hits: {result.content_hits})")

        if self.json_output:
            print(result.model_dump_json(indent=2))
        else:
            for i, item in enumerate(result.results, 1):
                self.print_content_item(item, i)

    async def search_people(self, query: str):
        """Search for people."""
        print(f"\n🔍 Searching people for: '{query}' (limit: {self.limit})")

        result = await search_people_async(
            query=query,
            limit=self.limit,
        )

        if result.error:
            print(f"\n❌ Error: {result.error}")
            return

        print(f"\n✅ Found {result.total_results} people")

        if self.json_output:
            print(result.model_dump_json(indent=2))
        else:
            for i, item in enumerate(result.results, 1):
                self.print_person_item(item, i)

    async def search_all(self, query: str):
        """Search for both content and people."""
        print(f"\n🔍 Searching all for: '{query}' (limit: {self.limit})")

        result = await search_all_async(
            query=query,
            limit=self.limit,
        )

        if result.error:
            print(f"\n❌ Error: {result.error}")
            return

        print(f"\n✅ Found {result.total_results} results")
        print(f"   Content: {result.content_hits} | People: {result.people_hits}")

        if self.json_output:
            print(result.model_dump_json(indent=2))
        else:
            for i, item in enumerate(result.results, 1):
                self.print_content_item(item, i)

    async def lookup_metrics(self, user_input: str):
        """Look up RT metrics for a title."""
        # Parse the metrics command: metrics <title> [year=YYYY] [star=Name]
        # Remove 'metrics ' prefix
        args_str = user_input[8:].strip()

        if not args_str:
            print("❌ Usage: metrics <title> [year=YYYY] [star=Name]")
            return

        # Extract year= and star= parameters
        year: int | None = None
        star: str | None = None

        # Find year=YYYY
        year_match = re.search(r"\byear=(\d{4})\b", args_str)
        if year_match:
            year = int(year_match.group(1))
            args_str = args_str[: year_match.start()] + args_str[year_match.end() :]

        # Find star=Name (handles quotes and unquoted)
        star_match = re.search(r'\bstar=(["\']?)([^"\']+)\1', args_str)
        if star_match:
            star = star_match.group(2).strip()
            args_str = args_str[: star_match.start()] + args_str[star_match.end() :]

        # Remaining text is the title
        title = args_str.strip()

        if not title:
            print("❌ Title is required. Usage: metrics <title> [year=YYYY] [star=Name]")
            return

        # Build display string
        filters = []
        if year:
            filters.append(f"year={year}")
        if star:
            filters.append(f"star={star}")
        filter_display = f" ({', '.join(filters)})" if filters else ""

        print(f"\n🎯 Looking up RT metrics for: '{title}'{filter_display}")

        result = await get_rt_metrics(title=title, year=year, star=star)

        if result is None:
            print("\n❌ No matching title found")
            return

        print("\n" + "─" * 40)
        print(f"🍅 {title}")
        if year:
            print(f"   Year: {year}")

        critics = result.get("critics")
        audience = result.get("audience")

        if critics is not None:
            emoji = "🍅" if critics >= 60 else "🤢"
            print(f"   Critics:  {emoji} {critics}%")
        else:
            print("   Critics:  N/A")

        if audience is not None:
            emoji = "🍿" if audience >= 60 else "😐"
            print(f"   Audience: {emoji} {audience}%")
        else:
            print("   Audience: N/A")

        print("─" * 40)

    def process_command(self, user_input: str) -> bool:
        """Process a command. Returns False if should exit."""
        cmd = user_input.strip().lower()

        if cmd in ("exit", "quit"):
            return False

        if cmd == "help":
            self.print_help()
            return True

        if cmd == "json":
            self.json_output = not self.json_output
            status = "ON" if self.json_output else "OFF"
            print(f"📋 JSON output: {status}")
            return True

        if cmd.startswith("mode "):
            mode = cmd[5:].strip()
            if mode in ("content", "people", "all"):
                self.mode = mode
                print(f"🔄 Mode set to: {self.mode}")
            else:
                print("❌ Invalid mode. Use: content, people, or all")
            return True

        if cmd.startswith("filter "):
            filter_type = cmd[7:].strip()
            if filter_type == "movie":
                self.media_filter = MCType.MOVIE
                print("🎬 Filter set to: movies only")
            elif filter_type == "tv":
                self.media_filter = MCType.TV_SERIES
                print("📺 Filter set to: TV shows only")
            elif filter_type == "none":
                self.media_filter = None
                print("🔄 Filter removed")
            else:
                print("❌ Invalid filter. Use: movie, tv, or none")
            return True

        if cmd.startswith("limit "):
            try:
                new_limit = int(cmd[6:].strip())
                if 1 <= new_limit <= 50:
                    self.limit = new_limit
                    print(f"📊 Limit set to: {self.limit}")
                else:
                    print("❌ Limit must be between 1 and 50")
            except ValueError:
                print("❌ Invalid limit. Use a number like: limit 20")
            return True

        # Not a command, treat as search query
        return True

    async def run(self):
        """Run the interactive test loop."""
        self.print_help()

        while True:
            try:
                user_input = input("\n🍅 Enter query or command: ").strip()

                if not user_input:
                    continue

                # Check if it's a command
                cmd_lower = user_input.lower()
                if cmd_lower in ("exit", "quit"):
                    print("\n👋 Goodbye!")
                    break

                if cmd_lower in ("help", "json") or cmd_lower.startswith(
                    ("mode ", "filter ", "limit ")
                ):
                    self.process_command(user_input)
                    continue

                # Check for metrics command
                if cmd_lower.startswith("metrics "):
                    await self.lookup_metrics(user_input)
                    continue

                # It's a search query
                if self.mode == "content":
                    await self.search_content(user_input)
                elif self.mode == "people":
                    await self.search_people(user_input)
                else:
                    await self.search_all(user_input)

            except KeyboardInterrupt:
                print("\n\n👋 Interrupted. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")


def main():
    """Main entry point."""
    test = InteractiveTest()
    asyncio.run(test.run())


if __name__ == "__main__":
    main()
