"""
FlixPatrol Core Service - Pure FlixPatrol data scraper.
Handles fetching and parsing FlixPatrol streaming rankings.
"""

import os
import re
from datetime import UTC, datetime
from typing import Any

import aiohttp
from bs4 import BeautifulSoup
from dotenv import find_dotenv, load_dotenv

from api.subapi.flixpatrol.models import (
    FlixPatrolMediaItem,
    FlixPatrolMetadata,
    FlixPatrolParsedData,
    FlixPatrolResponse,
)
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Load environment variables
load_dotenv(find_dotenv())

# Configuration
FLIXPATROL_URL = os.getenv("FLIXPATROL_URL", "https://flixpatrol.com/top10/")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", 15))

# Cache configuration - 24 hours for FlixPatrol data
# v2.0.0: Refactored modular structure, pure scraper without TMDB enrichment
CacheExpiration = 60 * 60 * 24  # 24 hours
FlixPatrolCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="flixpatrol",
    verbose=False,
    isClassMethod=True,
    version="2.0.1",  # Version bump for Redis migration
)

logger = get_logger(__name__)

# Platform mapping
PLATFORM_KEYWORDS = {
    "Netflix": "netflix",
    "HBO": "hbo",
    "Disney": "disney+",
    "Amazon Prime": "amazon prime",
    "Amazon": "amazon",
    "Apple": "apple",
    "Hulu": "hulu",
    "Paramount": "paramount+",
    "Peacock": "peacock",
    "Starz": "starz",
    "Fubo": "fubo",
}


class FlixPatrolService:
    """
    Pure FlixPatrol scraper service.
    Fetches and parses FlixPatrol streaming rankings without external enrichment.
    """

    def __init__(self):
        """Initialize FlixPatrol service."""
        pass  # noqa: PIE790

    def _detect_platform_and_type(
        self, header_text: str, section_id: str
    ) -> tuple[str, str] | None:
        """
        Infer platform name and content type from header and section ID.

        Args:
            header_text: Header text from HTML
            section_id: Section ID from HTML

        Returns:
            Tuple of (platform_name, content_type) or None if not detected
        """
        platform_name = None
        for key, val in PLATFORM_KEYWORDS.items():
            if key.lower() in header_text.lower():
                platform_name = val
                break

        if not platform_name:
            match = re.search(r"on\s+(.*?)\s+on", header_text, re.IGNORECASE)
            if match:
                platform_name = match.group(1).strip().lower()

        # Check for movies first (before tv) to avoid false matches like "Apple TV+"
        # Look for explicit patterns like "movies", "movie", or "tv shows", "tv-shows"
        header_lower = header_text.lower()
        section_id_lower = section_id.lower()

        if "movie" in header_lower or "movies" in section_id_lower:
            content_type = "movies"
        elif (
            "tv show" in header_lower
            or "tv-shows" in section_id_lower
            or (header_lower.startswith("top 10 tv") and "movie" not in header_lower)
        ):
            # Only match "tv" if it's explicitly "tv shows" or if the header starts with "top 10 tv"
            # This avoids false matches with "Apple TV+" or similar platform names
            content_type = "shows"
        else:
            content_type = None

        return (platform_name, content_type) if platform_name and content_type else None

    def _extract_table_entries(self, table: BeautifulSoup) -> list[dict[str, Any]]:
        """
        Extract rank, title, and score entries from a table.

        Args:
            table: BeautifulSoup table element

        Returns:
            List of dictionaries containing rank, title, and score
        """
        entries = []
        tbody = table.find("tbody")
        if tbody is None or isinstance(tbody, str):
            tbody = table

        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            rank_match = re.search(r"\d+", cells[0].get_text(strip=True))
            if not rank_match:
                continue

            title_link = cells[1].find("a")
            if not title_link:
                continue

            title = re.sub(r"\s+", " ", title_link.get_text(strip=True))
            score = cells[2].get_text(strip=True).replace(",", "")

            entries.append({"rank": int(rank_match.group(0)), "title": title, "score": int(score)})

        return entries

    def _parse_fallback_format(
        self, soup: BeautifulSoup, current_date: str
    ) -> FlixPatrolParsedData:
        """
        Fallback parser for alternative FlixPatrol HTML structure.

        Args:
            soup: BeautifulSoup object of HTML
            current_date: Current date string

        Returns:
            FlixPatrolParsedData model
        """
        result: dict[str, Any] = {"date": current_date, "shows": {}, "movies": {}}
        tables = soup.find_all("table", class_="card-table")

        for table in tables:
            header = table.find_previous("h2")
            if not header:
                continue

            detection = self._detect_platform_and_type(header.get_text(strip=True), "")
            if not detection:
                continue

            platform_name, content_type = detection
            content_dict = result[content_type]
            if isinstance(content_dict, dict):
                content_dict.setdefault(platform_name, [])
                entries = self._extract_table_entries(table)
                content_dict[platform_name].extend(entries)

        return FlixPatrolParsedData.model_validate(result)

    def parse_flixpatrol_html(self, html: str) -> FlixPatrolParsedData:
        """
        Parse FlixPatrol HTML into structured data.

        Args:
            html: HTML content from FlixPatrol

        Returns:
            FlixPatrolParsedData model with shows and movies by platform
        """
        soup = BeautifulSoup(html, "html.parser")
        current_date = datetime.now(UTC).strftime("%Y-%m-%d")

        result: dict[str, Any] = {"date": current_date, "shows": {}, "movies": {}}

        sections = soup.find_all("div", {"id": re.compile(r"toc-.*")})
        if not sections:
            logger.warning("No toc-* sections found. Trying fallback parser.")
            return self._parse_fallback_format(soup, current_date)

        for section in sections:
            header = section.find("h2")
            if not header:
                continue

            detection = self._detect_platform_and_type(
                header.get_text(strip=True), section.get("id", "")
            )
            if not detection:
                continue

            platform_name, content_type = detection
            result[content_type].setdefault(platform_name, [])

            table = section.find("table")
            if not table:
                continue

            entries = self._extract_table_entries(table)
            result[content_type][platform_name].extend(entries)

        # Sort ranks per platform
        for ctype in ("shows", "movies"):
            for platform in result[ctype]:
                result[ctype][platform].sort(key=lambda x: x["rank"])

        return FlixPatrolParsedData.model_validate(result)

    @RedisCache.use_cache(FlixPatrolCache, prefix="fetch")
    async def fetch_flixpatrol_data(self) -> str:
        """
        Fetch the HTML data from FlixPatrol.

        Returns:
            HTML content as string

        Raises:
            TimeoutError: If request times out
            aiohttp.ClientError: If request fails
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)

        try:
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(FLIXPATROL_URL, headers=headers) as response,
            ):
                response.raise_for_status()
                return await response.text()
        except TimeoutError:
            logger.error("Request to FlixPatrol timed out.")
            raise
        except aiohttp.ClientError as e:
            logger.exception("Error fetching FlixPatrol data: %s", e)
            raise

    @RedisCache.use_cache(FlixPatrolCache, prefix="get_flixpatrol_data")
    async def get_flixpatrol_data(
        self, providers: list[str] | None = None, **kwargs
    ) -> FlixPatrolResponse:
        """
        Get FlixPatrol data.

        Args:
            providers: List of providers to include (default: all major providers)

        Returns:
            FlixPatrolResponse model containing FlixPatrol data organized by platform
        """
        try:
            # Fetch and parse FlixPatrol data
            html = await self.fetch_flixpatrol_data()
            parsed = self.parse_flixpatrol_html(html)

            # Default providers
            if providers is None:
                providers = [
                    "netflix",
                    "hbo",
                    "disney+",
                    "amazon prime",
                    "paramount+",
                    "apple",
                    "amazon",
                    "fubo",
                ]

            # Convert raw dicts to FlixPatrolMediaItem models and remove duplicates
            shows_by_platform: dict[str, list[FlixPatrolMediaItem]] = {}
            for platform in parsed.shows:
                seen_titles = set()
                deduped = []
                for show in parsed.shows[platform]:
                    title = show.get("title")
                    if title and title not in seen_titles:
                        # Generate unique id from title, platform, and content_type
                        item_id = f"{platform}:{title}:tv"
                        deduped.append(
                            FlixPatrolMediaItem(
                                id=item_id,
                                rank=show["rank"],
                                title=show["title"],
                                score=show["score"],
                                platform=platform,
                                content_type="tv",
                            )
                        )
                        seen_titles.add(title)
                shows_by_platform[platform] = deduped

            movies_by_platform: dict[str, list[FlixPatrolMediaItem]] = {}
            for platform in parsed.movies:
                seen_titles = set()
                deduped = []
                for movie in parsed.movies[platform]:
                    title = movie.get("title")
                    if title and title not in seen_titles:
                        # Generate unique id from title, platform, and content_type
                        item_id = f"{platform}:{title}:movie"
                        deduped.append(
                            FlixPatrolMediaItem(
                                id=item_id,
                                rank=movie["rank"],
                                title=movie["title"],
                                score=movie["score"],
                                platform=platform,
                                content_type="movie",
                            )
                        )
                        seen_titles.add(title)
                movies_by_platform[platform] = deduped

            # Merge data from specified providers
            merged_shows: list[FlixPatrolMediaItem] = []
            merged_movies: list[FlixPatrolMediaItem] = []

            for provider in providers:
                shows = shows_by_platform.get(provider, [])
                movies = movies_by_platform.get(provider, [])
                merged_shows.extend(shows)
                merged_movies.extend(movies)

            # Sort by score descending
            merged_shows.sort(key=lambda x: x.score, reverse=True)
            merged_movies.sort(key=lambda x: x.score, reverse=True)

            # Create metadata
            all_platforms = set(shows_by_platform.keys()) | set(movies_by_platform.keys())
            metadata = FlixPatrolMetadata(
                source="FlixPatrol",
                total_shows=sum(len(shows) for shows in shows_by_platform.values()),
                total_movies=sum(len(movies) for movies in movies_by_platform.values()),
                platforms=sorted(all_platforms),
            )

            # Return FlixPatrolResponse model
            return FlixPatrolResponse(
                date=parsed.date,
                shows=shows_by_platform,
                movies=movies_by_platform,
                top_trending_tv_shows=merged_shows,
                top_trending_movies=merged_movies,
                metadata=metadata,
            )

        except Exception as e:
            logger.exception("Unhandled exception in FlixPatrol service.")
            # Return empty response with error field
            return FlixPatrolResponse(
                date=datetime.now(UTC).strftime("%Y-%m-%d"),
                error=str(e),
            )


flixpatrol_service = FlixPatrolService()
