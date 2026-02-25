"""
TMDB Person Composite Search - Person search with conditional credit fetching

Provides person search functionality with fuzzy name matching (via Levenshtein distance)
and conditional credit fetching based on media type (movies, TV shows, or both).
"""

import asyncio
import re
from collections.abc import Coroutine
from typing import Any, cast

from api.tmdb.models import MCPersonCreditsResult
from api.tmdb.person import TMDBPersonService
from contracts.models import MCBaseItem, MCSearchResponse, MCType
from utils.get_logger import get_logger

logger = get_logger(__name__)


def normalize_name(name: str) -> str:
    """
    Normalize a name for comparison by:
    - Converting to lowercase
    - Removing special characters
    - Removing extra whitespace

    Args:
        name: The name to normalize

    Returns:
        Normalized name string
    """
    if not name:
        return ""

    # Convert to lowercase
    normalized = name.lower()

    # Remove special characters (keep only letters, numbers, and spaces)
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)

    # Remove extra whitespace
    normalized = " ".join(normalized.split())

    return normalized


async def search_person_with_credits(
    query: str, mc_type: MCType = MCType.MIXED
) -> MCSearchResponse:
    """Search for people by name with fuzzy matching and conditional credit fetching.

    This function:
    1. Searches TMDB for people matching the query (uses Levenshtein fuzzy matching)
    2. Returns the best match sorted by popularity
    3. Fetches detailed person information and credit counts based on mc_type:
       - MCType.TV_SERIES: only fetch TV credits
       - MCType.MOVIE: only fetch movie credits
       - MCType.MIXED: fetch both movie and TV credits

    Args:
        query: The search query (person name) - REQUIRED
        mc_type: MCType enum (MCType.TV_SERIES, MCType.MOVIE, or MCType.MIXED)
                 Determines which credits to fetch. Default is MCType.MIXED.

    Returns:
        MCSearchResponse with person results (empty array if no matches)
        Status code: 200 for success, 400/500 for errors
    """
    if not query or not query.strip():
        return MCSearchResponse(
            results=[],
            total_results=0,
            query=query or "",
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            error="query parameter is required",
            status_code=400,
        )

    # Validate mc_type
    if mc_type not in [MCType.TV_SERIES, MCType.MOVIE, MCType.MIXED]:
        return MCSearchResponse(
            results=[],
            total_results=0,
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            error=f"Invalid mc_type: {mc_type}. Must be MCType.TV_SERIES, MCType.MOVIE, or MCType.MIXED",
            status_code=400,
        )

    try:
        service = TMDBPersonService()

        # Search using TMDB person search endpoint
        search_result = await service.search_people(query, page=1, limit=20)

        if not search_result.results:
            logger.info(f"No results found for person search: {query}")
            return MCSearchResponse(
                results=[],
                total_results=0,
                query=query,
                data_type=MCType.PERSON,
                data_source="TMDB Person Search",
                error="No results found",
                status_code=200,
            )

        # Normalize the search query for logging
        normalized_query = normalize_name(query)
        logger.info(f"Searching for person: '{query}' (normalized: '{normalized_query}')")

        # Trust the fuzzy-matched results from _search_person (which uses Levenshtein distance)
        # Sort by popularity (highest first) and use the top match
        matched_results = sorted(
            search_result.results, key=lambda x: float(x.popularity or 0), reverse=True
        )

        logger.info(
            f"Person search for '{query}': {len(matched_results)} match(es) found, "
            f"top match: '{matched_results[0].name}' (popularity: {matched_results[0].popularity})"
        )

        # Fetch detailed person information and credit counts for top match

        person_id = matched_results[0].id
        logger.info(f"Fetching detailed info and credits for person ID: {person_id}")

        # Build tasks list based on mc_type
        tasks: list[Coroutine[Any, Any, Any]] = [service.get_person_details(person_id)]

        if mc_type == MCType.MOVIE or mc_type == MCType.MIXED:
            # Fetch movie credits (limit=1 just to get count)
            tasks.append(service.get_person_movie_credits(person_id))

        if mc_type == MCType.TV_SERIES or mc_type == MCType.MIXED:
            # Fetch TV credits (limit=1 just to get count)
            tasks.append(service.get_person_tv_credits(person_id, limit=1))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        result_index = 0
        final_results: list[MCBaseItem] = []
        # First result is always person details
        if not isinstance(results[result_index], BaseException):
            person_detail = results[result_index]
            if person_detail is not None:
                final_results.append(cast(MCBaseItem, person_detail))
        result_index += 1

        # Next result(s) depend on mc_type
        if mc_type == MCType.MOVIE or mc_type == MCType.MIXED:
            if not isinstance(results[result_index], BaseException):
                credits_result = cast(MCPersonCreditsResult, results[result_index])
                final_results.extend(cast(list[MCBaseItem], credits_result.movies))
            result_index += 1

        if mc_type == MCType.TV_SERIES or mc_type == MCType.MIXED:
            if not isinstance(results[result_index], BaseException):
                credits_result = cast(MCPersonCreditsResult, results[result_index])
                final_results.extend(cast(list[MCBaseItem], credits_result.tv_shows))
            result_index += 1

        return MCSearchResponse(
            results=final_results,
            total_results=len(final_results),
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            status_code=200,
        )

    except Exception as e:
        logger.error(f"Error in search_person_with_credits: {e}")
        return MCSearchResponse(
            results=[],
            total_results=0,
            query=query,
            data_type=MCType.PERSON,
            data_source="TMDB Person Search",
            error=str(e),
            status_code=500,
        )
