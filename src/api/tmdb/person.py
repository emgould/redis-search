"""
TMDB Person Service - Person and cast operations for TMDB
Handles person details, credits, and search operations.
"""

import asyncio

from api.tmdb.core import TMDBService
from api.tmdb.models import (
    MCMovieCreditMediaItem,
    MCMovieItem,
    MCPersonCreditsResult,
    MCPersonItem,
    MCSearchPersonResponse,
    MCTvCreditMediaItem,
    MCTvItem,
)
from api.tmdb.tmdb_models import (
    TMDBPersonDetailsResult,
    TMDBPersonMovieCreditsResponse,
    TMDBPersonTvCastCredit,
    TMDBPersonTvCreditsResponse,
    TMDBSearchPersonItem,
    TMDBSearchPersonResult,
)
from utils.get_logger import get_logger

logger = get_logger(__name__)


# Only filter out shows with extremely low popularity
def credit_filter_for_tv_shows(shows: list[TMDBPersonTvCastCredit]) -> list[TMDBPersonTvCastCredit]:
    filtered_shows = []
    for show in shows:
        if show.popularity is not None and show.popularity < 0.1:
            continue

        # Filter out items without episode_count (test requirement: filters_no_episodes)
        # Also filter out crew credits that ended up in cast (no character field)
        if not show.episode_count or show.episode_count == 0:
            continue
        if not show.character:
            continue

        # Filter out all self appearances (e.g., "Self - Guest", "Self (archive footage)")
        character = show.character
        is_self_appearance = character and "self" in character.lower()

        if is_self_appearance:
            continue

        name = show.name
        if not name:
            continue

        # Check by genre IDs: 10767 = Talk, 10763 = News (often talk/news shows)
        genre_ids = getattr(show, "genre_ids", []) or []
        is_talk_show_by_genre = 10767 in genre_ids or 10763 in genre_ids

        # Only filter by genre - keyword matching is too aggressive
        # (e.g., "The Studio" is a scripted show that would be incorrectly filtered)
        # Shows with actual scripted characters should pass through
        if is_talk_show_by_genre:
            continue

        filtered_shows.append(show)
    return filtered_shows


class TMDBPersonService(TMDBService):
    """
    TMDB Person Service - Handles all person/cast operations.
    Extends TMDBService with person-specific functionality.
    """

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Calculate the Levenshtein edit distance between two strings.

        Used for fuzzy matching in search results to filter out irrelevant people.

        Args:
            s1: First string
            s2: Second string

        Returns:
            The minimum number of single-character edits (insertions, deletions, substitutions)
            required to transform s1 into s2
        """
        # Create a matrix to store distances
        len1, len2 = len(s1), len(s2)

        # Initialize the matrix
        dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        # Initialize first row and column
        for i in range(len1 + 1):
            dp[i][0] = i
        for j in range(len2 + 1):
            dp[0][j] = j

        # Fill the matrix using dynamic programming
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if s1[i - 1] == s2[j - 1]:
                    # Characters match, no edit needed
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    # Take minimum of insert, delete, or substitute
                    dp[i][j] = 1 + min(
                        dp[i - 1][j],  # deletion
                        dp[i][j - 1],  # insertion
                        dp[i - 1][j - 1],  # substitution
                    )

        return dp[len1][len2]

    async def get_person_tv_credits(self, person_id: int, limit: int = 50) -> MCPersonCreditsResult:
        """Get TV credits for a person.

        Args:
            person_id: TMDB person ID
            limit: Maximum number of results

        Returns:
            MCPersonCreditsResult with TV credits
        """
        endpoint = f"person/{person_id}/tv_credits"
        params = {"language": "en-US"}

        data = await self._make_request(endpoint, params)
        if not data:
            return MCPersonCreditsResult(
                person=None, movies=[], tv_shows=[], metadata={"total_tv_shows": 0}
            )
        person_tv_credits: TMDBPersonTvCreditsResponse = TMDBPersonTvCreditsResponse.model_validate(
            data
        )

        # Process and filter all TV credits (apply limit AFTER sorting)
        processed_tv_shows = []
        filtered_credits = credit_filter_for_tv_shows(person_tv_credits.cast)

        for idx, tv in enumerate(filtered_credits):
            tv_item = MCTvItem.from_tv_search(tv, self.image_base_url)
            tv_credit_item = MCTvCreditMediaItem.model_validate(tv_item.to_dict())

            tv_credit_item.character = tv.character
            tv_credit_item.credit_id = tv.credit_id
            tv_credit_item.episode_count = tv.episode_count
            tv_credit_item.order = idx
            processed_tv_shows.append(tv_credit_item)

        # Sort by recency first (most recent shows appear first)
        def get_tv_sort_key(show: MCTvItem) -> float:
            # Primary: recency (more recent shows get higher score)
            recency_score: float = -self._get_sort_date(show)
            return recency_score

        processed_tv_shows.sort(key=get_tv_sort_key, reverse=True)

        # Apply limit AFTER sorting
        limited_tv_shows = processed_tv_shows[:limit]

        metadata = {
            "total_results": len(filtered_credits),
            "total_tv_shows": len(limited_tv_shows),
            "data_source": "TMDB Person API",
        }

        return MCPersonCreditsResult(
            person=None,
            movies=[],
            tv_shows=limited_tv_shows,
            metadata=metadata,
        )

    async def fetch_movie_credits(self, person_id: int) -> MCPersonCreditsResult:
        """Fetch and process movie credits for a person.

        Args:
            person_id: TMDB person ID

        Returns:
            MCPersonCreditsResult with movie credits (empty on error)
        """
        try:
            result = await self.get_person_movie_credits(person_id)
            return result
        except Exception as e:
            logger.error(f"Error fetching movie credits for person {person_id}: {e}")
            return MCPersonCreditsResult(
                person=None, movies=[], tv_shows=[], metadata={"total_movies": 0}
            )

    async def fetch_tv_credits(self, person_id: int, limit: int = 50) -> MCPersonCreditsResult:
        """Fetch and process TV credits for a person.

        Args:
            person_id: TMDB person ID
            limit: Maximum number of TV credits

        Returns:
            MCPersonCreditsResult with TV credits (empty on error)
        """
        try:
            result = await self.get_person_tv_credits(person_id, limit)
            return result
        except Exception as e:
            logger.error(f"Error fetching TV credits for person {person_id}: {e}")
            return MCPersonCreditsResult(
                person=None, movies=[], tv_shows=[], metadata={"total_tv_shows": 0}
            )

    async def get_person_credits(
        self, person_id: int, limit: int = 50
    ) -> MCPersonCreditsResult | None:
        """Get complete cast details including person info and all credits.

        Args:
            person_id: TMDB person ID
            limit: Maximum number of credits per type

        Returns:
            MCPersonCreditsResult or None if person not found
        """
        # Validate person_id
        if not isinstance(person_id, int) or person_id <= 0:
            logger.error(f"Invalid person_id: {person_id}")
            return None

        # Get person details
        person_details = await self.get_person_details(person_id)
        if not person_details:
            return None

        # Get movie and TV credits concurrently
        movie_credits, tv_credits = await asyncio.gather(
            self.fetch_movie_credits(person_id),
            self.fetch_tv_credits(person_id, limit),
        )

        # Combine all data
        cast_details = MCPersonCreditsResult(
            person=person_details,
            movies=movie_credits.movies,
            tv_shows=tv_credits.tv_shows,
            metadata={
                "person_id": person_id,
                "total_results": 1 + len(movie_credits.movies) + len(tv_credits.tv_shows),
                "total_movies": len(movie_credits.movies),
                "total_tv_shows": len(tv_credits.tv_shows),
                "showing_movies": len(movie_credits.movies),
                "showing_tv_shows": len(tv_credits.tv_shows),
                "movies_limit": limit,
                "tv_limit": limit,
                "data_source": "TMDB Person API",
            },
        )

        return cast_details

    async def search_people(
        self, query: str, page: int = 1, limit: int = 20
    ) -> MCSearchPersonResponse:
        """Search for people/actors using TMDB's person search endpoint.

        Args:
            query: Search query
            page: Page number for pagination
            limit: Maximum number of results per page

        Returns:
            MCSearchPerson
        """
        search_response = await self._search_person(query, page)
        if search_response.error:
            return MCSearchPersonResponse(results=[], error=search_response.error)

        # Convert TMDBSearchPersonItem to MCPersonItem
        mc_person_results = [
            MCPersonItem.from_search_person(person_item, self.image_base_url)
            for person_item in search_response.results
        ]

        return MCSearchPersonResponse(
            results=mc_person_results,
            total_results=search_response.total_results,
            page=search_response.page,
            query=query,
        )

    async def _search_person(self, query: str, page: int = 1) -> TMDBSearchPersonResult:
        try:
            if not query.strip():
                return TMDBSearchPersonResult(results=[], total_results=0, total_pages=0, page=page)

            # Normalize query for comparison
            normalized_query = query.lower().strip()

            # Calculate Levenshtein distance threshold
            # Use same logic as LastFM: max(3, 30% of query length)
            # For very short queries (<=3 chars), require exact match
            max_distance_threshold = max(3, int(len(normalized_query) * 0.3))
            if len(normalized_query) <= 3:
                max_distance_threshold = 0

            # Use the person search endpoint
            endpoint = "search/person"
            params = {"query": query, "language": "en-US", "page": page}

            data = await self._make_request(endpoint, params)
            if not data:
                return TMDBSearchPersonResult(
                    results=[], total_results=0, total_pages=0, page=page, status_code=404
                )

            # Process person results with Levenshtein filtering
            processed_results: list[TMDBSearchPersonItem] = []
            filtered_results: list[tuple[TMDBSearchPersonItem, int]] = []

            for item in data.get("results", []):
                # Skip people without profile images
                if item.get("profile_path") is None:
                    continue

                processed_item = TMDBSearchPersonItem.model_validate(item)

                # Calculate Levenshtein distance from query
                person_name = processed_item.name.lower().strip()
                distance = self._levenshtein_distance(person_name, normalized_query)

                # Check if name matches within threshold
                if distance <= max_distance_threshold:
                    processed_results.append(processed_item)
                    logger.debug(
                        f"Person '{processed_item.name}' matches query '{query}' (distance: {distance})"
                    )
                else:
                    # Store filtered results for potential fallback
                    filtered_results.append((processed_item, distance))
                    logger.debug(
                        f"Person '{processed_item.name}' filtered out (distance: {distance} > threshold: {max_distance_threshold})"
                    )

            # If no matches found, use fallback with stricter threshold
            if not processed_results and filtered_results:
                # Maximum fallback threshold: 50% of query length (stricter than match threshold)
                max_fallback_threshold = max(5, int(len(normalized_query) * 0.5))
                fallback_results = [
                    person for person, dist in filtered_results if dist <= max_fallback_threshold
                ]
                if fallback_results:
                    processed_results = fallback_results
                    logger.info(
                        f"No strict matches found for '{query}', using {len(fallback_results)} fallback results"
                    )
                else:
                    logger.warning(
                        f"No matches found for '{query}' (all results exceed fallback threshold of {max_fallback_threshold})"
                    )

            # Apply sorting by popularity (higher first)
            processed_results.sort(key=lambda x: float(x.popularity or 0), reverse=True)

            return TMDBSearchPersonResult.model_validate(
                {
                    "results": processed_results,
                    "total_results": len(processed_results),
                    "total_pages": 1 if processed_results else 0,
                    "page": page,
                }
            )
        except Exception as e:
            logger.error(f"Error in _search_person: {e}")
            return TMDBSearchPersonResult(
                results=[], total_results=0, total_pages=0, page=page, error=str(e)
            )

    async def get_person_details(self, person_id: int) -> MCPersonItem | None:
        """Get detailed information for a person/actor.

        Args:
            person_id: TMDB person ID

        Returns:
            MCPersonItem or None if not found
        """
        endpoint = f"person/{person_id}"
        params = {"language": "en-US"}

        data = await self._make_request(endpoint, params)
        if not data:
            return None

        person_data: TMDBPersonDetailsResult = TMDBPersonDetailsResult.model_validate(data)

        person_item: MCPersonItem = MCPersonItem.from_person_details(
            person_data, self.image_base_url
        )

        return person_item

    async def get_person_movie_credits(self, person_id: int) -> MCPersonCreditsResult:
        """Get movie credits for a person.

        Args:
            person_id: TMDB person ID

        Returns:
            MCPersonCreditsResult with movie credits
        """
        endpoint = f"person/{person_id}/movie_credits"
        params = {"language": "en-US"}

        data = await self._make_request(endpoint, params)
        if not data:
            return MCPersonCreditsResult(
                person=None, movies=[], tv_shows=[], metadata={"total_movies": 0}
            )
        person_movie_credits: TMDBPersonMovieCreditsResponse = (
            TMDBPersonMovieCreditsResponse.model_validate(data)
        )

        # Process and filter movies
        processed_movies = []
        for movie in person_movie_credits.cast:
            # Filter out movies with very low popularity
            popularity = getattr(movie, "popularity", 0) or 0
            if popularity < 0.5:
                continue

            movie_item = MCMovieItem.from_movie_search(movie, self.image_base_url)
            movie_credit_item = MCMovieCreditMediaItem.model_validate(movie_item.to_dict())

            movie_credit_item.character = movie.character
            movie_credit_item.credit_id = movie.credit_id
            movie_credit_item.order = movie.order or 999
            processed_movies.append(movie_credit_item)

        # Sort by release date (most recent first), then popularity
        processed_movies.sort(
            key=lambda x: (
                self._get_sort_date(x),  # More recent first
                -float(x.popularity or 0),
            ),  # Higher popularity first
        )

        metadata = {
            "total_results": len(processed_movies),
            "total_movies": len(processed_movies),
            "data_source": "TMDB Person API",
        }

        return MCPersonCreditsResult(
            person=None,
            movies=processed_movies,
            tv_shows=[],
            metadata=metadata,
        )


# Create the handler instance
tmdb_person_service = TMDBPersonService()
