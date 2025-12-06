"""
Comscore Core Service - Service for box office rankings data
Handles fetching domestic box office rankings from Comscore API.
"""

from datetime import datetime
from typing import Any

from api.subapi.comscore.models import BoxOfficeData, BoxOfficeRanking
from utils.get_logger import get_logger
from utils.redis_cache import RedisCache

# Cache configuration - 24 hours for box office rankings (updates once per day typically)
CacheExpiration = 24 * 60 * 60  # 24 hours
ComscoreCache = RedisCache(
    defaultTTL=CacheExpiration,
    prefix="comscore",
    verbose=False,
    isClassMethod=True,  # For ComscoreService class methods
    version="2.0.1",  # Version bump for Redis migration
)

logger = get_logger(__name__)


class ComscoreService:
    """
    Comscore service for box office rankings operations.
    Handles domestic box office rankings and title matching.
    """

    def __init__(self):
        """Initialize Comscore service."""
        self.base_url = "https://movies.comscore.com/api.html"
        logger.info("ComscoreService initialized")

    async def _make_request(self, params: dict | None = None) -> dict[str, Any] | None:
        """Make async HTTP request to Comscore API.

        Args:
            params: Optional query parameters for the API request

        Returns:
            JSON response as dict, or None if request failed
        """
        try:
            import aiohttp

            timeout = aiohttp.ClientTimeout(total=10)

            # Default parameters for domestic rankings
            default_params = {"handler": "landing", "action": "get_domestic_rankings"}

            if params:
                default_params.update(params)

            async with (
                aiohttp.ClientSession() as session,
                session.get(self.base_url, params=default_params, timeout=timeout) as response,
            ):
                if response.status != 200:
                    logger.warning(f"Comscore API returned status {response.status}")
                    return None

                result: dict[str, Any] = await response.json()
                return result

        except Exception as e:
            logger.error(f"Error making Comscore request: {e}")
            return None

    def _process_rankings_data(self, data: dict) -> BoxOfficeData | None:
        """Process and normalize rankings data from Comscore API.

        Args:
            data: Raw API response data

        Returns:
            BoxOfficeData model with processed rankings, or None if processing failed
        """
        try:
            if not data or "rankings" not in data:
                logger.error("Invalid response format from Comscore API")
                return None

            rankings = []
            for ranking_item in data.get("rankings", []):
                try:
                    ranking = BoxOfficeRanking(
                        rank=int(ranking_item.get("rank", 0)),
                        title_name=ranking_item.get("titleName", ""),
                        weekend_estimate=str(ranking_item.get("weekendEstimate", "0")),
                        dom_distributor=ranking_item.get("domDistributor"),
                        intl_distributor=ranking_item.get("intlDistributor"),
                    )
                    rankings.append(ranking)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing ranking item: {e}")
                    continue

            return BoxOfficeData(
                rankings=rankings,
                exhibition_week=data.get("exhibitionWeek", ""),
                fetched_at=datetime.now().isoformat(),
            )

        except Exception as e:
            logger.error(f"Error processing rankings data: {e}")
            return None

    @RedisCache.use_cache(ComscoreCache, prefix="domestic_rankings")
    async def get_domestic_rankings(self) -> BoxOfficeData | None:
        """
        Get domestic box office rankings from Comscore.

        Returns:
            BoxOfficeData containing rankings and metadata, or None if failed
        """
        try:
            logger.info("Fetching domestic box office rankings from Comscore")

            # Make API request
            response_data = await self._make_request()

            if not response_data:
                logger.error("Failed to fetch data from Comscore API")
                return None

            # Process the response
            processed_data = self._process_rankings_data(response_data)

            if not processed_data:
                logger.error("Failed to process Comscore rankings data")
                return None

            logger.info(f"Successfully fetched {len(processed_data.rankings)} box office rankings")
            return processed_data

        except Exception as e:
            logger.error(f"Unexpected error fetching box office rankings: {e}")
            return None

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Calculate the Levenshtein edit distance between two strings.

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

    def match_movie_to_ranking(
        self, movie_title: str, rankings: list[BoxOfficeRanking]
    ) -> BoxOfficeRanking:
        """
        Match a movie title to a box office ranking entry.

        Uses Levenshtein edit distance for flexible matching to handle title variations.
        First attempts direct match, then uses edit distance with a threshold.

        Args:
            movie_title: The movie title to match
            rankings: List of box office rankings to search

        Returns:
            BoxOfficeRanking with match data or error information
        """
        try:
            if not movie_title or not rankings:
                return BoxOfficeRanking(
                    rank=0,
                    title_name=movie_title or "",
                    weekend_estimate="",
                    dom_distributor=None,
                    intl_distributor=None,
                    error="Invalid input: missing movie_title or rankings",
                    status_code=400,
                )

            # Normalize the movie title for comparison
            movie_title_normalized = movie_title.lower().strip()

            # Direct match first (distance = 0)
            for ranking in rankings:
                if ranking.title_name.lower().strip() == movie_title_normalized:
                    logger.debug(
                        f"Direct match found: {movie_title} -> {ranking.title_name} (rank {ranking.rank})"
                    )
                    return ranking

            # Use Levenshtein distance for fuzzy matching
            # Find the best match with minimum edit distance
            best_match = None
            min_distance = float("inf")

            # Set threshold based on title length (allow up to 30% character difference)
            max_distance_threshold = max(3, int(len(movie_title_normalized) * 0.3))

            for ranking in rankings:
                ranking_title_normalized = ranking.title_name.lower().strip()
                ranking_title_parts = ranking_title_normalized.split(",")
                if len(ranking_title_parts) > 1:
                    ranking_title_normalized = (
                        ranking_title_parts[1].strip() + " " + ranking_title_parts[0].strip()
                    )
                else:
                    ranking_title_normalized = ranking_title_parts[0]
                distance = self._levenshtein_distance(
                    movie_title_normalized, ranking_title_normalized
                )

                logger.debug(
                    f"Distance between '{movie_title}' and '{ranking.title_name}': {distance}"
                )

                if distance < min_distance and distance <= max_distance_threshold:
                    min_distance = distance
                    best_match = ranking

            if best_match:
                logger.debug(
                    f"Fuzzy match found: {movie_title} -> {best_match.title_name} (rank {best_match.rank}, distance {min_distance})"
                )
                return best_match

            # If no match found
            logger.debug(f"No match found for movie: {movie_title}")
            return BoxOfficeRanking(
                rank=0,
                title_name=movie_title,
                weekend_estimate="",
                dom_distributor=None,
                intl_distributor=None,
                error="No match found for movie",
                status_code=404,
            )

        except Exception as e:
            logger.error(f"Error matching movie to ranking: {e}")
            return BoxOfficeRanking(
                rank=0,
                title_name=movie_title,
                weekend_estimate="",
                dom_distributor=None,
                intl_distributor=None,
                error=str(e),
                status_code=500,
            )

    def create_ranking_map(self, rankings: list[BoxOfficeRanking]) -> dict[str, int]:
        """
        Create a mapping of movie titles to their box office ranks.

        Args:
            rankings: List of box office rankings

        Returns:
            Dictionary mapping normalized movie titles to their ranks
        """
        try:
            ranking_map = {}

            for ranking in rankings:
                if ranking.title_name:
                    # Normalize title for mapping
                    normalized_title = ranking.title_name.lower().strip()
                    ranking_map[normalized_title] = ranking.rank

            logger.debug(f"Created ranking map with {len(ranking_map)} entries")
            return ranking_map

        except Exception as e:
            logger.error(f"Error creating ranking map: {e}")
            return {}


comscore_service = ComscoreService()
